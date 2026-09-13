"""JSONL persistence for job snapshots, with dedup and flow (weekly-new) stats.

Note for the Vercel deployment: serverless functions there have no
persistent disk between invocations, so this file resets on every cold
start when hosted — dedup/flow-over-time only really accumulates when this
runs as one long-lived local process (`python3 app.py` or a cron machine
you control). That's a known, accepted limitation of the simplified
single-file deploy — see app.py's docstring.
"""
import json
import os
from datetime import datetime, timedelta, timezone

DATA_PATH = os.path.join(os.path.dirname(__file__), "data", "snapshots.jsonl")


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _load_existing():
    """Return {(source, job_id): row} for everything already on disk."""
    existing = {}
    if not os.path.exists(DATA_PATH):
        return existing
    with open(DATA_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            existing[(row["source"], row["job_id"])] = row
    return existing


def dedup_cross_source(jobs):
    """Collapse postings that are almost certainly the same underlying ad
    re-indexed by more than one aggregator (Adzuna and Careerjet both pull
    from the same original job boards, just with differently-formatted
    locations — "Davenport, Scott County" vs "Davenport, IA").

    Grouped by (company, title, first location segment) lowercased. A group
    is only collapsed when it spans more than one `source` — repeats within
    a single source are left untouched, since those are usually genuinely
    distinct concurrent openings (a source's own job_id already dedups exact
    re-fetches of the same ad). Groups with no location text are also left
    untouched, since an empty city token would otherwise merge unrelated
    postings that just happen to lack a location.

    Returns (deduped_jobs, removed_count).
    """
    groups = {}
    for job in jobs:
        city = (job.get("location") or "").split(",")[0].strip().lower()
        key = (
            (job.get("company") or "").strip().lower(),
            (job.get("title") or "").strip().lower(),
            city,
        )
        groups.setdefault(key, []).append(job)

    deduped = []
    removed = 0
    for (_, _, city), group in groups.items():
        sources = {j["source"] for j in group}
        if not city or len(sources) == 1:
            deduped.extend(group)
            continue
        group.sort(key=lambda j: (j.get("salary_min") is None, j["source"]))
        deduped.append(group[0])
        removed += len(group) - 1

    return deduped, removed


def append_snapshot(jobs, bucket_by_job_id):
    """Append one row per job, deduped by (source, job_id).

    first_seen is preserved for jobs already known; new jobs get first_seen=now.
    Returns (new_count, total_rows_written_this_run).

    On Vercel the deployed filesystem is read-only (only /tmp is writable,
    and that doesn't survive between invocations anyway, so it wouldn't
    help dedup/history even if used) — writing here raises OSError. That's
    a known, accepted limitation of running this without a real database,
    not something to crash the whole request over: catch it and return as
    if every job were new, so /refresh still succeeds and hands back a
    result, just without persisted dedup on that platform.
    """
    existing = _load_existing()
    run_at = _now_iso()
    new_count = 0
    rows = []
    for job in jobs:
        key = (job["source"], job["job_id"])
        prior = existing.get(key)
        first_seen = prior["first_seen"] if prior else run_at
        if not prior:
            new_count += 1
        rows.append({
            "run_at": run_at,
            "source": job["source"],
            "job_id": job["job_id"],
            "company": job.get("company", ""),
            "title": job.get("title", ""),
            "bucket": bucket_by_job_id.get(key, "drop"),
            "country": job.get("country", ""),
            "location": job.get("location", ""),
            "posted_at": job.get("posted_at", ""),
            "is_remote": job.get("is_remote", False),
            "channel": job.get("channel", "unspecified"),
            "is_bpo_vendor": job.get("is_bpo_vendor", False),
            "first_seen": first_seen,
        })

    try:
        os.makedirs(os.path.dirname(DATA_PATH), exist_ok=True)
        with open(DATA_PATH, "a", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row) + "\n")
    except OSError:
        pass

    return new_count, len(rows)


def history_days():
    """How many days of history we've accumulated (oldest run_at to now)."""
    if not os.path.exists(DATA_PATH):
        return 0
    oldest = None
    with open(DATA_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            ts = datetime.fromisoformat(json.loads(line)["run_at"])
            if oldest is None or ts < oldest:
                oldest = ts
    if oldest is None:
        return 0
    return (datetime.now(timezone.utc) - oldest).days


def flow_last_7_days():
    """Count of distinct (source, job_id) whose first_seen is within 7 days."""
    if not os.path.exists(DATA_PATH):
        return 0
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    seen = set()
    count = 0
    with open(DATA_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            key = (row["source"], row["job_id"])
            if key in seen:
                continue
            seen.add(key)
            if datetime.fromisoformat(row["first_seen"]) >= cutoff:
                count += 1
    return count

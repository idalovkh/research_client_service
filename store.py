"""Persistence for job snapshots, dedup, and flow (weekly-new) stats.

Backend is chosen automatically by environment:
  - If UPSTASH_REDIS_REST_URL / UPSTASH_REDIS_REST_TOKEN are set (as they are
    on Vercel via the Upstash Redis marketplace integration), state lives in
    Redis — a Vercel serverless function's local disk is wiped between
    invocations, so the JSONL file approach silently loses everything there.
  - Otherwise, falls back to the local data/snapshots.jsonl file used for
    local/CLI runs — nothing about local usage changes.

jobs.py and app.py call the same functions either way; they don't need to
know which backend is active.
"""
import json
import os
from datetime import datetime, timedelta, timezone

import requests

DATA_PATH = os.path.join(os.path.dirname(__file__), "data", "snapshots.jsonl")

UPSTASH_URL = os.environ.get("UPSTASH_REDIS_REST_URL", "")
UPSTASH_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "")
FIRST_SEEN_HASH = "dispatch:first_seen"       # field "{source}:{job_id}" -> ISO timestamp
LATEST_RESULT_KEY = "dispatch:latest_result"  # full run() result, as JSON
LATEST_RESULT_AT_KEY = "dispatch:latest_result_at"


def _using_redis():
    return bool(UPSTASH_URL and UPSTASH_TOKEN)


def _redis_cmd(*args):
    """One Upstash REST call. Raises on transport/HTTP errors; callers that
    can tolerate a cold/empty store should catch and treat as "no data" —
    a brand new Redis instance answers with nulls/empty, not errors, so this
    is only for genuine outages."""
    resp = requests.post(
        UPSTASH_URL,
        headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
        json=list(args),
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json().get("result")


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


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

    Returns (deduped_jobs, removed_count). Backend-independent — pure
    in-memory list processing.
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


# --- JSONL backend (local/CLI) ------------------------------------------

def _load_existing_jsonl():
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


def _append_snapshot_jsonl(jobs, bucket_by_job_id):
    os.makedirs(os.path.dirname(DATA_PATH), exist_ok=True)
    existing = _load_existing_jsonl()
    run_at = _now_iso()
    new_count = 0
    rows = []
    for job in jobs:
        key = (job["source"], job["job_id"])
        prior = existing.get(key)
        first_seen = prior["first_seen"] if prior else run_at
        if not prior:
            new_count += 1
        rows.append(_row(job, bucket_by_job_id, key, run_at, first_seen))

    with open(DATA_PATH, "a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")

    return new_count, len(rows)


def _history_days_jsonl():
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


def _flow_last_7_days_jsonl():
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


# --- Redis backend (Vercel) ----------------------------------------------
# Only first_seen needs to persist across invocations — everything else in
# a "row" is re-derived fresh from the source APIs every run, so there's no
# need to store full rows the way the JSONL file does. That keeps the Redis
# hash small regardless of how many times a job gets re-fetched.

def _append_snapshot_redis(jobs, bucket_by_job_id):
    existing = _redis_cmd("HGETALL", FIRST_SEEN_HASH) or []
    existing_map = dict(zip(existing[0::2], existing[1::2]))
    run_at = _now_iso()
    new_count = 0
    updates = {}
    for job in jobs:
        field = f"{job['source']}:{job['job_id']}"
        if field not in existing_map:
            new_count += 1
            updates[field] = run_at
    if updates:
        args = ["HSET", FIRST_SEEN_HASH]
        for field, ts in updates.items():
            args += [field, ts]
        _redis_cmd(*args)
    return new_count, len(jobs)


def _history_days_redis():
    existing = _redis_cmd("HGETALL", FIRST_SEEN_HASH) or []
    timestamps = existing[1::2]
    if not timestamps:
        return 0
    oldest = min(datetime.fromisoformat(t) for t in timestamps)
    return (datetime.now(timezone.utc) - oldest).days


def _flow_last_7_days_redis():
    existing = _redis_cmd("HGETALL", FIRST_SEEN_HASH) or []
    timestamps = existing[1::2]
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    return sum(1 for t in timestamps if datetime.fromisoformat(t) >= cutoff)


def save_latest_result(result):
    """Redis only — the cron endpoint calls this so the public page can
    serve instantly instead of re-running the ~60s scan per request."""
    if not _using_redis():
        return
    _redis_cmd("SET", LATEST_RESULT_KEY, json.dumps(result, default=str))
    _redis_cmd("SET", LATEST_RESULT_AT_KEY, _now_iso())


def load_latest_result():
    """Redis only — returns (result, updated_at) or (None, None)."""
    if not _using_redis():
        return None, None
    raw = _redis_cmd("GET", LATEST_RESULT_KEY)
    updated_at = _redis_cmd("GET", LATEST_RESULT_AT_KEY)
    return (json.loads(raw) if raw else None), updated_at


# --- Public API (dispatches on backend) -----------------------------------

def _row(job, bucket_by_job_id, key, run_at, first_seen):
    return {
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
    }


def append_snapshot(jobs, bucket_by_job_id):
    """Record first_seen for any job not seen before. Returns
    (new_count, total_rows_this_run)."""
    if _using_redis():
        return _append_snapshot_redis(jobs, bucket_by_job_id)
    return _append_snapshot_jsonl(jobs, bucket_by_job_id)


def history_days():
    """How many days of history we've accumulated."""
    if _using_redis():
        return _history_days_redis()
    return _history_days_jsonl()


def flow_last_7_days():
    """Count of distinct jobs whose first_seen is within the last 7 days."""
    if _using_redis():
        return _flow_last_7_days_redis()
    return _flow_last_7_days_jsonl()

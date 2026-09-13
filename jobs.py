"""Orchestration + CLI entry point.

Usage:
  python3 jobs.py                     print a summary
  python3 jobs.py --dump-titles       print every fetched title with its bucket
  python3 jobs.py --json              print the full run result as JSON
  python3 jobs.py --check-bpo-candidates
                                       list Wikipedia BPO-category companies
                                       not yet in classify.BPO_VENDORS, for
                                       manual review (does not modify anything)
"""
import argparse
import json
import os
import sys

from classify import classify, is_remote, channel, is_bpo_vendor, BPO_VENDORS
from sources import (
    fetch_all_ats,
    fetch_adzuna_multi,
    fetch_adzuna_histogram,
    fetch_adzuna_top_companies,
    fetch_bls_stats,
    fetch_wikipedia_bpo_candidates,
    fetch_usajobs_multi,
    fetch_careerjet_multi,
    fetch_arbeitnow,
)
from store import append_snapshot, dedup_cross_source, flow_last_7_days, history_days

COMPANIES_PATH = os.path.join(os.path.dirname(__file__), "companies.json")
# Aligned with classify.FRONTLINE_RE's vocabulary — Adzuna/USAJobs only ever
# return ads matching the search phrase text, so a classifier synonym with no
# matching query here never gets a chance to be seen in the first place.
SEARCH_QUERIES = [
    "customer service", "customer support", "customer care",
    "call center", "call centre", "contact center",
    "live chat support", "chat support",
    "claims representative", "patient service representative",
    "member service representative", "guest service representative",
    "billing support representative", "customer experience representative",
    "help desk",
]
ADZUNA_PAGES_PER_QUERY = 3


def load_env_file():
    """Minimal .env loader so this works without python-dotenv."""
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if not os.path.exists(env_path):
        return
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip()
            if key and key not in os.environ:
                os.environ[key] = value


def load_companies():
    with open(COMPANIES_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def run():
    """Fetch everything, classify, persist, and return a result dict."""
    load_env_file()
    companies = load_companies()

    ats_jobs, ats_statuses = fetch_all_ats(companies)
    adzuna_jobs, adzuna_counts, adzuna_search_status = fetch_adzuna_multi(SEARCH_QUERIES, pages=ADZUNA_PAGES_PER_QUERY)
    adzuna_histograms, adzuna_hist_status = fetch_adzuna_histogram()
    adzuna_top_companies, adzuna_top_status = fetch_adzuna_top_companies()
    bls_stats, bls_status = fetch_bls_stats()
    usajobs_jobs, usajobs_counts, usajobs_status = fetch_usajobs_multi(SEARCH_QUERIES)
    careerjet_jobs, careerjet_counts, careerjet_status = fetch_careerjet_multi(SEARCH_QUERIES)
    arbeitnow_jobs, arbeitnow_status = fetch_arbeitnow()

    all_jobs = ats_jobs + adzuna_jobs + usajobs_jobs + careerjet_jobs + arbeitnow_jobs
    all_jobs, cross_source_dupes_removed = dedup_cross_source(all_jobs)

    bucket_by_job_id = {}
    bucket_counts = {"frontline": 0, "adjacent": 0, "excluded": 0, "drop": 0}
    remote_frontline_count = 0
    phone_frontline_count = 0
    chat_frontline_count = 0
    bpo_frontline_count = 0
    classified_jobs = []
    for job in all_jobs:
        bucket = classify(job.get("title", ""))
        if "source_is_remote" in job:
            remote = job["source_is_remote"]
        else:
            remote = is_remote(job.get("location", ""), job.get("title", ""))
        job_channel = channel(job.get("title", ""))
        bpo_vendor = is_bpo_vendor(job.get("company", ""))
        bucket_by_job_id[(job["source"], job["job_id"])] = bucket
        bucket_counts[bucket] += 1
        if bucket == "frontline":
            if remote:
                remote_frontline_count += 1
            if job_channel == "phone":
                phone_frontline_count += 1
            elif job_channel == "chat":
                chat_frontline_count += 1
            if bpo_vendor:
                bpo_frontline_count += 1
        job = dict(job)
        job["bucket"] = bucket
        job["is_remote"] = remote
        job["channel"] = job_channel
        job["is_bpo_vendor"] = bpo_vendor
        classified_jobs.append(job)

    new_count, total_rows = append_snapshot(classified_jobs, bucket_by_job_id)

    companies_seen = {j["company"] for j in classified_jobs if j.get("company")}

    salaries = [
        j["salary_min"] for j in classified_jobs
        if j.get("bucket") in ("frontline", "adjacent") and j.get("salary_min")
    ]
    median_salary = None
    if salaries:
        salaries.sort()
        mid = len(salaries) // 2
        median_salary = (
            salaries[mid] if len(salaries) % 2
            else (salaries[mid - 1] + salaries[mid]) / 2
        )

    statuses = dict(ats_statuses)
    statuses["adzuna:search"] = adzuna_search_status
    statuses["adzuna:histogram"] = adzuna_hist_status
    statuses["adzuna:top_companies"] = adzuna_top_status
    statuses["bls"] = bls_status
    statuses["usajobs"] = usajobs_status
    statuses["careerjet"] = careerjet_status
    statuses["arbeitnow"] = arbeitnow_status

    return {
        "jobs": classified_jobs,
        "bucket_counts": bucket_counts,
        "remote_frontline_count": remote_frontline_count,
        "phone_frontline_count": phone_frontline_count,
        "chat_frontline_count": chat_frontline_count,
        "bpo_frontline_count": bpo_frontline_count,
        "cross_source_dupes_removed": cross_source_dupes_removed,
        "companies_seen": sorted(companies_seen),
        "median_salary": median_salary,
        "new_count": new_count,
        "total_rows_this_run": total_rows,
        "history_days": history_days(),
        "flow_7d": flow_last_7_days(),
        "adzuna_market_size": adzuna_counts,
        "usajobs_market_size": usajobs_counts,
        "careerjet_market_size": careerjet_counts,
        "adzuna_histograms": adzuna_histograms,
        "adzuna_top_companies": adzuna_top_companies,
        "bls_stats": bls_stats,
        "source_statuses": statuses,
    }


def print_summary(result):
    """Terminal-glance summary: only what's actionable at a glance. Full
    detail (per-query market size for all 15 phrases, every individual ATS
    company's status, etc.) is in the `result` dict — use --json for that."""
    print("=== Job scan summary ===")
    print(f"Buckets: {result['bucket_counts']}")
    print(f"Frontline channel: phone={result['phone_frontline_count']} chat={result['chat_frontline_count']} remote={result['remote_frontline_count']} bpo_vendor={result['bpo_frontline_count']}")
    print(f"Cross-source duplicates removed: {result['cross_source_dupes_removed']}")
    print(f"Unique companies seen: {len(result['companies_seen'])}")
    print(f"Median salary_min (frontline+adjacent): {result['median_salary']}")
    print(f"New job ids this run: {result['new_count']} / {result['total_rows_this_run']} rows written")
    days = result["history_days"]
    if days < 7:
        print(f"History accumulated: {days} day(s) — flow estimate not reliable yet")
    else:
        print(f"New postings in last 7 days: {result['flow_7d']}")

    cs_us = result["adzuna_market_size"].get("customer service", {}).get("us", "—")
    usa_cs = result["usajobs_market_size"].get("customer service", "—")
    cj_cs = result["careerjet_market_size"].get("customer service", "—")
    print(f"Market size, 'customer service' (US): Adzuna={cs_us} USAJobs={usa_cs} Careerjet={cj_cs}")
    print(f"BLS national stats (SOC 43-4051): {result['bls_stats']}")

    statuses = result["source_statuses"]
    failed = {k: v for k, v in statuses.items() if v != "ok"}
    print(f"Source statuses: {len(statuses) - len(failed)} ok, {len(failed)} not ok")
    for name, status in sorted(failed.items()):
        print(f"  {name}: {status}")


def print_titles(result):
    for job in result["jobs"]:
        remote_tag = "REMOTE" if job.get("is_remote") else "      "
        channel_tag = job.get("channel", "unspecified").upper()[:6].ljust(6)
        bpo_tag = "BPO" if job.get("is_bpo_vendor") else "   "
        print(f"[{job['bucket']:9s}] [{remote_tag}] [{channel_tag}] [{bpo_tag}] {job.get('company','')!r:30s} {job.get('title','')}")


def check_bpo_candidates():
    candidates, status = fetch_wikipedia_bpo_candidates()
    if status != "ok":
        print(f"Wikipedia fetch: {status}")
        return
    known = {v.lower() for v in BPO_VENDORS}
    new = [c for c in candidates if not any(v in c.lower() for v in known)]
    print(f"Wikipedia BPO categories: {len(candidates)} companies, {len(new)} not yet in BPO_VENDORS:")
    for name in new:
        print(f"  {name}")
    print("\nReview before adding — Wikipedia's categorization is crowd-maintained")
    print("and includes generic/tangential names (see the comment above BPO_VENDORS).")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dump-titles", action="store_true")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--check-bpo-candidates", action="store_true")
    args = parser.parse_args()

    if args.check_bpo_candidates:
        check_bpo_candidates()
        return

    result = run()

    if args.json:
        print(json.dumps(result, indent=2, default=str))
    elif args.dump_titles:
        print_titles(result)
    else:
        print_summary(result)


if __name__ == "__main__":
    main()

"""Fetchers for job data. Every fetcher returns (jobs, status).

jobs: list of dicts with keys:
  source, job_id, company, title, location, country, url, posted_at,
  salary_min, salary_max
status: "ok", "no_key", or "error: <msg>"

Every network call is wrapped so one dead source cannot take down the run.
"""
import hashlib
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests

TIMEOUT = 15
# Business focus is the US market specifically — querying 5 more countries
# per Adzuna call was 6x the runtime and ate into the free-tier rate limit
# (we hit 429/503 on de/nl during testing) for data nobody was using.
ADZUNA_COUNTRIES = ["us"]


def _get(url, params=None, headers=None, retries=2):
    """GET with a short backoff-and-retry on 429 — querying many search
    phrases in parallel (see fetch_adzuna_multi) reliably trips Adzuna's
    free-tier rate limit on at least one of them; a bare failure there would
    make the frontline count flicker between runs for no real reason."""
    for attempt in range(retries + 1):
        resp = requests.get(url, params=params, headers=headers, timeout=TIMEOUT)
        if resp.status_code == 429 and attempt < retries:
            time.sleep(2 * (attempt + 1))
            continue
        resp.raise_for_status()
        return resp.json()


def fetch_greenhouse(token):
    url = f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs"
    try:
        data = _get(url, {"content": "false"})
        jobs = []
        for j in data.get("jobs", []):
            loc = (j.get("location") or {}).get("name", "")
            jobs.append({
                "source": "greenhouse",
                "job_id": f"greenhouse:{token}:{j.get('id')}",
                "company": token,
                "title": j.get("title", ""),
                "location": loc,
                "country": "",
                "url": j.get("absolute_url", ""),
                "posted_at": j.get("updated_at", ""),
                "salary_min": None,
                "salary_max": None,
            })
        return jobs, "ok"
    except Exception as e:
        return [], f"error: {e}"


def fetch_ashby(token):
    url = f"https://api.ashbyhq.com/posting-api/job-board/{token}"
    try:
        data = _get(url)
        jobs = []
        for j in data.get("jobs", []):
            jobs.append({
                "source": "ashby",
                "job_id": f"ashby:{token}:{j.get('id')}",
                "company": token,
                "title": j.get("title", ""),
                "location": j.get("location", ""),
                "country": "",
                "url": j.get("jobUrl", ""),
                "posted_at": j.get("publishedAt", ""),
                "salary_min": None,
                "salary_max": None,
            })
        return jobs, "ok"
    except Exception as e:
        return [], f"error: {e}"


def fetch_lever(token):
    url = f"https://api.lever.co/v0/postings/{token}"
    try:
        data = _get(url, {"mode": "json"})
        jobs = []
        for j in data:
            categories = j.get("categories", {}) or {}
            jobs.append({
                "source": "lever",
                "job_id": f"lever:{token}:{j.get('id')}",
                "company": token,
                "title": j.get("text", ""),
                "location": categories.get("location", ""),
                "country": "",
                "url": j.get("hostedUrl", ""),
                "posted_at": j.get("createdAt", ""),
                "salary_min": None,
                "salary_max": None,
            })
        return jobs, "ok"
    except Exception as e:
        return [], f"error: {e}"


def fetch_smartrecruiters(token):
    """Documented public API, paginated via offset/limit (max 100/page)."""
    url = f"https://api.smartrecruiters.com/v1/companies/{token}/postings"
    try:
        jobs = []
        offset = 0
        while True:
            data = _get(url, {"limit": 100, "offset": offset})
            content = data.get("content", [])
            for j in content:
                loc = j.get("location") or {}
                jobs.append({
                    "source": "smartrecruiters",
                    "job_id": f"smartrecruiters:{token}:{j.get('id')}",
                    "company": (j.get("company") or {}).get("name", token),
                    "title": j.get("name", ""),
                    "location": loc.get("fullLocation", ""),
                    "country": (loc.get("country") or "").lower(),
                    "url": f"https://jobs.smartrecruiters.com/{token}/{j.get('id')}",
                    "posted_at": j.get("releasedDate", ""),
                    "salary_min": None,
                    "salary_max": None,
                })
            offset += len(content)
            if offset >= data.get("totalFound", 0) or not content:
                break
        return jobs, "ok"
    except Exception as e:
        return [], f"error: {e}"


def fetch_breezy(token):
    """Undocumented but stable public endpoint: {token}.breezy.hr/json
    returns a bare list, not an object."""
    url = f"https://{token}.breezy.hr/json"
    try:
        data = _get(url)
        jobs = []
        for j in data:
            loc = j.get("location") or {}
            country = ((loc.get("country") or {}).get("id") or "").lower()
            jobs.append({
                "source": "breezy",
                "job_id": f"breezy:{token}:{j.get('id')}",
                "company": (j.get("company") or {}).get("name", token),
                "title": j.get("name", ""),
                "location": loc.get("name", ""),
                "country": country,
                "url": j.get("url", ""),
                "posted_at": j.get("published_date", ""),
                "salary_min": None,
                "salary_max": None,
                "source_is_remote": bool(loc.get("is_remote")),
            })
        return jobs, "ok"
    except Exception as e:
        return [], f"error: {e}"


def fetch_recruitee(token):
    """Documented, keyless: {token}.recruitee.com/api/offers/"""
    url = f"https://{token}.recruitee.com/api/offers/"
    try:
        data = _get(url)
        jobs = []
        for j in data.get("offers", []):
            jobs.append({
                "source": "recruitee",
                "job_id": f"recruitee:{token}:{j.get('id')}",
                "company": token,
                "title": j.get("title", ""),
                "location": j.get("location", ""),
                "country": (j.get("country_code") or "").lower(),
                "url": j.get("careers_url", ""),
                "posted_at": j.get("created_at", ""),
                "salary_min": None,
                "salary_max": None,
                "source_is_remote": bool(j.get("remote")),
            })
        return jobs, "ok"
    except Exception as e:
        return [], f"error: {e}"


def fetch_all_ats(companies):
    """companies: {"greenhouse": [...], "ashby": [...], "lever": [...],
    "smartrecruiters": [...], "breezy": [...], "recruitee": [...]}

    Returns (jobs, statuses) where statuses is {"greenhouse:stripe": "ok", ...}
    """
    fetchers = {
        "greenhouse": fetch_greenhouse,
        "ashby": fetch_ashby,
        "lever": fetch_lever,
        "smartrecruiters": fetch_smartrecruiters,
        "breezy": fetch_breezy,
        "recruitee": fetch_recruitee,
    }
    tasks = []
    for ats, tokens in companies.items():
        fn = fetchers.get(ats)
        if not fn:
            continue
        for token in tokens:
            tasks.append((ats, token, fn))

    jobs = []
    statuses = {}
    with ThreadPoolExecutor(max_workers=8) as pool:
        future_map = {
            pool.submit(fn, token): (ats, token) for ats, token, fn in tasks
        }
        for future in as_completed(future_map):
            ats, token = future_map[future]
            job_list, status = future.result()
            statuses[f"{ats}:{token}"] = status
            jobs.extend(job_list)
    return jobs, statuses


def _adzuna_keys():
    # Hardcoded fallback so this runs on a fresh Vercel deploy with zero
    # dashboard configuration — a local .env value still wins if present.
    # These are free-tier keys tied to this project; rotate them at
    # developer.adzuna.com if this code is ever made public.
    # `... or default`, not `.get(key, default)` — an env var that exists
    # but is set to "" (e.g. left blank in a dashboard) makes .get() return
    # that empty string, not the default; `or` catches that case too.
    app_id = os.environ.get("ADZUNA_APP_ID") or "dda4307f"
    app_key = os.environ.get("ADZUNA_APP_KEY") or "517184a2ba78b22f2a7ce7b4882d3067"
    return app_id, app_key


def fetch_adzuna_search(what="customer service", pages=1):
    """Returns (jobs, count_by_country, status).

    Adzuna caps results_per_page at 50 regardless of what's requested, so
    reaching more than 50 results per query means paging through
    /search/1, /search/2, ... — each page is a separate HTTP call.
    """
    app_id, app_key = _adzuna_keys()
    if not app_id or not app_key:
        return [], {}, "no_key"

    jobs = []
    counts = {}
    errors = []
    for country in ADZUNA_COUNTRIES:
        try:
            for page in range(1, pages + 1):
                url = f"https://api.adzuna.com/v1/api/jobs/{country}/search/{page}"
                params = {
                    "app_id": app_id,
                    "app_key": app_key,
                    "what": what,
                    "results_per_page": 50,
                }
                data = _get(url, params)
                counts[country] = data.get("count", 0)
                results = data.get("results", [])
                for j in results:
                    jobs.append({
                        "source": "adzuna",
                        "job_id": f"adzuna:{j.get('id')}",
                        "company": (j.get("company") or {}).get("display_name", ""),
                        "title": j.get("title", ""),
                        "location": (j.get("location") or {}).get("display_name", ""),
                        "country": country,
                        "url": j.get("redirect_url", ""),
                        "posted_at": j.get("created", ""),
                        "salary_min": j.get("salary_min"),
                        "salary_max": j.get("salary_max"),
                    })
                if len(results) < 50:
                    break  # ran out of results before hitting the page cap
        except Exception as e:
            errors.append(f"{country}: {e}")

    status = "ok" if not errors else f"error: {'; '.join(errors)}"
    return jobs, counts, status


def fetch_adzuna_multi(whats, pages=1):
    """Run fetch_adzuna_search for each phrase in `whats`, merge + dedup jobs.

    The same posting can surface under more than one search phrase (e.g. a
    "Call Center Representative" ad matching both "customer service" and
    "call center"); job_id is Adzuna's own ad id, so it's a stable dedup key
    regardless of which query found it. The query itself is *not* trusted as
    a channel signal — classify.channel() re-derives that from the title.

    Returns (jobs, counts_by_query, status) where counts_by_query is
    {what: {country: count}}.
    """
    jobs_by_id = {}
    counts_by_query = {}
    statuses = []
    with ThreadPoolExecutor(max_workers=3) as pool:
        future_map = {pool.submit(fetch_adzuna_search, what, pages): what for what in whats}
        for future in as_completed(future_map):
            what = future_map[future]
            jobs, counts, status = future.result()
            counts_by_query[what] = counts
            statuses.append(status)
            for job in jobs:
                jobs_by_id.setdefault(job["job_id"], job)

    if all(s == "no_key" for s in statuses):
        overall_status = "no_key"
    elif any(s.startswith("error") for s in statuses):
        overall_status = "error: " + "; ".join(s for s in statuses if s.startswith("error"))
    else:
        overall_status = "ok"

    return list(jobs_by_id.values()), counts_by_query, overall_status


def fetch_adzuna_histogram(what="customer service"):
    app_id, app_key = _adzuna_keys()
    if not app_id or not app_key:
        return {}, "no_key"
    histograms = {}
    errors = []
    for country in ADZUNA_COUNTRIES:
        url = f"https://api.adzuna.com/v1/api/jobs/{country}/histogram"
        params = {"app_id": app_id, "app_key": app_key, "what": what}
        try:
            data = _get(url, params)
            histograms[country] = data.get("histogram", {})
        except Exception as e:
            errors.append(f"{country}: {e}")
    status = "ok" if not errors else f"error: {'; '.join(errors)}"
    return histograms, status


def fetch_adzuna_top_companies(what="customer service"):
    app_id, app_key = _adzuna_keys()
    if not app_id or not app_key:
        return [], "no_key"
    companies = []
    errors = []
    for country in ADZUNA_COUNTRIES:
        url = f"https://api.adzuna.com/v1/api/jobs/{country}/top_companies"
        params = {"app_id": app_id, "app_key": app_key, "what": what}
        try:
            data = _get(url, params)
            for c in data.get("leaderboard", []):
                companies.append({
                    "country": country,
                    "name": c.get("canonical_name", ""),
                    "count": c.get("count", 0),
                })
        except Exception as e:
            errors.append(f"{country}: {e}")
    status = "ok" if not errors else f"error: {'; '.join(errors)}"
    return companies, status


# BLS Occupational Employment and Wage Statistics (OEWS), national, all
# industries, SOC 43-4051 "Customer Service Representatives". No API key
# needed for the public v1 endpoint. Series ID layout: OEU + area-type(N) +
# area(7 digits, 0s = national) + industry(6 digits, 0s = cross-industry) +
# occupation(6 digits, SOC without the dash) + datatype(2 digits).
BLS_SOC_CUSTOMER_SERVICE_REP = "434051"
BLS_SERIES = {
    "employment": f"OEUN0000000000000{BLS_SOC_CUSTOMER_SERVICE_REP}01",
    "annual_mean_wage": f"OEUN0000000000000{BLS_SOC_CUSTOMER_SERVICE_REP}04",
    "annual_median_wage": f"OEUN0000000000000{BLS_SOC_CUSTOMER_SERVICE_REP}13",
}


def fetch_bls_stats():
    """National employment + wage stats for Customer Service Representatives.

    Returns (stats, status) where stats is
    {"employment": int, "annual_mean_wage": int, "annual_median_wage": int}
    (values None if BLS had no data for that series) or {} on failure.
    """
    url = "https://api.bls.gov/publicAPI/v1/timeseries/data/"
    series_ids = list(BLS_SERIES.values())
    try:
        resp = requests.post(url, json={"seriesid": series_ids}, timeout=TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        if data.get("status") != "REQUEST_SUCCEEDED":
            return {}, f"error: {'; '.join(data.get('message', ['unknown BLS error']))}"

        by_series_id = {s["seriesID"]: s for s in data["Results"]["series"]}
        stats = {}
        for key, series_id in BLS_SERIES.items():
            series = by_series_id.get(series_id)
            points = series["data"] if series else []
            stats[key] = int(points[0]["value"]) if points else None
        return stats, "ok"
    except Exception as e:
        return {}, f"error: {e}"


# Wikipedia categorizes BPO companies by name and by country. This is a
# discovery tool, not an auto-updater: it returns raw category members for a
# human to skim and decide which belong in classify.BPO_VENDORS — Wikipedia
# categorization is crowd-maintained and includes names too generic or too
# tangential (HR consultancies, defunct companies) to trust blindly as
# substring-match keys.
WIKIPEDIA_BPO_CATEGORIES = [
    "Business process outsourcing companies",
    "Business process outsourcing companies of the United States",
    "Business process outsourcing companies of the Philippines",
    "Business process outsourcing companies of India",
    "Business process outsourcing companies of the United Kingdom",
]


def fetch_wikipedia_bpo_candidates():
    """Return (candidates, status). candidates is a sorted list of company
    names from Wikipedia's BPO-company categories, excluding sub-category
    entries (titles starting with 'Category:')."""
    url = "https://en.wikipedia.org/w/api.php"
    # Wikimedia blocks the default "python-requests/x.x" user agent as a bot
    # signature; their API etiquette wants a descriptive one identifying the tool.
    headers = {"User-Agent": "dispatch-jobs-research-tool/1.0 (local script, no contact)"}
    names = set()
    errors = []
    for category in WIKIPEDIA_BPO_CATEGORIES:
        params = {
            "action": "query",
            "list": "categorymembers",
            "cmtitle": f"Category:{category.replace(' ', '_')}",
            "cmlimit": 100,
            "format": "json",
        }
        try:
            data = _get(url, params, headers=headers)
            for member in data.get("query", {}).get("categorymembers", []):
                title = member.get("title", "")
                if title and not title.startswith("Category:"):
                    names.add(title)
        except Exception as e:
            errors.append(f"{category}: {e}")

    status = "ok" if not errors else f"error: {'; '.join(errors)}"
    return sorted(names), status


def _usajobs_credentials():
    # Same hardcoded-fallback approach as Adzuna above — no dashboard
    # variables needed to deploy. Rotate at developer.usajobs.gov if this
    # code is ever made public.
    api_key = os.environ.get("USAJOBS_API_KEY") or "aWGEmaKHWrGi+fzuS6GkhwBsaGmVU2BCJcuN7UX4gZc="
    user_agent = os.environ.get("USAJOBS_USER_AGENT") or "kh.idalov@gmail.com"
    return api_key, user_agent


# USAJobs requires an annual-rate estimate to compare against Adzuna/BLS;
# postings quote a mix of hourly and annual rates, so hourly gets annualized
# at a standard full-time year. Anything else (daily, weekly, biweekly,
# monthly) is left unconverted (None) rather than guessed at.
HOURS_PER_YEAR = 2080


def _usajobs_annual_salary(remuneration):
    if not remuneration:
        return None, None
    r = remuneration[0]
    try:
        lo, hi = float(r["MinimumRange"]), float(r["MaximumRange"])
    except (KeyError, ValueError, TypeError):
        return None, None
    code = r.get("RateIntervalCode")
    if code == "PA":
        return lo, hi
    if code == "PH":
        return lo * HOURS_PER_YEAR, hi * HOURS_PER_YEAR
    return None, None


def fetch_usajobs(what):
    """Federal government postings. Returns (jobs, total_count, status) —
    matches fetch_usajobs_multi's unpacking; a 2-tuple here would raise
    ValueError there regardless of which branch returns it."""
    api_key, user_agent = _usajobs_credentials()
    if not api_key or not user_agent:
        return [], 0, "no_key"

    url = "https://data.usajobs.gov/api/search"
    headers = {
        "Authorization-Key": api_key,
        "User-Agent": user_agent,
        "Host": "data.usajobs.gov",
    }
    params = {"Keyword": what, "ResultsPerPage": 500}
    try:
        resp = requests.get(url, params=params, headers=headers, timeout=TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        result = data.get("SearchResult", {})
        total_count = result.get("SearchResultCountAll", 0)
        jobs = []
        for item in result.get("SearchResultItems", []):
            m = item.get("MatchedObjectDescriptor", {})
            salary_min, salary_max = _usajobs_annual_salary(m.get("PositionRemuneration"))
            urls = m.get("ApplyURI") or []
            # Unlike every other source, USAJobs gives an actual structured
            # remote/telework flag instead of leaving it to a text heuristic
            # over title/location — use it verbatim (see jobs.run()).
            details = m.get("UserArea", {}).get("Details", {})
            source_is_remote = bool(details.get("RemoteIndicator")) or bool(details.get("TeleworkEligible"))
            jobs.append({
                "source": "usajobs",
                "job_id": f"usajobs:{item.get('MatchedObjectId')}",
                "company": m.get("OrganizationName", ""),
                "title": m.get("PositionTitle", ""),
                "location": m.get("PositionLocationDisplay", ""),
                "country": "us",
                "url": urls[0] if urls else m.get("PositionURI", ""),
                "posted_at": m.get("PublicationStartDate", ""),
                "salary_min": salary_min,
                "salary_max": salary_max,
                "source_is_remote": source_is_remote,
            })
        return jobs, total_count, "ok"
    except Exception as e:
        return [], 0, f"error: {e}"


def fetch_usajobs_multi(whats):
    """Run fetch_usajobs for each phrase in `whats`, merge + dedup by job_id.

    Returns (jobs, counts_by_query, status) — counts_by_query is
    {what: total_matching_count} (USAJobs is US-only, so no per-country split).
    """
    jobs_by_id = {}
    counts_by_query = {}
    statuses = []
    with ThreadPoolExecutor(max_workers=5) as pool:
        future_map = {pool.submit(fetch_usajobs, what): what for what in whats}
        for future in as_completed(future_map):
            what = future_map[future]
            jobs, total_count, status = future.result()
            counts_by_query[what] = total_count
            statuses.append(status)
            for job in jobs:
                jobs_by_id.setdefault(job["job_id"], job)

    if all(s == "no_key" for s in statuses):
        overall_status = "no_key"
    elif any(s.startswith("error") for s in statuses):
        overall_status = "error: " + "; ".join(s for s in statuses if s.startswith("error"))
    else:
        overall_status = "ok"

    return list(jobs_by_id.values()), counts_by_query, overall_status


# Careerjet is a global aggregator like Adzuna, but its public search API
# doesn't require a registered affiliate id in practice — a placeholder
# affid plus a Referer header (which the API explicitly asks for in its
# error message when missing) is enough to get real results. Unlike every
# other source here, Careerjet gives no stable job id at all — its `url` is
# a per-request tracking redirect that changes between identical calls — so
# job_id is a hash of (company, title, location) instead.
CAREERJET_AFFID = "test"
CAREERJET_HOURS_PER_YEAR = 2080


def _careerjet_annual_salary(job):
    salary_type = job.get("salary_type")
    try:
        lo, hi = float(job["salary_min"]), float(job["salary_max"])
    except (KeyError, ValueError, TypeError):
        return None, None
    if salary_type == "Y":
        return lo, hi
    if salary_type == "H":
        return lo * CAREERJET_HOURS_PER_YEAR, hi * CAREERJET_HOURS_PER_YEAR
    return None, None


def fetch_careerjet_search(what="customer service", pages=1):
    """Returns (jobs, total_hits, status)."""
    headers = {"Referer": "https://example.com"}
    jobs = []
    total_hits = 0
    errors = []
    for page in range(1, pages + 1):
        params = {
            "keywords": what,
            "location": "usa",
            "affid": CAREERJET_AFFID,
            "user_ip": "1.1.1.1",
            "user_agent": "Mozilla/5.0",
            "url": "http://example.com",
            "page": page,
            "pagesize": 20,
        }
        try:
            data = _get("http://public.api.careerjet.net/search", params, headers=headers)
            total_hits = data.get("hits", 0)
            results = data.get("jobs", [])
            for j in results:
                salary_min, salary_max = _careerjet_annual_salary(j)
                dedup_key = f"{j.get('company','')}|{j.get('title','')}|{j.get('locations','')}"
                job_id = hashlib.sha1(dedup_key.encode()).hexdigest()[:16]
                jobs.append({
                    "source": "careerjet",
                    "job_id": f"careerjet:{job_id}",
                    "company": j.get("company", ""),
                    "title": j.get("title", ""),
                    "location": j.get("locations", ""),
                    "country": "us",
                    "url": j.get("url", ""),
                    "posted_at": j.get("date", ""),
                    "salary_min": salary_min,
                    "salary_max": salary_max,
                })
            if len(results) < 20:
                break
        except Exception as e:
            errors.append(str(e))
            break

    status = "ok" if not errors else f"error: {'; '.join(errors)}"
    return jobs, total_hits, status


def fetch_careerjet_multi(whats, pages=1):
    """Run fetch_careerjet_search for each phrase, merge + dedup by job_id."""
    jobs_by_id = {}
    counts_by_query = {}
    statuses = []
    with ThreadPoolExecutor(max_workers=3) as pool:
        future_map = {pool.submit(fetch_careerjet_search, what, pages): what for what in whats}
        for future in as_completed(future_map):
            what = future_map[future]
            jobs, total_hits, status = future.result()
            counts_by_query[what] = total_hits
            statuses.append(status)
            for job in jobs:
                jobs_by_id.setdefault(job["job_id"], job)

    overall_status = "ok" if not any(s.startswith("error") for s in statuses) else \
        "error: " + "; ".join(s for s in statuses if s.startswith("error"))
    return list(jobs_by_id.values()), counts_by_query, overall_status


# Arbeitnow has no keyword search — it's a firehose of every job it indexes,
# heavily Germany/EU-weighted (it's a German site). There's no way to ask it
# for "customer service" specifically, so we page through the raw feed and
# keep only listings that look US-relevant or explicitly remote; everything
# else (the bulk of it) is discarded client-side before it ever reaches
# classify.py. Expect a low yield from this source given the scope mismatch.
ARBEITNOW_US_RE = re.compile(r"\b(usa|united states|\bUS\b)\b", re.IGNORECASE)


def fetch_arbeitnow(pages=10):
    jobs = []
    errors = []
    for page in range(1, pages + 1):
        try:
            data = _get("https://www.arbeitnow.com/api/job-board-api", {"page": page})
            results = data.get("data", [])
            for j in results:
                loc = j.get("location", "") or ""
                remote = bool(j.get("remote"))
                if not remote and not ARBEITNOW_US_RE.search(loc):
                    continue
                created_at = j.get("created_at")
                posted_at = (
                    datetime.fromtimestamp(created_at, tz=timezone.utc).isoformat()
                    if created_at else ""
                )
                jobs.append({
                    "source": "arbeitnow",
                    "job_id": f"arbeitnow:{j.get('slug')}",
                    "company": j.get("company_name", ""),
                    "title": j.get("title", ""),
                    "location": loc,
                    "country": "us" if ARBEITNOW_US_RE.search(loc) else "",
                    "url": j.get("url", ""),
                    "posted_at": posted_at,
                    "salary_min": None,
                    "salary_max": None,
                    "source_is_remote": remote,
                })
            if not data.get("links", {}).get("next"):
                break
        except Exception as e:
            errors.append(str(e))
            break

    status = "ok" if not errors else f"error: {'; '.join(errors)}"
    return jobs, status

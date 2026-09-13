"""Flask app: dashboard for outsourced-support-demand signal.

Runs in two modes, chosen automatically by store._using_redis():

- Local (no Upstash env vars): behaves as before — GET / serves whatever's
  in the in-memory _cache, POST /refresh runs the ~60-90s scan synchronously
  and is open to anyone hitting this local server (i.e. you).
- Hosted on Vercel (Upstash configured): GET / reads the last cron-written
  result straight from Redis — instant, no live scan on page load, since a
  visitor triggering a 60-90s scan on every request would blow past
  serverless function time limits. POST /refresh becomes the cron target
  and requires CRON_SECRET — Vercel Cron sends it automatically as
  "Authorization: Bearer <CRON_SECRET>" when that env var is set on the
  project; without a match, the endpoint refuses so a random visitor can't
  trigger (and pay for) a full scan.

debug=False on purpose for local runs — Flask's reloader spawns a second
process, which would duplicate the in-memory cache and make /refresh
inconsistent.
"""
import os

from flask import Flask, jsonify, render_template, request

import store
from jobs import run

app = Flask(__name__)

# In-memory cache for local mode only — empty until the user hits Refresh.
_cache = {"result": None}

CRON_SECRET = os.environ.get("CRON_SECRET", "")


def _refresh_authorized():
    if not store._using_redis():
        return True  # local mode: this server is only reachable by you
    if not CRON_SECRET:
        return False  # hosted with no secret configured — fail closed
    return request.headers.get("Authorization") == f"Bearer {CRON_SECRET}"


@app.route("/")
def index():
    if store._using_redis():
        try:
            result, updated_at = store.load_latest_result()
        except Exception:
            # A Redis outage should degrade to an empty page, not a 500 —
            # there's nothing the visitor can do about it either way.
            result, updated_at = None, None
        return render_template("index.html", result=result, hosted=True, updated_at=updated_at)
    return render_template("index.html", result=_cache["result"], hosted=False, updated_at=None)


@app.route("/refresh", methods=["GET", "POST"])
def refresh():
    if not _refresh_authorized():
        return jsonify({"error": "unauthorized"}), 401
    result = run()
    if store._using_redis():
        try:
            store.save_latest_result(result)
        except Exception as e:
            # The scan itself succeeded — still hand the result back so a
            # manually-triggered refresh isn't silently lost, just flag that
            # persistence failed (the next cron run will retry the save).
            return jsonify({"warning": f"scan ok, save failed: {e}", **result})
    else:
        _cache["result"] = result
    return jsonify(result)


if __name__ == "__main__":
    app.run(port=5001, debug=False)

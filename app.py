"""Flask app: dashboard for outsourced-support-demand signal.

Single mode, everywhere: GET / serves the last cached result, POST /refresh
runs the scan synchronously and caches it. No env vars required to deploy —
API keys have hardcoded fallbacks in sources.py, used only when the
corresponding environment variable isn't set (so a local .env still takes
priority if you have one).

Known limitation on Vercel specifically: serverless functions there have no
persistent memory or disk between invocations, so this in-memory cache (and
the JSONL dedup file store.py falls back to) resets on every cold start —
dedup/flow-over-time only really works when this runs as one long-lived
local process. The scan itself (~60-90s) also risks Vercel's function
duration limit on the Hobby plan unless Fluid Compute is enabled in the
project's dashboard settings (a platform toggle, not a variable — vercel.json
sets maxDuration but can't turn that on by itself).

debug=False on purpose for local runs — Flask's reloader spawns a second
process, which would duplicate the in-memory cache and make /refresh
inconsistent.
"""
from flask import Flask, jsonify, render_template

from jobs import run

app = Flask(__name__)

# In-memory cache of the last run's result. Empty until something hits Refresh.
_cache = {"result": None}


@app.route("/")
def index():
    return render_template("index.html", result=_cache["result"])


@app.route("/refresh", methods=["POST"])
def refresh():
    result = run()
    _cache["result"] = result
    return jsonify(result)


if __name__ == "__main__":
    app.run(port=5001, debug=False)

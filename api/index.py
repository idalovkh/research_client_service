"""Vercel entrypoint. Vercel's Python builder looks for a WSGI `app`
object in files under api/ — this just re-exports the Flask app defined at
the project root so app.py stays the single source of truth for routes and
works identically for `python3 app.py` locally."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import app  # noqa: E402

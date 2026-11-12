#!/usr/bin/env python3
"""Boot the web admin on localhost for the DAST scan.

The app needs no Discord connection to serve HTTP: `create_web_app` only imports
`app.*` modules, and every integration callback is optional. The scan therefore
targets the real routing, session, CSRF and header middleware rather than a mock.

Used by .github/workflows/dast-zap.yml. Not part of the bot runtime.
"""
from __future__ import annotations

import os
import secrets
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

# Keep the scan target self-contained; nothing here should reach Discord or the
# production data directory.
DATA_DIR = Path(tempfile.mkdtemp(prefix="dast-web-"))
os.environ.setdefault("LOG_DIR", str(DATA_DIR / "logs"))
os.environ.setdefault("DATA_DIR", str(DATA_DIR))
os.environ.setdefault("WEB_ENV_FILE", str(DATA_DIR / "env.env"))
os.environ.setdefault("WEB_ENFORCE_CSRF", "true")
os.environ.setdefault("WEB_ENFORCE_SAME_ORIGIN_POSTS", "true")
os.environ.setdefault("WEB_SESSION_COOKIE_SECURE", "false")
os.environ.setdefault("WEB_ADMIN_SESSION_SECRET", "dast-only-not-a-real-secret-0000000000000000")

from web_admin import create_web_app  # noqa: E402

HOST = os.environ.get("DAST_HOST", "127.0.0.1")
PORT = int(os.environ.get("DAST_PORT", "8082"))


def _bootstrap_password() -> str:
    """A per-run password that satisfies the web GUI policy.

    ``_password_policy_errors`` in web_admin requires 6-16 characters, at least two
    digits, an uppercase letter and a symbol. ``secrets.token_urlsafe(24)`` is 32
    characters and often has fewer than two digits, so using it left the bootstrap
    admin uncreated and login disabled.

    The fixed "A!00" prefix guarantees the two digits, the uppercase letter and the
    symbol; the random suffix keeps the password unguessable per run.
    """
    override = os.environ.get("DAST_ADMIN_PASSWORD")
    if override:
        return override
    return "A!00" + secrets.token_urlsafe(9)


def main() -> int:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    app = create_web_app(
        data_dir=str(DATA_DIR),
        env_file_path=str(DATA_DIR / "env.env"),
        tag_responses_file=str(DATA_DIR / "tag_responses.json"),
        default_admin_email="dast@example.invalid",
        # Random per run: this bootstrap account exists only for the scan window,
        # and a literal here trips Bandit B106 and reads like a real credential.
        # DAST_ADMIN_PASSWORD lets an authenticated scan (CSRF / rate-limit checks)
        # log in without committing a password to the repo.
        default_admin_password=_bootstrap_password(),
    )
    app.run(host=HOST, port=PORT, debug=False, use_reloader=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

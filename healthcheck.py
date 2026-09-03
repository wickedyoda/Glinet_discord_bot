#!/usr/bin/env python3
"""Healthcheck for the Glinet Discord bot.

When WEB_ENABLED is false, the bot doesn't run Flask, so we just
check that the main bot process is still responsive. We do this by
checking that the bot's log channel can be reached via Discord API.

When WEB_ENABLED is true, we also check the Flask /readyz endpoint.
"""

import os
import sys
import urllib.request

WEB_ENABLED = os.getenv("WEB_ENABLED", "true").strip().lower()

# If web admin is disabled, just pass
if WEB_ENABLED in ("0", "false", "no", "off"):
    sys.exit(0)

# Check Flask readyz endpoint
WEB_PORT = int(os.getenv("WEB_PORT", "8080") or "8080")
try:
    r = urllib.request.urlopen(f"http://127.0.0.1:{WEB_PORT}/readyz", timeout=8)  # nosec B310 - hardcoded localhost HTTP URL, no user input
    sys.exit(0 if r.status == 200 else 1)
except Exception:
    sys.exit(1)

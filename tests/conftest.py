"""Pytest conftest — sets required env vars so bot.py can be imported in CI.

The CI Integrity workflow does not provide a DISCORD_TOKEN, but bot.py calls
get_required_env("DISCORD_TOKEN") at import time. This conftest sets stub values
before any test module imports bot, so collection succeeds.
"""

import os

os.environ.setdefault("DISCORD_TOKEN", "test_token")
os.environ.setdefault("GUILD_ID", "0")
os.environ.setdefault("LOG_DIR", "/tmp/bot_test_logs")

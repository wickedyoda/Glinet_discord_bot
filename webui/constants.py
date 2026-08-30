"""Shared constants for the Glinet bot web GUI."""

from __future__ import annotations

import re

SENSITIVE_ENV_KEYS = {
    "DISCORD_TOKEN",
    "WEB_ADMIN_DEFAULT_PASSWORD",
    "WEB_ADMIN_DEFAULT_PASSWORD_HASH",
    "WEB_ADMIN_SESSION_SECRET",
    "TRANSLATE_API_KEY",
    "UPTIME_KUMA_API_KEY",
}
SESSION_SAMESITE_OPTIONS = ("Lax", "Strict", "None")
BOOL_SELECT_OPTIONS = ("false", "true")
LOG_FILE_OPTIONS = (
    "bot.log",
    "bot_log.log",
    "container_errors.log",
    "web_admin.log",
    "web_audit.log",
)
AUTO_REFRESH_INTERVAL_OPTIONS = (0, 1, 5, 10, 30, 60, 120)
LOG_EMAIL_PATTERN = re.compile(r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b")
LOG_SECRET_PATTERN = re.compile(r"(?i)\b(discord_token|token|password|authorization|cookie|secret)\b\s*[:=]\s*([^\s,;]+)")
FEED_INTERVAL_OPTIONS = (
    (300, "5 minutes"),
    (600, "10 minutes"),
    (900, "15 minutes"),
    (1800, "30 minutes"),
    (3600, "1 hour"),
    (10800, "3 hours"),
    (21600, "6 hours"),
)
UPTIME_MONITOR_INTERVAL_OPTIONS = (
    (30, "30 seconds"),
    (60, "1 minute"),
    (120, "2 minutes"),
    (300, "5 minutes"),
    (600, "10 minutes"),
    (900, "15 minutes"),
    (1800, "30 minutes"),
    (3600, "1 hour"),
)
UPTIME_MONITOR_TIMEOUT_OPTIONS = (3, 5, 8, 10, 15, 30)
AUTH_MODE_STANDARD = "standard"
AUTH_MODE_REMEMBER = "remember"
REMEMBER_LOGIN_DAYS = 5
PASSWORD_ROTATION_DAYS = 90
PASSWORD_MIN_LENGTH = 6
SQLITE_TIMEOUT_SECONDS = 10

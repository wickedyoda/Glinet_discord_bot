import base64
import hashlib
import http.client
import ipaddress
import json
import os
import re
import secrets
import shutil
import socket
import ssl
import subprocess  # nosec B404
import threading
import time
import zipfile
from collections import deque
from datetime import UTC, datetime, timedelta
from functools import wraps
from html import escape
from io import BytesIO
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from croniter import croniter
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from flask import (
    Flask,
    flash,
    g,
    redirect,
    render_template_string,
    request,
    send_file,
    session,
    url_for,
)
from werkzeug.middleware.proxy_fix import ProxyFix
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.serving import make_server

from app.service_monitor import (
    build_glinet_domain_monitor_targets,
    merge_service_monitor_targets,
    normalize_service_monitor_targets,
    serialize_service_monitor_targets,
)
from app.uptime_status import (
    build_uptime_instance_urls,
    build_uptime_source_config,
    default_uptime_api_key,
    extract_service_monitor_targets_from_uptime_config,
    extract_service_monitor_targets_from_uptime_metrics,
    fetch_uptime_metrics_text,
    fetch_uptime_public_config,
)
from app.web_audit import should_log_web_audit_event
from app.web_guild_settings import process_guild_settings_submission, render_guild_settings_body
from app.web_moderation import process_moderation_submission, render_moderation_body
from app.web_role_access import process_role_access_submission, render_role_access_body
from app.web_time import format_timestamp_display, parse_iso_datetime_utc
from app.web_user_store import (
    current_time_iso as _store_now_iso,
)
from app.web_user_store import (
    ensure_default_admin as _store_ensure_default_admin,
)
from app.web_user_store import (
    normalize_guild_group_name as _store_normalize_guild_group_name,
)
from app.web_user_store import (
    normalize_id_string_list as _normalize_id_string_list,
)
from app.web_user_store import (
    normalize_string_id_list as _normalize_string_id_list,
)
from app.web_user_store import (
    read_guild_groups as _store_read_guild_groups,
)
from app.web_user_store import (
    read_users as _store_read_users,
)
from app.web_user_store import (
    save_guild_groups as _store_save_guild_groups,
)
from app.web_user_store import (
    save_users as _store_save_users,
)


def ensure_process_utc_timezone():
    os.environ["TZ"] = "UTC"
    if hasattr(time, "tzset"):
        time.tzset()


ensure_process_utc_timezone()

CHANNEL_ID_PATTERN = re.compile(r"^\d+$|^<#\d+>$")
PASSWORD_MAX_AGE_DAYS = 90
REMEMBER_LOGIN_DAYS = 5
AUTH_MODE_STANDARD = "standard"
AUTH_MODE_REMEMBER = "remember"
PASSWORD_HASH_METHOD = "pbkdf2:sha256:600000"  # nosec B105
SESSION_TIMEOUT_MINUTE_OPTIONS = (5, 10, 15, 20, 30, 45, 60, 90, 120)
WEB_INACTIVITY_TIMEOUT_MINUTES = 60
POST_FORM_TAG_PATTERN = re.compile(
    r"(<form\b[^>]*\bmethod\s*=\s*[\"']?post[\"']?[^>]*>)",
    re.IGNORECASE,
)
STATE_CHANGING_METHODS = {"POST", "PUT", "PATCH", "DELETE"}
READ_ONLY_WRITE_EXEMPT_ENDPOINTS = {"login", "logout", "account", "healthz", "select_guild"}
WEB_GUI_TITLE_SUFFIX = "GL.iNet UnOfficial Discord Bot Dashboard"
WEB_GUI_VERSION_PREFIX = "v1.0"
RECENT_NAV_SESSION_KEY = "recent_admin_pages"
RECENT_NAV_LIMIT = 6
THEME_OPTIONS = (
    {"value": "light", "label": "Light"},
    {"value": "black", "label": "Black"},
    {"value": "forest", "label": "Forest"},
    {"value": "ember", "label": "Ember"},
    {"value": "ice", "label": "Ice"},
)
OBSERVABILITY_LOG_LINE_LIMIT = 500
OBSERVABILITY_LOG_OPTIONS = (
    ("bot.log", "Bot Runtime Log"),
    ("bot_log.log", "Bot Channel Log"),
    ("container_errors.log", "Container Error Log"),
    ("web_gui_audit.log", "Web GUI Audit Log"),
)
AUTO_REFRESH_INTERVAL_OPTIONS = (0, 1, 5, 10, 30, 60, 120)
REDDIT_FEED_SCHEDULE_OPTIONS = (
    ("*/5 * * * *", "Every 5 minutes"),
    ("*/10 * * * *", "Every 10 minutes"),
    ("*/15 * * * *", "Every 15 minutes"),
    ("*/30 * * * *", "Every 30 minutes"),
    ("0 * * * *", "Every hour"),
    ("0 */2 * * *", "Every 2 hours"),
)
MONITOR_RECHECK_SCHEDULE_OPTIONS = REDDIT_FEED_SCHEDULE_OPTIONS
OBSERVABILITY_HISTORY_RETENTION_HOURS = 24
OBSERVABILITY_HISTORY_SAMPLE_SECONDS = 60
LOG_EXPORT_RETENTION_HOURS = 24
LOG_EMAIL_PATTERN = re.compile(r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b")
LOG_SECRET_PATTERN = re.compile(r"(?i)\b(discord_token|token|password|authorization|cookie|secret)\b\s*[:=]\s*([^\s,;]+)")
SAFE_OUTBOUND_MAX_REDIRECTS = 3
INT_KEYS = {
    "GUILD_ID",
    "BOT_LOG_CHANNEL_ID",
    "FORUM_MAX_RESULTS",
    "DOCS_MAX_RESULTS_PER_SITE",
    "DOCS_INDEX_TTL_SECONDS",
    "SEARCH_RESPONSE_MAX_CHARS",
    "KICK_PRUNE_HOURS",
    "MODERATOR_ROLE_ID",
    "ADMIN_ROLE_ID",
    "MOD_LOG_CHANNEL_ID",
    "CSV_ROLE_ASSIGN_MAX_NAMES",
    "FIRMWARE_REQUEST_TIMEOUT_SECONDS",
    "FIRMWARE_RELEASE_NOTES_MAX_CHARS",
    "PUPPY_IMAGE_TIMEOUT_SECONDS",
    "SHORTENER_TIMEOUT_SECONDS",
    "YOUTUBE_POLL_INTERVAL_SECONDS",
    "YOUTUBE_REQUEST_TIMEOUT_SECONDS",
    "LINKEDIN_POLL_INTERVAL_SECONDS",
    "LINKEDIN_REQUEST_TIMEOUT_SECONDS",
    "UPTIME_STATUS_TIMEOUT_SECONDS",
    "WEB_PORT",
    "WEB_HTTPS_PORT",
    "WEB_DISCORD_CATALOG_TTL_SECONDS",
    "WEB_DISCORD_CATALOG_FETCH_TIMEOUT_SECONDS",
    "WEB_BULK_ASSIGN_TIMEOUT_SECONDS",
    "WEB_BULK_ASSIGN_MAX_UPLOAD_BYTES",
    "WEB_BULK_ASSIGN_REPORT_LIST_LIMIT",
    "WEB_BOT_PROFILE_TIMEOUT_SECONDS",
    "WEB_AVATAR_MAX_UPLOAD_BYTES",
    "WEB_SESSION_TIMEOUT_MINUTES",
    "LOG_RETENTION_DAYS",
    "LOG_ROTATION_INTERVAL_DAYS",
}
SENSITIVE_KEYS = {
    "DISCORD_TOKEN",
    "WEB_ADMIN_DEFAULT_PASSWORD",
    "WEB_ADMIN_SESSION_SECRET",
    "UPTIME_STATUS_API_KEY",
}
FALLBACK_PROTECTED_ENV_KEYS = SENSITIVE_KEYS | {"WEB_ENV_FILE"}


def _resolve_web_gui_version_label() -> str:
    explicit = str(os.getenv("WEB_GUI_VERSION", "")).strip()
    if explicit:
        return explicit

    repo_root = Path(__file__).resolve().parent
    if (repo_root / ".git").exists():
        git_executable = shutil.which("git")
        if git_executable:
            try:
                completed = subprocess.run(
                    [
                        git_executable,
                        "log",
                        "-1",
                        "--date=format-local:%Y%m%d.%H%M%S",
                        "--format=%cd",
                    ],
                    cwd=repo_root,
                    check=True,
                    capture_output=True,
                    text=True,
                )  # nosec B603
                stamp = completed.stdout.strip()
                if stamp:
                    return f"{WEB_GUI_VERSION_PREFIX}-{stamp}"
            except (OSError, subprocess.SubprocessError, ValueError):
                pass

    try:
        modified_stamp = datetime.fromtimestamp(Path(__file__).stat().st_mtime, tz=UTC).strftime("%Y%m%d.%H%M%S")
        return f"{WEB_GUI_VERSION_PREFIX}-{modified_stamp}"
    except Exception:
        return f"{WEB_GUI_VERSION_PREFIX}-unknown"


WEB_GUI_VERSION_LABEL = _resolve_web_gui_version_label()


def _clip_text(value: str, max_chars: int = 120):
    text = str(value or "").strip()
    if not text:
        return ""
    if len(text) <= max_chars:
        return text
    return f"{text[: max_chars - 3]}..."


ENV_FIELDS = [
    ("DISCORD_TOKEN", "Discord Token", "Bot token for Discord authentication."),
    ("GUILD_ID", "Guild ID", "Primary guild (server) ID."),
    (
        "MANAGED_GUILD_IDS",
        "Managed Guild IDs",
        "Optional comma-separated guild IDs this bot should actively manage and sync.",
    ),
    (
        "BOT_LOG_CHANNEL_ID",
        "Bot Log Channel ID",
        "Global default bot log/activity channel. Guild settings override this per server.",
    ),
    ("LOG_LEVEL", "Log Level", "Bot log level (DEBUG, INFO, WARNING, ERROR)."),
    (
        "CONTAINER_LOG_LEVEL",
        "Container Log Level",
        "Container-wide error log level (DEBUG, INFO, WARNING, ERROR, CRITICAL).",
    ),
    (
        "DISCORD_LOG_LEVEL",
        "Discord Library Log Level",
        "Discord/werkzeug logger level to prevent noisy payload dumps (DEBUG, INFO, WARNING, ERROR, CRITICAL).",
    ),
    ("DATA_DIR", "Data Directory", "Persistent data directory inside container."),
    (
        "LOG_DIR",
        "Log Directory",
        "Directory for runtime log files (bot.log, bot_log.log, container_errors.log, web_gui_audit.log).",
    ),
    (
        "LOG_HARDEN_FILE_PERMISSIONS",
        "Log Harden File Permissions",
        "Attempt to enforce restrictive permissions on LOG_DIR and runtime log files.",
    ),
    (
        "LOG_RETENTION_DAYS",
        "Log Retention (Days)",
        "How many days of rotated logs are retained.",
    ),
    (
        "LOG_ROTATION_INTERVAL_DAYS",
        "Log Rotation Interval (Days)",
        "How often log files rotate (in days).",
    ),
    ("FORUM_BASE_URL", "Forum Base URL", "GL.iNet forum root URL."),
    ("FORUM_MAX_RESULTS", "Forum Max Results", "Max forum links returned per search."),
    (
        "DOCS_MAX_RESULTS_PER_SITE",
        "Docs Max/Site",
        "Max docs results for each docs source.",
    ),
    ("DOCS_INDEX_TTL_SECONDS", "Docs Index TTL", "Docs index cache TTL in seconds."),
    (
        "SEARCH_RESPONSE_MAX_CHARS",
        "Search Response Limit",
        "Max chars in search response message.",
    ),
    (
        "KICK_PRUNE_HOURS",
        "Kick Prune Hours",
        "Hours of message history to prune on kick.",
    ),
    (
        "MODERATOR_ROLE_ID",
        "Moderator Role ID",
        "Role ID allowed to run moderation commands.",
    ),
    ("ADMIN_ROLE_ID", "Admin Role ID", "Additional role ID allowed to moderate."),
    (
        "CSV_ROLE_ASSIGN_MAX_NAMES",
        "CSV Role Max Names",
        "Max unique names accepted per CSV bulk-assign.",
    ),
    (
        "MOD_LOG_CHANNEL_ID",
        "Mod Log Channel ID",
        "Global default moderation/server log channel. Guild settings override this per server.",
    ),
    (
        "firmware_notification_channel",
        "Firmware Notify Channel",
        "Global default firmware notify channel. Guild settings override this per server.",
    ),
    (
        "FIRMWARE_FEED_URL",
        "Firmware Feed URL",
        "Source URL used for firmware mirror checks.",
    ),
    (
        "firmware_check_schedule",
        "Firmware Cron Schedule",
        "5-field cron schedule in UTC.",
    ),
    (
        "FIRMWARE_REQUEST_TIMEOUT_SECONDS",
        "Firmware Request Timeout",
        "HTTP timeout for firmware fetch requests.",
    ),
    (
        "FIRMWARE_RELEASE_NOTES_MAX_CHARS",
        "Firmware Notes Max Chars",
        "Max release-notes excerpt size.",
    ),
    (
        "REDDIT_FEED_CHECK_SCHEDULE",
        "Reddit Feed Schedule",
        "5-field cron schedule in UTC for Reddit feed polling.",
    ),
    (
        "ENABLE_MEMBERS_INTENT",
        "Enable Members Intent",
        "Request Discord privileged members intent for join/member tracking features.",
    ),
    (
        "COMMAND_RESPONSES_EPHEMERAL",
        "Command Responses Ephemeral",
        "Default visibility for utility/help slash command responses.",
    ),
    (
        "PUPPY_IMAGE_API_URL",
        "Puppy Image API URL",
        "JSON endpoint used by /happy for a random puppy image.",
    ),
    (
        "PUPPY_IMAGE_TIMEOUT_SECONDS",
        "Puppy API Timeout",
        "Timeout in seconds for the puppy API request.",
    ),
    (
        "SHORTENER_ENABLED",
        "Shortener Enabled",
        "Enable /shorten and /expand.",
    ),
    (
        "SHORTENER_BASE_URL",
        "Shortener Base URL",
        "Base URL of the shortener service.",
    ),
    (
        "SHORTENER_TIMEOUT_SECONDS",
        "Shortener Timeout",
        "Timeout in seconds for shortener requests.",
    ),
    (
        "FIRMWARE_MONITOR_ENABLED",
        "Firmware Monitor Enabled",
        "Enable or disable firmware polling and posting without removing saved guild channel settings.",
    ),
    (
        "REDDIT_FEED_NOTIFY_ENABLED",
        "Reddit Feed Monitor Enabled",
        "Enable or disable Reddit feed polling and posting without removing saved feed subscriptions.",
    ),
    (
        "YOUTUBE_NOTIFY_ENABLED",
        "YouTube Notify Enabled",
        "Enable YouTube upload polling and posting.",
    ),
    (
        "YOUTUBE_POLL_INTERVAL_SECONDS",
        "YouTube Poll Interval",
        "Seconds between YouTube feed checks.",
    ),
    (
        "YOUTUBE_REQUEST_TIMEOUT_SECONDS",
        "YouTube Request Timeout",
        "Timeout in seconds for YouTube page/feed requests.",
    ),
    (
        "LINKEDIN_NOTIFY_ENABLED",
        "LinkedIn Notify Enabled",
        "Enable LinkedIn public-profile polling and posting.",
    ),
    (
        "LINKEDIN_POLL_INTERVAL_SECONDS",
        "LinkedIn Poll Interval",
        "Seconds between LinkedIn profile checks.",
    ),
    (
        "LINKEDIN_REQUEST_TIMEOUT_SECONDS",
        "LinkedIn Request Timeout",
        "Timeout in seconds for LinkedIn public-profile requests.",
    ),
    (
        "BETA_PROGRAM_NOTIFY_ENABLED",
        "Beta Program Monitor Enabled",
        "Enable or disable GL.iNet beta program polling and posting without removing saved monitors.",
    ),
    (
        "BETA_PROGRAM_POLL_INTERVAL_SECONDS",
        "Beta Program Poll Interval",
        "Seconds between GL.iNet beta program checks.",
    ),
    (
        "BETA_PROGRAM_REQUEST_TIMEOUT_SECONDS",
        "Beta Program Request Timeout",
        "Timeout in seconds for GL.iNet beta program requests.",
    ),
    (
        "SERVICE_MONITOR_ENABLED",
        "Service Monitor Enabled",
        "Enable or disable generic website/API outage checks.",
    ),
    (
        "SERVICE_MONITOR_DEFAULT_CHANNEL_ID",
        "Service Monitor Default Channel",
        "Discord text channel used for service outage alerts when a target does not specify its own channel_id.",
    ),
    (
        "SERVICE_MONITOR_CHECK_SCHEDULE",
        "Service Monitor Schedule",
        "5-field cron schedule in UTC for website/API checks.",
    ),
    (
        "SERVICE_MONITOR_REQUEST_TIMEOUT_SECONDS",
        "Service Monitor Timeout",
        "Default HTTP timeout in seconds for service checks.",
    ),
    (
        "SERVICE_MONITOR_TARGETS_JSON",
        "Service Monitor Targets JSON",
        "JSON array of service checks. Each item supports name, url, optional method, expected_status, contains_text, timeout_seconds, and channel_id.",
    ),
    (
        "UPTIME_STATUS_ENABLED",
        "Uptime Status Enabled",
        "Enable /uptime integration against either a public Uptime Kuma page or an authenticated Uptime Kuma instance.",
    ),
    (
        "UPTIME_STATUS_NOTIFY_ENABLED",
        "Uptime Status Alerting",
        "Enable scheduled outage/recovery notifications from the configured Uptime Kuma source.",
    ),
    (
        "UPTIME_STATUS_NOTIFY_CHANNEL_ID",
        "Uptime Status Notify Channel",
        "Discord text channel that receives Uptime Kuma outage and recovery alerts.",
    ),
    (
        "UPTIME_STATUS_CHECK_SCHEDULE",
        "Uptime Status Check Schedule",
        "5-field cron schedule in UTC for Uptime Kuma alert checks.",
    ),
    (
        "UPTIME_STATUS_PAGE_URL",
        "Uptime Status Page URL",
        "Optional public uptime page URL in /status/<slug> format.",
    ),
    (
        "UPTIME_STATUS_INSTANCE_URL",
        "Uptime Kuma Instance URL",
        "Optional authenticated Uptime Kuma base URL such as https://kuma.example.com/.",
    ),
    (
        "UPTIME_STATUS_API_KEY",
        "Uptime Kuma API Key",
        "Optional API key used to read the authenticated instance metrics endpoint.",
    ),
    (
        "UPTIME_STATUS_VERIFY_TLS",
        "Uptime Kuma Verify TLS",
        "Set to true/false to enforce or skip TLS certificate verification for Kuma requests.",
    ),
    (
        "UPTIME_STATUS_TIMEOUT_SECONDS",
        "Uptime Status Timeout",
        "Timeout in seconds for uptime status requests.",
    ),
    (
        "WEB_ENABLED",
        "Web UI Enabled",
        "Set to true/false to enable or disable the web admin UI.",
    ),
    ("WEB_BIND_HOST", "Web Bind Host", "Host/IP bind for web admin service."),
    (
        "WEB_PORT",
        "Web Container Port",
        "Internal HTTP port in the container (default 8080).",
    ),
    (
        "WEB_HTTPS_PORT",
        "Web HTTPS Port",
        "Internal HTTPS port in the container (default 8081).",
    ),
    (
        "WEB_HTTP_PUBLISH",
        "Web HTTP Publish",
        "Optional Docker Compose HTTP publish override, e.g. 8080 or 127.0.0.1:8080.",
    ),
    (
        "WEB_HTTPS_PUBLISH",
        "Web HTTPS Publish",
        "Optional Docker Compose HTTPS publish override, e.g. 8081 or 127.0.0.1:8081.",
    ),
    (
        "WEB_SESSION_TIMEOUT_MINUTES",
        "Web Auto Logout (Minutes)",
        "Session inactivity timeout in minutes (allowed: 5, 10, 15, 20, 30, 45, 60, 90, 120).",
    ),
    (
        "WEB_DISCORD_CATALOG_TTL_SECONDS",
        "Discord Catalog TTL",
        "Seconds to cache polled Discord channels/roles for dropdowns.",
    ),
    (
        "WEB_DISCORD_CATALOG_FETCH_TIMEOUT_SECONDS",
        "Discord Catalog Fetch Timeout",
        "Timeout in seconds when polling Discord for channels/roles.",
    ),
    (
        "WEB_BULK_ASSIGN_TIMEOUT_SECONDS",
        "Web Bulk Assign Timeout",
        "Timeout in seconds for web-triggered CSV role assignment.",
    ),
    (
        "WEB_BULK_ASSIGN_MAX_UPLOAD_BYTES",
        "Web Bulk Assign Max Upload",
        "Maximum CSV upload size in bytes for web bulk assignment.",
    ),
    (
        "WEB_BULK_ASSIGN_REPORT_LIST_LIMIT",
        "Web Bulk Assign Report Limit",
        "Maximum items displayed per section in web bulk-assignment details.",
    ),
    (
        "WEB_BOT_PROFILE_TIMEOUT_SECONDS",
        "Web Bot Profile Timeout",
        "Timeout in seconds for loading/updating bot profile from web UI.",
    ),
    (
        "WEB_AVATAR_MAX_UPLOAD_BYTES",
        "Web Avatar Max Upload",
        "Maximum avatar upload size in bytes for bot profile uploads.",
    ),
    (
        "WEB_RESTART_ENABLED",
        "Web Restart Enabled",
        "Enable admin restart button in the web header.",
    ),
    (
        "WEB_PUBLIC_BASE_URL",
        "Web Public Base URL",
        "Public external base URL used behind reverse proxies (for origin validation).",
    ),
    (
        "WEB_HTTPS_ENABLED",
        "Web HTTPS Enabled",
        "Enable the built-in HTTPS listener alongside HTTP.",
    ),
    (
        "WEB_SSL_DIR",
        "Web SSL Directory",
        "Directory used to store the HTTPS certificate and key (defaults to DATA_DIR/ssl).",
    ),
    (
        "WEB_SSL_CERT_FILE",
        "Web SSL Cert File",
        "Certificate filename or absolute path. Defaults to tls.crt inside WEB_SSL_DIR.",
    ),
    (
        "WEB_SSL_KEY_FILE",
        "Web SSL Key File",
        "Private key filename or absolute path. Defaults to tls.key inside WEB_SSL_DIR.",
    ),
    (
        "WEB_SSL_COMMON_NAME",
        "Web SSL Common Name",
        "Hostname used when generating the default self-signed certificate.",
    ),
    (
        "WEB_GITHUB_WIKI_URL",
        "Web GitHub Wiki URL",
        "External docs link shown in the web header.",
    ),
    (
        "WEB_ENV_FILE",
        "Web Env File Path",
        "Environment file path used by the web settings editor.",
    ),
    (
        "WEB_ADMIN_DEFAULT_USERNAME",
        "Default Admin Email",
        "Default admin email used for first boot user creation.",
    ),
    (
        "WEB_ADMIN_DEFAULT_PASSWORD",
        "Default Admin Password",
        "Default admin password for first boot user creation.",
    ),
    ("WEB_ADMIN_SESSION_SECRET", "Web Session Secret", "Flask session signing secret."),
    (
        "WEB_SESSION_COOKIE_SECURE",
        "Web Secure Session Cookie",
        "Set true to require HTTPS for session cookies.",
    ),
    (
        "WEB_SESSION_COOKIE_SAMESITE",
        "Web Session Cookie SameSite",
        "Session cookie SameSite policy: Lax, Strict, or None.",
    ),
    (
        "WEB_TRUST_PROXY_HEADERS",
        "Web Trust Proxy Headers",
        "Set true when running behind a trusted reverse proxy forwarding host/proto/IP headers.",
    ),
    (
        "WEB_ENFORCE_CSRF",
        "Web Enforce CSRF",
        "Enable CSRF token checks for POST/PUT/PATCH/DELETE requests.",
    ),
    (
        "WEB_ENFORCE_SAME_ORIGIN_POSTS",
        "Web Enforce Same-Origin POST",
        "Require POST/PUT/PATCH/DELETE requests to originate from the same host.",
    ),
    (
        "WEB_HARDEN_FILE_PERMISSIONS",
        "Web Harden File Permissions",
        "Attempt to enforce restrictive permissions on .env and data files.",
    ),
]
ENV_KEY_ALIASES = {
    "BOT_LOG_CHANNEL_ID": ("GENERAL_CHANNEL_ID",),
}
ENV_FIELD_SECTIONS = (
    (
        "Bot Identity And Scope",
        "Core Discord identity, guild scope, and command access defaults.",
        (
            "DISCORD_TOKEN",
            "GUILD_ID",
            "MANAGED_GUILD_IDS",
            "MODERATOR_ROLE_ID",
            "ADMIN_ROLE_ID",
            "ENABLE_MEMBERS_INTENT",
            "COMMAND_RESPONSES_EPHEMERAL",
        ),
    ),
    (
        "Logging And Storage",
        "Runtime log levels, log rotation, and persistent storage paths.",
        (
            "BOT_LOG_CHANNEL_ID",
            "MOD_LOG_CHANNEL_ID",
            "LOG_LEVEL",
            "CONTAINER_LOG_LEVEL",
            "DISCORD_LOG_LEVEL",
            "DATA_DIR",
            "LOG_DIR",
            "LOG_HARDEN_FILE_PERMISSIONS",
            "LOG_RETENTION_DAYS",
            "LOG_ROTATION_INTERVAL_DAYS",
        ),
    ),
    (
        "Search, Moderation, And Utilities",
        "Forum/docs search limits and utility command tuning.",
        (
            "FORUM_BASE_URL",
            "FORUM_MAX_RESULTS",
            "DOCS_MAX_RESULTS_PER_SITE",
            "DOCS_INDEX_TTL_SECONDS",
            "SEARCH_RESPONSE_MAX_CHARS",
            "KICK_PRUNE_HOURS",
            "CSV_ROLE_ASSIGN_MAX_NAMES",
            "PUPPY_IMAGE_API_URL",
            "PUPPY_IMAGE_TIMEOUT_SECONDS",
            "SHORTENER_ENABLED",
            "SHORTENER_BASE_URL",
            "SHORTENER_TIMEOUT_SECONDS",
        ),
    ),
    (
        "Feed And Status Monitors",
        "Global defaults for feed polling, notification delivery, and uptime integrations. Guild settings can override the on/off state and some channels per server.",
        (
            "firmware_notification_channel",
            "FIRMWARE_FEED_URL",
            "firmware_check_schedule",
            "FIRMWARE_REQUEST_TIMEOUT_SECONDS",
            "FIRMWARE_RELEASE_NOTES_MAX_CHARS",
            "FIRMWARE_MONITOR_ENABLED",
            "REDDIT_FEED_CHECK_SCHEDULE",
            "REDDIT_FEED_NOTIFY_ENABLED",
            "YOUTUBE_NOTIFY_ENABLED",
            "YOUTUBE_POLL_INTERVAL_SECONDS",
            "YOUTUBE_REQUEST_TIMEOUT_SECONDS",
            "LINKEDIN_NOTIFY_ENABLED",
            "LINKEDIN_POLL_INTERVAL_SECONDS",
            "LINKEDIN_REQUEST_TIMEOUT_SECONDS",
            "BETA_PROGRAM_NOTIFY_ENABLED",
            "BETA_PROGRAM_POLL_INTERVAL_SECONDS",
            "BETA_PROGRAM_REQUEST_TIMEOUT_SECONDS",
            "SERVICE_MONITOR_ENABLED",
            "SERVICE_MONITOR_DEFAULT_CHANNEL_ID",
            "SERVICE_MONITOR_CHECK_SCHEDULE",
            "SERVICE_MONITOR_REQUEST_TIMEOUT_SECONDS",
            "SERVICE_MONITOR_TARGETS_JSON",
            "UPTIME_STATUS_ENABLED",
            "UPTIME_STATUS_NOTIFY_ENABLED",
            "UPTIME_STATUS_NOTIFY_CHANNEL_ID",
            "UPTIME_STATUS_CHECK_SCHEDULE",
            "UPTIME_STATUS_PAGE_URL",
            "UPTIME_STATUS_INSTANCE_URL",
            "UPTIME_STATUS_API_KEY",
            "UPTIME_STATUS_TIMEOUT_SECONDS",
            "UPTIME_STATUS_VERIFY_TLS",
        ),
    ),
    (
        "Web UI Runtime And Security",
        "Ports, publish bindings, session policy, proxy behavior, TLS files, and web-admin bootstrap settings.",
        (
            "WEB_ENABLED",
            "WEB_BIND_HOST",
            "WEB_PORT",
            "WEB_HTTPS_PORT",
            "WEB_HTTP_PUBLISH",
            "WEB_HTTPS_PUBLISH",
            "WEB_SESSION_TIMEOUT_MINUTES",
            "WEB_DISCORD_CATALOG_TTL_SECONDS",
            "WEB_DISCORD_CATALOG_FETCH_TIMEOUT_SECONDS",
            "WEB_BULK_ASSIGN_TIMEOUT_SECONDS",
            "WEB_BULK_ASSIGN_MAX_UPLOAD_BYTES",
            "WEB_BULK_ASSIGN_REPORT_LIST_LIMIT",
            "WEB_BOT_PROFILE_TIMEOUT_SECONDS",
            "WEB_AVATAR_MAX_UPLOAD_BYTES",
            "WEB_RESTART_ENABLED",
            "WEB_PUBLIC_BASE_URL",
            "WEB_HTTPS_ENABLED",
            "WEB_SSL_DIR",
            "WEB_SSL_CERT_FILE",
            "WEB_SSL_KEY_FILE",
            "WEB_SSL_COMMON_NAME",
            "WEB_GITHUB_WIKI_URL",
            "WEB_ENV_FILE",
            "WEB_ADMIN_DEFAULT_USERNAME",
            "WEB_ADMIN_DEFAULT_PASSWORD",
            "WEB_ADMIN_SESSION_SECRET",
            "WEB_SESSION_COOKIE_SECURE",
            "WEB_SESSION_COOKIE_SAMESITE",
            "WEB_TRUST_PROXY_HEADERS",
            "WEB_ENFORCE_CSRF",
            "WEB_ENFORCE_SAME_ORIGIN_POSTS",
            "WEB_HARDEN_FILE_PERMISSIONS",
        ),
    ),
)
ENV_FIELD_SECTION_LOOKUP = {
    field_key: (section_title, section_description)
    for section_title, section_description, field_keys in ENV_FIELD_SECTIONS
    for field_key in field_keys
}


def _normalize_email(email: str) -> str:
    return (email or "").strip().lower()


def _is_valid_email(email: str) -> bool:
    candidate = _normalize_email(email)
    if not candidate or len(candidate) > 254:
        return False
    if any(char.isspace() for char in candidate):
        return False

    local, separator, domain = candidate.partition("@")
    if separator != "@" or not local or not domain:
        return False
    if "@" in domain or "." not in domain:
        return False
    if local.startswith(".") or local.endswith(".") or ".." in local:
        return False

    allowed_local = set("abcdefghijklmnopqrstuvwxyz0123456789!#$%&'*+/=?^_`{|}~.-")
    if any(char not in allowed_local for char in local):
        return False

    labels = domain.split(".")
    if len(labels) < 2:
        return False
    for label in labels:
        if not label or label.startswith("-") or label.endswith("-"):
            return False
        if any(not (char.isascii() and (char.isalnum() or char == "-")) for char in label):
            return False
    if len(labels[-1]) < 2:
        return False
    return True


def _password_policy_errors(password: str):
    candidate = password or ""
    length = len(candidate)
    digits = sum(1 for char in candidate if char.isdigit())
    uppercase = sum(1 for char in candidate if char.isupper())
    symbols = sum(1 for char in candidate if not char.isalnum())
    errors = []
    if length < 6:
        errors.append("Password must be at least 6 characters long.")
    if length > 16:
        errors.append("Password must be 16 characters or fewer.")
    if digits < 2:
        errors.append("Password must contain at least 2 numbers.")
    if uppercase < 1:
        errors.append("Password must contain at least 1 uppercase letter.")
    if symbols < 1:
        errors.append("Password must contain at least 1 symbol.")
    return errors


def _hash_password(password: str) -> str:
    return generate_password_hash(password, method=PASSWORD_HASH_METHOD)


def _password_hash_needs_upgrade(password_hash: str) -> bool:
    return not str(password_hash or "").startswith(f"{PASSWORD_HASH_METHOD}$")


def _normalize_session_timeout_minutes(raw_value, default_value: int = WEB_INACTIVITY_TIMEOUT_MINUTES) -> int:
    try:
        parsed = int(str(raw_value).strip())
    except (TypeError, ValueError):
        return default_value
    if parsed not in SESSION_TIMEOUT_MINUTE_OPTIONS:
        return default_value
    return parsed


def _normalize_session_cookie_samesite(raw_value, default_value: str = "Lax") -> str:
    candidate = str(raw_value or "").strip().lower()
    mapping = {"lax": "Lax", "strict": "Strict", "none": "None"}
    default_key = str(default_value or "").strip().lower()
    if not default_key:
        fallback = ""
    else:
        fallback = mapping.get(default_key, "Lax")
    return mapping.get(candidate, fallback)


def _clean_profile_text(value: str, max_length: int = 80) -> str:
    normalized = " ".join(str(value or "").strip().split())
    if len(normalized) > max_length:
        return normalized[:max_length].strip()
    return normalized


def _default_display_name(email: str) -> str:
    local = str(email or "").split("@", 1)[0]
    local = re.sub(r"[._-]+", " ", local)
    cleaned = _clean_profile_text(local, max_length=80)
    return cleaned.title() if cleaned else "User"


def _is_admin_user(user: dict | None) -> bool:
    return _normalize_web_user_role((user or {}).get("role", "")) == "admin"


def _normalize_web_user_role(value: str | None, *, is_admin: bool = False) -> str:
    raw = str(value or "").strip().lower()
    if raw == "glinet":
        return "glinet_read_only"
    if raw in {"admin", "read_only", "glinet_read_only", "glinet_rw", "guild_admin"}:
        return raw
    return "admin" if bool(is_admin) else "read_only"


def _is_glinet_read_only_user(user: dict | None) -> bool:
    return _normalize_web_user_role((user or {}).get("role", "")) == "glinet_read_only"


def _is_glinet_rw_user(user: dict | None) -> bool:
    return _normalize_web_user_role((user or {}).get("role", "")) == "glinet_rw"


def _is_guild_admin_user(user: dict | None) -> bool:
    return _normalize_web_user_role((user or {}).get("role", "")) == "guild_admin"


def _is_glinet_scoped_user(user: dict | None) -> bool:
    normalized = _normalize_web_user_role((user or {}).get("role", ""))
    return normalized in {"glinet_read_only", "glinet_rw"}


def _user_role_label(role_value: str | None = None, *, is_admin: bool = False) -> str:
    normalized = _normalize_web_user_role(role_value, is_admin=is_admin)
    if normalized == "admin":
        return "Admin"
    if normalized == "guild_admin":
        return "Guild Admin"
    if normalized == "glinet_rw":
        return "Glinet-RW"
    if normalized == "glinet_read_only":
        return "Glinet-Read-Only"
    return "Read-only"


def _audit_user_label_from_email(email: str) -> str:
    normalized_email = _normalize_email(email)
    if not normalized_email:
        return "anonymous"
    salt = (os.getenv("WEB_ADMIN_SESSION_SECRET", "") or "glinet-web-audit-label").encode("utf-8")
    digest = hashlib.scrypt(
        normalized_email.encode("utf-8"),
        salt=salt,
        n=2**14,
        r=8,
        p=1,
        dklen=12,
    ).hex()
    return f"user_{digest}"


def _format_bytes(value: int):
    try:
        size = float(max(0, int(value)))
    except (TypeError, ValueError):
        return "n/a"
    units = ["B", "KB", "MB", "GB", "TB"]
    idx = 0
    while size >= 1024.0 and idx < (len(units) - 1):
        size /= 1024.0
        idx += 1
    if idx == 0:
        return f"{int(size)} {units[idx]}"
    return f"{size:.2f} {units[idx]}"


def _format_uptime(seconds: float):
    try:
        total = max(0, int(float(seconds)))
    except (TypeError, ValueError):
        return "n/a"
    days, rem = divmod(total, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    if days > 0:
        return f"{days}d {hours}h {minutes}m {secs}s"
    if hours > 0:
        return f"{hours}h {minutes}m {secs}s"
    if minutes > 0:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


def _safe_read_text(path: Path):
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except (PermissionError, OSError):
        return ""


def _sanitize_log_preview(text: str):
    sanitized = str(text or "")
    sanitized = LOG_EMAIL_PATTERN.sub("[redacted-email]", sanitized)
    sanitized = LOG_SECRET_PATTERN.sub(r"\1=[redacted]", sanitized)
    return sanitized


def _read_latest_log_lines(path: Path, line_limit: int = OBSERVABILITY_LOG_LINE_LIMIT):
    if not path.exists() or not path.is_file():
        return f"Log file not found: {path}"
    try:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            lines = deque(handle, maxlen=max(1, int(line_limit)))
        if not lines:
            return "(log file is empty)"
        return _sanitize_log_preview("".join(lines))
    except (PermissionError, OSError) as exc:
        return f"Unable to read log file: {exc}"


def _resolve_observability_log_paths(log_dir: Path):
    try:
        base_dir = log_dir.resolve()
    except OSError:
        return {}

    allowed = {}
    for filename, _label in OBSERVABILITY_LOG_OPTIONS:
        safe_name = Path(str(filename)).name
        if safe_name != filename:
            continue
        try:
            candidate = (base_dir / safe_name).resolve()
            candidate.relative_to(base_dir)
        except (ValueError, OSError):
            continue
        allowed[filename] = candidate
    return allowed


def _parse_auto_refresh_seconds(raw_value, default_value: int = 0):
    try:
        parsed = int(str(raw_value or "").strip())
    except (TypeError, ValueError):
        return int(default_value)
    if parsed in AUTO_REFRESH_INTERVAL_OPTIONS:
        return parsed
    return int(default_value)


def _read_rss_bytes():
    for line in _safe_read_text(Path("/proc/self/status")).splitlines():
        if not line.startswith("VmRSS:"):
            continue
        parts = line.split()
        if len(parts) >= 2 and parts[1].isdigit():
            return int(parts[1]) * 1024
    return None


def _read_process_io_bytes():
    read_bytes = None
    write_bytes = None
    for line in _safe_read_text(Path("/proc/self/io")).splitlines():
        if ":" not in line:
            continue
        key, raw_value = line.split(":", 1)
        value = raw_value.strip()
        if not value.isdigit():
            continue
        if key.strip() == "read_bytes":
            read_bytes = int(value)
        elif key.strip() == "write_bytes":
            write_bytes = int(value)
    return {"read_bytes": read_bytes, "write_bytes": write_bytes}


def _read_network_bytes():
    text = _safe_read_text(Path("/proc/net/dev"))
    if not text:
        return {"rx_bytes": None, "tx_bytes": None}
    rx_total = 0
    tx_total = 0
    seen_interface = False
    for line in text.splitlines()[2:]:
        if ":" not in line:
            continue
        name_part, values_part = line.split(":", 1)
        iface = name_part.strip()
        if not iface:
            continue
        values = values_part.split()
        if len(values) < 16:
            continue
        if iface == "lo":
            continue
        if not values[0].isdigit() or not values[8].isdigit():
            continue
        rx_total += int(values[0])
        tx_total += int(values[8])
        seen_interface = True
    if not seen_interface:
        return {"rx_bytes": None, "tx_bytes": None}
    return {"rx_bytes": rx_total, "tx_bytes": tx_total}


def _read_cgroup_memory_usage():
    v2_usage = Path("/sys/fs/cgroup/memory.current")
    v2_max = Path("/sys/fs/cgroup/memory.max")
    if v2_usage.exists():
        usage_raw = _safe_read_text(v2_usage).strip()
        max_raw = _safe_read_text(v2_max).strip()
        usage = int(usage_raw) if usage_raw.isdigit() else None
        limit = int(max_raw) if max_raw.isdigit() else None
        return {"usage_bytes": usage, "limit_bytes": limit}

    v1_usage = Path("/sys/fs/cgroup/memory/memory.usage_in_bytes")
    v1_limit = Path("/sys/fs/cgroup/memory/memory.limit_in_bytes")
    if v1_usage.exists():
        usage_raw = _safe_read_text(v1_usage).strip()
        limit_raw = _safe_read_text(v1_limit).strip()
        usage = int(usage_raw) if usage_raw.isdigit() else None
        limit = int(limit_raw) if limit_raw.isdigit() else None
        if limit is not None and limit >= (1 << 60):
            limit = None
        return {"usage_bytes": usage, "limit_bytes": limit}

    return {"usage_bytes": None, "limit_bytes": None}


def _read_cgroup_cpu_seconds_total():
    v2 = Path("/sys/fs/cgroup/cpu.stat")
    if v2.exists():
        for line in _safe_read_text(v2).splitlines():
            parts = line.split()
            if len(parts) != 2 or parts[0] != "usage_usec":
                continue
            if parts[1].isdigit():
                return int(parts[1]) / 1_000_000.0
        return None

    v1 = Path("/sys/fs/cgroup/cpuacct/cpuacct.usage")
    if v1.exists():
        raw = _safe_read_text(v1).strip()
        if raw.isdigit():
            return int(raw) / 1_000_000_000.0
    return None


def _format_rate(delta_bytes: float, delta_seconds: float):
    if delta_bytes is None or delta_seconds is None or delta_seconds <= 0:
        return "n/a"
    return f"{_format_bytes(int(max(0, delta_bytes / delta_seconds)))}/s"


def _format_observability_stat_value(value: float, value_type: str):
    if not isinstance(value, (int, float)):
        return "n/a"
    if value_type == "percent":
        return f"{float(value):.2f}%"
    if value_type == "bytes":
        return _format_bytes(int(max(0, value)))
    if value_type == "bytes_per_sec":
        return f"{_format_bytes(int(max(0, value)))}/s"
    return f"{float(value):.2f}"


def _build_observability_history_summary(history_items: list[dict], current_snapshot: dict):
    specs = [
        ("Process CPU (delta)", "process_cpu_percent", "percent"),
        ("Container memory usage", "memory_percent", "percent"),
        ("Process RSS", "rss_bytes", "bytes"),
        ("I/O read rate", "io_read_rate_bps", "bytes_per_sec"),
        ("I/O write rate", "io_write_rate_bps", "bytes_per_sec"),
        ("Network RX rate", "net_rx_rate_bps", "bytes_per_sec"),
        ("Network TX rate", "net_tx_rate_bps", "bytes_per_sec"),
    ]

    rows = []
    for label, key, value_type in specs:
        numeric_values = []
        for item in history_items:
            value = item.get(key)
            if isinstance(value, (int, float)):
                numeric_values.append(float(value))
        if not numeric_values:
            rows.append(
                {
                    "label": label,
                    "current": _format_observability_stat_value(
                        current_snapshot.get(key),
                        value_type,
                    ),
                    "min": "n/a",
                    "avg": "n/a",
                    "max": "n/a",
                }
            )
            continue
        rows.append(
            {
                "label": label,
                "current": _format_observability_stat_value(
                    current_snapshot.get(key),
                    value_type,
                ),
                "min": _format_observability_stat_value(min(numeric_values), value_type),
                "avg": _format_observability_stat_value(sum(numeric_values) / len(numeric_values), value_type),
                "max": _format_observability_stat_value(max(numeric_values), value_type),
            }
        )
    return rows


def _collect_observability_snapshot(state: dict, started_monotonic: float):
    now_mono = time.monotonic()
    process_cpu_total = time.process_time()
    io_bytes = _read_process_io_bytes()
    net_bytes = _read_network_bytes()
    cgroup_mem = _read_cgroup_memory_usage()
    cgroup_cpu_total = _read_cgroup_cpu_seconds_total()

    prev_wall = state.get("wall")
    prev_proc_cpu = state.get("process_cpu_total")
    prev_io = state.get("io") or {}
    prev_net = state.get("net") or {}

    delta_wall = (now_mono - prev_wall) if isinstance(prev_wall, float) else None

    process_cpu_percent = None
    if delta_wall is not None and delta_wall > 0 and isinstance(prev_proc_cpu, float):
        process_cpu_percent = max(0.0, ((process_cpu_total - prev_proc_cpu) / delta_wall) * 100.0)

    io_read_rate = None
    io_write_rate = None
    if delta_wall is not None and delta_wall > 0:
        if isinstance(io_bytes.get("read_bytes"), int) and isinstance(prev_io.get("read_bytes"), int):
            io_read_rate = io_bytes["read_bytes"] - prev_io["read_bytes"]
        if isinstance(io_bytes.get("write_bytes"), int) and isinstance(prev_io.get("write_bytes"), int):
            io_write_rate = io_bytes["write_bytes"] - prev_io["write_bytes"]

    net_rx_rate = None
    net_tx_rate = None
    if delta_wall is not None and delta_wall > 0:
        if isinstance(net_bytes.get("rx_bytes"), int) and isinstance(prev_net.get("rx_bytes"), int):
            net_rx_rate = net_bytes["rx_bytes"] - prev_net["rx_bytes"]
        if isinstance(net_bytes.get("tx_bytes"), int) and isinstance(prev_net.get("tx_bytes"), int):
            net_tx_rate = net_bytes["tx_bytes"] - prev_net["tx_bytes"]

    io_read_rate_bps = None
    io_write_rate_bps = None
    net_rx_rate_bps = None
    net_tx_rate_bps = None
    if delta_wall is not None and delta_wall > 0:
        if isinstance(io_read_rate, (int, float)):
            io_read_rate_bps = max(0.0, float(io_read_rate) / delta_wall)
        if isinstance(io_write_rate, (int, float)):
            io_write_rate_bps = max(0.0, float(io_write_rate) / delta_wall)
        if isinstance(net_rx_rate, (int, float)):
            net_rx_rate_bps = max(0.0, float(net_rx_rate) / delta_wall)
        if isinstance(net_tx_rate, (int, float)):
            net_tx_rate_bps = max(0.0, float(net_tx_rate) / delta_wall)

    state["wall"] = now_mono
    state["process_cpu_total"] = process_cpu_total
    state["io"] = io_bytes
    state["net"] = net_bytes

    memory_usage = cgroup_mem.get("usage_bytes")
    memory_limit = cgroup_mem.get("limit_bytes")
    memory_pct = None
    if isinstance(memory_usage, int) and isinstance(memory_limit, int) and memory_limit > 0:
        memory_pct = (memory_usage / memory_limit) * 100.0

    return {
        "process_cpu_percent": process_cpu_percent,
        "process_cpu_total": process_cpu_total,
        "rss_bytes": _read_rss_bytes(),
        "memory_usage_bytes": memory_usage,
        "memory_limit_bytes": memory_limit,
        "memory_percent": memory_pct,
        "io_read_bytes": io_bytes.get("read_bytes"),
        "io_write_bytes": io_bytes.get("write_bytes"),
        "io_read_rate_bps": io_read_rate_bps,
        "io_write_rate_bps": io_write_rate_bps,
        "io_read_rate": _format_rate(io_read_rate, delta_wall),
        "io_write_rate": _format_rate(io_write_rate, delta_wall),
        "net_rx_bytes": net_bytes.get("rx_bytes"),
        "net_tx_bytes": net_bytes.get("tx_bytes"),
        "net_rx_rate_bps": net_rx_rate_bps,
        "net_tx_rate_bps": net_tx_rate_bps,
        "net_rx_rate": _format_rate(net_rx_rate, delta_wall),
        "net_tx_rate": _format_rate(net_tx_rate, delta_wall),
        "uptime_seconds": max(0.0, now_mono - float(started_monotonic or now_mono)),
        "cgroup_cpu_seconds": cgroup_cpu_total,
        "sample_interval_seconds": delta_wall,
        "sampled_at": datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "sampled_at_epoch": time.time(),
    }


def _parse_iso_datetime(raw_value: str):
    return parse_iso_datetime_utc(raw_value)


def _password_change_required(user: dict) -> bool:
    baseline = (
        _parse_iso_datetime(user.get("password_changed_at"))
        or _parse_iso_datetime(user.get("updated_at"))
        or _parse_iso_datetime(user.get("created_at"))
    )
    if baseline is None:
        return True
    return datetime.now(UTC) >= (baseline + timedelta(days=PASSWORD_MAX_AGE_DAYS))


def _password_age_days(user: dict) -> int:
    baseline = (
        _parse_iso_datetime(user.get("password_changed_at"))
        or _parse_iso_datetime(user.get("updated_at"))
        or _parse_iso_datetime(user.get("created_at"))
    )
    if baseline is None:
        return PASSWORD_MAX_AGE_DAYS
    delta = datetime.now(UTC) - baseline
    return max(0, delta.days)


def _now_iso():
    return _store_now_iso()


def _normalize_guild_group_name(value: str):
    return _store_normalize_guild_group_name(value, clean_profile_text=_clean_profile_text)


def _read_users(users_db_file: Path):
    return _store_read_users(
        users_db_file,
        normalize_role=_normalize_web_user_role,
        clean_profile_text=_clean_profile_text,
        default_display_name=_default_display_name,
    )


def _save_users(users_db_file: Path, users):
    _store_save_users(
        users_db_file,
        users,
        normalize_email=_normalize_email,
        normalize_role=_normalize_web_user_role,
        clean_profile_text=_clean_profile_text,
        default_display_name=_default_display_name,
    )


def _ensure_default_admin(users_db_file: Path, default_email: str, default_password: str, logger):
    _store_ensure_default_admin(
        users_db_file,
        default_email,
        default_password,
        logger,
        read_users_func=_read_users,
        normalize_email=_normalize_email,
        is_valid_email=_is_valid_email,
        password_policy_errors=_password_policy_errors,
        hash_password=_hash_password,
        default_display_name=_default_display_name,
    )


def _read_guild_groups(users_db_file: Path):
    return _store_read_guild_groups(users_db_file, clean_profile_text=_clean_profile_text)


def _save_guild_groups(users_db_file: Path, groups):
    _store_save_guild_groups(users_db_file, groups, clean_profile_text=_clean_profile_text)


def _parse_env_file(env_file: Path):
    if not env_file.exists():
        return {}
    values = {}
    for raw_line in env_file.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in raw_line:
            continue
        key, value = raw_line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if value.startswith('"') and value.endswith('"') and len(value) >= 2:
            value = value[1:-1].replace('\\"', '"')
        values[key] = value
    return values


def _encode_env_value(value: str):
    if value is None:
        return ""
    text = str(value)
    if not text:
        return ""
    if any(char.isspace() for char in text) or "#" in text or '"' in text:
        escaped = text.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'
    return text


def _write_env_file(env_file: Path, values: dict):
    lines = []
    for key in sorted(values.keys()):
        value = values[key]
        if value is None or str(value) == "":
            continue
        lines.append(f"{key}={_encode_env_value(str(value))}")
    env_file.write_text("\n".join(lines) + ("\n" if lines else ""))
    try:
        os.chmod(env_file, 0o600)
    except (PermissionError, OSError):
        pass


def _format_env_write_error(env_file: Path, exc: OSError) -> str:
    errno_value = getattr(exc, "errno", None)
    if isinstance(exc, PermissionError) or errno_value in {1, 13, 30}:
        return (
            f"Could not save settings to {env_file}. That path is read-only or not writable in this container. "
            "Set WEB_ENV_FILE to a writable file such as /app/data/web-settings.env."
        )
    return f"Could not save settings to {env_file}: {exc}"


def _try_write_env_file(env_file: Path, values: dict) -> tuple[bool, str]:
    try:
        env_file.parent.mkdir(parents=True, exist_ok=True)
        _write_env_file(env_file, values)
    except OSError as exc:
        return False, _format_env_write_error(env_file, exc)
    return True, ""


def _env_fallback_file_path(data_dir: str) -> Path:
    return Path(data_dir) / "web-settings.env"


def _filter_fallback_env_values(values: dict) -> tuple[dict, tuple[str, ...]]:
    filtered = {}
    skipped = []
    for key, value in values.items():
        if key in FALLBACK_PROTECTED_ENV_KEYS:
            skipped.append(str(key))
            continue
        filtered[key] = value
    return filtered, tuple(sorted(skipped))


def _load_effective_env_values(primary_env_file: Path, fallback_env_file: Path) -> dict:
    values = _parse_env_file(primary_env_file)
    if fallback_env_file != primary_env_file and fallback_env_file.exists():
        fallback_values, _ = _filter_fallback_env_values(_parse_env_file(fallback_env_file))
        if fallback_values:
            values.update(fallback_values)
    return values


def _try_write_env_file_with_fallback(
    primary_env_file: Path,
    fallback_env_file: Path,
    values: dict,
) -> tuple[bool, str, Path, tuple[str, ...]]:
    saved, save_error = _try_write_env_file(primary_env_file, values)
    if saved:
        return True, "", primary_env_file, ()
    if fallback_env_file == primary_env_file:
        return False, save_error, primary_env_file, ()

    fallback_values, skipped_keys = _filter_fallback_env_values(values)
    fallback_saved, fallback_error = _try_write_env_file(fallback_env_file, fallback_values)
    if fallback_saved:
        return True, "", fallback_env_file, skipped_keys
    return False, fallback_error or save_error, fallback_env_file, ()


def _normalize_url_env_value(value: str):
    text = str(value or "").strip()
    if not text:
        return ""
    if text.startswith(("http://", "https://")):
        return text
    return f"https://{text.lstrip('/')}"


def _normalize_env_updates(updated_values: dict):
    normalized = dict(updated_values)
    for preferred_key, legacy_keys in ENV_KEY_ALIASES.items():
        current_value = str(normalized.get(preferred_key, "") or "").strip()
        if not current_value:
            for legacy_key in legacy_keys:
                legacy_value = str(normalized.get(legacy_key, "") or "").strip()
                if legacy_value:
                    normalized[preferred_key] = legacy_value
                    break
        for legacy_key in legacy_keys:
            normalized.pop(legacy_key, None)
    for key in ("WEB_GITHUB_WIKI_URL", "WEB_PUBLIC_BASE_URL", "FIRMWARE_FEED_URL"):
        raw = normalized.get(key, "")
        if raw:
            normalized[key] = _normalize_url_env_value(raw)
    return normalized


def _chmod_if_possible(path: Path, mode: int):
    try:
        os.chmod(path, mode)
        return True
    except (PermissionError, OSError):
        return False


def _get_web_ssl_dir(data_dir: str) -> Path:
    configured = str(os.getenv("WEB_SSL_DIR", "")).strip()
    if configured:
        candidate = Path(configured)
        if not candidate.is_absolute():
            candidate = Path(data_dir) / candidate
        return candidate
    return Path(data_dir) / "ssl"


def _resolve_ssl_file_path(ssl_dir: Path, raw_value: str, default_name: str) -> Path:
    cleaned = str(raw_value or "").strip()
    if not cleaned:
        return ssl_dir / default_name
    candidate = Path(cleaned)
    if candidate.is_absolute():
        return candidate
    return ssl_dir / candidate


def _build_self_signed_certificate(cert_path: Path, key_path: Path, common_name: str):
    safe_common_name = (common_name or "localhost").strip() or "localhost"
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = issuer = x509.Name(
        [
            x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "WickedYoda Bot"),
            x509.NameAttribute(NameOID.COMMON_NAME, safe_common_name),
        ]
    )
    san_entries = [x509.DNSName("localhost"), x509.DNSName(safe_common_name)]
    try:
        san_entries.append(x509.IPAddress(ipaddress.ip_address("127.0.0.1")))
    except ValueError:
        pass
    try:
        san_entries.append(x509.IPAddress(ipaddress.ip_address(safe_common_name)))
    except ValueError:
        parsed_host = urlparse(safe_common_name).hostname if "://" in safe_common_name else None
        if parsed_host:
            san_entries.append(x509.DNSName(parsed_host))

    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.now(UTC) - timedelta(minutes=5))
        .not_valid_after(datetime.now(UTC) + timedelta(days=825))
        .add_extension(x509.SubjectAlternativeName(san_entries), critical=False)
        .sign(key, hashes.SHA256())
    )

    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )


def _ensure_https_ssl_context(data_dir: str, harden_file_permissions: bool, logger=None):
    ssl_dir = _get_web_ssl_dir(data_dir)
    ssl_dir.mkdir(parents=True, exist_ok=True)
    cert_path = _resolve_ssl_file_path(ssl_dir, os.getenv("WEB_SSL_CERT_FILE", ""), "tls.crt")
    key_path = _resolve_ssl_file_path(ssl_dir, os.getenv("WEB_SSL_KEY_FILE", ""), "tls.key")
    cert_path.parent.mkdir(parents=True, exist_ok=True)
    key_path.parent.mkdir(parents=True, exist_ok=True)

    cert_exists = cert_path.exists()
    key_exists = key_path.exists()
    generated = False
    if cert_exists != key_exists:
        raise ValueError(f"Both TLS files must exist together. cert={cert_path} key={key_path}")
    if not cert_exists and not key_exists:
        common_name = (
            str(os.getenv("WEB_SSL_COMMON_NAME", "")).strip()
            or urlparse(str(os.getenv("WEB_PUBLIC_BASE_URL", "")).strip()).hostname
            or "localhost"
        )
        _build_self_signed_certificate(cert_path, key_path, common_name)
        generated = True
        if logger:
            logger.warning(
                "Generated default self-signed HTTPS certificate at %s and %s. Replace these files with your own trusted certificate if desired.",
                cert_path,
                key_path,
            )

    if harden_file_permissions:
        _chmod_if_possible(ssl_dir, 0o700)
        _chmod_if_possible(cert_path, 0o600)
        _chmod_if_possible(key_path, 0o600)

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certfile=str(cert_path), keyfile=str(key_path))
    return context, cert_path, key_path, generated


def _read_env_value(file_values: dict, key: str):
    direct_value = file_values.get(key, os.getenv(key, ""))
    if str(direct_value or "").strip():
        return direct_value
    for alias_key in ENV_KEY_ALIASES.get(key, ()):
        alias_value = file_values.get(alias_key, os.getenv(alias_key, ""))
        if str(alias_value or "").strip():
            return alias_value
    return direct_value


def _validate_env_updates(updated_values: dict):
    truthy_values = {"1", "0", "true", "false", "yes", "no", "on", "off"}
    valid_log_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
    errors = []
    for key, value in updated_values.items():
        if value == "":
            continue
        if key in INT_KEYS:
            try:
                int(value)
            except ValueError:
                errors.append(f"{key} must be an integer.")
        if key == "firmware_check_schedule" and value and not croniter.is_valid(value):
            errors.append("firmware_check_schedule must be a valid 5-field cron expression.")
        if key == "REDDIT_FEED_CHECK_SCHEDULE" and value and not croniter.is_valid(value):
            errors.append("REDDIT_FEED_CHECK_SCHEDULE must be a valid 5-field cron expression.")
        if key == "SERVICE_MONITOR_CHECK_SCHEDULE" and value and not croniter.is_valid(value):
            errors.append("SERVICE_MONITOR_CHECK_SCHEDULE must be a valid 5-field cron expression.")
        if key == "UPTIME_STATUS_CHECK_SCHEDULE" and value and not croniter.is_valid(value):
            errors.append("UPTIME_STATUS_CHECK_SCHEDULE must be a valid 5-field cron expression.")
        if key == "firmware_notification_channel" and value and not CHANNEL_ID_PATTERN.fullmatch(value):
            errors.append("firmware_notification_channel must be numeric ID or <#channel> format.")
        if key == "SERVICE_MONITOR_DEFAULT_CHANNEL_ID" and value and not CHANNEL_ID_PATTERN.fullmatch(value):
            errors.append("SERVICE_MONITOR_DEFAULT_CHANNEL_ID must be numeric ID or <#channel> format.")
        if key == "UPTIME_STATUS_NOTIFY_CHANNEL_ID" and value and not CHANNEL_ID_PATTERN.fullmatch(value):
            errors.append("UPTIME_STATUS_NOTIFY_CHANNEL_ID must be numeric ID or <#channel> format.")
        if key == "UPTIME_STATUS_PAGE_URL" and value:
            try:
                _validate_http_url = build_uptime_source_config(page_url=value)
                _ = _validate_http_url
            except ValueError as exc:
                errors.append(str(exc))
        if key == "UPTIME_STATUS_INSTANCE_URL" and value:
            try:
                build_uptime_instance_urls(value)
            except ValueError as exc:
                errors.append(str(exc))
        if key == "SERVICE_MONITOR_TARGETS_JSON" and value:
            try:
                normalize_service_monitor_targets(
                    value,
                    default_timeout_seconds=10,
                    default_channel_id=0,
                )
            except ValueError as exc:
                errors.append(str(exc))
        if key == "WEB_ADMIN_DEFAULT_USERNAME" and value and not _is_valid_email(value):
            errors.append("WEB_ADMIN_DEFAULT_USERNAME must be a valid email.")
        if key == "WEB_ADMIN_DEFAULT_PASSWORD" and value:
            errors.extend(_password_policy_errors(value))
        if key in {"LOG_LEVEL", "CONTAINER_LOG_LEVEL", "DISCORD_LOG_LEVEL"}:
            if value.upper() not in valid_log_levels:
                errors.append(f"{key} must be one of: DEBUG, INFO, WARNING, ERROR, CRITICAL.")
        if key == "WEB_SESSION_COOKIE_SAMESITE":
            normalized = _normalize_session_cookie_samesite(value, default_value="")
            if normalized not in {"Lax", "Strict", "None"}:
                errors.append("WEB_SESSION_COOKIE_SAMESITE must be Lax, Strict, or None.")
        if key == "WEB_RESTART_ENABLED" and value.lower() not in truthy_values:
            errors.append("WEB_RESTART_ENABLED must be true/false (or 1/0, yes/no, on/off).")
        if key == "UPTIME_STATUS_VERIFY_TLS" and value.lower() not in truthy_values:
            errors.append("UPTIME_STATUS_VERIFY_TLS must be true/false (or 1/0, yes/no, on/off).")
        if (
            key
            in {
                "WEB_SESSION_COOKIE_SECURE",
                "WEB_TRUST_PROXY_HEADERS",
                "WEB_ENFORCE_CSRF",
                "WEB_ENFORCE_SAME_ORIGIN_POSTS",
                "WEB_HARDEN_FILE_PERMISSIONS",
                "WEB_HTTPS_ENABLED",
            }
            and value.lower() not in truthy_values
        ):
            errors.append(f"{key} must be true/false (or 1/0, yes/no, on/off).")
        if key == "WEB_SESSION_TIMEOUT_MINUTES":
            parsed = _normalize_session_timeout_minutes(value, default_value=-1)
            if parsed == -1:
                errors.append("WEB_SESSION_TIMEOUT_MINUTES must be one of: 5, 10, 15, 20, 30, 45, 60, 90, 120.")
        if key == "WEB_GITHUB_WIKI_URL" and value and not value.startswith(("http://", "https://")):
            errors.append("WEB_GITHUB_WIKI_URL must start with http:// or https://.")
        if key == "WEB_PUBLIC_BASE_URL" and value and not value.startswith(("http://", "https://")):
            errors.append("WEB_PUBLIC_BASE_URL must start with http:// or https://.")
        if key == "FIRMWARE_FEED_URL" and value and not value.startswith(("http://", "https://")):
            errors.append("FIRMWARE_FEED_URL must start with http:// or https://.")
    return errors


def _get_int_env(name: str, default: int, minimum: int = 0):
    raw = os.getenv(name, str(default))
    try:
        parsed = int(str(raw).strip())
    except (TypeError, ValueError):
        return default
    if parsed < minimum:
        return default
    return parsed


def _is_truthy_env_value(value: str):
    return str(value or "").strip().lower() not in {"0", "false", "no", "off"}


def _normalize_select_value(value: str):
    selected = str(value or "").strip()
    if selected.startswith("<#") and selected.endswith(">"):
        selected = selected[2:-1]
    if selected.startswith("<@&") and selected.endswith(">"):
        selected = selected[3:-1]
    return selected


def _render_select_input(name: str, selected_value: str, options: list[dict], placeholder: str = "Select..."):
    selected = _normalize_select_value(selected_value)
    rows = [f"<option value=''>{escape(placeholder)}</option>"]
    seen = set()
    for option in options:
        option_id = str(option.get("id", "")).strip()
        if not option_id:
            continue
        seen.add(option_id)
        label = str(option.get("label") or option.get("name") or option_id)
        selected_attr = " selected" if option_id == selected else ""
        rows.append(f"<option value='{escape(option_id, quote=True)}'{selected_attr}>{escape(label)} ({escape(option_id)})</option>")
    if selected and selected not in seen:
        rows.append(f"<option value='{escape(selected, quote=True)}' selected>Current value (not found): {escape(selected)}</option>")
    return f"<select name='{escape(name, quote=True)}'>" + "".join(rows) + "</select>"


def _render_fixed_select_input(name: str, selected_value: str, options: list[dict], placeholder: str = "Select..."):
    selected = str(selected_value or "").strip()
    rows = [f"<option value=''>{escape(placeholder)}</option>"]
    seen = set()
    for option in options:
        option_value = str(option.get("value", "")).strip()
        if not option_value:
            continue
        seen.add(option_value)
        label = str(option.get("label") or option_value)
        selected_attr = " selected" if option_value == selected else ""
        rows.append(f"<option value='{escape(option_value, quote=True)}'{selected_attr}>{escape(label)}</option>")
    if selected and selected not in seen:
        rows.append(f"<option value='{escape(selected, quote=True)}' selected>Current value: {escape(selected)}</option>")
    return f"<select name='{escape(name, quote=True)}'>" + "".join(rows) + "</select>"


def _extract_hostname_from_value(value: str):
    text = str(value or "").strip()
    if not text:
        return ""
    parsed = urlparse(text if "://" in text else f"//{text}")
    return str(parsed.hostname or "").strip().lower()


def _is_private_or_local_ip(raw_value: str):
    ip_value = ipaddress.ip_address(str(raw_value or "").strip())
    return (
        ip_value.is_loopback
        or ip_value.is_private
        or ip_value.is_link_local
        or ip_value.is_multicast
        or ip_value.is_reserved
        or ip_value.is_unspecified
    )


def _validate_safe_outbound_url(url: str, *, field_name: str = "URL"):
    normalized = str(url or "").strip()
    parsed = urlparse(normalized)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError(f"{field_name} must start with http:// or https://.")

    hostname = _extract_hostname_from_value(normalized)
    if not hostname:
        raise ValueError(f"{field_name} is missing a hostname.")
    if hostname == "localhost" or hostname.endswith(".local") or "." not in hostname:
        raise ValueError(f"{field_name} must not target localhost or a private host.")

    try:
        if _is_private_or_local_ip(hostname):
            raise ValueError(f"{field_name} must not resolve to a private or local address.")
        return normalized
    except ValueError as err:
        try:
            address_info = socket.getaddrinfo(hostname, parsed.port or None, type=socket.SOCK_STREAM)
        except socket.gaierror as exc:
            raise ValueError(f"{field_name} hostname could not be resolved.") from exc

        resolved_ips = set()
        for entry in address_info:
            sockaddr = entry[4]
            if not sockaddr:
                continue
            ip_text = str(sockaddr[0] or "").strip()
            if not ip_text:
                continue
            resolved_ips.add(ip_text)
        if not resolved_ips:
            raise ValueError(f"{field_name} hostname could not be resolved.") from err
        if any(_is_private_or_local_ip(ip_text) for ip_text in resolved_ips):
            raise ValueError(f"{field_name} must not resolve to a private or local address.") from err
        return normalized


def _resolve_public_ip_for_hostname(hostname: str, port: int | None):
    try:
        address_info = socket.getaddrinfo(hostname, port, type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        raise ValueError("Outbound URL hostname could not be resolved.") from exc

    public_ip = ""
    for entry in address_info:
        sockaddr = entry[4]
        if not sockaddr:
            continue
        ip_text = str(sockaddr[0] or "").strip()
        if not ip_text:
            continue
        if _is_private_or_local_ip(ip_text):
            raise ValueError("Outbound URL must not resolve to a private or local address.")
        if not public_ip:
            public_ip = ip_text
    if not public_ip:
        raise ValueError("Outbound URL hostname could not be resolved.")
    return public_ip


class _FixedHostHTTPConnection(http.client.HTTPConnection):
    def __init__(self, connect_host: str, request_host: str, **kwargs):
        self._connect_host = connect_host
        super().__init__(request_host, **kwargs)

    def connect(self):
        self.sock = self._create_connection(
            (self._connect_host, self.port),
            self.timeout,
            self.source_address,
        )
        if self._tunnel_host:
            self._tunnel()


class _FixedHostHTTPSConnection(http.client.HTTPSConnection):
    def __init__(self, connect_host: str, request_host: str, **kwargs):
        self._connect_host = connect_host
        self._request_host = request_host
        super().__init__(request_host, **kwargs)

    def connect(self):
        sock = self._create_connection(
            (self._connect_host, self.port),
            self.timeout,
            self.source_address,
        )
        if self._tunnel_host:
            self.sock = sock
            self._tunnel()
            sock = self.sock
        self.sock = self._context.wrap_socket(sock, server_hostname=self._request_host)


class _SafeOutboundResponse:
    def __init__(self, *, url: str, status_code: int, reason: str, headers: dict[str, str], body: bytes):
        self.url = url
        self.status_code = int(status_code)
        self.reason = str(reason or "")
        self.headers = headers
        self._body = body

    @property
    def text(self):
        return self._body.decode("utf-8", errors="replace")

    def json(self):
        return json.loads(self.text)

    @property
    def is_redirect(self):
        return self.status_code in {301, 302, 303, 307, 308}

    @property
    def is_permanent_redirect(self):
        return self.status_code in {301, 308}

    def raise_for_status(self):
        if 400 <= self.status_code:
            raise requests.HTTPError(
                f"{self.status_code} {self.reason}".strip(),
                response=self,
            )


def _perform_safe_outbound_get(url: str, *, headers: dict[str, str], timeout: int, verify_tls: bool = True):
    parsed = urlparse(url)
    hostname = str(parsed.hostname or "").strip()
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    if not hostname:
        raise ValueError("Outbound URL is missing a hostname.")

    connect_ip = _resolve_public_ip_for_hostname(hostname, port)
    request_target = parsed.path or "/"
    if parsed.query:
        request_target += f"?{parsed.query}"

    request_headers = dict(headers)
    request_headers["Host"] = parsed.netloc

    connection_kwargs = {"timeout": timeout}
    if parsed.scheme == "https":
        # This branch preserves the existing admin-controlled "verify TLS" toggle.
        ssl_context = ssl.create_default_context() if verify_tls else ssl._create_unverified_context()  # nosec B323
        connection = _FixedHostHTTPSConnection(
            connect_ip,
            hostname,
            port=port,
            context=ssl_context,
            **connection_kwargs,
        )
    else:
        connection = _FixedHostHTTPConnection(
            connect_ip,
            hostname,
            port=port,
            **connection_kwargs,
        )

    try:
        connection.request("GET", request_target, headers=request_headers)
        raw_response = connection.getresponse()
        body = raw_response.read()
    except (OSError, ssl.SSLError, http.client.HTTPException) as exc:
        raise requests.RequestException(f"Outbound request failed: {exc}") from exc
    finally:
        connection.close()

    return _SafeOutboundResponse(
        url=url,
        status_code=raw_response.status,
        reason=raw_response.reason,
        headers={str(key): str(value) for key, value in raw_response.headers.items()},
        body=body,
    )


def _safe_outbound_get(url: str, *, headers: dict[str, str], timeout: int, verify_tls: bool = True):
    current_url = _validate_safe_outbound_url(url, field_name="Outbound URL")
    for _redirect_count in range(SAFE_OUTBOUND_MAX_REDIRECTS + 1):
        response = _perform_safe_outbound_get(
            current_url,
            timeout=timeout,
            verify_tls=verify_tls,
            headers=headers,
        )
        if response.is_redirect or response.is_permanent_redirect:
            location = str(response.headers.get("Location") or "").strip()
            if not location:
                return response
            current_url = _validate_safe_outbound_url(
                urljoin(current_url, location),
                field_name="Redirect URL",
            )
            continue
        return response
    raise ValueError("Outbound request exceeded the maximum redirect limit.")


def _render_multi_select_input(name: str, selected_values, options: list[dict], size: int = 8):
    selected_set = set()
    if isinstance(selected_values, str):
        selected_values = [selected_values]
    if not isinstance(selected_values, list):
        selected_values = []
    for value in selected_values:
        normalized = _normalize_select_value(str(value))
        if normalized:
            selected_set.add(normalized)

    rows = []
    seen = set()
    for option in options:
        option_id = str(option.get("id", "")).strip()
        if not option_id:
            continue
        seen.add(option_id)
        label = str(option.get("label") or option.get("name") or option_id)
        selected_attr = " selected" if option_id in selected_set else ""
        rows.append(f"<option value='{escape(option_id, quote=True)}'{selected_attr}>{escape(label)} ({escape(option_id)})</option>")

    for missing_value in sorted(selected_set - seen):
        rows.append(
            f"<option value='{escape(missing_value, quote=True)}' selected>Current value (not found): {escape(missing_value)}</option>"
        )

    return f"<select name='{escape(name, quote=True)}' multiple size='{max(4, int(size))}'>" + "".join(rows) + "</select>"


def _dashboard_command_access_label(command_entry: dict):
    if not isinstance(command_entry, dict):
        return "Unknown"
    mode = str(command_entry.get("mode") or "default").strip().lower()
    default_policy = str(command_entry.get("default_policy") or "").strip().lower()
    role_ids = command_entry.get("role_ids", []) or []

    if mode == "disabled":
        return "Disabled"
    if mode == "public":
        return "Public"
    if mode == "custom_roles":
        return f"Custom Roles ({len(role_ids)})" if role_ids else "Custom Roles"
    if default_policy == "moderator_role_ids":
        return "Mod Only"
    if default_policy == "allowed_role_names":
        return "Named Roles"
    return "Public"


def _dashboard_command_enabled_label(command_entry: dict):
    if not isinstance(command_entry, dict):
        return "Unknown"
    mode = str(command_entry.get("mode") or "default").strip().lower()
    return "Disabled" if mode == "disabled" else "Enabled"


def _inject_csrf_token_inputs(body_html: str, csrf_token: str) -> str:
    token = str(csrf_token or "").strip()
    if not token:
        return body_html
    hidden_input = f"<input type='hidden' name='csrf_token' value='{escape(token, quote=True)}' />"
    return POST_FORM_TAG_PATTERN.sub(
        lambda match: match.group(1) + hidden_input,
        str(body_html or ""),
    )


def _render_layout(
    title: str,
    body_html: str,
    current_email: str,
    current_display_name: str,
    csrf_token: str,
    is_admin: bool,
    current_role_label: str = "Read-only",
    current_role: str = "read_only",
    current_guild_name: str = "",
    github_wiki_url: str = "",
    restart_enabled: bool = False,
):
    theme_values = [str(option.get("value") or "").strip() for option in THEME_OPTIONS if str(option.get("value") or "").strip()]
    return render_template_string(
        """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <meta name="csrf-token" content="{{ csrf_token }}" />
  <title>{{ title }} | {{ title_suffix }}</title>
  <link rel="icon" type="image/png" href="{{ url_for('favicon') }}" />
  <link rel="apple-touch-icon" href="{{ url_for('favicon') }}" />
  <style>
    * { box-sizing: border-box; }
    html { -webkit-text-size-adjust: 100%; }
    :root {
      --bg: #0a0a0a;
      --bg-grad-a: #101010;
      --bg-grad-b: #141923;
      --fg: #e7edf7;
      --muted: #94a3b8;
      --card: #12161d;
      --border: #243047;
      --header: #06070a;
      --link: #7cc4ff;
      --btn-bg: #2563eb;
      --btn-secondary: #374151;
      --btn-danger: #dc2626;
      --flash-err-bg: #3b1318;
      --flash-err-fg: #fecaca;
      --flash-ok-bg: #102c1c;
      --flash-ok-fg: #bbf7d0;
      --input-bg: #0f141d;
      --input-fg: #e7edf7;
    }
    body[data-theme="light"] {
      --bg: #eef3fb;
      --bg-grad-a: #eef3fb;
      --bg-grad-b: #f8fbff;
      --fg: #1e293b;
      --muted: #64748b;
      --card: #ffffff;
      --border: #d6dee9;
      --header: #ffffff;
      --link: #1d4ed8;
      --btn-bg: #2563eb;
      --btn-secondary: #475569;
      --btn-danger: #dc2626;
      --flash-err-bg: #fee2e2;
      --flash-err-fg: #991b1b;
      --flash-ok-bg: #dcfce7;
      --flash-ok-fg: #166534;
      --input-bg: #ffffff;
      --input-fg: #1e293b;
    }
    body[data-theme="forest"] {
      --bg: #0b1511;
      --bg-grad-a: #102018;
      --bg-grad-b: #183329;
      --fg: #ecfdf5;
      --muted: #9ec7b2;
      --card: #11211a;
      --border: #24503d;
      --header: #09110d;
      --link: #86efac;
      --btn-bg: #15803d;
      --btn-secondary: #365346;
      --btn-danger: #b91c1c;
      --flash-err-bg: #3f1717;
      --flash-err-fg: #fecaca;
      --flash-ok-bg: #103522;
      --flash-ok-fg: #bbf7d0;
      --input-bg: #0f1a15;
      --input-fg: #ecfdf5;
    }
    body[data-theme="ember"] {
      --bg: #1a1010;
      --bg-grad-a: #241414;
      --bg-grad-b: #48221a;
      --fg: #fff4ec;
      --muted: #e4b8a0;
      --card: #241616;
      --border: #5c342b;
      --header: #140b0b;
      --link: #fdba74;
      --btn-bg: #ea580c;
      --btn-secondary: #6b463f;
      --btn-danger: #dc2626;
      --flash-err-bg: #491b1b;
      --flash-err-fg: #fecaca;
      --flash-ok-bg: #3a2411;
      --flash-ok-fg: #fde68a;
      --input-bg: #1d1111;
      --input-fg: #fff4ec;
    }
    body[data-theme="ice"] {
      --bg: #eef6fb;
      --bg-grad-a: #eef6fb;
      --bg-grad-b: #dbeafe;
      --fg: #102132;
      --muted: #4b6b84;
      --card: #f9fcff;
      --border: #bfd5e8;
      --header: #e9f4fb;
      --link: #0369a1;
      --btn-bg: #0284c7;
      --btn-secondary: #5b7a90;
      --btn-danger: #dc2626;
      --flash-err-bg: #fee2e2;
      --flash-err-fg: #991b1b;
      --flash-ok-bg: #d1fae5;
      --flash-ok-fg: #065f46;
      --input-bg: #ffffff;
      --input-fg: #102132;
    }
    body {
      font-family: "Trebuchet MS", "Lucida Sans", "Segoe UI", sans-serif;
      margin: 0;
      color: var(--fg);
      background:
        radial-gradient(1100px 450px at 20% -20%, var(--bg-grad-b), transparent 55%),
        radial-gradient(900px 360px at 100% 0%, #10213d, transparent 50%),
        var(--bg);
    }
    a { color: var(--link); }
    header {
      background: var(--header);
      border-bottom: 1px solid var(--border);
      color: var(--fg);
      padding: 12px 18px;
      display: flex;
      flex-direction: column;
      align-items: stretch;
      gap: 14px;
      position: sticky;
      top: 0;
      z-index: 10;
    }
    .header-toprow { display: flex; align-items: center; justify-content: space-between; gap: 14px; }
    .header-brand { min-width: 170px; }
    .header-brand strong { display: block; }
    .header-version {
      display: inline-block;
      margin-top: 4px;
      font-size: 0.82rem;
      color: var(--muted);
      letter-spacing: 0.02em;
    }
    .header-tools { display: flex; align-items: center; gap: 12px; margin-left: auto; }
    .header-right { display: flex; align-items: center; gap: 14px; flex-wrap: wrap; justify-content: center; }
    .desktop-nav { display: flex; }
    .mobile-quickbar { display: none; }
    .nav-controls { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; justify-content: center; }
    .nav-controls a { text-decoration: none; }
    .current-user { color: var(--muted); font-size: 0.95rem; }
    .current-user-email { color: var(--muted); font-size: 0.85rem; }
    .header-chip {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      min-height: 36px;
      padding: 7px 12px;
      border: 1px solid var(--border);
      border-radius: 999px;
      background: rgba(255, 255, 255, 0.03);
      color: var(--fg);
      font-size: 0.88rem;
      line-height: 1.2;
    }
    .header-chip strong {
      font-size: 0.78rem;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      color: var(--muted);
    }
    .wrap { max-width: 1200px; margin: 22px auto; padding: 0 16px; }
    .card { background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 18px; margin-bottom: 16px; }
    .flash { padding: 10px 12px; border-radius: 8px; margin-bottom: 10px; border: 1px solid var(--border); }
    .flash.error { background: var(--flash-err-bg); color: var(--flash-err-fg); }
    .flash.success { background: var(--flash-ok-bg); color: var(--flash-ok-fg); }
    table { width: 100%; border-collapse: collapse; }
    th, td { border-bottom: 1px solid var(--border); padding: 10px; text-align: left; vertical-align: top; }
    input[type=text], input[type=email], input[type=password], textarea, select {
      width: 100%;
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 8px;
      min-height: 44px;
      font-size: 16px;
      background: var(--input-bg);
      color: var(--input-fg);
    }
    textarea { min-height: 220px; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
    .btn {
      background: var(--btn-bg);
      border: 0;
      color: #fff;
      padding: 9px 14px;
      border-radius: 8px;
      cursor: pointer;
      text-decoration: none;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 44px;
    }
    .btn.secondary { background: var(--btn-secondary); }
    .btn.danger { background: var(--btn-danger); }
    .inline-form { display: inline-flex; margin-left: 0; }
    .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
    .muted { color: var(--muted); font-size: 0.9rem; }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
    .theme-switch { display: inline-flex; border: 1px solid var(--border); border-radius: 10px; overflow: hidden; }
    .theme-btn {
      border: 0;
      background: transparent;
      color: var(--fg);
      padding: 7px 11px;
      cursor: pointer;
      font-weight: 700;
      letter-spacing: 0.02em;
    }
    .theme-btn.active { background: var(--btn-bg); color: #fff; }
    .nav-select {
      width: 280px;
      max-width: 70vw;
      min-width: 190px;
      padding: 7px 9px;
    }
    .mobile-nav { display: none; position: relative; }
    .mobile-nav summary {
      list-style: none;
      cursor: pointer;
      user-select: none;
      min-height: 44px;
      padding: 10px 14px;
      border-radius: 10px;
      background: var(--btn-bg);
      color: #fff;
      font-weight: 700;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 8px;
      border: 0;
    }
    .mobile-nav summary::-webkit-details-marker { display: none; }
    .mobile-nav-panel {
      margin-top: 10px;
      padding: 14px;
      border: 1px solid var(--border);
      border-radius: 12px;
      background: var(--card);
      box-shadow: 0 16px 40px rgba(0, 0, 0, 0.2);
      display: grid;
      gap: 12px;
    }
    .mobile-user-block {
      display: grid;
      gap: 4px;
      padding-bottom: 8px;
      border-bottom: 1px solid var(--border);
    }
    .mobile-link-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
    }
    .mobile-panel-section { display: grid; gap: 8px; }
    .mobile-panel-title {
      margin: 0;
      color: var(--muted);
      font-size: 0.76rem;
      font-weight: 700;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }
    .mobile-link-grid .btn,
    .mobile-nav-panel .btn,
    .mobile-nav-panel .inline-form,
    .mobile-nav-panel .inline-form .btn,
    .mobile-nav-panel .nav-select {
      width: 100%;
    }
    .sr-only {
      position: absolute;
      width: 1px;
      height: 1px;
      padding: 0;
      margin: -1px;
      overflow: hidden;
      clip: rect(0, 0, 0, 0);
      white-space: nowrap;
      border: 0;
    }
    .dash-grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 14px; }
    .dashboard-shell { display: grid; gap: 18px; }
    .dashboard-hero {
      display: grid;
      grid-template-columns: minmax(0, 1.7fr) minmax(280px, 1fr);
      gap: 16px;
      align-items: stretch;
    }
    .dashboard-hero-main,
    .dashboard-hero-side,
    .dashboard-section,
    .dash-card {
      position: relative;
      overflow: hidden;
    }
    .dashboard-hero-main::before,
    .dashboard-hero-side::before,
    .dash-card::before {
      content: "";
      position: absolute;
      inset: 0 auto auto 0;
      width: 100%;
      height: 3px;
      background: linear-gradient(90deg, var(--btn-bg), transparent 78%);
      opacity: 0.75;
      pointer-events: none;
    }
    .dashboard-hero-main h2,
    .dashboard-hero-side h3,
    .dashboard-section-head h3,
    .dash-card h3 {
      margin-top: 0;
    }
    .dashboard-hero-main p,
    .dashboard-hero-side p,
    .dash-card p {
      margin-top: 0;
    }
    .dashboard-hero-main {
      display: grid;
      gap: 14px;
      align-content: start;
      padding-top: 20px;
    }
    .dashboard-hero-lead {
      font-size: 1.02rem;
      line-height: 1.55;
      max-width: 58ch;
    }
    .dashboard-pill-row {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
    }
    .dashboard-pill {
      display: grid;
      gap: 2px;
      min-width: 132px;
      padding: 11px 13px;
      border-radius: 12px;
      border: 1px solid var(--border);
      background: rgba(255, 255, 255, 0.03);
    }
    .dashboard-pill strong {
      font-size: 0.74rem;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      color: var(--muted);
    }
    .dashboard-pill span {
      font-size: 0.96rem;
      font-weight: 700;
      line-height: 1.3;
    }
    .dashboard-hero-side {
      display: grid;
      gap: 14px;
      align-content: start;
      padding-top: 20px;
    }
    .dashboard-list {
      display: grid;
      gap: 10px;
    }
    .dashboard-list-item {
      display: grid;
      gap: 4px;
      padding: 11px 12px;
      border-radius: 12px;
      border: 1px solid var(--border);
      background: rgba(255, 255, 255, 0.03);
    }
    .dashboard-list-item strong {
      font-size: 0.9rem;
    }
    .dashboard-section {
      display: grid;
      gap: 12px;
      padding-top: 18px;
    }
    .dashboard-section-head {
      display: flex;
      align-items: end;
      justify-content: space-between;
      gap: 12px;
    }
    .dashboard-section-head p {
      margin: 0;
      max-width: 70ch;
    }
    .dashboard-section-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
      gap: 14px;
    }
    .dash-card {
      display: grid;
      gap: 12px;
      align-content: start;
      min-height: 100%;
      padding-top: 20px;
    }
    .dash-card h3 { margin-bottom: 2px; }
    .dash-card p { min-height: 0; line-height: 1.5; }
    .dash-card.primary {
      background: linear-gradient(180deg, rgba(255, 255, 255, 0.03), transparent 48%), var(--card);
    }
    .dash-actions { display: flex; gap: 10px; flex-wrap: wrap; margin-top: auto; }
    .dash-actions .btn { min-width: 0; }
    .dashboard-note {
      margin: 0;
      color: var(--muted);
      font-size: 0.88rem;
      line-height: 1.45;
    }
    .metric-card h3 { margin: 0 0 14px; }
    .metric-card .table-scroll { border-radius: 10px; }
    .metric-card .table-scroll > table {
      min-width: 0;
      width: 100%;
      table-layout: fixed;
    }
    .metric-table td {
      vertical-align: middle;
      line-height: 1.35;
    }
    .metric-table td:first-child {
      width: 58%;
      font-weight: 600;
    }
    .metric-table td:last-child {
      width: 42%;
      text-align: right;
      white-space: nowrap;
      font-variant-numeric: tabular-nums;
    }
    .metric-table tr:last-child td { border-bottom: 0; }
    .history-table th:not(:first-child),
    .history-table td:not(:first-child) { text-align: right; }
    .history-table td:first-child { width: 44%; }
    .table-scroll {
      width: 100%;
      overflow-x: auto;
      -webkit-overflow-scrolling: touch;
      border: 1px solid var(--border);
      border-radius: 10px;
    }
    .table-scroll > table {
      min-width: 760px;
      margin: 0;
    }
    @media (max-width: 1180px) {
      .dashboard-hero { grid-template-columns: 1fr; }
    }
    @media (max-width: 1080px) { .dash-grid { grid-template-columns: 1fr 1fr; } }
    @media (max-width: 900px) {
      .grid { grid-template-columns: 1fr; }
      .dash-grid { grid-template-columns: 1fr; }
      .dashboard-section-head { align-items: start; flex-direction: column; }
      .dashboard-section-grid { grid-template-columns: 1fr 1fr; }
      header { padding: 10px 12px; align-items: center; }
      .wrap { margin: 14px auto; padding: 0 10px; }
      .card { padding: 14px; }
      .header-toprow { width: 100%; align-items: flex-start; }
      .header-tools { margin-left: 0; width: auto; flex-shrink: 0; }
      .header-right.desktop-nav { display: none; }
      .mobile-quickbar { display: grid; grid-template-columns: 1fr; gap: 10px; width: 100%; }
      .mobile-nav { display: block; width: 100%; }
      .nav-select { width: 100%; max-width: 100%; min-width: 0; }
      .theme-switch { width: 100%; }
      .header-tools .theme-switch { display: none; }
      .theme-btn { flex: 1; min-height: 42px; }
      .current-user-email { display: block; }
      .dash-actions .btn { width: 100%; }
      .dashboard-pill { min-width: 0; flex: 1 1 180px; }
      th, td { padding: 8px; }
      .table-scroll > table { min-width: 680px; }
    }
    @media (max-width: 600px) {
      .card { border-radius: 10px; }
      .table-scroll > table { min-width: 620px; }
      .header-toprow { flex-direction: column; align-items: stretch; }
      .header-tools { width: 100%; flex-direction: column; align-items: stretch; }
      .dashboard-section-grid { grid-template-columns: 1fr; }
      .dashboard-pill-row { display: grid; grid-template-columns: 1fr 1fr; }
      .dashboard-pill { min-width: 0; }
      .mobile-link-grid { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body data-theme="black">
  <header>
    <div class="header-toprow">
      <div class="header-brand">
        <strong>Discord Bot Admin</strong>
        <span class="header-version">{{ web_gui_version }}</span>
      </div>
      <div class="header-tools">
        {% if current_email %}
        <details class="mobile-nav">
          <summary>Menu</summary>
          <div class="mobile-nav-panel">
            <div class="mobile-user-block">
              <span class="current-user">{{ current_display_name or current_email }} ({{ current_role_label }})</span>
              {% if current_display_name and current_display_name != current_email %}
                <span class="current-user-email">{{ current_email }}</span>
              {% endif %}
              {% if current_guild_name %}<span class="current-user">Server: {{ current_guild_name }}</span>{% endif %}
            </div>
            <div class="mobile-panel-section">
              <p class="mobile-panel-title">Quick Jump</p>
              <label class="sr-only" for="mobile-nav-page-select">Open page</label>
              <select id="mobile-nav-page-select" class="nav-select nav-page-select">
                <option value="">Go to page...</option>
                <option value="{{ url_for('guilds_page') }}">Servers</option>
                <option value="{{ url_for('account') }}">My Account</option>
                <option value="{{ url_for('member_activity_page') }}">Member Activity</option>
                {% if current_role in ("glinet_read_only", "glinet_rw") %}
                <option value="{{ url_for('dashboard') }}">Dashboard</option>
                <option value="{{ url_for('bot_profile') }}">Bot Profile</option>
                <option value="{{ url_for('command_status') }}">Command Status</option>
                <option value="{{ url_for('command_permissions') }}">Command Permissions</option>
                <option value="{{ url_for('moderation_page') }}">Moderation</option>
                <option value="{{ url_for('actions_page') }}">Action History</option>
                <option value="{{ url_for('reddit_feeds') }}">Reddit Feeds</option>
                <option value="{{ url_for('service_monitors_page') }}">Service Monitors</option>
                <option value="{{ url_for('youtube_subscriptions') }}">YouTube Subscriptions</option>
                <option value="{{ url_for('linkedin_subscriptions') }}">LinkedIn Profiles</option>
                <option value="{{ url_for('beta_program_subscriptions') }}">GL.iNet Beta Programs</option>
                <option value="{{ url_for('role_access_page') }}">Role Access</option>
                <option value="{{ url_for('guild_settings') }}">Guild Settings</option>
                <option value="{{ url_for('tag_responses') }}">Tag Responses</option>
                <option value="{{ url_for('bulk_role_csv') }}">Bulk Role CSV</option>
                {% else %}
                <option value="{{ url_for('bot_profile') }}">Bot Profile</option>
                <option value="{{ url_for('command_status') }}">Command Status</option>
                <option value="{{ url_for('command_permissions') }}">Command Permissions</option>
                <option value="{{ url_for('moderation_page') }}">Moderation</option>
                <option value="{{ url_for('actions_page') }}">Action History</option>
                <option value="{{ url_for('reddit_feeds') }}">Reddit Feeds</option>
                <option value="{{ url_for('service_monitors_page') }}">Service Monitors</option>
                <option value="{{ url_for('youtube_subscriptions') }}">YouTube Subscriptions</option>
                <option value="{{ url_for('linkedin_subscriptions') }}">LinkedIn Profiles</option>
                <option value="{{ url_for('beta_program_subscriptions') }}">GL.iNet Beta Programs</option>
                <option value="{{ url_for('role_access_page') }}">Role Access</option>
                <option value="{{ url_for('guild_settings') }}">Guild Settings</option>
                <option value="{{ url_for('settings') }}">Global Settings</option>
                <option value="{{ url_for('public_observability') }}">Observability</option>
                <option value="{{ url_for('admin_logs') }}">Logs</option>
                <option value="{{ url_for('documentation') }}">Documentation</option>
                <option value="{{ url_for('documentation') }}">Wiki Viewer</option>
                {% if github_wiki_url %}<option value="{{ github_wiki_url }}" data-external="1">GitHub Wiki</option>{% endif %}
                <option value="{{ url_for('tag_responses') }}">Tag Responses</option>
                <option value="{{ url_for('bulk_role_csv') }}">Bulk Role CSV</option>
                <option value="{{ url_for('users') }}">Users</option>
                {% endif %}
                <option value="{{ url_for('logout') }}">Logout</option>
              </select>
            </div>
            <div class="mobile-panel-section">
              <p class="mobile-panel-title">Primary Actions</p>
              <div class="mobile-link-grid">
                <a class="btn secondary" href="{{ url_for('guilds_page') }}">Servers</a>
                <a class="btn secondary" href="{{ url_for('account') }}">My Account</a>
                <a class="btn secondary" href="{{ url_for('member_activity_page') }}">Member Activity</a>
                {% if current_role in ("glinet_read_only", "glinet_rw") %}
                <a class="btn secondary" href="{{ url_for('dashboard') }}">Dashboard</a>
                <a class="btn secondary" href="{{ url_for('command_status') }}">Command Status</a>
                <a class="btn secondary" href="{{ url_for('command_permissions') }}">Permissions</a>
                <a class="btn secondary" href="{{ url_for('moderation_page') }}">Moderation</a>
                <a class="btn secondary" href="{{ url_for('role_access_page') }}">Role Access</a>
                <a class="btn secondary" href="{{ url_for('guild_settings') }}">Settings</a>
                {% else %}
                <a class="btn secondary" href="{{ url_for('dashboard') }}">Dashboard</a>
                <a class="btn secondary" href="{{ url_for('command_status') }}">Command Status</a>
                <a class="btn secondary" href="{{ url_for('command_permissions') }}">Permissions</a>
                <a class="btn secondary" href="{{ url_for('moderation_page') }}">Moderation</a>
                <a class="btn secondary" href="{{ url_for('role_access_page') }}">Role Access</a>
                <a class="btn secondary" href="{{ url_for('admin_logs') }}">Logs</a>
                {% endif %}
              </div>
            </div>
            <div class="mobile-panel-section">
              <p class="mobile-panel-title">Theme</p>
              <div class="theme-switch" aria-label="Theme selector">
                {% for theme_option in theme_options %}
                <button type="button" class="theme-btn" data-theme-choice="{{ theme_option.value }}">{{ theme_option.label }}</button>
                {% endfor %}
              </div>
            </div>
          </div>
        </details>
        {% endif %}
        <div class="theme-switch" aria-label="Theme selector">
          {% for theme_option in theme_options %}
          <button type="button" class="theme-btn" data-theme-choice="{{ theme_option.value }}">{{ theme_option.label }}</button>
          {% endfor %}
        </div>
      </div>
    </div>
    {% if current_email %}
    <div class="mobile-quickbar">
      <div class="header-chip">
        <strong>Server</strong>
        <span>{{ current_guild_name or "No server selected" }}</span>
      </div>
      <div class="mobile-link-grid">
        <a class="btn secondary" href="{{ url_for('guilds_page') }}">Servers</a>
        <a class="btn secondary" href="{{ url_for('account') }}">My Account</a>
        <a class="btn secondary" href="{{ url_for('member_activity_page') }}">Member Activity</a>
        <a class="btn secondary" href="{{ url_for('logout') }}">Logout</a>
        {% if current_role not in ("glinet_read_only", "glinet_rw") %}
        <a class="btn secondary" href="{{ url_for('dashboard') }}">Dashboard</a>
        {% endif %}
      </div>
    </div>
    {% endif %}
    <div class="header-right desktop-nav">
      {% if current_email %}
        <nav class="nav-controls">
          <span class="current-user">{{ current_display_name or current_email }} ({{ current_role_label }})</span>
          {% if current_display_name and current_display_name != current_email %}
            <span class="current-user-email">({{ current_email }})</span>
          {% endif %}
          {% if current_guild_name %}<span class="current-user">Server: {{ current_guild_name }}</span>{% endif %}
          {% if current_role not in ("glinet_read_only", "glinet_rw") %}
          <a class="btn secondary" href="{{ url_for('guilds_page') }}">Servers</a>
          <a class="btn secondary" href="{{ url_for('dashboard') }}">Dashboard</a>
          {% endif %}
          <a class="btn secondary" href="{{ url_for('logout') }}">Logout</a>
          <label class="sr-only" for="desktop-nav-page-select">Open page</label>
          <select id="desktop-nav-page-select" class="nav-select nav-page-select">
            <option value="">Go to page...</option>
            <option value="{{ url_for('guilds_page') }}">Servers</option>
            <option value="{{ url_for('account') }}">My Account</option>
            <option value="{{ url_for('member_activity_page') }}">Member Activity</option>
            {% if current_role in ("glinet_read_only", "glinet_rw") %}
            <option value="{{ url_for('dashboard') }}">Dashboard</option>
            <option value="{{ url_for('bot_profile') }}">Bot Profile</option>
            <option value="{{ url_for('command_status') }}">Command Status</option>
            <option value="{{ url_for('command_permissions') }}">Command Permissions</option>
            <option value="{{ url_for('moderation_page') }}">Moderation</option>
            <option value="{{ url_for('actions_page') }}">Action History</option>
            <option value="{{ url_for('reddit_feeds') }}">Reddit Feeds</option>
            <option value="{{ url_for('service_monitors_page') }}">Service Monitors</option>
            <option value="{{ url_for('youtube_subscriptions') }}">YouTube Subscriptions</option>
            <option value="{{ url_for('linkedin_subscriptions') }}">LinkedIn Profiles</option>
            <option value="{{ url_for('beta_program_subscriptions') }}">GL.iNet Beta Programs</option>
            <option value="{{ url_for('role_access_page') }}">Role Access</option>
            <option value="{{ url_for('guild_settings') }}">Guild Settings</option>
            <option value="{{ url_for('tag_responses') }}">Tag Responses</option>
            <option value="{{ url_for('bulk_role_csv') }}">Bulk Role CSV</option>
            {% else %}
            <option value="{{ url_for('bot_profile') }}">Bot Profile</option>
            <option value="{{ url_for('command_status') }}">Command Status</option>
            <option value="{{ url_for('command_permissions') }}">Command Permissions</option>
            <option value="{{ url_for('moderation_page') }}">Moderation</option>
            <option value="{{ url_for('actions_page') }}">Action History</option>
            <option value="{{ url_for('reddit_feeds') }}">Reddit Feeds</option>
            <option value="{{ url_for('service_monitors_page') }}">Service Monitors</option>
            <option value="{{ url_for('youtube_subscriptions') }}">YouTube Subscriptions</option>
            <option value="{{ url_for('linkedin_subscriptions') }}">LinkedIn Profiles</option>
            <option value="{{ url_for('beta_program_subscriptions') }}">GL.iNet Beta Programs</option>
            <option value="{{ url_for('role_access_page') }}">Role Access</option>
            <option value="{{ url_for('guild_settings') }}">Guild Settings</option>
            <option value="{{ url_for('settings') }}">Global Settings</option>
            <option value="{{ url_for('public_observability') }}">Observability</option>
            <option value="{{ url_for('admin_logs') }}">Logs</option>
            <option value="{{ url_for('documentation') }}">Documentation</option>
            <option value="{{ url_for('documentation') }}">Wiki Viewer</option>
            {% if github_wiki_url %}<option value="{{ github_wiki_url }}" data-external="1">GitHub Wiki</option>{% endif %}
            <option value="{{ url_for('tag_responses') }}">Tag Responses</option>
            <option value="{{ url_for('bulk_role_csv') }}">Bulk Role CSV</option>
            <option value="{{ url_for('users') }}">Users</option>
            {% endif %}
            <option value="{{ url_for('logout') }}">Logout</option>
          </select>
        </nav>
      {% endif %}
    </div>
  </header>
  <div class="wrap">
    {% with messages = get_flashed_messages(with_categories=true) %}
      {% for category, message in messages %}
        <div class="flash {{ category }}">{{ message }}</div>
      {% endfor %}
    {% endwith %}
    {% if current_email and current_role == "read_only" %}
      <div class="flash">Read-only account: you can view all pages, but configuration and management changes are blocked.</div>
    {% elif current_email and current_role == "glinet_read_only" %}
      <div class="flash">Glinet-Read-Only account: access is pinned to the primary GL.iNet Community Discord server and limited to view-only access there.</div>
    {% elif current_email and current_role == "glinet_rw" %}
      <div class="flash">Glinet-RW account: access is pinned to the primary GL.iNet Community Discord server and limited to guild-scoped changes there.</div>
    {% endif %}
    {{ body_html | safe }}
  </div>
  <script>
    (function () {
      const storageKey = "web_theme_choice";
      const fallbackTheme = "black";
      const allowedChoices = {{ theme_values_json | safe }};
      const allowed = Object.fromEntries(allowedChoices.map((themeName) => [themeName, true]));

      function setTheme(theme) {
        const selected = allowed[theme] ? theme : fallbackTheme;
        document.body.setAttribute("data-theme", selected);
        try {
          window.localStorage.setItem(storageKey, selected);
        } catch (error) {}
        document.querySelectorAll("[data-theme-choice]").forEach((btn) => {
          btn.classList.toggle("active", btn.getAttribute("data-theme-choice") === selected);
        });
      }

      let stored = fallbackTheme;
      try {
        stored = window.localStorage.getItem(storageKey) || fallbackTheme;
      } catch (error) {}
      setTheme(stored);

      document.querySelectorAll("[data-theme-choice]").forEach((btn) => {
        btn.addEventListener("click", function () {
          setTheme(btn.getAttribute("data-theme-choice"));
        });
      });

      document.querySelectorAll(".nav-page-select").forEach((navPageSelect) => {
        navPageSelect.addEventListener("change", function () {
          const option = navPageSelect.options[navPageSelect.selectedIndex];
          const target = option ? option.value : "";
          if (!target) {
            return;
          }
          const external = option.getAttribute("data-external") === "1";
          if (external) {
            window.open(target, "_blank", "noopener,noreferrer");
          } else {
            window.location.href = target;
          }
          navPageSelect.value = "";
        });
      });

      document.querySelectorAll(".wrap table").forEach((table) => {
        const parent = table.parentElement;
        if (!parent || parent.classList.contains("table-scroll")) {
          return;
        }
        const wrapper = document.createElement("div");
        wrapper.className = "table-scroll";
        parent.insertBefore(wrapper, table);
        wrapper.appendChild(table);
      });
    })();
  </script>
</body>
</html>
        """,
        title=title,
        title_suffix=WEB_GUI_TITLE_SUFFIX,
        body_html=body_html,
        current_email=current_email,
        current_display_name=current_display_name,
        csrf_token=csrf_token,
        is_admin=is_admin,
        current_role_label=current_role_label,
        current_role=current_role,
        current_guild_name=current_guild_name,
        github_wiki_url=github_wiki_url,
        restart_enabled=restart_enabled,
        web_gui_version=WEB_GUI_VERSION_LABEL,
        theme_options=THEME_OPTIONS,
        theme_values_json=json.dumps(theme_values),
    )


def create_web_app(
    data_dir: str,
    env_file_path: str,
    tag_responses_file: str,
    default_admin_email: str,
    default_admin_password: str,
    on_get_guilds=None,
    on_get_guild_settings=None,
    on_save_guild_settings=None,
    on_env_settings_saved=None,
    on_get_tag_responses=None,
    on_save_tag_responses=None,
    on_tag_responses_saved=None,
    on_bulk_assign_role_csv=None,
    on_get_discord_catalog=None,
    on_get_command_permissions=None,
    on_save_command_permissions=None,
    on_get_actions=None,
    on_get_member_activity=None,
    on_export_member_activity=None,
    on_get_reddit_feeds=None,
    on_manage_reddit_feeds=None,
    on_get_youtube_subscriptions=None,
    on_manage_youtube_subscriptions=None,
    on_get_linkedin_subscriptions=None,
    on_manage_linkedin_subscriptions=None,
    on_get_beta_program_subscriptions=None,
    on_manage_beta_program_subscriptions=None,
    on_get_role_access_mappings=None,
    on_manage_role_access_mappings=None,
    on_get_bot_profile=None,
    on_update_bot_profile=None,
    on_update_bot_avatar=None,
    on_request_restart=None,
    on_leave_guild=None,
    logger=None,
):
    app = Flask(__name__)
    trust_proxy_headers = _is_truthy_env_value(os.getenv("WEB_TRUST_PROXY_HEADERS", "true"))
    if trust_proxy_headers:
        app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1)

    app.secret_key = os.getenv("WEB_ADMIN_SESSION_SECRET", "") or secrets.token_hex(32)
    max_bulk_upload = _get_int_env("WEB_BULK_ASSIGN_MAX_UPLOAD_BYTES", 2 * 1024 * 1024, minimum=1024)
    max_avatar_upload = _get_int_env("WEB_AVATAR_MAX_UPLOAD_BYTES", 2 * 1024 * 1024, minimum=1024)
    secure_session_cookie = _is_truthy_env_value(os.getenv("WEB_SESSION_COOKIE_SECURE", "true"))
    session_cookie_samesite = _normalize_session_cookie_samesite(
        os.getenv("WEB_SESSION_COOKIE_SAMESITE", "Lax"),
        default_value="Lax",
    )
    enforce_csrf = _is_truthy_env_value(os.getenv("WEB_ENFORCE_CSRF", "true"))
    enforce_same_origin_posts = _is_truthy_env_value(os.getenv("WEB_ENFORCE_SAME_ORIGIN_POSTS", "true"))
    harden_file_permissions = _is_truthy_env_value(os.getenv("WEB_HARDEN_FILE_PERMISSIONS", "true"))
    web_session_timeout_minutes = _normalize_session_timeout_minutes(
        os.getenv("WEB_SESSION_TIMEOUT_MINUTES", str(WEB_INACTIVITY_TIMEOUT_MINUTES)),
        default_value=WEB_INACTIVITY_TIMEOUT_MINUTES,
    )
    session_timeout_state = {"minutes": web_session_timeout_minutes}
    app.config.update(
        SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SAMESITE=session_cookie_samesite,
        SESSION_COOKIE_SECURE=secure_session_cookie,
        SESSION_REFRESH_EACH_REQUEST=True,
        PERMANENT_SESSION_LIFETIME=timedelta(days=REMEMBER_LOGIN_DAYS),
        MAX_CONTENT_LENGTH=max(max_bulk_upload, max_avatar_upload) + (256 * 1024),
    )
    login_window_seconds = 15 * 60
    login_max_attempts = 6
    login_attempts = {}
    recent_login_success = {}
    observability_state = {}
    observability_started_monotonic = time.monotonic()
    observability_lock = threading.Lock()
    observability_history = deque()
    observability_history_retention_seconds = OBSERVABILITY_HISTORY_RETENTION_HOURS * 60 * 60
    observability_history_sample_seconds = OBSERVABILITY_HISTORY_SAMPLE_SECONDS

    def _collect_and_store_observability_snapshot():
        with observability_lock:
            snapshot = _collect_observability_snapshot(
                observability_state,
                observability_started_monotonic,
            )
            now_epoch = float(snapshot.get("sampled_at_epoch") or time.time())
            cutoff_epoch = now_epoch - float(observability_history_retention_seconds)
            observability_history.append(snapshot)
            while observability_history:
                oldest_epoch = float(observability_history[0].get("sampled_at_epoch") or 0.0)
                if oldest_epoch >= cutoff_epoch:
                    break
                observability_history.popleft()
            return snapshot, list(observability_history)

    def _observability_sampler_loop():
        while True:
            try:
                _collect_and_store_observability_snapshot()
            except Exception:
                if logger:
                    logger.exception("Observability background sampler failed.")
            time.sleep(max(1, int(observability_history_sample_seconds)))

    threading.Thread(
        target=_observability_sampler_loop,
        name="web_observability_sampler",
        daemon=True,
    ).start()

    @app.before_request
    def mark_request_start():
        g.request_started_monotonic = time.perf_counter()
        return None

    @app.after_request
    def apply_security_headers(response):
        g.response_status_code = int(getattr(response, "status_code", 0) or 0)
        _remember_navigation_entry()
        request_host = _extract_hostname(str(request.host or ""))
        is_local_request = False
        if request_host:
            if request_host in {"localhost", "127.0.0.1", "::1"}:
                is_local_request = True
            elif request_host.endswith(".local"):
                is_local_request = True
            elif "." not in request_host:
                # Single-label hostnames are commonly local/private network names.
                is_local_request = True
            else:
                try:
                    ip_value = ipaddress.ip_address(request_host)
                    is_local_request = ip_value.is_loopback or ip_value.is_private or ip_value.is_link_local
                except ValueError:
                    is_local_request = False
        forwarded_proto = str(request.headers.get("X-Forwarded-Proto", "")).strip().lower()
        if "," in forwarded_proto:
            forwarded_proto = forwarded_proto.split(",", 1)[0].strip().lower()
        is_effectively_secure = bool(request.is_secure or forwarded_proto == "https")
        allow_coop = bool(is_effectively_secure or is_local_request)
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("Referrer-Policy", "no-referrer")
        response.headers.setdefault("X-Permitted-Cross-Domain-Policies", "none")
        if allow_coop:
            response.headers.setdefault("Cross-Origin-Opener-Policy", "same-origin")
        else:
            response.headers.pop("Cross-Origin-Opener-Policy", None)
        response.headers.setdefault("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
        response.headers.setdefault("Cross-Origin-Resource-Policy", "same-origin")
        response.headers.setdefault("Cache-Control", "no-store")
        response.headers.setdefault("Pragma", "no-cache")
        response.headers.setdefault(
            "Content-Security-Policy",
            "default-src 'self'; img-src 'self' https: data:; style-src 'self' 'unsafe-inline'; "
            "script-src 'self' 'unsafe-inline'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'",
        )
        if is_effectively_secure:
            response.headers.setdefault(
                "Strict-Transport-Security",
                "max-age=31536000; includeSubDomains; preload",
            )

        # If the effective request scheme is HTTP, strip the Secure flag so
        # direct/local access can maintain a session. HTTPS requests retain it.
        if secure_session_cookie and (not is_effectively_secure):
            session_cookie_name = str(app.config.get("SESSION_COOKIE_NAME", "session"))
            set_cookie_headers = response.headers.getlist("Set-Cookie")
            if set_cookie_headers:
                rewritten_headers = []
                stripped_secure = False
                for header_value in set_cookie_headers:
                    rewritten = header_value
                    if rewritten.startswith(f"{session_cookie_name}="):
                        next_value = re.sub(
                            r";\s*Secure(?=;|$)",
                            "",
                            rewritten,
                            flags=re.IGNORECASE,
                        )
                        if next_value != rewritten:
                            stripped_secure = True
                        rewritten = next_value
                    rewritten_headers.append(rewritten)
                if rewritten_headers != set_cookie_headers:
                    response.headers.pop("Set-Cookie", None)
                    for header_value in rewritten_headers:
                        response.headers.add("Set-Cookie", header_value)
                    if stripped_secure and logger:
                        logger.warning(
                            "Session cookie Secure flag removed for non-HTTPS request: host=%s x_forwarded_proto=%s ip=%s",
                            str(request.host or ""),
                            str(request.headers.get("X-Forwarded-Proto", "") or ""),
                            _client_ip(),
                        )
        authenticated = bool(session.get("auth_email"))
        if logger and should_log_web_audit_event(
            endpoint=request.endpoint,
            status_code=int(getattr(response, "status_code", 0) or 0),
            authenticated=authenticated,
        ):
            started = getattr(g, "request_started_monotonic", None)
            if started is None:
                duration_ms = -1
            else:
                duration_ms = int(max(0.0, (time.perf_counter() - started) * 1000.0))
            logger.info(
                "WEB_AUDIT method=%s path=%s endpoint=%s status=%s ip=%s user=%s duration_ms=%s",
                request.method,
                request.path,
                request.endpoint or "unknown",
                int(getattr(response, "status_code", 0) or 0),
                _client_ip(),
                _audit_user_label_from_email(session.get("auth_email", "")),
                duration_ms,
            )
        return response

    users_file = Path(data_dir) / "bot_data.db"
    users_file.parent.mkdir(parents=True, exist_ok=True)
    env_file = Path(env_file_path)
    fallback_env_file = _env_fallback_file_path(data_dir)
    if harden_file_permissions:
        try:
            os.chmod(users_file.parent, 0o700)
        except (PermissionError, OSError):
            pass
        ssl_dir = _get_web_ssl_dir(data_dir)
        if ssl_dir.exists():
            _chmod_if_possible(ssl_dir, 0o700)
        if env_file.exists():
            try:
                os.chmod(env_file, 0o600)
            except (PermissionError, OSError):
                pass
        if fallback_env_file.exists():
            try:
                os.chmod(fallback_env_file, 0o600)
            except (PermissionError, OSError):
                pass

    _ensure_default_admin(users_file, default_admin_email, default_admin_password, logger)
    favicon_file = Path(__file__).resolve().parent / "assets" / "images" / "glinet-bot-round.png"
    wiki_dir = Path(__file__).resolve().parent / "wiki"
    wiki_dir_resolved = wiki_dir.resolve()

    def _is_within_wiki_dir(path: Path):
        try:
            path.resolve().relative_to(wiki_dir_resolved)
            return True
        except (OSError, ValueError):
            return False

    def _get_wiki_page_map():
        page_map = {}
        if not wiki_dir.exists():
            return page_map
        for path in wiki_dir.glob("*.md"):
            if not path.is_file() or path.name.startswith("_"):
                continue
            if not _is_within_wiki_dir(path):
                continue
            resolved = path.resolve()
            page_map[path.stem.casefold()] = resolved
        return page_map

    def _current_user():
        if not _is_active_auth_session():
            return None
        email = _normalize_email(session.get("auth_email", ""))
        if not email:
            return None
        for user in _read_users(users_file):
            if user["email"] == email:
                return user
        return None

    def _guild_groups_by_id():
        return {
            str(entry.get("id") or "").strip(): entry
            for entry in _read_guild_groups(users_file)
            if str(entry.get("id") or "").strip()
        }

    def _allowed_guild_ids_for_user(user: dict | None):
        if not _is_guild_admin_user(user):
            return None
        allowed_ids = []
        seen = set()
        for group_id in _normalize_string_id_list((user or {}).get("guild_group_ids", [])):
            group_entry = _guild_groups_by_id().get(group_id)
            if not isinstance(group_entry, dict):
                continue
            for guild_id in _normalize_id_string_list(group_entry.get("guild_ids", [])):
                if guild_id in seen:
                    continue
                allowed_ids.append(guild_id)
                seen.add(guild_id)
        return allowed_ids

    def _filter_guilds_for_user(guilds: list[dict], user: dict | None):
        if _is_glinet_scoped_user(user):
            preferred = _preferred_glinet_guild()
            return [preferred] if isinstance(preferred, dict) else []
        allowed_guild_ids = _allowed_guild_ids_for_user(user)
        if allowed_guild_ids is None:
            return list(guilds)
        allowed_set = set(allowed_guild_ids)
        return [entry for entry in guilds if str(entry.get("id") or "").strip() in allowed_set]

    def _load_all_guilds():
        payload = on_get_guilds() if callable(on_get_guilds) else None
        if not isinstance(payload, dict) or not payload.get("ok"):
            return [], str(payload.get("error") or "") if isinstance(payload, dict) else ""
        guilds = payload.get("guilds", []) or []
        normalized = []
        for entry in guilds:
            if not isinstance(entry, dict):
                continue
            guild_id = str(entry.get("id") or "").strip()
            guild_name = str(entry.get("name") or "").strip()
            if not guild_id or not guild_name:
                continue
            normalized.append(
                {
                    "id": guild_id,
                    "name": guild_name,
                    "icon_url": str(entry.get("icon_url") or "").strip(),
                    "member_count": int(entry.get("member_count") or 0),
                    "is_primary": bool(entry.get("is_primary")),
                }
            )
        normalized.sort(key=lambda item: item["name"].casefold())
        return normalized, ""

    def _load_available_guilds():
        guilds, error_text = _load_all_guilds()
        return _filter_guilds_for_user(guilds, _current_user()), error_text

    def _selected_guild_id():
        user = _current_user()
        if _is_glinet_scoped_user(user):
            preferred = _preferred_glinet_guild()
            if preferred is not None:
                preferred_id = str(preferred.get("id") or "").strip()
                if preferred_id:
                    session["selected_guild_id"] = preferred_id
                    return preferred_id
            session.pop("selected_guild_id", None)
            return ""

        selected = str(session.get("selected_guild_id", "")).strip()
        guilds, _error_text = _load_available_guilds()
        valid_ids = {str(entry.get("id") or "").strip() for entry in guilds}
        if selected and selected in valid_ids:
            return selected
        if selected and selected not in valid_ids:
            session.pop("selected_guild_id", None)
        if _is_guild_admin_user(user) and guilds:
            selected = str(guilds[0].get("id") or "").strip()
            if selected:
                session["selected_guild_id"] = selected
                return selected
        return ""

    def _selected_guild():
        selected_id = _selected_guild_id()
        if not selected_id:
            return None
        guilds, _error_text = _load_available_guilds()
        for entry in guilds:
            if str(entry.get("id") or "").strip() == selected_id:
                return entry
        return None

    def _set_selected_guild_id(guild_id: str):
        selected = str(guild_id or "").strip()
        user = _current_user()
        if _is_glinet_scoped_user(user):
            preferred = _preferred_glinet_guild()
            if preferred is not None:
                preferred_id = str(preferred.get("id") or "").strip()
                if preferred_id:
                    session["selected_guild_id"] = preferred_id
                    return True
            session.pop("selected_guild_id", None)
            return False

        guilds, _error_text = _load_available_guilds()
        valid_ids = {str(entry.get("id") or "").strip() for entry in guilds}
        if selected and selected in valid_ids:
            session["selected_guild_id"] = selected
            return True
        session.pop("selected_guild_id", None)
        return False

    def _preferred_glinet_guild():
        guilds, _error_text = _load_all_guilds()
        if not guilds:
            return None
        for entry in guilds:
            if bool(entry.get("is_primary")):
                return entry
        for entry in guilds:
            guild_name = str(entry.get("name") or "").casefold()
            if "gl.i.net community" in guild_name or "glinet community" in guild_name:
                return entry
        if len(guilds) == 1:
            return guilds[0]
        return None

    def _require_selected_guild_redirect():
        if _selected_guild() is not None:
            return None
        flash("Select a Discord server before opening that page.", "error")
        return redirect(url_for("guilds_page"))

    def _github_wiki_url():
        value = os.getenv(
            "WEB_GITHUB_WIKI_URL",
            "http://discord.glinet.wickedyoda.com/wiki",
        )
        return str(value or "").strip()

    def _restart_enabled():
        return _is_truthy_env_value(os.getenv("WEB_RESTART_ENABLED", "true"))

    def _public_base_url():
        return str(os.getenv("WEB_PUBLIC_BASE_URL", "")).strip()

    def _extract_hostname(value: str):
        text = str(value or "").strip()
        if not text:
            return ""
        parsed = urlparse(text if "://" in text else f"//{text}")
        return str(parsed.hostname or "").strip().lower()

    def _client_ip():
        x_forwarded_for = str(request.headers.get("X-Forwarded-For", "")).strip()
        if trust_proxy_headers and x_forwarded_for:
            parts = [part.strip() for part in x_forwarded_for.split(",") if part.strip()]
            if parts:
                return parts[0]
        return str(request.remote_addr or "unknown")

    def _ensure_csrf_token():
        token = str(session.get("csrf_token", "")).strip()
        if token:
            return token
        token = secrets.token_urlsafe(32)
        session["csrf_token"] = token
        return token

    def _clear_auth_session():
        session.pop("auth_email", None)
        session.pop("auth_mode", None)
        session.pop("auth_issued_at", None)
        session.pop("auth_last_seen", None)
        session.pop("auth_remember_until", None)
        session.pop("force_password_change_notice_shown", None)
        session.pop("selected_guild_id", None)

    def _set_auth_session(email: str, remember_login: bool):
        now_dt = datetime.now(UTC)
        now_iso = now_dt.isoformat()
        session["auth_email"] = _normalize_email(email)
        session["auth_mode"] = AUTH_MODE_REMEMBER if remember_login else AUTH_MODE_STANDARD
        session["auth_issued_at"] = now_iso
        session["auth_last_seen"] = now_iso
        if remember_login:
            session["auth_remember_until"] = (now_dt + timedelta(days=REMEMBER_LOGIN_DAYS)).isoformat()
        else:
            session.pop("auth_remember_until", None)
        session.permanent = True

    def _session_timeout_minutes():
        return _normalize_session_timeout_minutes(
            session_timeout_state.get("minutes", WEB_INACTIVITY_TIMEOUT_MINUTES),
            default_value=WEB_INACTIVITY_TIMEOUT_MINUTES,
        )

    def _navigation_label_for_endpoint(endpoint: str) -> str:
        labels = {
            "dashboard": "Dashboard",
            "guild_settings": "Guild Settings",
            "moderation_page": "Moderation",
            "command_status": "Command Status",
            "command_permissions": "Command Permissions",
            "bot_profile": "Bot Profile",
            "member_activity_page": "Member Activity",
            "role_access_page": "Role Access",
            "tag_responses": "Tag Responses",
            "actions_page": "Action History",
            "bulk_role_csv": "Bulk Role CSV",
            "reddit_feeds": "Reddit Feeds",
            "service_monitors_page": "Service Monitors",
            "youtube_subscriptions": "YouTube",
            "linkedin_subscriptions": "LinkedIn",
            "beta_program_subscriptions": "Beta Programs",
            "account": "My Account",
            "settings": "Global Settings",
            "public_observability": "Observability",
            "admin_logs": "Logs",
            "users": "Users",
            "documentation": "Documentation",
            "wiki_proxy": "Wiki",
        }
        return labels.get(str(endpoint or "").strip(), "")

    def _recent_navigation_entries() -> list[dict]:
        entries = session.get(RECENT_NAV_SESSION_KEY, [])
        if not isinstance(entries, list):
            return []
        normalized = []
        for item in entries:
            if not isinstance(item, dict):
                continue
            label = str(item.get("label") or "").strip()
            href = str(item.get("href") or "").strip()
            if not label or not href:
                continue
            normalized.append({"label": label, "href": href})
        return normalized[:RECENT_NAV_LIMIT]

    def _remember_navigation_entry() -> None:
        if request.method != "GET":
            return
        if int(getattr(g, "response_status_code", 0) or 0) >= 400:
            return
        if not _normalize_email(session.get("auth_email", "")):
            return
        endpoint = str(request.endpoint or "").strip()
        label = _navigation_label_for_endpoint(endpoint)
        if not label or endpoint == "dashboard":
            return
        href = str(request.full_path or request.path or "").strip()
        if href.endswith("?"):
            href = href[:-1]
        if not href.startswith("/"):
            href = str(request.path or "").strip()
        current_entries = _recent_navigation_entries()
        updated_entries = [{"label": label, "href": href}]
        for item in current_entries:
            if str(item.get("href") or "").strip() == href:
                continue
            updated_entries.append(item)
        session[RECENT_NAV_SESSION_KEY] = updated_entries[:RECENT_NAV_LIMIT]

    def _is_active_auth_session():
        email = _normalize_email(session.get("auth_email", ""))
        if not email:
            return False

        now_dt = datetime.now(UTC)
        mode = str(session.get("auth_mode", AUTH_MODE_STANDARD)).strip().lower()
        if mode not in {AUTH_MODE_STANDARD, AUTH_MODE_REMEMBER}:
            mode = AUTH_MODE_STANDARD

        issued_dt = _parse_iso_datetime(session.get("auth_issued_at", ""))
        last_seen_dt = _parse_iso_datetime(session.get("auth_last_seen", ""))
        if issued_dt is None and last_seen_dt is None:
            _clear_auth_session()
            flash("Your session has expired. Please log in again.", "error")
            return False
        if issued_dt is None:
            issued_dt = last_seen_dt
            session["auth_issued_at"] = issued_dt.isoformat()
        if last_seen_dt is None:
            last_seen_dt = issued_dt

        if mode == AUTH_MODE_REMEMBER:
            remember_until = _parse_iso_datetime(session.get("auth_remember_until", ""))
            if remember_until is None:
                remember_until = issued_dt + timedelta(days=REMEMBER_LOGIN_DAYS)
                session["auth_remember_until"] = remember_until.isoformat()
            if now_dt > remember_until:
                _clear_auth_session()
                flash("Your saved login expired. Please log in again.", "error")
                return False

        inactivity_limit = timedelta(minutes=_session_timeout_minutes())
        if (now_dt - last_seen_dt) > inactivity_limit:
            _clear_auth_session()
            flash("You were logged out due to inactivity.", "error")
            return False

        session["auth_mode"] = mode
        session["auth_last_seen"] = now_dt.isoformat()
        session.permanent = True
        return True

    def _is_same_origin_request():
        allowed_hosts = set()
        request_host = _extract_hostname(str(request.host or ""))
        if request_host:
            allowed_hosts.add(request_host)

        if trust_proxy_headers:
            forwarded_host = str(request.headers.get("X-Forwarded-Host", "")).strip()
            if forwarded_host:
                forwarded_host_name = _extract_hostname(forwarded_host.split(",")[0])
                if forwarded_host_name:
                    allowed_hosts.add(forwarded_host_name)
            original_host = str(request.headers.get("X-Original-Host", "")).strip()
            if original_host:
                original_host_name = _extract_hostname(original_host.split(",")[0])
                if original_host_name:
                    allowed_hosts.add(original_host_name)
            forwarded_header = str(request.headers.get("Forwarded", "")).strip()
            if forwarded_header:
                forwarded_match = re.search(
                    r"(?i)\bhost=([^;,\s]+)",
                    forwarded_header,
                )
                if forwarded_match:
                    forwarded_token = str(forwarded_match.group(1) or "").strip().strip('"')
                    forwarded_name = _extract_hostname(forwarded_token)
                    if forwarded_name:
                        allowed_hosts.add(forwarded_name)

        public_base_url = _public_base_url()
        if public_base_url:
            public_host = _extract_hostname(public_base_url)
            if public_host:
                allowed_hosts.add(public_host)

        if not allowed_hosts:
            return False

        def _match_allowed_host_from_url(raw_value: str):
            text = str(raw_value or "").strip()
            if not text:
                return None
            parsed = urlparse(text)
            if parsed.scheme not in {"http", "https"}:
                return None
            host = _extract_hostname(text)
            if not host:
                return None
            return host in allowed_hosts

        origin = str(request.headers.get("Origin", "")).strip()
        if origin.lower() == "null":
            origin = ""
        origin_allowed = _match_allowed_host_from_url(origin)
        if origin_allowed is True:
            return True

        referer = str(request.headers.get("Referer", "")).strip()
        if referer.lower() == "null":
            referer = ""
        referer_allowed = _match_allowed_host_from_url(referer)
        if referer_allowed is True:
            return True

        # If either header was present but neither matched an allowed host, reject.
        if origin or referer:
            return False

        # Some clients/proxies omit Origin/Referer on same-site form submits.
        # CSRF validation still protects state-changing routes.
        return True if enforce_csrf else False

    @app.before_request
    def enforce_request_security():
        if request.method not in STATE_CHANGING_METHODS:
            return None
        if request.endpoint == "healthz":
            return None

        if enforce_same_origin_posts and not _is_same_origin_request():
            if logger:
                logger.warning(
                    "Blocked request due to origin policy: endpoint=%s method=%s ip=%s host=%s origin=%s referer=%s x_forwarded_host=%s x_forwarded_proto=%s",
                    request.endpoint,
                    request.method,
                    _client_ip(),
                    str(request.host or ""),
                    str(request.headers.get("Origin", "") or ""),
                    str(request.headers.get("Referer", "") or ""),
                    str(request.headers.get("X-Forwarded-Host", "") or ""),
                    str(request.headers.get("X-Forwarded-Proto", "") or ""),
                )
            flash("Blocked request due to origin policy.", "error")
            user = _current_user()
            if user:
                return redirect(url_for("dashboard"))
            return redirect(url_for("login"))

        if enforce_csrf:
            expected = str(session.get("csrf_token", "")).strip()
            submitted = str(request.form.get("csrf_token", "") or request.headers.get("X-CSRF-Token", "")).strip()
            # Recover login form flow when a prior session token is absent but the
            # submitted form token is present (for example after cookie loss).
            if request.endpoint == "login" and request.method == "POST" and not expected and submitted:
                session["csrf_token"] = submitted
                expected = submitted
            if not expected or not submitted or not secrets.compare_digest(expected, submitted):
                if logger:
                    logger.warning(
                        "Blocked request due to CSRF validation: endpoint=%s method=%s ip=%s has_expected=%s has_submitted=%s",
                        request.endpoint,
                        request.method,
                        _client_ip(),
                        bool(expected),
                        bool(submitted),
                    )
                flash("Session security token check failed. Please retry.", "error")
                user = _current_user()
                if user:
                    return redirect(url_for("dashboard"))
                return redirect(url_for("login"))
        return None

    @app.before_request
    def enforce_read_only_write_restrictions():
        if request.method not in STATE_CHANGING_METHODS:
            return None
        if request.endpoint in READ_ONLY_WRITE_EXEMPT_ENDPOINTS:
            return None
        user = _current_user()
        if user is None:
            return None
        if _is_admin_user(user):
            return None
        if _is_glinet_rw_user(user):
            glinet_rw_write_endpoints = {
                "account",
                "command_status",
                "guild_settings",
                "moderation_page",
                "command_permissions",
                "reddit_feeds",
                "service_monitors_page",
                "youtube_subscriptions",
                "linkedin_subscriptions",
                "beta_program_subscriptions",
                "role_access_page",
                "tag_responses",
                "bot_profile",
            }
            if request.endpoint in glinet_rw_write_endpoints:
                if request.endpoint == "bot_profile":
                    action = str(request.form.get("action", "") or "").strip().lower()
                    if action not in {"nickname"}:
                        flash("Glinet-RW can only edit the bot nickname for the GL.iNet Community Discord.", "error")
                        return redirect(url_for("bot_profile"))
                return None
        if _is_guild_admin_user(user):
            guild_admin_write_endpoints = {
                "account",
                "command_status",
                "guild_settings",
                "moderation_page",
                "command_permissions",
                "reddit_feeds",
                "service_monitors_page",
                "youtube_subscriptions",
                "linkedin_subscriptions",
                "beta_program_subscriptions",
                "role_access_page",
                "tag_responses",
                "bot_profile",
            }
            if request.endpoint in guild_admin_write_endpoints:
                if request.endpoint == "bot_profile":
                    action = str(request.form.get("action", "") or "").strip().lower()
                    if action not in {"nickname"}:
                        flash("Guild Admin can only edit the bot nickname inside allowed servers.", "error")
                        return redirect(url_for("bot_profile"))
                return None
        if logger:
            logger.warning(
                "Blocked write request for read-only user: endpoint=%s method=%s ip=%s",
                request.endpoint,
                request.method,
                _client_ip(),
            )
        flash("Read-only account: this action is not allowed.", "error")
        safe_view_endpoints = {
            "bot_profile",
            "command_status",
            "command_permissions",
            "moderation_page",
            "reddit_feeds",
            "service_monitors_page",
            "youtube_subscriptions",
            "linkedin_subscriptions",
            "beta_program_subscriptions",
            "role_access_page",
            "settings",
            "tag_responses",
            "bulk_role_csv",
            "users",
        }
        if request.endpoint in safe_view_endpoints:
            return redirect(url_for(str(request.endpoint)))
        return redirect(url_for("dashboard"))

    @app.before_request
    def enforce_glinet_role_route_restrictions():
        user = _current_user()
        if user is None or (not _is_glinet_scoped_user(user) and not _is_guild_admin_user(user)):
            return None
        allowed_endpoints = {
            "index",
            "login",
            "logout",
            "healthz",
            "favicon",
            "account",
            "guilds_page",
            "select_guild",
            "dashboard",
            "guild_settings",
            "moderation_page",
            "actions_page",
            "member_activity_page",
            "member_activity_export",
            "command_status",
            "command_permissions",
            "reddit_feeds",
            "service_monitors_page",
            "youtube_subscriptions",
            "linkedin_subscriptions",
            "beta_program_subscriptions",
            "role_access_page",
            "tag_responses",
            "bulk_role_csv",
            "bot_profile",
        }
        if request.endpoint in allowed_endpoints:
            return None
        if _is_guild_admin_user(user):
            flash("Guild Admin access is limited to assigned Discord server groups.", "error")
        else:
            flash("GL.iNet-scoped access is limited to the primary GL.iNet Community Discord server.", "error")
        if _selected_guild():
            return redirect(url_for("dashboard"))
        return redirect(url_for("guilds_page"))

    def _prune_login_attempts(client_ip: str):
        now_ts = time.time()
        entries = login_attempts.get(client_ip, [])
        fresh_entries = [ts for ts in entries if (now_ts - ts) < login_window_seconds]
        if fresh_entries:
            login_attempts[client_ip] = fresh_entries
        else:
            login_attempts.pop(client_ip, None)
        return fresh_entries

    def _prune_recent_login_success(client_ip: str):
        now_ts = time.time()
        entries = recent_login_success.get(client_ip, [])
        fresh_entries = [ts for ts in entries if (now_ts - ts) < 120]
        if fresh_entries:
            recent_login_success[client_ip] = fresh_entries
        else:
            recent_login_success.pop(client_ip, None)
        return fresh_entries

    @app.errorhandler(413)
    def payload_too_large(_exc):
        flash("Upload exceeds maximum allowed request size.", "error")
        user = _current_user()
        if user and user.get("is_admin"):
            return redirect(url_for("dashboard"))
        return redirect(url_for("login"))

    def _render_page(
        title: str,
        body_html: str,
        current_email: str,
        is_admin: bool,
        current_display_name: str = "",
    ):
        csrf_token = _ensure_csrf_token()
        resolved_display_name = _clean_profile_text(current_display_name, max_length=80)
        normalized_email = _normalize_email(current_email)
        current_role = _normalize_web_user_role("", is_admin=is_admin)
        if not resolved_display_name and normalized_email:
            for account in _read_users(users_file):
                if account.get("email") == normalized_email:
                    resolved_display_name = _clean_profile_text(
                        str(account.get("display_name", "")),
                        max_length=80,
                    )
                    current_role = _normalize_web_user_role(
                        str(account.get("role", "")),
                        is_admin=bool(account.get("is_admin")),
                    )
                    break
        if not resolved_display_name and normalized_email:
            resolved_display_name = _default_display_name(normalized_email)
        current_guild = _selected_guild() if normalized_email else None
        return _render_layout(
            title,
            _inject_csrf_token_inputs(body_html, csrf_token),
            current_email,
            resolved_display_name,
            csrf_token,
            is_admin,
            current_role_label=_user_role_label(current_role, is_admin=is_admin),
            current_role=current_role,
            current_guild_name=(str(current_guild.get("name") or "") if isinstance(current_guild, dict) else ""),
            github_wiki_url=_github_wiki_url(),
            restart_enabled=_restart_enabled(),
        )

    def _load_discord_catalog_options(selected_guild_id: str, *, channel_type: str | None = None):
        discord_catalog = on_get_discord_catalog(selected_guild_id) if callable(on_get_discord_catalog) and selected_guild_id else None
        channel_options = []
        role_options = []
        catalog_error = ""
        if isinstance(discord_catalog, dict):
            if discord_catalog.get("ok"):
                channel_options = discord_catalog.get("channels", []) or []
                role_options = discord_catalog.get("roles", []) or []
            else:
                catalog_error = str(discord_catalog.get("error") or "")
        if channel_type is not None:
            expected_type = str(channel_type or "").strip().lower()
            channel_options = [
                option
                for option in channel_options
                if str(option.get("type") or "").strip().lower() == expected_type
            ]
        return channel_options, role_options, catalog_error

    def _redirect_for_password_rotation(user: dict):
        if not user:
            return None
        if not _password_change_required(user):
            session.pop("force_password_change_notice_shown", None)
            return None
        if request.endpoint in {"account", "logout", "login", "healthz"}:
            return None
        if not session.get("force_password_change_notice_shown"):
            flash(
                f"Password expired. You must change it every {PASSWORD_MAX_AGE_DAYS} days.",
                "error",
            )
            session["force_password_change_notice_shown"] = True
        return redirect(url_for("account"))

    def login_required(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            user = _current_user()
            if user is None:
                return redirect(url_for("login"))
            rotation_redirect = _redirect_for_password_rotation(user)
            if rotation_redirect is not None:
                return rotation_redirect
            return fn(*args, **kwargs)

        return wrapper

    def admin_required(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            user = _current_user()
            if user is None:
                return redirect(url_for("login"))
            rotation_redirect = _redirect_for_password_rotation(user)
            if rotation_redirect is not None:
                return rotation_redirect
            if not user.get("is_admin"):
                flash("Admin privileges are required.", "error")
                return redirect(url_for("dashboard"))
            return fn(*args, **kwargs)

        return wrapper

    @app.route("/healthz", methods=["GET"])
    def healthz():
        return {"ok": True}, 200

    @app.route("/favicon.ico", methods=["GET"])
    def favicon():
        if favicon_file.exists() and favicon_file.is_file():
            return send_file(favicon_file, mimetype="image/png", max_age=86400)
        return ("", 204)

    @app.route("/", methods=["GET"])
    def index():
        if _current_user():
            return redirect(url_for("guilds_page"))
        return redirect(url_for("login"))

    @app.route("/login", methods=["GET", "POST"])
    def login():
        client_ip = _client_ip()
        if request.method == "GET":
            recent_success = _prune_recent_login_success(client_ip)
            if recent_success and logger:
                logger.warning(
                    "Recent login success from ip=%s but no active session on subsequent GET /login; verify reverse proxy host/proto forwarding and session cookie policy.",
                    client_ip,
                )
        if request.method == "POST":
            attempts = _prune_login_attempts(client_ip)
            if len(attempts) >= login_max_attempts:
                if logger:
                    logger.warning(
                        "Login rate limit triggered for ip=%s attempts=%s",
                        client_ip,
                        len(attempts),
                    )
                flash("Too many login attempts. Try again in 15 minutes.", "error")
                return redirect(url_for("login"))
            email = _normalize_email(request.form.get("email", ""))
            password = request.form.get("password", "")
            remember_login = bool(request.form.get("remember_login"))
            user = next(
                (entry for entry in _read_users(users_file) if entry["email"] == email),
                None,
            )
            if user and check_password_hash(user["password_hash"], password):
                if _password_hash_needs_upgrade(user.get("password_hash", "")):
                    users_data = _read_users(users_file)
                    for entry in users_data:
                        if entry.get("email") == user.get("email"):
                            entry["password_hash"] = _hash_password(password)
                            _save_users(users_file, users_data)
                            break
                login_attempts.pop(client_ip, None)
                recent_entries = _prune_recent_login_success(client_ip)
                recent_entries.append(time.time())
                recent_login_success[client_ip] = recent_entries[-5:]
                _set_auth_session(user["email"], remember_login=remember_login)
                if _password_change_required(user):
                    session["force_password_change_notice_shown"] = True
                    flash(
                        f"Password expired. You must change it every {PASSWORD_MAX_AGE_DAYS} days.",
                        "error",
                    )
                    return redirect(url_for("account"))
                return redirect(url_for("guilds_page"))
            attempts.append(time.time())
            login_attempts[client_ip] = attempts[-login_max_attempts:]
            flash("Invalid email or password.", "error")

        return _render_page(
            "Login",
            f"""
            <div class="card" style="max-width:520px;margin:30px auto;">
              <h2>Web Login</h2>
              <p class="muted">Web GUI login with email/password. Users are created by an admin only.</p>
              <form method="post">
                <label for="login_email">Email</label>
                <input id="login_email" type="email" name="email" placeholder="admin@example.com" autocomplete="username" autocapitalize="none" spellcheck="false" required />
                <label for="login_password" style="margin-top:10px;display:block;">Password</label>
                <input id="login_password" type="password" name="password" autocomplete="current-password" required />
                <label style="margin-top:10px;display:block;">
                  <input type="checkbox" name="remember_login" value="1" />
                  Keep me signed in for {REMEMBER_LOGIN_DAYS} days on this device
                </label>
                <div style="margin-top:14px;">
                  <button class="btn" type="submit">Login</button>
                </div>
              </form>
            </div>
            """,
            "",
            False,
        )

    @app.route("/logout", methods=["GET"])
    def logout():
        client_ip = _client_ip()
        login_attempts.pop(client_ip, None)
        recent_login_success.pop(client_ip, None)
        session.clear()
        return redirect(url_for("login"))

    @app.route("/admin/account", methods=["GET", "POST"])
    @login_required
    def account():
        user = _current_user()
        if not user:
            return redirect(url_for("login"))

        if request.method == "POST":
            action = str(request.form.get("action", "")).strip().lower()
            users_data = _read_users(users_file)
            user_index = next(
                (idx for idx, entry in enumerate(users_data) if entry.get("email") == user.get("email")),
                -1,
            )
            if user_index < 0:
                session.clear()
                flash("Your account was not found. Please log in again.", "error")
                return redirect(url_for("login"))

            entry = users_data[user_index]
            password_expired = _password_change_required(entry)

            if action == "profile":
                if password_expired:
                    flash(
                        "Password expired. Change your password before updating other account fields.",
                        "error",
                    )
                else:
                    first_name = _clean_profile_text(request.form.get("first_name", ""), max_length=80)
                    last_name = _clean_profile_text(request.form.get("last_name", ""), max_length=80)
                    display_name = _clean_profile_text(request.form.get("display_name", ""), max_length=80)
                    next_email = _normalize_email(request.form.get("email", ""))
                    current_password = request.form.get("current_password", "")

                    validation_errors = []
                    if not first_name:
                        validation_errors.append("First name is required.")
                    if not last_name:
                        validation_errors.append("Last name is required.")
                    if not display_name:
                        validation_errors.append("Display name is required.")
                    if not _is_valid_email(next_email):
                        validation_errors.append("Enter a valid email.")
                    if any(row.get("email") == next_email and row.get("email") != entry.get("email") for row in users_data):
                        validation_errors.append("Another account already uses that email.")
                    if not check_password_hash(entry["password_hash"], current_password):
                        validation_errors.append("Current password is required to update account details.")

                    if validation_errors:
                        for message in validation_errors:
                            flash(message, "error")
                    else:
                        previous_email = entry["email"]
                        now_iso = _now_iso()
                        entry["first_name"] = first_name
                        entry["last_name"] = last_name
                        entry["display_name"] = display_name
                        entry["email"] = next_email
                        if next_email != previous_email:
                            entry["email_changed_at"] = now_iso
                            session["auth_email"] = next_email
                        _save_users(users_file, users_data)
                        flash("Account profile updated.", "success")

            elif action == "password":
                current_password = request.form.get("current_password", "")
                new_password = request.form.get("new_password", "")
                confirm_password = request.form.get("confirm_password", "")

                validation_errors = []
                if not str(current_password or ""):
                    validation_errors.append("Current password is required.")
                elif not check_password_hash(entry["password_hash"], current_password):
                    validation_errors.append("Current password is incorrect.")
                if not str(new_password or ""):
                    validation_errors.append("New password is required.")
                if not str(confirm_password or ""):
                    validation_errors.append("Confirm new password is required.")
                if str(new_password or "") and str(confirm_password or "") and new_password != confirm_password:
                    validation_errors.append("New password and confirmation must match.")
                if str(new_password or ""):
                    validation_errors.extend(_password_policy_errors(new_password))
                if str(new_password or "") and check_password_hash(entry["password_hash"], new_password):
                    validation_errors.append("New password must be different from the current password.")

                if validation_errors:
                    for message in validation_errors:
                        flash(message, "error")
                else:
                    now_iso = _now_iso()
                    entry["password_hash"] = _hash_password(new_password)
                    entry["password_changed_at"] = now_iso
                    _save_users(users_file, users_data)
                    session.pop("force_password_change_notice_shown", None)
                    flash("Password updated successfully.", "success")

            else:
                flash("Invalid account action.", "error")

            user = _current_user() or user

        password_expired = _password_change_required(user)
        password_age_days = _password_age_days(user)
        days_remaining = max(0, PASSWORD_MAX_AGE_DAYS - password_age_days)
        profile_disabled_attr = " disabled" if password_expired else ""
        profile_note = (
            f"<p class='muted'>Password is expired (older than {PASSWORD_MAX_AGE_DAYS} days). "
            "Update your password to unlock profile/email changes.</p>"
            if password_expired
            else (f"<p class='muted'>Password age: {password_age_days} day(s). Days remaining before forced reset: {days_remaining}.</p>")
        )

        body = f"""
        <div class="grid">
          <div class="card">
            <h2>My Account</h2>
            <p class="muted">Update your identity details used in the web GUI header and account records.</p>
            {profile_note}
            <form method="post">
              <input type="hidden" name="action" value="profile" />
              <label>First Name</label>
              <input type="text" name="first_name" autocomplete="given-name" value="{escape(str(user.get("first_name", "")), quote=True)}" required{profile_disabled_attr} />
              <label style="margin-top:10px;display:block;">Last Name</label>
              <input type="text" name="last_name" autocomplete="family-name" value="{escape(str(user.get("last_name", "")), quote=True)}" required{profile_disabled_attr} />
              <label style="margin-top:10px;display:block;">Display Name</label>
              <input type="text" name="display_name" autocomplete="nickname" value="{escape(str(user.get("display_name", "")), quote=True)}" required{profile_disabled_attr} />
              <label style="margin-top:10px;display:block;">Email</label>
              <input type="email" name="email" autocomplete="email" autocapitalize="none" spellcheck="false" value="{escape(str(user.get("email", "")), quote=True)}" required{profile_disabled_attr} />
              <label style="margin-top:10px;display:block;">Current Password (required to save profile/email)</label>
              <input id="account_profile_current_password" type="password" name="current_password" autocomplete="current-password" required{profile_disabled_attr} />
              <label style="margin-top:8px;display:block;">
                <input type="checkbox"
                  onchange="document.getElementById('account_profile_current_password').type=this.checked?'text':'password';"{profile_disabled_attr} />
                Show password
              </label>
              <div style="margin-top:14px;">
                <button class="btn" type="submit"{profile_disabled_attr}>Update Profile</button>
              </div>
            </form>
          </div>
          <div class="card">
            <h2>Change Password</h2>
            <p class="muted">Password policy: 6-16 characters, at least 2 numbers, 1 uppercase letter, and 1 symbol.</p>
            <p class="muted">Password changes are required every {PASSWORD_MAX_AGE_DAYS} days.</p>
            <form method="post" onsubmit="return validateAccountPasswordChangeForm();">
              <input type="hidden" name="action" value="password" />
              <label>Current Password</label>
              <input id="account_password_current" type="password" name="current_password" autocomplete="current-password" required />
              <label style="margin-top:10px;display:block;">New Password</label>
              <input id="account_password_new" type="password" name="new_password" autocomplete="new-password" required oninput="validateAccountPasswordChangeForm();" />
              <label style="margin-top:10px;display:block;">Confirm New Password</label>
              <input id="account_password_confirm" type="password" name="confirm_password" autocomplete="new-password" required oninput="validateAccountPasswordChangeForm();" />
              <label style="margin-top:8px;display:block;">
                <input type="checkbox"
                  onchange="document.getElementById('account_password_current').type=this.checked?'text':'password';document.getElementById('account_password_new').type=this.checked?'text':'password';document.getElementById('account_password_confirm').type=this.checked?'text':'password';" />
                Show passwords
              </label>
              <div style="margin-top:14px;">
                <button class="btn" type="submit">Update Password</button>
              </div>
            </form>
            <script>
              function validateAccountPasswordChangeForm() {{
                var nextInput = document.getElementById('account_password_new');
                var confirmInput = document.getElementById('account_password_confirm');
                if (!nextInput || !confirmInput) {{
                  return true;
                }}
                if (confirmInput.value && nextInput.value !== confirmInput.value) {{
                  confirmInput.setCustomValidity('New password and confirmation must match.');
                }} else {{
                  confirmInput.setCustomValidity('');
                }}
                return true;
              }}
            </script>
          </div>
        </div>
        """

        return _render_page(
            "My Account",
            body,
            user["email"],
            bool(user.get("is_admin")),
            str(user.get("display_name") or ""),
        )

    @app.route("/admin", methods=["GET"])
    @login_required
    def guilds_page():
        user = _current_user()
        is_admin = _is_admin_user(user)
        guilds, guild_error = _load_available_guilds()
        selected_guild_id = _selected_guild_id()
        selected_guild = _selected_guild()

        if _is_glinet_scoped_user(user):
            if selected_guild is not None:
                return redirect(url_for("dashboard"))
            flash("No primary Discord server is available for the GL.iNet-scoped role.", "error")
            return redirect(url_for("account"))

        cards = []
        for guild in guilds:
            guild_id = str(guild.get("id") or "")
            guild_name = str(guild.get("name") or "Unknown Server")
            member_count = int(guild.get("member_count") or 0)
            icon_url = str(guild.get("icon_url") or "").strip()
            is_selected = guild_id == selected_guild_id
            primary_note = "<p class='muted'>Primary configured guild</p>" if guild.get("is_primary") else ""
            icon_html = (
                f"<img src='{escape(icon_url, quote=True)}' alt='{escape(guild_name)} icon' "
                "style='width:56px;height:56px;border-radius:14px;border:1px solid var(--border);object-fit:cover;' />"
                if icon_url
                else "<div style='width:56px;height:56px;border-radius:14px;border:1px solid var(--border);display:flex;align-items:center;justify-content:center;font-weight:700;'>#</div>"
            )
            cards.append(
                f"""
                <div class="card dash-card">
                  <div style="display:flex;align-items:center;gap:12px;margin-bottom:12px;">
                    {icon_html}
                    <div>
                      <h3 style="margin:0 0 6px;">{escape(guild_name)}</h3>
                      <div class="muted mono">{escape(guild_id)}</div>
                    </div>
                  </div>
                  <p class="muted">Members: {member_count}</p>
                  {primary_note}
                  <form method="post" action="{escape(url_for("select_guild"), quote=True)}">
                    <input type="hidden" name="guild_id" value="{escape(guild_id, quote=True)}" />
                    <button class="btn" type="submit"{" disabled" if is_selected else ""}>{"Currently Selected" if is_selected else "Manage This Server"}</button>
                  </form>
                  {""
                    if not is_admin else
                    f'''
                    <form method="post" action="{escape(url_for("leave_guild"), quote=True)}" style="margin-top:10px;" onsubmit="return confirm('Remove the bot from {escape(guild_name)}? This cannot be undone from the web GUI and will immediately disconnect the bot from that server.');">
                      <input type="hidden" name="guild_id" value="{escape(guild_id, quote=True)}" />
                      <input type="hidden" name="confirm" value="yes" />
                      <button class="btn danger" type="submit">Remove Bot</button>
                    </form>
                    '''
                  }
                </div>
                """
            )

        selected_note = ""
        if isinstance(selected_guild, dict):
            selected_target = url_for("dashboard")
            selected_note = (
                f"<p>Current server: <strong>{escape(str(selected_guild.get('name') or 'Unknown'))}</strong> "
                f"(<span class='mono'>{escape(str(selected_guild.get('id') or ''))}</span>). "
                f"<a href='{escape(selected_target, quote=True)}'>Open dashboard</a>.</p>"
            )
        error_html = f"<p class='muted'>Could not load guild list: {escape(guild_error)}</p>" if guild_error else ""
        body = f"""
        <div class="card">
          <h2>Discord Servers</h2>
          <p>Select the Discord server you want to manage in the web GUI. Guild-scoped pages use the selected server context.</p>
          {selected_note}
          {error_html}
        </div>
        <div class="dash-grid">
          {"".join(cards) if cards else "<div class='card'><p class='muted'>No Discord servers are available to this bot right now.</p></div>"}
        </div>
        """
        return _render_page("Servers", body, user["email"], is_admin)

    @app.route("/admin/select-guild", methods=["POST"])
    @login_required
    def select_guild():
        user = _current_user()
        if _is_glinet_scoped_user(user):
            if not _set_selected_guild_id(""):
                flash("No primary Discord server is available for the GL.iNet-scoped role.", "error")
                return redirect(url_for("account"))
            return redirect(url_for("dashboard"))

        guild_id = str(request.form.get("guild_id", "")).strip()
        if not guild_id:
            flash("Choose a Discord server first.", "error")
            return redirect(url_for("guilds_page"))
        if not _set_selected_guild_id(guild_id):
            flash("That Discord server is no longer available to the bot.", "error")
            return redirect(url_for("guilds_page"))
        flash("Discord server context updated.", "success")
        if _is_glinet_scoped_user(user):
            return redirect(url_for("dashboard"))
        return redirect(url_for("dashboard"))

    @app.route("/admin/leave-guild", methods=["POST"])
    @admin_required
    def leave_guild():
        user = _current_user()
        guild_id = str(request.form.get("guild_id", "")).strip()
        if request.form.get("confirm", "").strip().lower() != "yes":
            flash("Leave-server confirmation is required.", "error")
            return redirect(url_for("guilds_page"))
        if not guild_id:
            flash("A Discord server must be selected.", "error")
            return redirect(url_for("guilds_page"))
        if not callable(on_leave_guild):
            flash("Leave-server callback is not configured in this runtime.", "error")
            return redirect(url_for("guilds_page"))

        response = on_leave_guild(guild_id, user["email"])
        if not isinstance(response, dict):
            flash("Invalid response from leave-server handler.", "error")
        elif response.get("ok"):
            flash(
                response.get(
                    "message",
                    "The bot has left the selected Discord server.",
                ),
                "success",
            )
        else:
            flash(response.get("error", "Failed to remove the bot from that Discord server."), "error")
        return redirect(url_for("guilds_page"))

    @app.route("/admin/dashboard", methods=["GET"])
    @login_required
    def dashboard():
        user = _current_user()
        is_admin = _is_admin_user(user)
        selection_redirect = _require_selected_guild_redirect()
        if selection_redirect is not None:
            return selection_redirect
        selected_guild = _selected_guild()
        selected_guild_name = str(selected_guild.get("name") or "selected server")

        role_key = str(user.get("role") or "").strip().lower()
        role_label = _user_role_label(role_key, is_admin=is_admin)
        wiki_url = _github_wiki_url()
        restart_enabled = _restart_enabled()

        def build_dashboard_card(
            title: str,
            description: str,
            href: str,
            button_label: str,
            *,
            external: bool = False,
            primary: bool = False,
            extra_html: str = "",
        ) -> str:
            link_target = " target='_blank' rel='noopener noreferrer'" if external else ""
            classes = "card dash-card"
            if primary:
                classes += " primary"
            return f"""
            <div class="{classes}">
              <h3>{escape(title)}</h3>
              <p class="muted">{escape(description)}</p>
              {extra_html}
              <div class="dash-actions">
                <a class="btn secondary" href="{escape(href, quote=True)}"{link_target}>{escape(button_label)}</a>
              </div>
            </div>
            """

        def render_dashboard_section(title: str, description: str, section_cards: list[str]) -> str:
            return f"""
            <section class="card dashboard-section">
              <div class="dashboard-section-head">
                <div>
                  <h3>{escape(title)}</h3>
                  <p class="muted">{escape(description)}</p>
                </div>
              </div>
              <div class="dashboard-section-grid">
                {"".join(section_cards)}
              </div>
            </section>
            """

        core_cards = [
            build_dashboard_card(
                "Guild Settings",
                "Set server-specific channels, welcome behavior, and override global defaults for this guild.",
                url_for("guild_settings"),
                "Open Guild Settings",
                primary=True,
                extra_html="<p class='dashboard-note'>Use this first when a server needs channels, feeds, or feature toggles that differ from the global defaults.</p>",
            ),
            build_dashboard_card(
                "Command Status",
                "Quickly enable or disable commands for the selected Discord server.",
                url_for("command_status"),
                "Open Command Status",
                primary=True,
            ),
            build_dashboard_card(
                "Command Permissions",
                "Set access mode per command and pick restricted roles from Discord role lists.",
                url_for("command_permissions"),
                "Open Permissions",
            ),
            build_dashboard_card(
                "Moderation",
                "Configure bad-word filtering, warning thresholds, timeout escalation, and the moderation log channel.",
                url_for("moderation_page"),
                "Open Moderation",
            ),
            build_dashboard_card(
                "Bot Profile",
                "Rename the bot, update the server nickname, and upload avatar assets.",
                url_for("bot_profile"),
                "Open Bot Profile",
            ),
        ]

        community_cards = [
            build_dashboard_card(
                "Member Activity",
                "Review top 20 member activity windows for the selected Discord server.",
                url_for("member_activity_page"),
                "Open Member Activity",
            ),
            build_dashboard_card(
                "Role Access",
                "Review and control invite links with their paired 6-digit access codes for the selected Discord server.",
                url_for("role_access_page"),
                "Open Role Access",
            ),
            build_dashboard_card(
                "Tag Responses",
                "Manage dynamic tag-response mappings and keep quick answers organized.",
                url_for("tag_responses"),
                "Open Tag Responses",
            ),
            build_dashboard_card(
                "Action History",
                "Review recent guild-scoped bot actions and utility activity.",
                url_for("actions_page"),
                "Open Actions",
            ),
            build_dashboard_card(
                "Bulk Role CSV",
                "Upload a CSV of names and assign a role with a detailed result report.",
                url_for("bulk_role_csv"),
                "Open Bulk CSV",
            ),
        ]

        feed_cards = [
            build_dashboard_card(
                "Reddit Feeds",
                "Map subreddit feeds to Discord channels and schedule automatic post checks.",
                url_for("reddit_feeds"),
                "Open Reddit Feeds",
            ),
            build_dashboard_card(
                "Service Monitors",
                "Manage direct website and API checks, plus Uptime Kuma-based imports and alerts.",
                url_for("service_monitors_page"),
                "Open Service Monitors",
            ),
            build_dashboard_card(
                "YouTube Subscriptions",
                "Map YouTube channels to Discord channels and post new uploads automatically.",
                url_for("youtube_subscriptions"),
                "Open YouTube",
            ),
            build_dashboard_card(
                "LinkedIn Profiles",
                "Map public LinkedIn profiles to Discord channels and post new profile activity automatically.",
                url_for("linkedin_subscriptions"),
                "Open LinkedIn",
            ),
            build_dashboard_card(
                "GL.iNet Beta Programs",
                "Monitor the GL.iNet beta testing page and notify a Discord channel when programs are added or removed.",
                url_for("beta_program_subscriptions"),
                "Open Beta Programs",
            ),
        ]

        operations_cards = [
            build_dashboard_card(
                "My Account",
                "Change your password, update your email, and manage profile display details.",
                url_for("account"),
                "Open My Account",
            ),
            build_dashboard_card(
                "Settings",
                "Edit global runtime environment settings shared across all Discord servers.",
                url_for("settings"),
                "Open Global Settings",
            ),
            build_dashboard_card(
                "Observability",
                "View container runtime metrics and tail recent log entries.",
                url_for("public_observability"),
                "Open Observability",
            ),
            build_dashboard_card(
                "Logs",
                "View recent runtime logs with log file selection and audit context.",
                url_for("admin_logs"),
                "Open Logs",
            ),
            build_dashboard_card(
                "Users",
                "Create web users, scope access, and reset credentials.",
                url_for("users"),
                "Open Users",
            ),
            build_dashboard_card(
                "Documentation",
                "Browse embedded docs for commands, deployment, and operations.",
                url_for("documentation"),
                "Open Docs",
            ),
        ]

        if wiki_url:
            operations_cards.append(
                build_dashboard_card(
                    "GitHub Wiki",
                    "Open the external project wiki in a new tab.",
                    wiki_url,
                    "Open GitHub Wiki",
                    external=True,
                )
            )

        if restart_enabled:
            if is_admin:
                operations_cards.append(
                    f"""
                    <div class="card dash-card">
                      <h3>Restart Container</h3>
                      <p class="muted">Apply runtime-level changes that require a process restart.</p>
                      <p class="dashboard-note">Use this after changes that affect startup-time settings, Discord sync, or container-bound runtime behavior.</p>
                      <form method="post" action="{escape(url_for("restart_service"), quote=True)}"
                        onsubmit="return confirm('WARNING: This will restart the container and temporarily disconnect the bot. Continue?');">
                        <input type="hidden" name="confirm" value="yes" />
                        <button class="btn danger" type="submit">Restart Container</button>
                      </form>
                    </div>
                    """
                )
            else:
                operations_cards.append(
                    """
                    <div class="card dash-card">
                      <h3>Restart Container</h3>
                      <p class="muted">Read-only accounts can view this option but cannot restart the container.</p>
                      <button class="btn danger" type="button" disabled>Restart Container</button>
                    </div>
                    """
                )

        role_scope_text = (
            "Pinned to the primary GL.iNet Community Discord server."
            if role_key in {"glinet_read_only", "glinet_rw"}
            else "Can switch between managed Discord servers."
        )
        management_text = (
            "This account can apply configuration changes directly from the dashboard links."
            if is_admin or role_key == "glinet_rw"
            else "This account can review configuration safely. Write actions remain restricted."
        )
        startup_note = (
            "Some Discord metadata changes still require a container restart after saving."
            if is_admin
            else "Use the grouped sections below to reach the areas this account is allowed to manage."
        )
        recent_navigation_html = "".join(
            f"<div><a href='{escape(str(item.get('href') or ''), quote=True)}'>{escape(str(item.get('label') or 'Open page'))}</a></div>"
            for item in _recent_navigation_entries()
        ) or "No recent pages yet."

        body = f"""
        <div class="dashboard-shell">
          <section class="dashboard-hero">
            <div class="card dashboard-hero-main">
              <div>
                <h2>Dashboard</h2>
                <p class="dashboard-hero-lead">Operational control for <strong>{escape(selected_guild_name)}</strong>. The sections below separate guild controls, community tools, feed automations, and runtime operations so the most-used actions are easier to reach on desktop, tablet, and mobile.</p>
              </div>
              <div class="dashboard-pill-row">
                <div class="dashboard-pill">
                  <strong>Server</strong>
                  <span>{escape(selected_guild_name)}</span>
                </div>
                <div class="dashboard-pill">
                  <strong>Access</strong>
                  <span>{escape(role_label)}</span>
                </div>
                <div class="dashboard-pill">
                  <strong>Scope</strong>
                  <span>{escape(role_scope_text)}</span>
                </div>
                <div class="dashboard-pill">
                  <strong>Restart</strong>
                  <span>{'Enabled' if restart_enabled else 'Disabled'}</span>
                </div>
              </div>
              <p class="dashboard-note">{escape(startup_note)}</p>
            </div>
            <div class="card dashboard-hero-side">
              <div>
                <h3>Quick Notes</h3>
                <p class="muted">Use the grouped cards below instead of hunting through one long grid.</p>
              </div>
              <div class="dashboard-list">
                <div class="dashboard-list-item">
                  <strong>Configuration</strong>
                  <div class="muted">{escape(management_text)}</div>
                </div>
                <div class="dashboard-list-item">
                  <strong>Most common path</strong>
                  <div class="muted"><a href="{escape(url_for('guild_settings'), quote=True)}">Guild Settings</a>, then <a href="{escape(url_for('command_status'), quote=True)}">Command Status</a>, then feed pages for channel routing.</div>
                </div>
                <div class="dashboard-list-item">
                  <strong>Recent pages</strong>
                  <div class="muted">{recent_navigation_html}</div>
                </div>
                <div class="dashboard-list-item">
                  <strong>Documentation</strong>
                  <div class="muted">{'GitHub Wiki is linked here as an external reference.' if wiki_url else 'Embedded docs remain available from this dashboard.'}</div>
                </div>
              </div>
            </div>
          </section>
          {render_dashboard_section("Core Controls", "Primary guild-level configuration and command access controls.", core_cards)}
          {render_dashboard_section("Community Tools", "Member-facing utilities, access workflows, and guild operational history.", community_cards)}
          {render_dashboard_section("Notification Feeds", "External monitors and feed-to-channel routing for the selected guild.", feed_cards)}
          {render_dashboard_section("Runtime And Administration", "Account, logging, environment, and maintenance controls.", operations_cards)}
        </div>
        """

        return _render_page("Dashboard", body, user["email"], is_admin)

    @app.route("/admin/restart", methods=["POST"])
    @admin_required
    def restart_service():
        user = _current_user()
        if request.form.get("confirm", "").strip().lower() != "yes":
            flash("Restart confirmation is required.", "error")
        elif not _restart_enabled():
            flash("Restart is disabled via WEB_RESTART_ENABLED.", "error")
        elif not callable(on_request_restart):
            flash("Restart callback is not configured in this runtime.", "error")
        else:
            response = on_request_restart(user["email"])
            if not isinstance(response, dict):
                flash("Invalid response from restart handler.", "error")
            elif response.get("ok"):
                flash(
                    response.get(
                        "message",
                        "Restart requested. The container will restart shortly.",
                    ),
                    "success",
                )
            else:
                flash(response.get("error", "Failed to request restart."), "error")
        return redirect(url_for("dashboard"))

    def _render_observability_view(page_title: str):
        metrics, history_items = _collect_and_store_observability_snapshot()
        history_rows = _build_observability_history_summary(history_items, metrics)
        history_sample_count = len(history_items)
        history_oldest = history_items[0] if history_items else {}
        history_newest = history_items[-1] if history_items else {}
        history_oldest_label = str(history_oldest.get("sampled_at") or "n/a")
        history_newest_label = str(history_newest.get("sampled_at") or "n/a")
        history_table_rows = []
        for row in history_rows:
            history_table_rows.append(
                "<tr>"
                f"<td>{escape(str(row.get('label') or 'n/a'))}</td>"
                f"<td class='mono'>{escape(str(row.get('current') or 'n/a'))}</td>"
                f"<td class='mono'>{escape(str(row.get('min') or 'n/a'))}</td>"
                f"<td class='mono'>{escape(str(row.get('avg') or 'n/a'))}</td>"
                f"<td class='mono'>{escape(str(row.get('max') or 'n/a'))}</td>"
                "</tr>"
            )
        selected_refresh_seconds = _parse_auto_refresh_seconds(
            request.args.get("refresh", "0"),
            default_value=0,
        )
        refresh_options_html = []
        for refresh_seconds in AUTO_REFRESH_INTERVAL_OPTIONS:
            label = "Manual (off)" if refresh_seconds == 0 else f"{refresh_seconds} second{'s' if refresh_seconds != 1 else ''}"
            selected_attr = " selected" if refresh_seconds == selected_refresh_seconds else ""
            refresh_options_html.append(f"<option value='{refresh_seconds}'{selected_attr}>{escape(label)}</option>")

        process_cpu_pct = metrics.get("process_cpu_percent")
        process_cpu_pct_text = (
            f"{float(process_cpu_pct):.2f}%" if isinstance(process_cpu_pct, (int, float)) else "n/a (refresh again for delta sample)"
        )

        memory_usage_bytes = metrics.get("memory_usage_bytes")
        memory_limit_bytes = metrics.get("memory_limit_bytes")
        memory_pct = metrics.get("memory_percent")
        memory_usage_text = _format_bytes(memory_usage_bytes)
        memory_limit_text = _format_bytes(memory_limit_bytes)
        memory_pct_text = f"{float(memory_pct):.2f}%" if isinstance(memory_pct, (int, float)) else "n/a"

        sample_interval = metrics.get("sample_interval_seconds")
        sample_interval_text = f"{float(sample_interval):.2f}s" if isinstance(sample_interval, (int, float)) else "first sample"
        auto_refresh_note = (
            f"Auto refresh enabled every {selected_refresh_seconds} second{'s' if selected_refresh_seconds != 1 else ''}."
            if selected_refresh_seconds > 0
            else "Auto refresh is disabled."
        )
        auto_refresh_script = (
            f"""
            <script>
              (function() {{
                var intervalMs = {selected_refresh_seconds * 1000};
                if (intervalMs <= 0) {{
                  return;
                }}
                window.setTimeout(function() {{
                  window.location.reload();
                }}, intervalMs);
              }})();
            </script>
            """
            if selected_refresh_seconds > 0
            else ""
        )

        body = f"""
        <div class="card">
          <h2>{escape(page_title)}</h2>
          <p class="muted">Runtime snapshot for process/container activity. Metrics update each time you refresh this page.</p>
          <p class="muted">This page is read-only and safe to share publicly for status visibility. Log viewing requires web GUI login at <span class="mono">/admin/logs</span>.</p>
          <p class="muted">Sample captured: {escape(str(metrics.get("sampled_at") or "n/a"))}. Sample interval: {escape(sample_interval_text)}.</p>
          <p class="muted">Historical metrics retention: {OBSERVABILITY_HISTORY_RETENTION_HOURS} hours. Collection interval: every {OBSERVABILITY_HISTORY_SAMPLE_SECONDS} seconds.</p>
          <form method="get" action="{escape(url_for("public_observability"), quote=True)}" style="margin-top:10px;">
            <label for="status_refresh">Auto refresh</label>
            <select id="status_refresh" name="refresh" onchange="this.form.submit();">
              {"".join(refresh_options_html)}
            </select>
            <noscript>
              <button class="btn" type="submit" style="margin-left:8px;">Apply</button>
            </noscript>
          </form>
          <p class="muted">{escape(auto_refresh_note)}</p>
        </div>

        <div class="card metric-card">
          <h3>Last {OBSERVABILITY_HISTORY_RETENTION_HOURS} Hours Summary</h3>
          <p class="muted">Samples stored: {history_sample_count}. Window: {escape(history_oldest_label)} to {escape(history_newest_label)}.</p>
          <table class="metric-table history-table">
            <thead>
              <tr><th>Metric</th><th>Current</th><th>Min</th><th>Avg</th><th>Max</th></tr>
            </thead>
            <tbody>
              {"".join(history_table_rows)}
            </tbody>
          </table>
        </div>

        <div class="grid">
          <div class="card metric-card">
            <h3>CPU</h3>
            <table class="metric-table">
              <tbody>
                <tr><td>Process CPU (delta)</td><td class="mono">{escape(process_cpu_pct_text)}</td></tr>
                <tr><td>Process CPU time (total)</td><td class="mono">{escape(f"{float(metrics.get('process_cpu_total') or 0.0):.2f}s")}</td></tr>
                <tr><td>Container CPU time (cgroup)</td><td class="mono">{escape(f"{float(metrics.get('cgroup_cpu_seconds') or 0.0):.2f}s" if metrics.get("cgroup_cpu_seconds") is not None else "n/a")}</td></tr>
              </tbody>
            </table>
          </div>
          <div class="card metric-card">
            <h3>Memory</h3>
            <table class="metric-table">
              <tbody>
                <tr><td>Process RSS</td><td class="mono">{escape(_format_bytes(metrics.get("rss_bytes")))}</td></tr>
                <tr><td>Container memory usage</td><td class="mono">{escape(memory_usage_text)}</td></tr>
                <tr><td>Container memory limit</td><td class="mono">{escape(memory_limit_text)}</td></tr>
                <tr><td>Memory usage percent</td><td class="mono">{escape(memory_pct_text)}</td></tr>
              </tbody>
            </table>
          </div>
          <div class="card metric-card">
            <h3>I/O</h3>
            <table class="metric-table">
              <tbody>
                <tr><td>Read bytes (total)</td><td class="mono">{escape(_format_bytes(metrics.get("io_read_bytes")))}</td></tr>
                <tr><td>Write bytes (total)</td><td class="mono">{escape(_format_bytes(metrics.get("io_write_bytes")))}</td></tr>
                <tr><td>Read rate</td><td class="mono">{escape(str(metrics.get("io_read_rate") or "n/a"))}</td></tr>
                <tr><td>Write rate</td><td class="mono">{escape(str(metrics.get("io_write_rate") or "n/a"))}</td></tr>
              </tbody>
            </table>
          </div>
          <div class="card metric-card">
            <h3>Network</h3>
            <table class="metric-table">
              <tbody>
                <tr><td>RX bytes (total)</td><td class="mono">{escape(_format_bytes(metrics.get("net_rx_bytes")))}</td></tr>
                <tr><td>TX bytes (total)</td><td class="mono">{escape(_format_bytes(metrics.get("net_tx_bytes")))}</td></tr>
                <tr><td>RX rate</td><td class="mono">{escape(str(metrics.get("net_rx_rate") or "n/a"))}</td></tr>
                <tr><td>TX rate</td><td class="mono">{escape(str(metrics.get("net_tx_rate") or "n/a"))}</td></tr>
              </tbody>
            </table>
          </div>
          <div class="card metric-card">
            <h3>Uptime</h3>
            <table class="metric-table">
              <tbody>
                <tr><td>Process uptime</td><td class="mono">{escape(_format_uptime(metrics.get("uptime_seconds") or 0))}</td></tr>
              </tbody>
            </table>
          </div>
        </div>
        {auto_refresh_script}
        """
        return body

    def _render_log_view():
        log_dir = Path(str(os.getenv("LOG_DIR", "/logs")).strip() or "/logs")
        selected_refresh_seconds = _parse_auto_refresh_seconds(
            request.args.get("refresh", "0"),
            default_value=0,
        )
        allowed_log_paths = _resolve_observability_log_paths(log_dir)
        log_selection_map = {
            "bot": "bot.log",
            "bot_channel": "bot_log.log",
            "container_errors": "container_errors.log",
            "web_gui_audit": "web_gui_audit.log",
        }
        label_by_filename = {name: label for name, label in OBSERVABILITY_LOG_OPTIONS}
        valid_selection_map = {key: filename for key, filename in log_selection_map.items() if filename in allowed_log_paths}

        requested_selection = str(request.args.get("log", "container_errors") or "container_errors").strip()
        # Backward compatibility for older links that still pass filename.
        if requested_selection in label_by_filename:
            reverse_map = {value: key for key, value in log_selection_map.items()}
            requested_selection = reverse_map.get(requested_selection, "")

        if requested_selection not in valid_selection_map:
            requested_selection = "container_errors"
        if requested_selection not in valid_selection_map:
            requested_selection = next(iter(valid_selection_map.keys()), "")

        selected_log = valid_selection_map.get(requested_selection, "")
        selected_log_path = allowed_log_paths.get(selected_log)
        if not selected_log_path:
            log_preview = "Invalid log selection."
        else:
            log_preview = _read_latest_log_lines(
                selected_log_path,
                line_limit=OBSERVABILITY_LOG_LINE_LIMIT,
            )

        options_html = []
        for selection_key, file_name in log_selection_map.items():
            label = label_by_filename.get(file_name, file_name)
            if file_name not in allowed_log_paths:
                continue
            selected_attr = " selected" if selection_key == requested_selection else ""
            options_html.append(
                f"<option value='{escape(selection_key, quote=True)}'{selected_attr}>{escape(label)} ({escape(file_name)})</option>"
            )
        refresh_options_html = []
        for refresh_seconds in AUTO_REFRESH_INTERVAL_OPTIONS:
            label = "Manual (off)" if refresh_seconds == 0 else f"{refresh_seconds} second{'s' if refresh_seconds != 1 else ''}"
            selected_attr = " selected" if refresh_seconds == selected_refresh_seconds else ""
            refresh_options_html.append(f"<option value='{refresh_seconds}'{selected_attr}>{escape(label)}</option>")
        auto_refresh_note = (
            f"Auto refresh enabled every {selected_refresh_seconds} second{'s' if selected_refresh_seconds != 1 else ''}."
            if selected_refresh_seconds > 0
            else "Auto refresh is disabled."
        )
        auto_refresh_script = (
            f"""
            <script>
              (function() {{
                var intervalMs = {selected_refresh_seconds * 1000};
                if (intervalMs <= 0) {{
                  return;
                }}
                window.setTimeout(function() {{
                  window.location.reload();
                }}, intervalMs);
              }})();
            </script>
            """
            if selected_refresh_seconds > 0
            else ""
        )
        export_logs_href = url_for("admin_logs_export")

        return f"""
        <div class="card">
          <h2>Log Viewer</h2>
          <p class="muted">Login required. Showing the most recent {OBSERVABILITY_LOG_LINE_LIMIT} lines from {escape(selected_log)}.</p>
          <p class="muted">Sensitive values are redacted in this preview.</p>
          <form method="get" action="{escape(url_for("admin_logs"), quote=True)}">
            <label for="log">Select log</label>
            <select id="log" name="log" onchange="this.form.submit();">
              {"".join(options_html)}
            </select>
            <label for="log_refresh" style="margin-left:10px;">Auto refresh</label>
            <select id="log_refresh" name="refresh" onchange="this.form.submit();">
              {"".join(refresh_options_html)}
            </select>
            <div style="margin-top:14px;">
              <button class="btn" type="submit">Refresh</button>
            </div>
          </form>
          <p class="muted">{escape(auto_refresh_note)}</p>
          <div class="dash-actions" style="margin-top:14px;">
            <a class="btn secondary" href="{escape(export_logs_href, quote=True)}">Export All Logs</a>
          </div>
          <div style="margin-top:14px;">
            <textarea readonly style="min-height:520px;">{escape(log_preview)}</textarea>
          </div>
        </div>
        {auto_refresh_script}
        """

    def _prune_expired_log_exports(log_dir: Path):
        export_dir = log_dir / "exports"
        cutoff_timestamp = time.time() - (LOG_EXPORT_RETENTION_HOURS * 3600)
        if not export_dir.exists():
            return export_dir
        for export_path in export_dir.glob("discord_bot_logs_*.zip"):
            try:
                if export_path.is_file() and export_path.stat().st_mtime < cutoff_timestamp:
                    export_path.unlink()
            except OSError:
                continue
        return export_dir

    def _schedule_log_export_cleanup(archive_path: Path):
        def _cleanup():
            try:
                archive_path.unlink(missing_ok=True)
            except OSError:
                pass

        cleanup_timer = threading.Timer(LOG_EXPORT_RETENTION_HOURS * 3600, _cleanup)
        cleanup_timer.daemon = True
        cleanup_timer.start()

    def _build_logs_export_payload():
        log_dir = Path(str(os.getenv("LOG_DIR", "/logs")).strip() or "/logs")
        allowed_log_paths = _resolve_observability_log_paths(log_dir)
        if not allowed_log_paths:
            return None
        export_dir = _prune_expired_log_exports(log_dir)
        export_dir.mkdir(parents=True, exist_ok=True)
        _chmod_if_possible(export_dir, 0o700)

        exported_count = 0
        generated_at = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        archive_path = export_dir / f"discord_bot_logs_{generated_at}.zip"
        with zipfile.ZipFile(archive_path, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
            for filename, _label in OBSERVABILITY_LOG_OPTIONS:
                log_path = allowed_log_paths.get(filename)
                if log_path is None or not log_path.exists() or not log_path.is_file():
                    continue
                try:
                    archive.write(log_path, arcname=filename)
                except OSError:
                    continue
                exported_count += 1
            archive.writestr(
                "manifest.txt",
                "\n".join(
                    [
                        "GL.iNet UnOfficial Discord Bot log export",
                        f"generated_at_utc={generated_at}",
                        f"log_dir={log_dir}",
                        f"exported_files={exported_count}",
                    ]
                )
                + "\n",
            )

        if exported_count <= 0:
            try:
                archive_path.unlink(missing_ok=True)
            except OSError:
                pass
            return None
        _chmod_if_possible(archive_path, 0o600)
        _schedule_log_export_cleanup(archive_path)
        return {
            "filename": archive_path.name,
            "content_type": "application/zip",
            "path": str(archive_path),
        }

    @app.route("/status", methods=["GET"])
    def public_observability():
        body = _render_observability_view(
            page_title="Status Observability",
        )
        return _render_page(
            "Status",
            body,
            "",
            False,
            "",
        )

    @app.route("/staus", methods=["GET"])
    def public_observability_alias():
        return redirect(url_for("public_observability", **request.args.to_dict(flat=True)))

    @app.route("/status/everything", methods=["GET"])
    def public_observability_everything():
        return redirect(url_for("public_observability", **request.args.to_dict(flat=True)))

    @app.route("/admin/observability", methods=["GET"])
    def observability():
        return redirect(url_for("public_observability", **request.args.to_dict(flat=True)))

    @app.route("/admin/logs", methods=["GET"])
    @login_required
    def admin_logs():
        user = _current_user()
        _prune_expired_log_exports(Path(str(os.getenv("LOG_DIR", "/logs")).strip() or "/logs"))
        body = _render_log_view()
        return _render_page(
            "Log Viewer",
            body,
            user["email"],
            bool(user.get("is_admin")),
            str(user.get("display_name") or ""),
        )

    @app.route("/admin/logs/export", methods=["GET"])
    @login_required
    def admin_logs_export():
        payload = _build_logs_export_payload()
        if not payload:
            flash("No runtime log files are available to export.", "error")
            return redirect(url_for("admin_logs"))
        return send_file(
            str(payload["path"]),
            mimetype=str(payload.get("content_type") or "application/octet-stream"),
            as_attachment=True,
            download_name=str(payload.get("filename") or "logs.zip"),
        )

    @app.route("/admin/actions", methods=["GET"])
    @login_required
    def actions_page():
        user = _current_user()
        selection_redirect = _require_selected_guild_redirect()
        if selection_redirect is not None:
            return selection_redirect
        selected_guild = _selected_guild() or {}
        selected_guild_id = str(selected_guild.get("id") or "")
        payload = (
            on_get_actions(selected_guild_id)
            if callable(on_get_actions)
            else {"ok": False, "error": "Action history callback is not configured."}
        )
        actions = payload.get("actions", []) if isinstance(payload, dict) else []
        actions_error = str(payload.get("error") or "") if isinstance(payload, dict) and not payload.get("ok") else ""
        rows = []
        for item in actions:
            rows.append(
                "<tr>"
                f"<td class='mono'>{escape(format_timestamp_display(item.get('created_at'), blank=''))}</td>"
                f"<td>{escape(str(item.get('action') or ''))}</td>"
                f"<td>{escape(str(item.get('status') or ''))}</td>"
                f"<td>{escape(str(item.get('moderator') or ''))}</td>"
                f"<td>{escape(str(item.get('target') or ''))}</td>"
                f"<td>{escape(str(item.get('reason') or ''))}</td>"
                "</tr>"
            )
        body = f"""
        <div class="card">
          <h2>Action History</h2>
          <p class="muted">Selected server: <strong>{escape(str(selected_guild.get("name") or "Unknown"))}</strong></p>
          <p class="muted">Recent bot actions recorded for this server.</p>
          {"<p class='muted'>" + escape(actions_error) + "</p>" if actions_error else ""}
        </div>
        <div class="card">
          <table class="history-table">
            <thead><tr><th>Created</th><th>Action</th><th>Status</th><th>Actor</th><th>Target</th><th>Reason</th></tr></thead>
            <tbody>{"".join(rows) if rows else "<tr><td colspan='6' class='muted'>No action history recorded yet.</td></tr>"}</tbody>
          </table>
        </div>
        """
        return _render_page(
            "Action History",
            body,
            user["email"],
            bool(user.get("is_admin")),
            str(user.get("display_name") or ""),
        )

    @app.route("/admin/member-activity", methods=["GET"])
    @login_required
    def member_activity_page():
        user = _current_user()
        selection_redirect = _require_selected_guild_redirect()
        if selection_redirect is not None:
            return selection_redirect
        selected_guild = _selected_guild() or {}
        selected_guild_id = str(selected_guild.get("id") or "")
        selected_role_id = str(request.args.get("role_id", "") or "").strip()
        payload = (
            on_get_member_activity(selected_guild_id, selected_role_id)
            if callable(on_get_member_activity)
            else {"ok": False, "error": "Member activity callback is not configured."}
        )
        discord_catalog = on_get_discord_catalog(selected_guild_id) if callable(on_get_discord_catalog) else None
        windows = payload.get("windows", []) if isinstance(payload, dict) else []
        activity_error = str(payload.get("error") or "") if isinstance(payload, dict) and not payload.get("ok") else ""
        top_limit = int(payload.get("top_limit") or 20) if isinstance(payload, dict) else 20
        excluded_role_ids = {
            str(item)
            for item in (payload.get("excluded_role_ids", []) if isinstance(payload, dict) else [])
            if str(item).strip()
        }
        excluded_role_names = {
            str(item).strip().casefold()
            for item in (payload.get("excluded_role_names", []) if isinstance(payload, dict) else [])
            if str(item).strip()
        }
        role_options = [{"value": "", "label": "All eligible members"}]
        if isinstance(discord_catalog, dict) and discord_catalog.get("ok"):
            for role in discord_catalog.get("roles", []) or []:
                role_id_value = str(role.get("id") or "").strip()
                role_name = str(role.get("name") or "").strip()
                if not role_id_value or not role_name:
                    continue
                if role_id_value in excluded_role_ids or role_name.casefold() in excluded_role_names:
                    continue
                role_options.append({"value": role_id_value, "label": f"@{role_name}"})
        selected_role_label = "All eligible members"
        for option in role_options:
            if option["value"] == selected_role_id:
                selected_role_label = option["label"]
                break
        role_filter_select = _render_fixed_select_input(
            "role_id",
            selected_role_id,
            role_options,
            placeholder="All eligible members",
        )
        export_html = ""
        if callable(on_export_member_activity):
            export_url = url_for("member_activity_export")
            if selected_role_id:
                export_url = url_for("member_activity_export", role_id=selected_role_id)
            export_html = (
                f"<div class='card'>"
                f"<h3>Export Activity Data</h3>"
                f"<p class='muted'>Download the selected server's member activity as a compressed ZIP archive.</p>"
                f"<a class='btn secondary' href='{escape(export_url, quote=True)}'>Download Activity Export</a>"
                f"</div>"
            )

        window_cards = []
        for window in windows:
            members = window.get("members", []) if isinstance(window, dict) else []
            rows = []
            for member in members:
                display_name = str(member.get("display_name") or member.get("username") or member.get("user_id") or "Unknown")
                username = str(member.get("username") or "")
                secondary_name = f"<div class='muted mono'>{escape(username)}</div>" if username and username != display_name else ""
                rows.append(
                    "<tr>"
                    f"<td>{escape(str(member.get('rank') or ''))}</td>"
                    f"<td><strong>{escape(display_name)}</strong>{secondary_name}</td>"
                    f"<td>{escape(str(member.get('message_count') or 0))}</td>"
                    f"<td>{escape(str(member.get('active_days') or 0))}</td>"
                    f"<td class='mono'>{escape(format_timestamp_display(member.get('last_message_at')))}</td>"
                    "</tr>"
                )
            window_cards.append(
                f"""
                <div class="card table-scroll">
                  <h3>{escape(str(window.get("label") or "Activity Window"))}</h3>
                  <table class="history-table">
                    <thead>
                      <tr>
                        <th>Rank</th>
                        <th>Member</th>
                        <th>Messages</th>
                        <th>Active Days</th>
                        <th>Last Seen</th>
                      </tr>
                    </thead>
                    <tbody>{"".join(rows) if rows else "<tr><td colspan='5' class='muted'>No member activity recorded in this window yet.</td></tr>"}</tbody>
                  </table>
                </div>
                """
            )

        body = f"""
        <div class="card">
          <h2>Member Activity</h2>
          <p class="muted">Selected server: <strong>{escape(str(selected_guild.get("name") or "Unknown"))}</strong></p>
          <form method="get" style="margin:14px 0;">
            <div style="display:grid; grid-template-columns:minmax(220px, 360px) auto; gap:10px; align-items:end;">
              <div>
                <label for="member-activity-role-filter"><strong>Top 20 by role</strong></label>
                {role_filter_select.replace("<select ", "<select id='member-activity-role-filter' ")}
              </div>
              <div><button class="btn secondary" type="submit">Apply Filter</button></div>
            </div>
          </form>
          <p class="muted">Showing the top {escape(str(top_limit))} eligible members by message activity for each time window.</p>
          <p class="muted">Current filter: <strong>{escape(selected_role_label)}</strong>. Members with moderator/admin/employee-style access are excluded from rankings.</p>
          <p class="muted">Columns show exact messages sent in the selected period, active days in that period, and the most recent message timestamp.</p>
          {"<p class='muted'>" + escape(activity_error) + "</p>" if activity_error else ""}
        </div>
        {"".join(window_cards) if window_cards else "<div class='card'><p class='muted'>No member activity windows are available yet.</p></div>"}
        {export_html}
        """
        return _render_page(
            "Member Activity",
            body,
            user["email"],
            bool(user.get("is_admin")),
            str(user.get("display_name") or ""),
        )

    @app.route("/admin/member-activity/export", methods=["GET"])
    @login_required
    def member_activity_export():
        selection_redirect = _require_selected_guild_redirect()
        if selection_redirect is not None:
            return selection_redirect
        selected_guild = _selected_guild() or {}
        selected_guild_id = str(selected_guild.get("id") or "")
        selected_role_id = str(request.args.get("role_id", "") or "").strip()
        if not callable(on_export_member_activity):
            flash("Member activity export is not configured.", "error")
            return redirect(url_for("member_activity_page"))
        payload = on_export_member_activity(selected_guild_id, selected_role_id)
        if not isinstance(payload, dict) or not payload.get("ok"):
            flash(
                str(payload.get("error") or "Failed to export member activity.")
                if isinstance(payload, dict)
                else "Failed to export member activity.",
                "error",
            )
            return redirect(url_for("member_activity_page"))
        file_name = str(payload.get("filename") or "member_activity.zip")
        content_type = str(payload.get("content_type") or "application/octet-stream")
        data = payload.get("data") or b""
        return send_file(
            BytesIO(data),
            mimetype=content_type,
            as_attachment=True,
            download_name=file_name,
        )

    @app.route("/admin/documentation", methods=["GET"])
    @login_required
    def documentation():
        user = _current_user()
        page_paths = list(_get_wiki_page_map().values())

        def sort_key(path: Path):
            if path.stem.lower() == "home":
                return (0, path.stem.casefold())
            return (1, path.stem.casefold())

        page_paths.sort(key=sort_key)
        if not page_paths:
            body = "<div class='card'><h2>Documentation</h2><p class='muted'>No wiki pages were found in the runtime image.</p></div>"
            return _render_page("Documentation", body, user["email"], bool(user.get("is_admin")))

        page_rows = []
        for path in page_paths:
            slug = path.stem
            label = slug.replace("-", " ")
            page_rows.append(
                f"<li><a href='{url_for('documentation_page', page_slug=slug)}'>{escape(label)}</a>"
                f" <span class='muted mono'>({escape(path.name)})</span></li>"
            )
        body = (
            "<div class='card'><h2>Documentation</h2>"
            "<p class='muted'>Browse wiki pages packaged with this bot image.</p>"
            f"<ul>{''.join(page_rows)}</ul></div>"
        )
        return _render_page("Documentation", body, user["email"], bool(user.get("is_admin")))

    @app.route("/admin/documentation/<page_slug>", methods=["GET"])
    @login_required
    def documentation_page(page_slug: str):
        user = _current_user()
        if not re.fullmatch(r"[A-Za-z0-9_-]+", page_slug or ""):
            return {"ok": False, "error": "Invalid documentation page."}, 404

        page_path = _get_wiki_page_map().get(page_slug.casefold())
        if page_path is None:
            return {"ok": False, "error": "Documentation page not found."}, 404
        if not page_path.exists() or not page_path.is_file() or page_path.name.startswith("_"):
            return {"ok": False, "error": "Documentation page not found."}, 404
        try:
            resolved = page_path.resolve()
        except OSError:
            return {"ok": False, "error": "Documentation page not found."}, 404
        if not _is_within_wiki_dir(resolved):
            return {"ok": False, "error": "Documentation page not found."}, 404

        content = resolved.read_text(encoding="utf-8", errors="replace")
        title = page_slug.replace("-", " ")
        first_line = content.splitlines()[0].strip() if content else ""
        if first_line.startswith("#"):
            title = first_line.lstrip("#").strip() or title
        body = (
            "<div class='card'>"
            f"<h2>{escape(title)}</h2>"
            f"<p><a href='{url_for('documentation')}'>Back to documentation index</a></p>"
            f"<pre class='mono' style='white-space:pre-wrap;line-height:1.45;'>{escape(content)}</pre>"
            "</div>"
        )
        return _render_page(title, body, user["email"], bool(user.get("is_admin")))

    @app.route("/admin/wiki", methods=["GET"])
    @login_required
    def wiki_viewer():
        return redirect(url_for("documentation"))

    @app.route("/admin/bot-profile", methods=["GET", "POST"])
    @login_required
    def bot_profile():
        user = _current_user()
        selection_redirect = _require_selected_guild_redirect()
        if selection_redirect is not None:
            return selection_redirect
        selected_guild = _selected_guild() or {}
        selected_guild_id = str(selected_guild.get("id") or "")
        max_avatar_upload_bytes = _get_int_env("WEB_AVATAR_MAX_UPLOAD_BYTES", 2 * 1024 * 1024, minimum=1024)
        profile = on_get_bot_profile(selected_guild_id) if callable(on_get_bot_profile) else {"ok": False, "error": "Not configured"}

        if request.method == "POST":
            action = str(request.form.get("action", "avatar")).strip().lower()
            if action in {"nickname", "username"}:
                if not callable(on_update_bot_profile):
                    flash("Bot profile update callback is not configured.", "error")
                else:
                    response = None
                    if action == "username":
                        username_input = str(request.form.get("bot_name", ""))
                        username_value = username_input.strip() or None
                        if username_value is None:
                            flash("Enter a global bot username to update.", "error")
                        else:
                            response = on_update_bot_profile(
                                selected_guild_id,
                                username_value,
                                None,
                                False,
                                user["email"],
                            )
                    else:
                        server_nickname_input = str(request.form.get("server_nickname", ""))
                        clear_server_nickname = str(request.form.get("clear_server_nickname", "")).strip().lower() in {
                            "1",
                            "true",
                            "yes",
                            "on",
                        }
                        server_nickname_value = server_nickname_input.strip() or None
                        response = on_update_bot_profile(
                            selected_guild_id,
                            None,
                            server_nickname_value,
                            clear_server_nickname,
                            user["email"],
                        )
                    if response is None:
                        pass
                    elif not isinstance(response, dict):
                        flash("Invalid response from bot profile update handler.", "error")
                    elif not response.get("ok"):
                        flash(
                            response.get("error", "Failed to update bot profile."),
                            "error",
                        )
                    else:
                        profile = response
                        flash(
                            str(response.get("message") or "Bot profile updated successfully."),
                            "success",
                        )
            elif action == "avatar":
                uploaded_file = request.files.get("avatar_file")
                if uploaded_file is None or not uploaded_file.filename:
                    flash("Avatar image file is required.", "error")
                elif not callable(on_update_bot_avatar):
                    flash("Avatar update callback is not configured.", "error")
                else:
                    payload = uploaded_file.read()
                    lowered_name = uploaded_file.filename.lower()
                    allowed_extensions = (".png", ".jpg", ".jpeg", ".webp", ".gif")
                    if not payload:
                        flash("Uploaded avatar file is empty.", "error")
                    elif len(payload) > max_avatar_upload_bytes:
                        flash(
                            f"Avatar file is too large ({len(payload)} bytes). Max allowed is {max_avatar_upload_bytes} bytes.",
                            "error",
                        )
                    elif not lowered_name.endswith(allowed_extensions):
                        flash("Avatar must be PNG, JPG, JPEG, WEBP, or GIF.", "error")
                    else:
                        response = on_update_bot_avatar(payload, uploaded_file.filename, user["email"])
                        if not isinstance(response, dict):
                            flash("Invalid response from avatar update handler.", "error")
                        elif not response.get("ok"):
                            flash(
                                response.get("error", "Failed to update bot avatar."),
                                "error",
                            )
                        else:
                            profile = response
                            flash("Bot avatar updated successfully.", "success")
            else:
                flash("Invalid bot profile action.", "error")

        profile_html = ""
        if isinstance(profile, dict) and profile.get("ok"):
            avatar_url = str(profile.get("avatar_url") or "").strip()
            username = str(profile.get("name") or "unknown")
            global_name = str(profile.get("global_name") or profile.get("display_name") or "Not set")
            server_display_name = str(profile.get("server_display_name") or profile.get("display_name") or username)
            server_nickname = str(profile.get("server_nickname") or "Not set")
            guild_name = str(profile.get("guild_name") or "Configured guild unavailable")
            avatar_image = (
                f"<img src='{escape(avatar_url, quote=True)}' alt='Bot avatar' "
                "style='max-width:160px;max-height:160px;border-radius:12px;border:1px solid #d1d5db;' />"
                if avatar_url
                else "<p class='muted'>No avatar is currently set.</p>"
            )
            profile_html = f"""
            <div class="card">
              <h3>Current Bot Profile</h3>
              <p><strong>Username:</strong> {escape(username)}</p>
              <p><strong>Global Display Name:</strong> {escape(global_name)}</p>
              <p><strong>Server Display Name:</strong> {escape(server_display_name)}</p>
              <p><strong>Server Nickname:</strong> {escape(server_nickname)}</p>
              <p><strong>Guild:</strong> {escape(guild_name)}</p>
              <p><strong>ID:</strong> <span class="mono">{escape(str(profile.get("id") or "unknown"))}</span></p>
              {avatar_image}
            </div>
            """
        else:
            profile_error = str(profile.get("error") if isinstance(profile, dict) else "Unable to load profile.")
            profile_html = f"<div class='card'><p class='muted'>Could not load bot profile: {escape(profile_error)}</p></div>"

        body = f"""
        <div class="grid">
          <div class="card">
            <h2>Server Nickname</h2>
            <p class="muted">Update the nickname used in <strong>{escape(str(selected_guild.get("name") or "this server"))}</strong>. This does not change the bot's main Discord username.</p>
            <form method="post">
              <input type="hidden" name="action" value="nickname" />
              <label>Server nickname (this guild)</label>
              <input type="text" name="server_nickname" placeholder="Leave blank to keep current nickname" />
              <label style="margin-top:10px;display:block;">
                <input type="checkbox" name="clear_server_nickname" value="1" />
                Clear server nickname
              </label>
              <div style="margin-top:14px;">
                <button class="btn" type="submit">Update Server Nickname</button>
              </div>
            </form>
          </div>
          <div class="card">
            <h2>Global Bot Username</h2>
            <p class="muted">This changes the bot's main Discord username everywhere. Discord may rate-limit username changes, so it is intentionally separate from guild nickname edits.</p>
            <form method="post">
              <input type="hidden" name="action" value="username" />
              <label>Bot username (global)</label>
              <input type="text" name="bot_name" placeholder="GL.iNet UnOfficial Discord Bot" />
              <div style="margin-top:14px;">
                <button class="btn secondary" type="submit">Update Global Username</button>
              </div>
            </form>
          </div>
          <div class="card">
            <h2>Bot Avatar</h2>
            <p class="muted">Upload a new bot avatar. Max size is {max_avatar_upload_bytes} bytes.</p>
            <form method="post" enctype="multipart/form-data">
              <input type="hidden" name="action" value="avatar" />
              <label>Avatar image (PNG/JPG/WEBP/GIF)</label>
              <input type="file" name="avatar_file" accept=".png,.jpg,.jpeg,.webp,.gif,image/*" required />
              <div style="margin-top:14px;">
                <button class="btn" type="submit">Upload Avatar</button>
              </div>
            </form>
          </div>
        </div>
        <div style="margin-top:16px;">
          {profile_html}
        </div>
        """
        return _render_page("Bot Profile", body, user["email"], bool(user.get("is_admin")))

    @app.route("/admin/reddit-feeds", methods=["GET", "POST"])
    @login_required
    def reddit_feeds():
        user = _current_user()
        is_admin = _is_admin_user(user)
        selection_redirect = _require_selected_guild_redirect()
        if selection_redirect is not None:
            return selection_redirect
        selected_guild = _selected_guild() or {}
        selected_guild_id = str(selected_guild.get("id") or "")
        file_values = _parse_env_file(env_file)
        current_schedule = str(
            file_values.get(
                "REDDIT_FEED_CHECK_SCHEDULE",
                os.getenv("REDDIT_FEED_CHECK_SCHEDULE", "*/30 * * * *"),
            )
            or "*/30 * * * *"
        ).strip()
        if not croniter.is_valid(current_schedule):
            current_schedule = "*/30 * * * *"

        text_channel_options, _role_options, catalog_error = _load_discord_catalog_options(
            selected_guild_id,
            channel_type="text",
        )
        channel_labels = {
            str(option.get("id") or "").strip(): str(option.get("label") or option.get("name") or option.get("id") or "Unknown")
            for option in text_channel_options
            if str(option.get("id") or "").strip()
        }

        payload = (
            on_get_reddit_feeds(selected_guild_id)
            if callable(on_get_reddit_feeds)
            else {"ok": False, "error": "Reddit feed callbacks are not configured."}
        )

        if request.method == "POST":
            action = str(request.form.get("action") or "").strip().lower()
            if action == "schedule":
                selected_schedule = str(request.form.get("reddit_feed_schedule") or "").strip()
                allowed_schedules = {value for value, _ in REDDIT_FEED_SCHEDULE_OPTIONS}
                if selected_schedule not in allowed_schedules:
                    flash("Choose a valid Reddit feed schedule option.", "error")
                else:
                    updated_file_values = dict(file_values)
                    updated_file_values["REDDIT_FEED_CHECK_SCHEDULE"] = selected_schedule
                    saved, save_error, saved_env_file, _ = _try_write_env_file_with_fallback(
                        env_file,
                        fallback_env_file,
                        updated_file_values,
                    )
                    if not saved:
                        flash(save_error, "error")
                    else:
                        file_values = updated_file_values
                        os.environ["REDDIT_FEED_CHECK_SCHEDULE"] = selected_schedule
                        os.environ["WEB_ENV_FILE"] = str(saved_env_file)
                        if callable(on_env_settings_saved):
                            on_env_settings_saved(
                                {
                                    "REDDIT_FEED_CHECK_SCHEDULE": selected_schedule,
                                    "WEB_ENV_FILE": str(saved_env_file),
                                }
                            )
                        current_schedule = selected_schedule
                        if saved_env_file != env_file:
                            flash(
                                f"Reddit feed schedule updated and saved to fallback env file {saved_env_file}.",
                                "success",
                            )
                        else:
                            flash("Reddit feed schedule updated.", "success")
            elif not callable(on_manage_reddit_feeds):
                flash("Reddit feed update callback is not configured.", "error")
            else:
                callback_payload = {"action": action}
                if action == "add":
                    selected_channel_id = str(request.form.get("channel_id", "")).strip()
                    valid_text_channel_ids = {
                        str(option.get("id") or "").strip() for option in text_channel_options if str(option.get("id") or "").strip()
                    }
                    if selected_channel_id and valid_text_channel_ids and selected_channel_id not in valid_text_channel_ids:
                        flash("Choose a valid Discord text channel.", "error")
                        callback_payload = None
                    else:
                        callback_payload["subreddit"] = request.form.get("subreddit", "")
                        callback_payload["channel_id"] = selected_channel_id
                elif action == "edit":
                    selected_channel_id = str(request.form.get("channel_id", "")).strip()
                    valid_text_channel_ids = {
                        str(option.get("id") or "").strip() for option in text_channel_options if str(option.get("id") or "").strip()
                    }
                    if selected_channel_id and valid_text_channel_ids and selected_channel_id not in valid_text_channel_ids:
                        flash("Choose a valid Discord text channel.", "error")
                        callback_payload = None
                    else:
                        callback_payload["feed_id"] = request.form.get("feed_id", "")
                        callback_payload["subreddit"] = request.form.get("subreddit", "")
                        callback_payload["channel_id"] = selected_channel_id
                elif action == "toggle":
                    callback_payload["feed_id"] = request.form.get("feed_id", "")
                    callback_payload["enabled"] = request.form.get("enabled", "")
                elif action == "delete":
                    callback_payload["feed_id"] = request.form.get("feed_id", "")
                else:
                    flash("Invalid Reddit feed action.", "error")
                    callback_payload = None

                if callback_payload is not None:
                    response = on_manage_reddit_feeds(callback_payload, user["email"], selected_guild_id)
                    if not isinstance(response, dict):
                        flash("Invalid response from Reddit feed handler.", "error")
                    elif response.get("ok"):
                        payload = response
                        flash(
                            str(response.get("message") or "Reddit feed updated."),
                            "success",
                        )
                    else:
                        flash(
                            str(response.get("error") or "Failed to update Reddit feeds."),
                            "error",
                        )

            payload = (
                on_get_reddit_feeds(selected_guild_id)
                if callable(on_get_reddit_feeds)
                else {"ok": False, "error": "Reddit feed callbacks are not configured."}
            )

        feeds = payload.get("feeds", []) if isinstance(payload, dict) else []
        feeds_error = str(payload.get("error") or "") if isinstance(payload, dict) and not payload.get("ok") else ""
        schedule_select_html = _render_fixed_select_input(
            "reddit_feed_schedule",
            current_schedule,
            [{"value": value, "label": label} for value, label in REDDIT_FEED_SCHEDULE_OPTIONS],
            placeholder="Select Reddit poll interval...",
        )
        channel_select_html = _render_select_input(
            "channel_id",
            "",
            text_channel_options,
            placeholder="Select a Discord text channel...",
        )
        feed_rows = []
        for feed in feeds:
            feed_id = str(feed.get("id") or "")
            subreddit = str(feed.get("subreddit") or "").strip()
            channel_id = str(feed.get("channel_id") or "").strip()
            enabled = bool(feed.get("enabled"))
            last_checked_at = format_timestamp_display(feed.get("last_checked_at"), blank="Never")
            last_posted_at = format_timestamp_display(feed.get("last_posted_at"), blank="Never")
            last_error = str(feed.get("last_error") or "").strip()
            status_label = "Enabled" if enabled else "Disabled"
            if last_error:
                status_label = f"{status_label} | {last_error}"
            channel_label = channel_labels.get(channel_id, f"Unknown channel ({channel_id or 'not set'})")
            action_html = ""
            if is_admin:
                toggle_label = "Disable" if enabled else "Enable"
                toggle_value = "0" if enabled else "1"
                edit_channel_select_html = _render_select_input(
                    "channel_id",
                    channel_id,
                    text_channel_options,
                    placeholder="Select a Discord text channel...",
                )
                action_html = f"""
                <div class="dash-actions">
                  <form method="post" style="display:inline-block;min-width:260px;">
                    <input type="hidden" name="action" value="edit" />
                    <input type="hidden" name="feed_id" value="{escape(feed_id, quote=True)}" />
                    <input type="text" name="subreddit" value="{escape(subreddit, quote=True)}" placeholder="Subreddit" required style="margin-bottom:8px;" />
                    {edit_channel_select_html}
                    <button class="btn" type="submit" style="margin-top:8px;">Save</button>
                  </form>
                  <form method="post" style="display:inline;">
                    <input type="hidden" name="action" value="toggle" />
                    <input type="hidden" name="feed_id" value="{escape(feed_id, quote=True)}" />
                    <input type="hidden" name="enabled" value="{escape(toggle_value, quote=True)}" />
                    <button class="btn secondary" type="submit">{escape(toggle_label)}</button>
                  </form>
                  <form method="post" style="display:inline;" onsubmit="return confirm('Delete this Reddit feed subscription?');">
                    <input type="hidden" name="action" value="delete" />
                    <input type="hidden" name="feed_id" value="{escape(feed_id, quote=True)}" />
                    <button class="btn danger" type="submit">Delete</button>
                  </form>
                </div>
                """
            else:
                action_html = (
                    "<div class='dash-actions'>"
                    "<button class='btn' type='button' disabled>Edit</button>"
                    "<button class='btn secondary' type='button' disabled>Enable/Disable</button>"
                    "<button class='btn danger' type='button' disabled>Delete</button>"
                    "</div>"
                )
            feed_rows.append(
                f"""
                <tr>
                  <td><strong>r/{escape(subreddit)}</strong></td>
                  <td>{escape(channel_label)}<div class="muted mono">{escape(channel_id)}</div></td>
                  <td>{"Yes" if enabled else "No"}</td>
                  <td class="muted">{escape(last_checked_at)}</td>
                  <td class="muted">{escape(last_posted_at)}</td>
                  <td class="muted">{escape(status_label)}</td>
                  <td>{action_html}</td>
                </tr>
                """
            )

        catalog_note = ""
        if text_channel_options:
            catalog_note = f"<p class='muted'>Loaded {len(text_channel_options)} text channel options from Discord for feed targets.</p>"
        elif catalog_error:
            catalog_note = f"<p class='muted'>Could not load Discord text channels: {escape(catalog_error)}</p>"
        else:
            catalog_note = "<p class='muted'>No Discord text channels are currently available for selection.</p>"

        management_note = (
            "<p class='muted'>Add subreddit watchers here and the bot will post new Reddit submissions to the selected Discord channel.</p>"
            if is_admin
            else "<p class='muted'>Read-only account: you can view feed mappings and schedule, but cannot change them.</p>"
        )
        add_disabled_attr = ""
        add_disabled_note = ""
        if not text_channel_options:
            add_disabled_attr = " disabled"
            add_disabled_note = "<p class='muted'>A Discord text channel must be available before you can add a Reddit feed.</p>"

        body = f"""
        <div class="grid">
          <div class="card">
            <h2>Reddit Feed Schedule</h2>
            <p class="muted">Choose how often the bot polls Reddit for new posts. Default is every 30 minutes.</p>
            <form method="post">
              <input type="hidden" name="action" value="schedule" />
              <label>Polling interval</label>
              {schedule_select_html}
              <div style="margin-top:14px;">
                <button class="btn" type="submit"{"" if is_admin else " disabled"}>Save Schedule</button>
              </div>
            </form>
          </div>
          <div class="card">
            <h2>Add Reddit Feed</h2>
            <p class="muted">Selected server: <strong>{escape(str(selected_guild.get("name") or "Unknown"))}</strong></p>
            {management_note}
            {catalog_note}
            {add_disabled_note}
            <form method="post">
              <input type="hidden" name="action" value="add" />
              <label>Subreddit</label>
              <input type="text" name="subreddit" placeholder="GlInet or https://www.reddit.com/r/GlInet/" required{add_disabled_attr} />
              <label style="margin-top:10px;display:block;">Discord channel</label>
              {channel_select_html}
              <div style="margin-top:14px;">
                <button class="btn" type="submit"{add_disabled_attr if is_admin else " disabled"}>Add Feed</button>
              </div>
            </form>
          </div>
        </div>
        <div class="card" style="margin-top:16px;">
          <h2>Configured Reddit Feeds</h2>
          <p class="muted">New subscriptions establish a baseline on first check and only post newer Reddit submissions after that.</p>
          {f"<p class='muted'>Could not load feeds: {escape(feeds_error)}</p>" if feeds_error else ""}
          <table>
            <thead>
              <tr>
                <th>Subreddit</th>
                <th>Discord Channel</th>
                <th>Enabled</th>
                <th>Last Checked</th>
                <th>Last Posted</th>
                <th>Status</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {"".join(feed_rows) if feed_rows else "<tr><td colspan='7' class='muted'>No Reddit feeds are configured yet.</td></tr>"}
            </tbody>
          </table>
        </div>
        """
        return _render_page("Reddit Feeds", body, user["email"], bool(user.get("is_admin")))

    @app.route("/admin/youtube", methods=["GET", "POST"])
    @login_required
    def youtube_subscriptions():
        user = _current_user()
        is_admin = _is_admin_user(user)
        selection_redirect = _require_selected_guild_redirect()
        if selection_redirect is not None:
            return selection_redirect
        selected_guild = _selected_guild() or {}
        selected_guild_id = str(selected_guild.get("id") or "")

        text_channel_options, _role_options, catalog_error = _load_discord_catalog_options(
            selected_guild_id,
            channel_type="text",
        )
        channel_labels = {
            str(option.get("id") or "").strip(): str(option.get("label") or option.get("name") or option.get("id") or "Unknown")
            for option in text_channel_options
            if str(option.get("id") or "").strip()
        }

        payload = (
            on_get_youtube_subscriptions(selected_guild_id)
            if callable(on_get_youtube_subscriptions)
            else {"ok": False, "error": "YouTube subscription callbacks are not configured."}
        )

        if request.method == "POST":
            action = str(request.form.get("action") or "").strip().lower()
            if not callable(on_manage_youtube_subscriptions):
                flash("YouTube subscription update callback is not configured.", "error")
            else:
                callback_payload = {"action": action}
                if action == "add":
                    selected_channel_id = str(request.form.get("channel_id", "")).strip()
                    valid_text_channel_ids = {
                        str(option.get("id") or "").strip() for option in text_channel_options if str(option.get("id") or "").strip()
                    }
                    if selected_channel_id and valid_text_channel_ids and selected_channel_id not in valid_text_channel_ids:
                        flash("Choose a valid Discord text channel.", "error")
                        callback_payload = None
                    else:
                        callback_payload["source_url"] = request.form.get("source_url", "")
                        callback_payload["channel_id"] = selected_channel_id
                elif action == "edit":
                    selected_channel_id = str(request.form.get("channel_id", "")).strip()
                    valid_text_channel_ids = {
                        str(option.get("id") or "").strip() for option in text_channel_options if str(option.get("id") or "").strip()
                    }
                    if selected_channel_id and valid_text_channel_ids and selected_channel_id not in valid_text_channel_ids:
                        flash("Choose a valid Discord text channel.", "error")
                        callback_payload = None
                    else:
                        callback_payload["subscription_id"] = request.form.get("subscription_id", "")
                        callback_payload["source_url"] = request.form.get("source_url", "")
                        callback_payload["channel_id"] = selected_channel_id
                elif action == "delete":
                    callback_payload["subscription_id"] = request.form.get("subscription_id", "")
                else:
                    flash("Invalid YouTube subscription action.", "error")
                    callback_payload = None

                if callback_payload is not None:
                    response = on_manage_youtube_subscriptions(callback_payload, user["email"], selected_guild_id)
                    if not isinstance(response, dict):
                        flash("Invalid response from YouTube subscription handler.", "error")
                    elif response.get("ok"):
                        payload = response
                        flash(
                            str(response.get("message") or "YouTube subscriptions updated."),
                            "success",
                        )
                    else:
                        flash(
                            str(response.get("error") or "Failed to update YouTube subscriptions."),
                            "error",
                        )

            payload = (
                on_get_youtube_subscriptions(selected_guild_id)
                if callable(on_get_youtube_subscriptions)
                else {"ok": False, "error": "YouTube subscription callbacks are not configured."}
            )

        subscriptions = payload.get("subscriptions", []) if isinstance(payload, dict) else []
        subscriptions_error = str(payload.get("error") or "") if isinstance(payload, dict) and not payload.get("ok") else ""
        channel_select_html = _render_select_input(
            "channel_id",
            "",
            text_channel_options,
            placeholder="Select a Discord text channel...",
        )
        rows = []
        for subscription in subscriptions:
            subscription_id = str(subscription.get("id") or "")
            channel_id = str(subscription.get("target_channel_id") or "").strip()
            channel_label = channel_labels.get(channel_id, f"Unknown channel ({channel_id or 'not set'})")
            edit_channel_select_html = _render_select_input(
                "channel_id",
                channel_id,
                text_channel_options,
                placeholder="Select a Discord text channel...",
            )
            actions_html = (
                f"""
                <form method="post" style="display:inline-block;min-width:260px;">
                  <input type="hidden" name="action" value="edit" />
                  <input type="hidden" name="subscription_id" value="{escape(subscription_id, quote=True)}" />
                  <input type="text" name="source_url" value="{escape(str(subscription.get("source_url") or ""), quote=True)}" placeholder="https://www.youtube.com/@example" required style="margin-bottom:8px;" />
                  {edit_channel_select_html}
                  <button class="btn" type="submit" style="margin-top:8px;">Save</button>
                </form>
                <form method="post" style="display:inline;" onsubmit="return confirm('Delete this YouTube subscription?');">
                  <input type="hidden" name="action" value="delete" />
                  <input type="hidden" name="subscription_id" value="{escape(subscription_id, quote=True)}" />
                  <button class="btn danger" type="submit">Delete</button>
                </form>
                """
                if is_admin
                else "<div class='dash-actions'><button class='btn' type='button' disabled>Edit</button><button class='btn danger' type='button' disabled>Delete</button></div>"
            )
            rows.append(
                f"""
                <tr>
                  <td>{escape(str(subscription.get("channel_title") or ""))}<div class="muted mono">{escape(str(subscription.get("channel_id") or ""))}</div></td>
                  <td>{escape(str(subscription.get("source_url") or ""))}</td>
                  <td>{escape(channel_label)}<div class="muted mono">{escape(channel_id)}</div></td>
                  <td>{escape(str(subscription.get("last_video_title") or "Unknown"))}</td>
                  <td class="muted">{escape(format_timestamp_display(subscription.get("last_published_at"), blank="Never"))}</td>
                  <td>{actions_html}</td>
                </tr>
                """
            )

        catalog_note = (
            f"<p class='muted'>Loaded {len(text_channel_options)} text channel options from Discord.</p>"
            if text_channel_options
            else (
                f"<p class='muted'>Could not load Discord text channels: {escape(catalog_error)}</p>"
                if catalog_error
                else "<p class='muted'>No Discord text channels are currently available for selection.</p>"
            )
        )
        add_disabled_attr = "" if text_channel_options and is_admin else " disabled"
        body = f"""
        <div class="grid">
          <div class="card">
            <h2>Add YouTube Subscription</h2>
            <p class="muted">Selected server: <strong>{escape(str(selected_guild.get("name") or "Unknown"))}</strong></p>
            <p class="muted">Track a YouTube channel and post new uploads into a Discord text channel.</p>
            {catalog_note}
            <form method="post">
              <input type="hidden" name="action" value="add" />
              <label>YouTube channel URL</label>
              <input type="text" name="source_url" placeholder="https://www.youtube.com/@example" required{add_disabled_attr} />
              <label style="margin-top:10px;display:block;">Discord channel</label>
              {channel_select_html}
              <div style="margin-top:14px;">
                <button class="btn" type="submit"{add_disabled_attr}>Save Subscription</button>
              </div>
            </form>
          </div>
        </div>
        <div class="card" style="margin-top:16px;">
          <h2>Configured YouTube Subscriptions</h2>
          {f"<p class='muted'>Could not load subscriptions: {escape(subscriptions_error)}</p>" if subscriptions_error else ""}
          <table>
            <thead>
              <tr>
                <th>Channel</th>
                <th>Source URL</th>
                <th>Discord Channel</th>
                <th>Last Video</th>
                <th>Last Published</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {"".join(rows) if rows else "<tr><td colspan='6' class='muted'>No YouTube subscriptions are configured yet.</td></tr>"}
            </tbody>
          </table>
        </div>
        """
        return _render_page(
            "YouTube Subscriptions",
            body,
            user["email"],
            bool(user.get("is_admin")),
            str(user.get("display_name") or ""),
        )

    @app.route("/admin/linkedin", methods=["GET", "POST"])
    @login_required
    def linkedin_subscriptions():
        user = _current_user()
        is_admin = _is_admin_user(user)
        selection_redirect = _require_selected_guild_redirect()
        if selection_redirect is not None:
            return selection_redirect
        selected_guild = _selected_guild() or {}
        selected_guild_id = str(selected_guild.get("id") or "")

        text_channel_options, _role_options, catalog_error = _load_discord_catalog_options(
            selected_guild_id,
            channel_type="text",
        )
        channel_labels = {
            str(option.get("id") or "").strip(): str(option.get("label") or option.get("name") or option.get("id") or "Unknown")
            for option in text_channel_options
            if str(option.get("id") or "").strip()
        }

        payload = (
            on_get_linkedin_subscriptions(selected_guild_id)
            if callable(on_get_linkedin_subscriptions)
            else {"ok": False, "error": "LinkedIn subscription callbacks are not configured."}
        )

        if request.method == "POST":
            action = str(request.form.get("action") or "").strip().lower()
            if not callable(on_manage_linkedin_subscriptions):
                flash("LinkedIn subscription update callback is not configured.", "error")
            else:
                callback_payload = {"action": action}
                if action == "add":
                    selected_channel_id = str(request.form.get("channel_id", "")).strip()
                    valid_text_channel_ids = {
                        str(option.get("id") or "").strip() for option in text_channel_options if str(option.get("id") or "").strip()
                    }
                    if selected_channel_id and valid_text_channel_ids and selected_channel_id not in valid_text_channel_ids:
                        flash("Choose a valid Discord text channel.", "error")
                        callback_payload = None
                    else:
                        callback_payload["source_url"] = request.form.get("source_url", "")
                        callback_payload["channel_id"] = selected_channel_id
                elif action == "edit":
                    selected_channel_id = str(request.form.get("channel_id", "")).strip()
                    valid_text_channel_ids = {
                        str(option.get("id") or "").strip() for option in text_channel_options if str(option.get("id") or "").strip()
                    }
                    if selected_channel_id and valid_text_channel_ids and selected_channel_id not in valid_text_channel_ids:
                        flash("Choose a valid Discord text channel.", "error")
                        callback_payload = None
                    else:
                        callback_payload["subscription_id"] = request.form.get("subscription_id", "")
                        callback_payload["source_url"] = request.form.get("source_url", "")
                        callback_payload["channel_id"] = selected_channel_id
                elif action == "delete":
                    callback_payload["subscription_id"] = request.form.get("subscription_id", "")
                else:
                    flash("Invalid LinkedIn subscription action.", "error")
                    callback_payload = None

                if callback_payload is not None:
                    response = on_manage_linkedin_subscriptions(callback_payload, user["email"], selected_guild_id)
                    if not isinstance(response, dict):
                        flash("Invalid response from LinkedIn subscription handler.", "error")
                    elif response.get("ok"):
                        payload = response
                        flash(
                            str(response.get("message") or "LinkedIn subscriptions updated."),
                            "success",
                        )
                    else:
                        flash(
                            str(response.get("error") or "Failed to update LinkedIn subscriptions."),
                            "error",
                        )

            payload = (
                on_get_linkedin_subscriptions(selected_guild_id)
                if callable(on_get_linkedin_subscriptions)
                else {"ok": False, "error": "LinkedIn subscription callbacks are not configured."}
            )

        subscriptions = payload.get("subscriptions", []) if isinstance(payload, dict) else []
        subscriptions_error = str(payload.get("error") or "") if isinstance(payload, dict) and not payload.get("ok") else ""
        channel_select_html = _render_select_input(
            "channel_id",
            "",
            text_channel_options,
            placeholder="Select a Discord text channel...",
        )
        rows = []
        for subscription in subscriptions:
            subscription_id = str(subscription.get("id") or "")
            channel_id = str(subscription.get("target_channel_id") or "").strip()
            channel_label = channel_labels.get(channel_id, f"Unknown channel ({channel_id or 'not set'})")
            edit_channel_select_html = _render_select_input(
                "channel_id",
                channel_id,
                text_channel_options,
                placeholder="Select a Discord text channel...",
            )
            actions_html = (
                f"""
                <form method="post" style="display:inline-block;min-width:260px;">
                  <input type="hidden" name="action" value="edit" />
                  <input type="hidden" name="subscription_id" value="{escape(subscription_id, quote=True)}" />
                  <input type="text" name="source_url" value="{escape(str(subscription.get("source_url") or ""), quote=True)}" placeholder="https://www.linkedin.com/in/example" required style="margin-bottom:8px;" />
                  {edit_channel_select_html}
                  <button class="btn" type="submit" style="margin-top:8px;">Save</button>
                </form>
                <form method="post" style="display:inline;" onsubmit="return confirm('Delete this LinkedIn subscription?');">
                  <input type="hidden" name="action" value="delete" />
                  <input type="hidden" name="subscription_id" value="{escape(subscription_id, quote=True)}" />
                  <button class="btn danger" type="submit">Delete</button>
                </form>
                """
                if is_admin
                else "<div class='dash-actions'><button class='btn' type='button' disabled>Edit</button><button class='btn danger' type='button' disabled>Delete</button></div>"
            )
            rows.append(
                f"""
                <tr>
                  <td>{escape(str(subscription.get("profile_name") or "Unknown profile"))}</td>
                  <td>{escape(str(subscription.get("source_url") or ""))}</td>
                  <td>{escape(channel_label)}<div class="muted mono">{escape(channel_id)}</div></td>
                  <td>{escape(_clip_text(str(subscription.get("last_post_text") or "No post captured yet."), max_chars=100))}</td>
                  <td class="muted">{escape(format_timestamp_display(subscription.get("last_published_at"), blank="Never"))}</td>
                  <td class="muted">{escape(format_timestamp_display(subscription.get("last_checked_at"), blank="Never"))}</td>
                  <td class="muted">{escape(str(subscription.get("last_error") or "")) or "OK"}</td>
                  <td>{actions_html}</td>
                </tr>
                """
            )

        catalog_note = (
            f"<p class='muted'>Loaded {len(text_channel_options)} text channel options from Discord.</p>"
            if text_channel_options
            else (
                f"<p class='muted'>Could not load Discord text channels: {escape(catalog_error)}</p>"
                if catalog_error
                else "<p class='muted'>No Discord text channels are currently available for selection.</p>"
            )
        )
        add_disabled_attr = "" if text_channel_options and is_admin else " disabled"
        body = f"""
        <div class="grid">
          <div class="card">
            <h2>Add LinkedIn Profile</h2>
            <p class="muted">Selected server: <strong>{escape(str(selected_guild.get("name") or "Unknown"))}</strong></p>
            <p class="muted">Track a public LinkedIn profile and post new profile activity into a Discord text channel.</p>
            <p class="muted">Use the public profile URL, for example <span class="mono">https://www.linkedin.com/in/example</span>.</p>
            {catalog_note}
            <form method="post">
              <input type="hidden" name="action" value="add" />
              <label>LinkedIn profile URL</label>
              <input type="text" name="source_url" placeholder="https://www.linkedin.com/in/example" required{add_disabled_attr} />
              <label style="margin-top:10px;display:block;">Discord channel</label>
              {channel_select_html}
              <div style="margin-top:14px;">
                <button class="btn" type="submit"{add_disabled_attr}>Save Subscription</button>
              </div>
            </form>
          </div>
        </div>
        <div class="card" style="margin-top:16px;">
          <h2>Configured LinkedIn Profiles</h2>
          <p class="muted">This watcher works best for public LinkedIn profiles whose recent posts are visible on the public profile page.</p>
          {f"<p class='muted'>Could not load subscriptions: {escape(subscriptions_error)}</p>" if subscriptions_error else ""}
          <table>
            <thead>
              <tr>
                <th>Profile</th>
                <th>Source URL</th>
                <th>Discord Channel</th>
                <th>Last Post Preview</th>
                <th>Last Published</th>
                <th>Last Checked</th>
                <th>Status</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {"".join(rows) if rows else "<tr><td colspan='8' class='muted'>No LinkedIn profiles are configured yet.</td></tr>"}
            </tbody>
          </table>
        </div>
        """
        return _render_page(
            "LinkedIn Profiles",
            body,
            user["email"],
            bool(user.get("is_admin")),
            str(user.get("display_name") or ""),
        )

    @app.route("/admin/beta-programs", methods=["GET", "POST"])
    @login_required
    def beta_program_subscriptions():
        user = _current_user()
        is_admin = _is_admin_user(user)
        selection_redirect = _require_selected_guild_redirect()
        if selection_redirect is not None:
            return selection_redirect
        selected_guild = _selected_guild() or {}
        selected_guild_id = str(selected_guild.get("id") or "")

        text_channel_options, _role_options, catalog_error = _load_discord_catalog_options(
            selected_guild_id,
            channel_type="text",
        )
        channel_labels = {
            str(option.get("id") or "").strip(): str(option.get("label") or option.get("name") or option.get("id") or "Unknown")
            for option in text_channel_options
            if str(option.get("id") or "").strip()
        }

        payload = (
            on_get_beta_program_subscriptions(selected_guild_id)
            if callable(on_get_beta_program_subscriptions)
            else {"ok": False, "error": "GL.iNet beta program callbacks are not configured."}
        )

        if request.method == "POST":
            action = str(request.form.get("action") or "").strip().lower()
            if not callable(on_manage_beta_program_subscriptions):
                flash("GL.iNet beta program update callback is not configured.", "error")
            else:
                callback_payload = {"action": action}
                if action == "add":
                    callback_payload["channel_id"] = str(request.form.get("channel_id", "")).strip()
                elif action == "delete":
                    callback_payload["subscription_id"] = request.form.get("subscription_id", "")
                else:
                    flash("Invalid GL.iNet beta program action.", "error")
                    callback_payload = None

                if callback_payload is not None:
                    response = on_manage_beta_program_subscriptions(callback_payload, user["email"], selected_guild_id)
                    if not isinstance(response, dict):
                        flash("Invalid response from GL.iNet beta program handler.", "error")
                    elif response.get("ok"):
                        payload = response
                        flash(
                            str(response.get("message") or "GL.iNet beta programs updated."),
                            "success",
                        )
                    else:
                        flash(
                            str(response.get("error") or "Failed to update GL.iNet beta programs."),
                            "error",
                        )

            payload = (
                on_get_beta_program_subscriptions(selected_guild_id)
                if callable(on_get_beta_program_subscriptions)
                else {"ok": False, "error": "GL.iNet beta program callbacks are not configured."}
            )

        subscriptions = payload.get("subscriptions", []) if isinstance(payload, dict) else []
        subscriptions_error = str(payload.get("error") or "") if isinstance(payload, dict) and not payload.get("ok") else ""
        source_url = str(payload.get("source_url") or "https://www.gl-inet.com/beta-testing/#register")
        channel_select_html = _render_select_input(
            "channel_id",
            "",
            text_channel_options,
            placeholder="Select a Discord text channel...",
        )
        rows = []
        for subscription in subscriptions:
            subscription_id = str(subscription.get("id") or "")
            channel_id = str(subscription.get("target_channel_id") or "").strip()
            channel_label = channel_labels.get(channel_id, f"Unknown channel ({channel_id or 'not set'})")
            program_count = len(subscription.get("programs") or [])
            actions_html = (
                f"""
                <form method="post" style="display:inline;" onsubmit="return confirm('Delete this GL.iNet beta program monitor?');">
                  <input type="hidden" name="action" value="delete" />
                  <input type="hidden" name="subscription_id" value="{escape(subscription_id, quote=True)}" />
                  <button class="btn danger" type="submit">Delete</button>
                </form>
                """
                if is_admin
                else "<button class='btn danger' type='button' disabled>Delete</button>"
            )
            rows.append(
                f"""
                <tr>
                  <td>{escape(str(subscription.get("source_name") or "GL.iNet Beta Programs"))}</td>
                  <td><a href="{escape(str(subscription.get("source_url") or source_url), quote=True)}" target="_blank" rel="noopener">{escape(str(subscription.get("source_url") or source_url))}</a></td>
                  <td>{escape(channel_label)}<div class="muted mono">{escape(channel_id)}</div></td>
                  <td>{program_count}</td>
                  <td class="muted">{escape(format_timestamp_display(subscription.get("last_checked_at"), blank="Never"))}</td>
                  <td class="muted">{escape(str(subscription.get("last_error") or "")) or "OK"}</td>
                  <td>{actions_html}</td>
                </tr>
                """
            )

        catalog_note = (
            f"<p class='muted'>Loaded {len(text_channel_options)} text channel options from Discord.</p>"
            if text_channel_options
            else (
                f"<p class='muted'>Could not load Discord text channels: {escape(catalog_error)}</p>"
                if catalog_error
                else "<p class='muted'>No Discord text channels are currently available for selection.</p>"
            )
        )
        add_disabled_attr = "" if text_channel_options and is_admin else " disabled"
        body = f"""
        <div class="grid">
          <div class="card">
            <h2>Add GL.iNet Beta Program Monitor</h2>
            <p class="muted">Selected server: <strong>{escape(str(selected_guild.get("name") or "Unknown"))}</strong></p>
            <p class="muted">Monitor the GL.iNet beta testing page and notify a Discord text channel whenever beta programs are added or removed.</p>
            <p class="muted">Source page: <a href="{escape(source_url, quote=True)}" target="_blank" rel="noopener">{escape(source_url)}</a></p>
            {catalog_note}
            <form method="post">
              <input type="hidden" name="action" value="add" />
              <label style="margin-top:10px;display:block;">Discord channel</label>
              {channel_select_html}
              <div style="margin-top:14px;">
                <button class="btn" type="submit"{add_disabled_attr}>Save Monitor</button>
              </div>
            </form>
          </div>
        </div>
        <div class="card" style="margin-top:16px;">
          <h2>Configured GL.iNet Beta Program Monitors</h2>
          <p class="muted">This watcher polls the public GL.iNet beta page and compares the current program list against the last seen snapshot for the selected guild.</p>
          {f"<p class='muted'>Could not load subscriptions: {escape(subscriptions_error)}</p>" if subscriptions_error else ""}
          <table>
            <thead>
              <tr>
                <th>Source</th>
                <th>Page URL</th>
                <th>Discord Channel</th>
                <th>Known Programs</th>
                <th>Last Checked</th>
                <th>Status</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {"".join(rows) if rows else "<tr><td colspan='7' class='muted'>No GL.iNet beta program monitors are configured yet.</td></tr>"}
            </tbody>
          </table>
        </div>
        """
        return _render_page(
            "GL.iNet Beta Programs",
            body,
            user["email"],
            bool(user.get("is_admin")),
            str(user.get("display_name") or ""),
        )

    @app.route("/admin/service-monitors", methods=["GET", "POST"])
    @login_required
    def service_monitors_page():
        user = _current_user()
        can_manage = _is_admin_user(user) or _is_glinet_rw_user(user) or _is_guild_admin_user(user)
        selection_redirect = _require_selected_guild_redirect()
        if selection_redirect is not None:
            return selection_redirect

        selected_guild = _selected_guild() or {}
        selected_guild_id = str(selected_guild.get("id") or "").strip()
        selected_guild_id_int = int(selected_guild_id) if selected_guild_id.isdigit() else 0
        guild_name = str(selected_guild.get("name") or "Unknown")
        fallback_env_file = _env_fallback_file_path(data_dir)

        def _fetch_json_url(url: str, *, verify_tls: bool = True):
            response = _safe_outbound_get(
                str(url or "").strip(),
                timeout=15,
                verify_tls=verify_tls,
                headers={
                    "User-Agent": "glinet-discord-bot-web-admin/1.0",
                    "Accept": "application/json",
                },
            )
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise ValueError("Status page API returned an unexpected payload.")
            return payload

        def _fetch_text_url(url: str, *, api_key: str = "", verify_tls: bool = True):
            request_headers = {
                "User-Agent": "glinet-discord-bot-web-admin/1.0",
                "Accept": "text/plain, application/openmetrics-text;q=0.9, */*;q=0.8",
            }
            normalized_api_key = str(api_key or "").strip()
            auth_variants = [{}]
            if normalized_api_key:
                encoded = base64.b64encode(f":{normalized_api_key}".encode()).decode("ascii")
                auth_variants = [
                    {"Authorization": f"Basic {encoded}"},
                    {"Authorization": f"Bearer {normalized_api_key}"},
                    {"X-API-Key": normalized_api_key},
                ]
            last_response = None
            for auth_headers in auth_variants:
                response = _safe_outbound_get(
                    str(url or "").strip(),
                    timeout=15,
                    verify_tls=verify_tls,
                    headers={**request_headers, **auth_headers},
                )
                last_response = response
                if response.status_code == 401 and normalized_api_key and auth_headers is not auth_variants[-1]:
                    continue
                response.raise_for_status()
                return response.text
            if last_response is not None:
                last_response.raise_for_status()
            raise ValueError("Uptime Kuma instance did not return any response.")

        def _load_page_state():
            file_values = _load_effective_env_values(env_file, fallback_env_file)
            raw_default_channel = str(_read_env_value(file_values, "SERVICE_MONITOR_DEFAULT_CHANNEL_ID") or "").strip()
            try:
                default_channel_id = int(raw_default_channel or 0)
            except ValueError:
                default_channel_id = 0
            raw_timeout = str(_read_env_value(file_values, "SERVICE_MONITOR_REQUEST_TIMEOUT_SECONDS") or "10").strip()
            try:
                service_timeout = max(3, int(raw_timeout or 10))
            except ValueError:
                service_timeout = 10
            raw_targets = _read_env_value(file_values, "SERVICE_MONITOR_TARGETS_JSON")
            targets_error = ""
            try:
                all_targets = normalize_service_monitor_targets(
                    raw_targets,
                    default_timeout_seconds=service_timeout,
                    default_channel_id=default_channel_id,
                )
            except ValueError as exc:
                all_targets = []
                targets_error = str(exc)

            visible_targets = []
            for target in all_targets:
                target_guild_id = int(target.get("guild_id") or 0)
                if target_guild_id not in {0, selected_guild_id_int}:
                    continue
                visible_targets.append(dict(target))

            service_schedule = str(
                _read_env_value(file_values, "SERVICE_MONITOR_CHECK_SCHEDULE") or "*/5 * * * *"
            ).strip() or "*/5 * * * *"
            if not croniter.is_valid(service_schedule):
                service_schedule = "*/5 * * * *"

            uptime_schedule = str(
                _read_env_value(file_values, "UPTIME_STATUS_CHECK_SCHEDULE") or "*/5 * * * *"
            ).strip() or "*/5 * * * *"
            if not croniter.is_valid(uptime_schedule):
                uptime_schedule = "*/5 * * * *"

            raw_uptime_timeout = str(_read_env_value(file_values, "UPTIME_STATUS_TIMEOUT_SECONDS") or "15").strip()
            try:
                uptime_timeout = max(3, int(raw_uptime_timeout or 15))
            except ValueError:
                uptime_timeout = 15

            return {
                "file_values": file_values,
                "all_targets": all_targets,
                "visible_targets": visible_targets,
                "targets_error": targets_error,
                "service_enabled": str(_read_env_value(file_values, "SERVICE_MONITOR_ENABLED") or "false").strip().lower()
                in {"1", "true", "yes", "on"},
                "service_default_channel_id": default_channel_id,
                "service_schedule": service_schedule,
                "service_timeout": service_timeout,
                "uptime_enabled": str(_read_env_value(file_values, "UPTIME_STATUS_ENABLED") or "false").strip().lower()
                in {"1", "true", "yes", "on"},
                "uptime_notify_enabled": str(_read_env_value(file_values, "UPTIME_STATUS_NOTIFY_ENABLED") or "false").strip().lower()
                in {"1", "true", "yes", "on"},
                "uptime_page_url": str(_read_env_value(file_values, "UPTIME_STATUS_PAGE_URL") or "").strip(),
                "uptime_instance_url": str(_read_env_value(file_values, "UPTIME_STATUS_INSTANCE_URL") or "").strip(),
                "uptime_api_key_configured": bool(
                    str(_read_env_value(file_values, "UPTIME_STATUS_API_KEY") or "").strip()
                    or default_uptime_api_key(str(_read_env_value(file_values, "UPTIME_STATUS_INSTANCE_URL") or "").strip())
                ),
                "uptime_notify_channel_id": str(_read_env_value(file_values, "UPTIME_STATUS_NOTIFY_CHANNEL_ID") or "").strip(),
                "uptime_schedule": uptime_schedule,
                "uptime_timeout": uptime_timeout,
                "uptime_verify_tls": str(_read_env_value(file_values, "UPTIME_STATUS_VERIFY_TLS") or "true").strip().lower()
                in {"1", "true", "yes", "on"},
            }

        def _persist_monitor_updates(applied_updates: dict):
            merged_values = _load_effective_env_values(env_file, fallback_env_file)
            for key, value in applied_updates.items():
                merged_values[str(key)] = "" if value is None else str(value)
            normalized_values = _normalize_env_updates(merged_values)
            validation_errors = _validate_env_updates(applied_updates)
            if validation_errors:
                return False, validation_errors[0]

            saved, save_error, saved_env_file, skipped_keys = _try_write_env_file_with_fallback(
                env_file,
                fallback_env_file,
                normalized_values,
            )
            if not saved:
                return False, save_error

            for key, value in applied_updates.items():
                os.environ[str(key)] = "" if value is None else str(value)
            os.environ["WEB_ENV_FILE"] = str(saved_env_file)
            if callable(on_env_settings_saved):
                on_env_settings_saved({**applied_updates, "WEB_ENV_FILE": str(saved_env_file)})
            if saved_env_file != env_file:
                flash(
                    f"Monitor settings saved to fallback env file {saved_env_file}.",
                    "success",
                )
            return True, ""

        text_channel_options, _role_options, catalog_error = _load_discord_catalog_options(
            selected_guild_id,
            channel_type="text",
        )
        channel_labels = {
            str(option.get("id") or "").strip(): str(option.get("label") or option.get("name") or option.get("id") or "Unknown")
            for option in text_channel_options
            if str(option.get("id") or "").strip()
        }

        page_state = _load_page_state()
        if request.method == "POST":
            action = str(request.form.get("action") or "").strip().lower()
            if not can_manage:
                flash("Read-only account: this action is not allowed.", "error")
            elif action == "save_service_settings":
                selected_default_channel_id = str(request.form.get("default_channel_id") or "").strip()
                updates = {
                    "SERVICE_MONITOR_ENABLED": "true"
                    if str(request.form.get("service_monitor_enabled") or "").strip().lower() in {"1", "true", "yes", "on"}
                    else "false",
                    "SERVICE_MONITOR_CHECK_SCHEDULE": str(request.form.get("service_monitor_schedule") or "*/5 * * * *").strip(),
                    "SERVICE_MONITOR_REQUEST_TIMEOUT_SECONDS": str(
                        request.form.get("service_monitor_timeout") or page_state["service_timeout"] or 10
                    ).strip(),
                    "SERVICE_MONITOR_DEFAULT_CHANNEL_ID": selected_default_channel_id,
                }
                ok, error_text = _persist_monitor_updates(updates)
                if ok:
                    flash("Direct service monitor settings updated.", "success")
                else:
                    flash(error_text, "error")
            elif action in {
                "add_target",
                "edit_target",
                "delete_target",
                "import_uptime_targets",
                "import_uptime_instance_targets",
                "add_tailscale_status",
                "add_glinet_domain_set",
            }:
                all_targets = list(page_state["all_targets"])
                if action == "delete_target":
                    target_id = str(request.form.get("target_id") or "").strip()
                    next_targets = []
                    removed = False
                    for target in all_targets:
                        target_guild_id = int(target.get("guild_id") or 0)
                        if str(target.get("id") or "") == target_id and target_guild_id in {0, selected_guild_id_int}:
                            removed = True
                            continue
                        next_targets.append(target)
                    if not removed:
                        flash("Service monitor entry was not found.", "error")
                    else:
                        ok, error_text = _persist_monitor_updates(
                            {"SERVICE_MONITOR_TARGETS_JSON": serialize_service_monitor_targets(next_targets)}
                        )
                        if ok:
                            flash("Service monitor deleted.", "success")
                        else:
                            flash(error_text, "error")
                elif action == "import_uptime_targets":
                    import_page_url = str(request.form.get("uptime_import_page_url") or page_state["uptime_page_url"] or "").strip()
                    import_channel_id = str(request.form.get("uptime_import_channel_id") or "").strip()
                    if not import_channel_id:
                        import_channel_id = str(page_state["service_default_channel_id"] or "").strip()
                    valid_text_channel_ids = {
                        str(option.get("id") or "").strip() for option in text_channel_options if str(option.get("id") or "").strip()
                    }
                    if import_channel_id and valid_text_channel_ids and import_channel_id not in valid_text_channel_ids:
                        flash("Choose a valid Discord text channel for imported service monitors.", "error")
                    else:
                        try:
                            channel_id_int = int(import_channel_id or 0)
                            config_payload = fetch_uptime_public_config(
                                page_url=import_page_url,
                                fetch_json=lambda url: _fetch_json_url(url, verify_tls=page_state["uptime_verify_tls"]),
                            )
                            extracted = extract_service_monitor_targets_from_uptime_config(
                                config_payload,
                                guild_id=selected_guild_id_int,
                                channel_id=channel_id_int,
                                timeout_seconds=page_state["service_timeout"],
                            )
                            merge_result = merge_service_monitor_targets(all_targets, extracted.get("targets", []))
                            next_targets = merge_result["targets"]
                            ok, error_text = _persist_monitor_updates(
                                {
                                    "SERVICE_MONITOR_TARGETS_JSON": serialize_service_monitor_targets(next_targets),
                                    "UPTIME_STATUS_PAGE_URL": import_page_url,
                                }
                            )
                            if ok:
                                skipped_count = len(extracted.get("skipped", []) or [])
                                summary = f"Imported {merge_result['added']} new direct service monitor(s)"
                                if merge_result["updated"]:
                                    summary += f", updated {merge_result['updated']}"
                                if merge_result["deduped"]:
                                    summary += f", removed {merge_result['deduped']} duplicate(s)"
                                if skipped_count:
                                    summary += f", skipped {skipped_count} page-only monitor(s)"
                                summary += "."
                                flash(summary, "success")
                        except (requests.RequestException, ValueError) as exc:
                            flash(str(exc), "error")
                elif action == "import_uptime_instance_targets":
                    import_instance_url = str(
                        request.form.get("uptime_import_instance_url") or page_state["uptime_instance_url"] or ""
                    ).strip()
                    import_channel_id = str(request.form.get("uptime_import_channel_id") or "").strip()
                    if not import_channel_id:
                        import_channel_id = str(page_state["service_default_channel_id"] or "").strip()
                    import_verify_tls = (
                        str(request.form.get("uptime_import_verify_tls") or "").strip().lower()
                        in {"1", "true", "yes", "on"}
                    )
                    stored_api_key = str(_read_env_value(page_state["file_values"], "UPTIME_STATUS_API_KEY") or "").strip()
                    if not stored_api_key:
                        stored_api_key = default_uptime_api_key(import_instance_url)
                    import_api_key = str(request.form.get("uptime_import_api_key") or "").strip() or stored_api_key
                    valid_text_channel_ids = {
                        str(option.get("id") or "").strip() for option in text_channel_options if str(option.get("id") or "").strip()
                    }
                    if import_channel_id and valid_text_channel_ids and import_channel_id not in valid_text_channel_ids:
                        flash("Choose a valid Discord text channel for imported service monitors.", "error")
                    else:
                        try:
                            channel_id_int = int(import_channel_id or 0)
                            metrics_text = fetch_uptime_metrics_text(
                                instance_url=import_instance_url,
                                api_key=import_api_key,
                                fetch_text=lambda url, api_key="": _fetch_text_url(
                                    url,
                                    api_key=api_key,
                                    verify_tls=import_verify_tls,
                                ),
                            )
                            extracted = extract_service_monitor_targets_from_uptime_metrics(
                                metrics_text,
                                guild_id=selected_guild_id_int,
                                channel_id=channel_id_int,
                                timeout_seconds=page_state["service_timeout"],
                            )
                            merge_result = merge_service_monitor_targets(all_targets, extracted.get("targets", []))
                            next_targets = merge_result["targets"]
                            import_updates = {
                                "SERVICE_MONITOR_TARGETS_JSON": serialize_service_monitor_targets(next_targets),
                                "UPTIME_STATUS_INSTANCE_URL": import_instance_url,
                                "UPTIME_STATUS_VERIFY_TLS": "true" if import_verify_tls else "false",
                            }
                            if str(request.form.get("uptime_import_api_key") or "").strip():
                                import_updates["UPTIME_STATUS_API_KEY"] = str(request.form.get("uptime_import_api_key") or "").strip()
                            ok, error_text = _persist_monitor_updates(import_updates)
                            if ok:
                                skipped_count = len(extracted.get("skipped", []) or [])
                                summary = f"Imported {merge_result['added']} new direct service monitor(s)"
                                if merge_result["updated"]:
                                    summary += f", updated {merge_result['updated']}"
                                if merge_result["deduped"]:
                                    summary += f", removed {merge_result['deduped']} duplicate(s)"
                                if skipped_count:
                                    summary += f", skipped {skipped_count} page-only monitor(s)"
                                summary += "."
                                flash(summary, "success")
                            else:
                                flash(error_text, "error")
                        except (requests.RequestException, ValueError) as exc:
                            flash(str(exc), "error")
                else:
                    target_id = str(request.form.get("target_id") or "").strip()
                    if action == "add_tailscale_status":
                        name = "Tailscale Status"
                        url = "https://status.tailscale.com/"
                        method = "GET"
                        expected_status = "200"
                        contains_text = ""
                        channel_id = str(request.form.get("preset_channel_id") or "").strip()
                        timeout_seconds = str(
                            request.form.get("preset_timeout_seconds") or page_state["service_timeout"] or 10
                        ).strip()
                    elif action == "add_glinet_domain_set":
                        channel_id = str(request.form.get("glinet_preset_channel_id") or "").strip()
                        timeout_seconds = str(
                            request.form.get("glinet_preset_timeout_seconds") or page_state["service_timeout"] or 10
                        ).strip()
                        name = ""
                        url = ""
                        method = "GET"
                        expected_status = "200"
                        contains_text = ""
                    else:
                        name = str(request.form.get("name") or "").strip()
                        url = str(request.form.get("url") or "").strip()
                        method = str(request.form.get("method") or "GET").strip().upper()
                        expected_status = str(request.form.get("expected_status") or "200").strip() or "200"
                        contains_text = str(request.form.get("contains_text") or "").strip()
                        channel_id = str(request.form.get("channel_id") or "").strip()
                        timeout_seconds = str(
                            request.form.get("timeout_seconds") or page_state["service_timeout"] or 10
                        ).strip()
                    valid_text_channel_ids = {
                        str(option.get("id") or "").strip() for option in text_channel_options if str(option.get("id") or "").strip()
                    }
                    if channel_id and valid_text_channel_ids and channel_id not in valid_text_channel_ids:
                        flash("Choose a valid Discord text channel.", "error")
                    else:
                        channel_id_value = int(channel_id or page_state["service_default_channel_id"] or 0)
                        try:
                            if action == "add_glinet_domain_set":
                                incoming_targets = build_glinet_domain_monitor_targets(
                                    guild_id=selected_guild_id_int,
                                    channel_id=channel_id_value,
                                    timeout_seconds=int(timeout_seconds or page_state["service_timeout"] or 10),
                                )
                                merge_result = merge_service_monitor_targets(all_targets, incoming_targets)
                                next_targets = merge_result["targets"]
                            else:
                                candidate = {
                                    "guild_id": selected_guild_id_int,
                                    "name": name,
                                    "url": url,
                                    "method": method,
                                    "expected_status": expected_status,
                                    "contains_text": contains_text,
                                    "timeout_seconds": timeout_seconds,
                                    "channel_id": channel_id_value,
                                }
                                normalized_candidate = normalize_service_monitor_targets(
                                    [candidate],
                                    default_timeout_seconds=page_state["service_timeout"],
                                    default_channel_id=page_state["service_default_channel_id"],
                                )[0]
                                next_targets = []
                                merge_result = None
                            if action == "edit_target":
                                replaced = False
                                for target in all_targets:
                                    target_guild_id = int(target.get("guild_id") or 0)
                                    if str(target.get("id") or "") == target_id and target_guild_id in {0, selected_guild_id_int}:
                                        next_targets.append(normalized_candidate)
                                        replaced = True
                                    else:
                                        next_targets.append(target)
                                if not replaced:
                                    flash("Service monitor entry was not found.", "error")
                                    page_state = _load_page_state()
                                    target_rows = []
                                    # fall through to render
                                    next_targets = None
                            elif action == "add_glinet_domain_set":
                                pass
                            else:
                                next_targets = list(all_targets) + [normalized_candidate]
                            if next_targets is not None:
                                ok, error_text = _persist_monitor_updates(
                                    {"SERVICE_MONITOR_TARGETS_JSON": serialize_service_monitor_targets(next_targets)}
                                )
                                if ok:
                                    flash(
                                        "Service monitor updated."
                                        if action == "edit_target"
                                        else (
                                            f"Added {merge_result['added']} GL.iNet domain monitor(s)"
                                            + (
                                                f", updated {merge_result['updated']}"
                                                if merge_result and merge_result["updated"]
                                                else ""
                                            )
                                            + (
                                                f", removed {merge_result['deduped']} duplicate(s)"
                                                if merge_result and merge_result["deduped"]
                                                else ""
                                            )
                                            + "."
                                        )
                                        if action == "add_glinet_domain_set"
                                        else "Tailscale status monitor added."
                                        if action == "add_tailscale_status"
                                        else "Service monitor added.",
                                        "success",
                                    )
                                else:
                                    flash(error_text, "error")
                        except ValueError as exc:
                            flash(str(exc), "error")
            elif action == "save_uptime_settings":
                notify_channel_id = str(request.form.get("uptime_notify_channel_id") or "").strip()
                valid_text_channel_ids = {
                    str(option.get("id") or "").strip() for option in text_channel_options if str(option.get("id") or "").strip()
                }
                if notify_channel_id and valid_text_channel_ids and notify_channel_id not in valid_text_channel_ids:
                    flash("Choose a valid Discord text channel for Uptime Kuma alerts.", "error")
                else:
                    uptime_api_key_value = str(request.form.get("uptime_status_api_key") or "").strip()
                    updates = {
                        "UPTIME_STATUS_ENABLED": "true"
                        if str(request.form.get("uptime_status_enabled") or "").strip().lower() in {"1", "true", "yes", "on"}
                        else "false",
                        "UPTIME_STATUS_NOTIFY_ENABLED": "true"
                        if str(request.form.get("uptime_status_notify_enabled") or "").strip().lower() in {"1", "true", "yes", "on"}
                        else "false",
                        "UPTIME_STATUS_PAGE_URL": str(request.form.get("uptime_status_page_url") or "").strip(),
                        "UPTIME_STATUS_INSTANCE_URL": str(request.form.get("uptime_status_instance_url") or "").strip(),
                        "UPTIME_STATUS_NOTIFY_CHANNEL_ID": notify_channel_id,
                        "UPTIME_STATUS_CHECK_SCHEDULE": str(request.form.get("uptime_status_schedule") or "*/5 * * * *").strip(),
                        "UPTIME_STATUS_TIMEOUT_SECONDS": str(
                            request.form.get("uptime_status_timeout") or page_state["uptime_timeout"] or 15
                        ).strip(),
                        "UPTIME_STATUS_VERIFY_TLS": "true"
                        if str(request.form.get("uptime_status_verify_tls") or "").strip().lower() in {"1", "true", "yes", "on"}
                        else "false",
                    }
                    if uptime_api_key_value:
                        updates["UPTIME_STATUS_API_KEY"] = uptime_api_key_value
                    elif str(request.form.get("uptime_status_api_key_clear") or "").strip().lower() in {"1", "true", "yes", "on"}:
                        updates["UPTIME_STATUS_API_KEY"] = ""
                    ok, error_text = _persist_monitor_updates(updates)
                    if ok:
                        flash("Uptime Kuma watcher settings updated.", "success")
                    else:
                        flash(error_text, "error")
            else:
                flash("Invalid service monitor action.", "error")
            page_state = _load_page_state()

        enabled_options = [
            {"value": "true", "label": "Enabled"},
            {"value": "false", "label": "Disabled"},
        ]
        channel_catalog_note = (
            f"<p class='muted'>Loaded {len(text_channel_options)} text channel options from Discord for <strong>{escape(guild_name)}</strong>.</p>"
            if text_channel_options
            else (
                f"<p class='muted'>Could not load Discord text channels: {escape(catalog_error)}</p>"
                if catalog_error
                else "<p class='muted'>No Discord text channels are currently available for selection.</p>"
            )
        )
        direct_settings_channel_select = _render_select_input(
            "default_channel_id",
            str(page_state["service_default_channel_id"] or ""),
            text_channel_options,
            placeholder="Optional fallback Discord text channel...",
        )
        add_target_channel_select = _render_select_input(
            "channel_id",
            str(page_state["service_default_channel_id"] or ""),
            text_channel_options,
            placeholder="Choose the Discord text channel...",
        )
        glinet_preset_target_channel_select = _render_select_input(
            "glinet_preset_channel_id",
            str(page_state["service_default_channel_id"] or ""),
            text_channel_options,
            placeholder="Choose the Discord text channel...",
        )
        import_target_channel_select = _render_select_input(
            "uptime_import_channel_id",
            str(page_state["service_default_channel_id"] or ""),
            text_channel_options,
            placeholder="Choose the Discord text channel...",
        )
        uptime_notify_channel_select = _render_select_input(
            "uptime_notify_channel_id",
            str(page_state["uptime_notify_channel_id"] or ""),
            text_channel_options,
            placeholder="Choose the Discord text channel...",
        )
        uptime_verify_tls_select = _render_fixed_select_input(
            "uptime_status_verify_tls",
            "true" if page_state["uptime_verify_tls"] else "false",
            enabled_options,
            placeholder="Select state...",
        )
        import_verify_tls_select = _render_fixed_select_input(
            "uptime_import_verify_tls",
            "true" if page_state["uptime_verify_tls"] else "false",
            enabled_options,
            placeholder="Select state...",
        )
        service_schedule_select = _render_fixed_select_input(
            "service_monitor_schedule",
            page_state["service_schedule"],
            [{"value": value, "label": label} for value, label in MONITOR_RECHECK_SCHEDULE_OPTIONS],
            placeholder="Select recheck interval...",
        )
        uptime_schedule_select = _render_fixed_select_input(
            "uptime_status_schedule",
            page_state["uptime_schedule"],
            [{"value": value, "label": label} for value, label in MONITOR_RECHECK_SCHEDULE_OPTIONS],
            placeholder="Select recheck interval...",
        )
        service_enabled_select = _render_fixed_select_input(
            "service_monitor_enabled",
            "true" if page_state["service_enabled"] else "false",
            enabled_options,
            placeholder="Select state...",
        )
        uptime_enabled_select = _render_fixed_select_input(
            "uptime_status_enabled",
            "true" if page_state["uptime_enabled"] else "false",
            enabled_options,
            placeholder="Select state...",
        )
        uptime_notify_enabled_select = _render_fixed_select_input(
            "uptime_status_notify_enabled",
            "true" if page_state["uptime_notify_enabled"] else "false",
            enabled_options,
            placeholder="Select state...",
        )
        method_options = [{"value": "GET", "label": "GET"}, {"value": "HEAD", "label": "HEAD"}]
        target_rows = []
        for target in page_state["visible_targets"]:
            target_id = str(target.get("id") or "").strip()
            target_channel_id = str(target.get("channel_id") or "").strip()
            target_channel_label = channel_labels.get(target_channel_id, f"Unknown channel ({target_channel_id or 'not set'})")
            edit_channel_select = _render_select_input(
                "channel_id",
                target_channel_id,
                text_channel_options,
                placeholder="Choose the Discord text channel...",
            )
            edit_method_select = _render_fixed_select_input(
                "method",
                str(target.get("method") or "GET").strip().upper(),
                method_options,
                placeholder="Choose method...",
            )
            actions_html = (
                f"""
                <form method="post" style="display:inline-block;min-width:320px;">
                  <input type="hidden" name="action" value="edit_target" />
                  <input type="hidden" name="target_id" value="{escape(target_id, quote=True)}" />
                  <input type="text" name="name" value="{escape(str(target.get('name') or ''), quote=True)}" placeholder="Friendly name" required style="margin-bottom:8px;" />
                  <input type="text" name="url" value="{escape(str(target.get('url') or ''), quote=True)}" placeholder="https://example.com/health" required style="margin-bottom:8px;" />
                  {edit_method_select}
                  <input type="number" name="expected_status" min="100" max="599" value="{escape(str(target.get('expected_status') or 200), quote=True)}" placeholder="Expected HTTP status" style="margin-top:8px;" />
                  <input type="text" name="contains_text" value="{escape(str(target.get('contains_text') or ''), quote=True)}" placeholder="Optional required text" style="margin-top:8px;" />
                  {edit_channel_select}
                  <input type="number" name="timeout_seconds" min="3" max="120" value="{escape(str(target.get('timeout_seconds') or page_state['service_timeout']), quote=True)}" placeholder="Timeout seconds" style="margin-top:8px;" />
                  <button class="btn" type="submit" style="margin-top:8px;">Save</button>
                </form>
                <form method="post" style="display:inline;" onsubmit="return confirm('Delete this direct service monitor?');">
                  <input type="hidden" name="action" value="delete_target" />
                  <input type="hidden" name="target_id" value="{escape(target_id, quote=True)}" />
                  <button class="btn danger" type="submit">Delete</button>
                </form>
                """
                if can_manage
                else "<div class='dash-actions'><button class='btn' type='button' disabled>Edit</button><button class='btn danger' type='button' disabled>Delete</button></div>"
            )
            target_rows.append(
                f"""
                <tr>
                  <td><strong>{escape(str(target.get("name") or ""))}</strong></td>
                  <td><a href="{escape(str(target.get("url") or ""), quote=True)}" target="_blank" rel="noopener">{escape(str(target.get("url") or ""))}</a></td>
                  <td>{escape(str(target.get("method") or "GET"))}<div class="muted">HTTP {escape(str(target.get("expected_status") or 200))}</div></td>
                  <td>{escape(target_channel_label)}<div class="muted mono">{escape(target_channel_id)}</div></td>
                  <td class="muted">{escape(str(target.get("contains_text") or "")) or "Any valid response"}</td>
                  <td class="muted">{escape(str(target.get("timeout_seconds") or page_state["service_timeout"]))}s</td>
                  <td>{actions_html}</td>
                </tr>
                """
            )

        manage_disabled_attr = "" if can_manage else " disabled"
        body = f"""
        <div class="grid">
          <div class="card">
            <h2>Direct Service Monitor Settings</h2>
            <p class="muted">These checks watch websites and API endpoints directly and only alert on state changes. The current page manages the entries scoped to <strong>{escape(guild_name)}</strong>.</p>
            {channel_catalog_note}
            <form method="post">
              <input type="hidden" name="action" value="save_service_settings" />
              <label>Direct monitor state</label>
              {service_enabled_select}
              <label style="margin-top:10px;display:block;">Recheck interval</label>
              {service_schedule_select}
              <label>Request timeout (seconds)</label>
              <input type="number" name="service_monitor_timeout" min="3" max="120" value="{escape(str(page_state['service_timeout']), quote=True)}"{manage_disabled_attr} />
              <label style="margin-top:10px;display:block;">Fallback Discord channel</label>
              {direct_settings_channel_select}
              <div style="margin-top:14px;">
                <button class="btn" type="submit"{manage_disabled_attr}>Save Direct Monitor Settings</button>
              </div>
            </form>
          </div>
          <div class="card">
            <h2>Import From Uptime Kuma</h2>
            <p class="muted">Seed direct service checks from either a public Uptime Kuma status page or an authenticated Kuma instance. Only monitors with a public HTTP(S) URL can become direct checks.</p>
            <div class="grid" style="grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:16px;">
              <div class="card" style="margin:0;">
                <h3 style="margin-top:0;">From Public Page</h3>
                <form method="post">
                  <input type="hidden" name="action" value="import_uptime_targets" />
                  <label>Public status page URL</label>
                  <input type="text" name="uptime_import_page_url" value="{escape(page_state['uptime_page_url'] or 'https://status.glinet.admon.me/status/default', quote=True)}" placeholder="https://status.example.com/status/default" required{manage_disabled_attr} />
                  <label style="margin-top:10px;display:block;">Discord channel</label>
                  {import_target_channel_select}
                  <div style="margin-top:14px;">
                    <button class="btn" type="submit"{manage_disabled_attr}>Import Direct Checks</button>
                  </div>
                </form>
              </div>
              <div class="card" style="margin:0;">
                <h3 style="margin-top:0;">From Authenticated Instance</h3>
                <form method="post">
                  <input type="hidden" name="action" value="import_uptime_instance_targets" />
                  <label>Authenticated instance URL</label>
                  <input type="text" name="uptime_import_instance_url" value="{escape(page_state['uptime_instance_url'], quote=True)}" placeholder="https://kuma.example.com/" required{manage_disabled_attr} />
                  <label>API key</label>
                  <input type="password" name="uptime_import_api_key" value="" placeholder="{'•••••• (stored key will be used if blank)' if page_state['uptime_api_key_configured'] else 'Paste a Uptime Kuma API key'}"{manage_disabled_attr} />
                  <label style="margin-top:10px;display:block;">Verify TLS certificates</label>
                  {import_verify_tls_select}
                  <label style="margin-top:10px;display:block;">Discord channel</label>
                  {import_target_channel_select}
                  <div style="margin-top:14px;">
                    <button class="btn" type="submit"{manage_disabled_attr}>Import From Instance</button>
                  </div>
                </form>
              </div>
            </div>
          </div>
        </div>
        <div class="grid" style="margin-top:16px;">
          <div class="card">
            <h2>Add Direct Service Monitor</h2>
            <p class="muted">Create a direct website or API check for <strong>{escape(guild_name)}</strong>. Use optional required text when a simple HTTP status code is not enough.</p>
            <div class="card" style="margin:0 0 16px 0;">
              <h3 style="margin-top:0;">Quick Preset: GL.iNet Domains</h3>
              <p class="muted">Add the standard GL.iNet domain set for this guild and automatically remove duplicates by URL.</p>
              <form method="post">
                <input type="hidden" name="action" value="add_glinet_domain_set" />
                <label>Discord channel</label>
                {glinet_preset_target_channel_select}
                <label>Request timeout (seconds)</label>
                <input type="number" name="glinet_preset_timeout_seconds" min="3" max="120" value="{escape(str(page_state['service_timeout']), quote=True)}"{manage_disabled_attr} />
                <div style="margin-top:14px;">
                  <button class="btn" type="submit"{manage_disabled_attr}>Add GL.iNet Domain Set</button>
                </div>
              </form>
            </div>
            <div class="card" style="margin:0 0 16px 0;">
              <h3 style="margin-top:0;">Quick Preset: Tailscale</h3>
              <p class="muted">Add a ready-to-use monitor for <span class="mono">https://status.tailscale.com/</span>.</p>
              <form method="post">
                <input type="hidden" name="action" value="add_tailscale_status" />
                <label>Discord channel</label>
                {_render_select_input(
                    "preset_channel_id",
                    str(page_state["service_default_channel_id"] or ""),
                    text_channel_options,
                    placeholder="Choose the Discord text channel...",
                )}
                <label>Request timeout (seconds)</label>
                <input type="number" name="preset_timeout_seconds" min="3" max="120" value="{escape(str(page_state['service_timeout']), quote=True)}"{manage_disabled_attr} />
                <div style="margin-top:14px;">
                  <button class="btn" type="submit"{manage_disabled_attr}>Add Tailscale Status</button>
                </div>
              </form>
            </div>
            <form method="post">
              <input type="hidden" name="action" value="add_target" />
              <label>Friendly name</label>
              <input type="text" name="name" placeholder="Discord Status" required{manage_disabled_attr} />
              <label>URL</label>
              <input type="text" name="url" placeholder="https://status.discord.com" required{manage_disabled_attr} />
              <label>Method</label>
              {_render_fixed_select_input("method", "GET", method_options, placeholder="Choose method...")}
              <label>Expected HTTP status</label>
              <input type="number" name="expected_status" min="100" max="599" value="200"{manage_disabled_attr} />
              <label>Required text (optional)</label>
              <input type="text" name="contains_text" placeholder="Optional response text"{manage_disabled_attr} />
              <label style="margin-top:10px;display:block;">Discord channel</label>
              {add_target_channel_select}
              <label>Request timeout (seconds)</label>
              <input type="number" name="timeout_seconds" min="3" max="120" value="{escape(str(page_state['service_timeout']), quote=True)}"{manage_disabled_attr} />
              <div style="margin-top:14px;">
                <button class="btn" type="submit"{manage_disabled_attr}>Add Direct Monitor</button>
              </div>
            </form>
          </div>
          <div class="card">
            <h2>Uptime Kuma Watcher</h2>
            <p class="muted">This watcher can read either a public Uptime Kuma status page or an authenticated Kuma instance. If both are configured, the authenticated instance takes priority and covers all monitors exposed by the instance metrics endpoint.</p>
            <form method="post">
              <input type="hidden" name="action" value="save_uptime_settings" />
              <label>Watcher enabled</label>
              {uptime_enabled_select}
              <label style="margin-top:10px;display:block;">Discord alerting</label>
              {uptime_notify_enabled_select}
              <label>Public status page URL (optional)</label>
              <input type="text" name="uptime_status_page_url" value="{escape(page_state['uptime_page_url'], quote=True)}" placeholder="https://status.example.com/status/default"{manage_disabled_attr} />
              <label>Authenticated instance URL (optional)</label>
              <input type="text" name="uptime_status_instance_url" value="{escape(page_state['uptime_instance_url'], quote=True)}" placeholder="https://kuma.example.com/"{manage_disabled_attr} />
              <label>API key (optional)</label>
              <input type="password" name="uptime_status_api_key" value="" placeholder="{'•••••• (unchanged if blank)' if page_state['uptime_api_key_configured'] else 'Paste a Uptime Kuma API key'}"{manage_disabled_attr} />
              <label class="checkbox" style="margin-top:8px;">
                <input type="checkbox" name="uptime_status_api_key_clear" value="true"{manage_disabled_attr} />
                Clear stored API key
              </label>
              <label style="margin-top:10px;display:block;">Discord channel</label>
              {uptime_notify_channel_select}
              <label style="margin-top:10px;display:block;">Verify TLS certificates</label>
              {uptime_verify_tls_select}
              <label style="margin-top:10px;display:block;">Recheck interval</label>
              {uptime_schedule_select}
              <label>Request timeout (seconds)</label>
              <input type="number" name="uptime_status_timeout" min="3" max="120" value="{escape(str(page_state['uptime_timeout']), quote=True)}"{manage_disabled_attr} />
              <div style="margin-top:14px;">
                <button class="btn" type="submit"{manage_disabled_attr}>Save Uptime Kuma Watcher</button>
              </div>
            </form>
          </div>
        </div>
        <div class="card" style="margin-top:16px;">
          <h2>Configured Direct Service Monitors</h2>
          <p class="muted">These checks belong to <strong>{escape(guild_name)}</strong> and use the global direct-monitor interval shown above. Alerts are sent only when a service changes from up to down or recovers from down to up.</p>
          {f"<p class='muted'>Current target configuration could not be parsed: {escape(page_state['targets_error'])}</p>" if page_state["targets_error"] else ""}
          <table>
            <thead>
              <tr>
                <th>Name</th>
                <th>URL</th>
                <th>Request</th>
                <th>Discord Channel</th>
                <th>Required Text</th>
                <th>Timeout</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {"".join(target_rows) if target_rows else "<tr><td colspan='7' class='muted'>No direct service monitors are configured yet.</td></tr>"}
            </tbody>
          </table>
        </div>
        """
        return _render_page("Service Monitors", body, user["email"], bool(user.get("is_admin")), str(user.get("display_name") or ""))

    @app.route("/admin/role-access", methods=["GET", "POST"])
    @login_required
    def role_access_page():
        user = _current_user()
        selection_redirect = _require_selected_guild_redirect()
        if selection_redirect is not None:
            return selection_redirect
        selected_guild = _selected_guild() or {}
        selected_guild_id = str(selected_guild.get("id") or "")
        guild_name = str(selected_guild.get("name") or "Unknown")
        _channel_options, role_options, catalog_error = _load_discord_catalog_options(selected_guild_id)

        payload = (
            on_get_role_access_mappings(selected_guild_id)
            if callable(on_get_role_access_mappings)
            else {"ok": False, "error": "Role access callbacks are not configured."}
        )

        if request.method == "POST":
            response, messages = process_role_access_submission(
                form=request.form,
                on_manage_role_access_mappings=on_manage_role_access_mappings,
                actor_email=user["email"],
                selected_guild_id=selected_guild_id,
            )
            for message, category in messages:
                flash(message, category)
            if isinstance(response, dict):
                payload = response

        if not isinstance(payload, dict) or not payload.get("ok"):
            error_text = (
                str(payload.get("error") or "Unable to load role access mappings.")
                if isinstance(payload, dict)
                else "Unable to load role access mappings."
            )
            body = (
                f"<div class='card'><h2>Role Access</h2><p class='muted'>Could not load role access mappings: {escape(error_text)}</p></div>"
            )
            return _render_page("Role Access", body, user["email"], bool(user.get("is_admin")))

        body = render_role_access_body(
            guild_name=guild_name,
            mappings=payload.get("mappings", []) or [],
            role_options=role_options,
            catalog_error=catalog_error,
            render_select_input=_render_select_input,
            render_fixed_select_input=_render_fixed_select_input,
        )
        return _render_page("Role Access", body, user["email"], bool(user.get("is_admin")))

    @app.route("/admin/command-status", methods=["GET", "POST"])
    @login_required
    def command_status():
        user = _current_user()
        selection_redirect = _require_selected_guild_redirect()
        if selection_redirect is not None:
            return selection_redirect
        selected_guild = _selected_guild() or {}
        selected_guild_id = str(selected_guild.get("id") or "")
        permissions_payload = (
            on_get_command_permissions(selected_guild_id)
            if callable(on_get_command_permissions)
            else {"ok": False, "error": "Not configured"}
        )

        if request.method == "POST":
            if not callable(on_save_command_permissions):
                flash("Command status save callback is not configured.", "error")
            else:
                command_updates = {}
                for command_key in request.form.getlist("command_key"):
                    current_mode = str(request.form.get(f"current_mode__{command_key}", "default") or "default").strip()
                    current_role_ids = request.form.get(f"current_role_ids__{command_key}", "")
                    enabled_value = str(request.form.get(f"enabled__{command_key}", "enabled") or "enabled").strip().lower()
                    next_mode = "disabled" if enabled_value == "disabled" else (current_mode if current_mode != "disabled" else "default")
                    command_updates[command_key] = {
                        "mode": next_mode,
                        "role_ids": current_role_ids,
                    }
                response = on_save_command_permissions({"commands": command_updates}, user["email"], selected_guild_id)
                if not isinstance(response, dict):
                    flash("Invalid response from command status save handler.", "error")
                elif not response.get("ok"):
                    flash(response.get("error", "Failed to save command status."), "error")
                else:
                    permissions_payload = response
                    flash(response.get("message", "Command status updated."), "success")

        if not isinstance(permissions_payload, dict) or not permissions_payload.get("ok"):
            error_text = str(
                permissions_payload.get("error") if isinstance(permissions_payload, dict) else "Unable to load command status."
            )
            body = (
                "<div class='card'><h2>Command Status</h2>"
                f"<p class='muted'>Could not load command status: {escape(error_text)}</p></div>"
            )
            return _render_page("Command Status", body, user["email"], bool(user.get("is_admin")))

        rows = []
        for entry in permissions_payload.get("commands", []) or []:
            command_key = str(entry.get("key") or "").strip()
            if not command_key:
                continue
            label = str(entry.get("label") or command_key)
            description = str(entry.get("description") or "").strip()
            current_mode = str(entry.get("mode") or "default").strip()
            role_ids = entry.get("role_ids", []) or []
            enabled_value = "disabled" if current_mode == "disabled" else "enabled"
            description_html = f"<div class='muted'>{escape(description)}</div>" if description else ""
            rows.append(
                f"""
                <tr>
                  <td>
                    <strong>{escape(label)}</strong>
                    {description_html}
                    <input type="hidden" name="command_key" value="{escape(command_key, quote=True)}" />
                    <input type="hidden" name="current_mode__{escape(command_key, quote=True)}" value="{escape(current_mode, quote=True)}" />
                    <input type="hidden" name="current_role_ids__{escape(command_key, quote=True)}" value="{escape(','.join(str(value) for value in role_ids), quote=True)}" />
                  </td>
                  <td>{escape(_dashboard_command_access_label(entry))}</td>
                  <td>
                    {_render_fixed_select_input(
                        f"enabled__{command_key}",
                        enabled_value,
                        [
                            {"value": "enabled", "label": "Enabled"},
                            {"value": "disabled", "label": "Disabled"},
                        ],
                        placeholder="Select status...",
                    )}
                  </td>
                </tr>
                """
            )

        body = f"""
        <div class="card">
          <h2>Command Status</h2>
          <p class="muted">Selected server: <strong>{escape(str(selected_guild.get("name") or "Unknown"))}</strong></p>
          <p class="muted">Use this page for quick on/off control. Use <a href="{escape(url_for("command_permissions"), quote=True)}">Command Permissions</a> to change access rules or custom-role gates.</p>
          <form method="post">
            <div class="table-scroll">
              <table>
                <thead>
                  <tr><th>Command</th><th>Access</th><th>Status</th></tr>
                </thead>
                <tbody>
                  {"".join(rows) if rows else "<tr><td colspan='3' class='muted'>No command metadata is available for this server.</td></tr>"}
                </tbody>
              </table>
            </div>
            <div style="margin-top:14px;">
              <button class="btn" type="submit">Save Command Status</button>
            </div>
          </form>
        </div>
        """
        return _render_page("Command Status", body, user["email"], bool(user.get("is_admin")))

    @app.route("/admin/command-permissions", methods=["GET", "POST"])
    @login_required
    def command_permissions():
        user = _current_user()
        selection_redirect = _require_selected_guild_redirect()
        if selection_redirect is not None:
            return selection_redirect
        selected_guild = _selected_guild() or {}
        selected_guild_id = str(selected_guild.get("id") or "")
        permissions_payload = (
            on_get_command_permissions(selected_guild_id)
            if callable(on_get_command_permissions)
            else {"ok": False, "error": "Not configured"}
        )
        _channel_options, role_options, catalog_error = _load_discord_catalog_options(selected_guild_id)

        if request.method == "POST":
            if not callable(on_save_command_permissions):
                flash("Command permission save callback is not configured.", "error")
            else:
                command_updates = {}
                for command_key in request.form.getlist("command_key"):
                    selected_role_ids = request.form.getlist(f"role_ids__{command_key}")
                    manual_role_ids = request.form.get(f"role_ids_text__{command_key}", "")
                    role_ids_payload = selected_role_ids if role_options else manual_role_ids
                    if role_options and not selected_role_ids and manual_role_ids.strip():
                        role_ids_payload = manual_role_ids
                    enabled = request.form.get(f"enabled__{command_key}") == "1"
                    selected_mode = request.form.get(f"mode__{command_key}", "default")
                    command_updates[command_key] = {
                        "mode": selected_mode if enabled else "disabled",
                        "role_ids": role_ids_payload,
                    }
                response = on_save_command_permissions({"commands": command_updates}, user["email"], selected_guild_id)
                if not isinstance(response, dict):
                    flash(
                        "Invalid response from command permissions save handler.",
                        "error",
                    )
                elif not response.get("ok"):
                    flash(
                        response.get("error", "Failed to save command permissions."),
                        "error",
                    )
                else:
                    permissions_payload = response
                    flash(
                        response.get("message", "Command permissions updated."),
                        "success",
                    )

        if not isinstance(permissions_payload, dict) or not permissions_payload.get("ok"):
            error_text = str(
                permissions_payload.get("error") if isinstance(permissions_payload, dict) else "Unable to load command permissions."
            )
            body = (
                "<div class='card'><h2>Command Permissions</h2>"
                f"<p class='muted'>Could not load command permissions: {escape(error_text)}</p></div>"
            )
            return _render_page("Command Permissions", body, user["email"], bool(user.get("is_admin")))

        commands = permissions_payload.get("commands", []) or []
        rows = []
        for entry in commands:
            command_key = str(entry.get("key") or "").strip()
            if not command_key:
                continue
            label = str(entry.get("label") or command_key)
            description = str(entry.get("description") or "")
            default_policy_label = str(entry.get("default_policy_label") or "")
            mode = str(entry.get("mode") or "default")
            role_ids = entry.get("role_ids", []) or []
            role_ids_value = ",".join(str(value) for value in role_ids)
            default_selected = " selected" if mode == "default" else ""
            public_selected = " selected" if mode == "public" else ""
            custom_selected = " selected" if mode == "custom_roles" else ""
            enabled_checked = "" if mode == "disabled" else " checked"
            if role_options:
                role_input_html = (
                    _render_multi_select_input(
                        name=f"role_ids__{command_key}",
                        selected_values=[str(value) for value in role_ids],
                        options=role_options,
                        size=7,
                    )
                    + f"<input type='text' name='role_ids_text__{escape(command_key, quote=True)}' "
                    "placeholder='Optional: comma-separated role IDs not listed above' />"
                )
            else:
                role_input_html = (
                    f"<input type='text' name='role_ids__{escape(command_key, quote=True)}' "
                    f"value='{escape(role_ids_value, quote=True)}' "
                    "placeholder='Comma-separated role IDs (for custom mode)' />"
                )
            rows.append(
                f"""
                <tr>
                  <td>
                    <strong>{escape(label)}</strong>
                    <div class="muted mono">{escape(command_key)}</div>
                    <div class="muted">{escape(description)}</div>
                    <input type="hidden" name="command_key" value="{escape(command_key, quote=True)}" />
                  </td>
                  <td class="muted">{escape(default_policy_label)}</td>
                  <td>
                    <label><input type="checkbox" name="enabled__{escape(command_key, quote=True)}" value="1"{enabled_checked} /> Enabled</label>
                  </td>
                  <td>
                    <select name="mode__{escape(command_key, quote=True)}">
                      <option value="default"{default_selected}>Default rule</option>
                      <option value="public"{public_selected}>Public (any member)</option>
                      <option value="custom_roles"{custom_selected}>Custom roles</option>
                    </select>
                  </td>
                  <td>
                    {role_input_html}
                  </td>
                </tr>
                """
            )

        role_hint_html = ""
        if catalog_error:
            role_hint_html = f"<p class='muted'>Could not load guild roles: {escape(catalog_error)}</p>"
        elif role_options:
            role_hint_html = (
                "<p class='muted'>Role dropdown loaded from Discord. Use Ctrl/Cmd-click to select multiple roles per command.</p>"
            )

        allowed_role_names = permissions_payload.get("allowed_role_names", []) or []
        moderator_role_ids = permissions_payload.get("moderator_role_ids", []) or []
        body = f"""
        <div class="card">
          <h2>Command Permissions</h2>
          <p class="muted">Selected server: <strong>{escape(str(selected_guild.get("name") or "Unknown"))}</strong></p>
          <p class="muted">Use the Enabled checkbox to turn a command on or off for this server. Access mode controls who can run enabled commands. Custom mode requires at least one role ID.</p>
          <p class="muted">Default named-role gate: {escape(", ".join(str(item) for item in allowed_role_names) or "None")}</p>
          <p class="muted">Current moderator role IDs: <span class="mono">{escape(",".join(str(item) for item in moderator_role_ids) or "None")}</span></p>
          {role_hint_html}
          <form method="post">
            <table>
              <thead>
                <tr><th>Command</th><th>Default Access</th><th>Enabled</th><th>Mode</th><th>Custom Role Selection</th></tr>
              </thead>
              <tbody>
                {"".join(rows)}
              </tbody>
            </table>
            <div style="margin-top:14px;">
              <button class="btn" type="submit">Save Command Permissions</button>
            </div>
          </form>
        </div>
        """
        return _render_page("Command Permissions", body, user["email"], bool(user.get("is_admin")))

    @app.route("/admin/guild-settings", methods=["GET", "POST"])
    @login_required
    def guild_settings():
        user = _current_user()
        selection_redirect = _require_selected_guild_redirect()
        if selection_redirect is not None:
            return selection_redirect
        selected_guild = _selected_guild() or {}
        selected_guild_id = str(selected_guild.get("id") or "")
        guild_name = str(selected_guild.get("name") or "Unknown")
        settings_payload = (
            on_get_guild_settings(selected_guild_id)
            if callable(on_get_guild_settings)
            else {"ok": False, "error": "Guild settings callbacks are not configured."}
        )
        channel_options, role_options, catalog_error = _load_discord_catalog_options(selected_guild_id)
        text_channel_options = [
            option for option in channel_options if str(option.get("type") or "").strip().lower() == "text"
        ]
        max_welcome_image_upload_bytes = _get_int_env("WEB_AVATAR_MAX_UPLOAD_BYTES", 2 * 1024 * 1024, minimum=1024)

        if request.method == "POST":
            response, messages = process_guild_settings_submission(
                form=request.form,
                files=request.files,
                on_save_guild_settings=on_save_guild_settings,
                actor_email=user["email"],
                selected_guild_id=selected_guild_id,
                max_welcome_image_upload_bytes=max_welcome_image_upload_bytes,
            )
            for message, category in messages:
                flash(message, category)
            if isinstance(response, dict):
                settings_payload = response

        if not isinstance(settings_payload, dict) or not settings_payload.get("ok"):
            error_text = (
                str(settings_payload.get("error") or "Unable to load guild settings.")
                if isinstance(settings_payload, dict)
                else "Unable to load guild settings."
            )
            body = (
                f"<div class='card'><h2>Guild Settings</h2><p class='muted'>Could not load guild settings: {escape(error_text)}</p></div>"
            )
            return _render_page("Guild Settings", body, user["email"], bool(user.get("is_admin")))

        current_settings = settings_payload.get("settings", {}) or {}
        effective_settings = settings_payload.get("effective", {}) or {}
        body = render_guild_settings_body(
            guild_name=guild_name,
            current_settings=current_settings,
            effective_settings=effective_settings,
            text_channel_options=text_channel_options,
            role_options=role_options,
            catalog_error=catalog_error,
            max_welcome_image_upload_bytes=max_welcome_image_upload_bytes,
            render_select_input=_render_select_input,
        )
        return _render_page("Guild Settings", body, user["email"], bool(user.get("is_admin")))

    @app.route("/admin/moderation", methods=["GET", "POST"])
    @login_required
    def moderation_page():
        user = _current_user()
        selection_redirect = _require_selected_guild_redirect()
        if selection_redirect is not None:
            return selection_redirect
        selected_guild = _selected_guild() or {}
        selected_guild_id = str(selected_guild.get("id") or "")
        guild_name = str(selected_guild.get("name") or "Unknown")
        settings_payload = (
            on_get_guild_settings(selected_guild_id)
            if callable(on_get_guild_settings)
            else {"ok": False, "error": "Moderation callbacks are not configured."}
        )
        channel_options, _, catalog_error = _load_discord_catalog_options(selected_guild_id)
        text_channel_options = [
            option for option in channel_options if str(option.get("type") or "").strip().lower() == "text"
        ]

        if request.method == "POST":
            response, messages = process_moderation_submission(
                form=request.form,
                on_save_guild_settings=on_save_guild_settings,
                actor_email=user["email"],
                selected_guild_id=selected_guild_id,
            )
            for message, category in messages:
                flash(message, category)
            if isinstance(response, dict):
                settings_payload = response

        if not isinstance(settings_payload, dict) or not settings_payload.get("ok"):
            error_text = (
                str(settings_payload.get("error") or "Unable to load moderation settings.")
                if isinstance(settings_payload, dict)
                else "Unable to load moderation settings."
            )
            body = (
                f"<div class='card'><h2>Moderation</h2><p class='muted'>Could not load moderation settings: {escape(error_text)}</p></div>"
            )
            return _render_page("Moderation", body, user["email"], bool(user.get("is_admin")))

        current_settings = settings_payload.get("settings", {}) or {}
        effective_settings = settings_payload.get("effective", {}) or {}
        body = render_moderation_body(
            guild_name=guild_name,
            current_settings=current_settings,
            effective_settings=effective_settings,
            text_channel_options=text_channel_options,
            catalog_error=catalog_error,
            render_select_input=_render_select_input,
            render_fixed_select_input=_render_fixed_select_input,
        )
        return _render_page("Moderation", body, user["email"], bool(user.get("is_admin")))

    @app.route("/admin/settings", methods=["GET", "POST"])
    @login_required
    def settings():
        user = _current_user()
        selected_guild = _selected_guild() or {}
        selected_guild_id = str(selected_guild.get("id") or "")
        file_values = _load_effective_env_values(env_file, fallback_env_file)
        normalized_file_values = _normalize_env_updates(file_values)
        if normalized_file_values != file_values:
            saved, save_error, saved_env_file, _ = _try_write_env_file_with_fallback(
                env_file,
                fallback_env_file,
                normalized_file_values,
            )
            if not saved:
                flash(save_error, "warning")
            else:
                file_values = normalized_file_values
                os.environ["WEB_ENV_FILE"] = str(saved_env_file)
                for key, value in file_values.items():
                    os.environ[key] = value
        channel_options, role_options, catalog_error = _load_discord_catalog_options(selected_guild_id)

        if request.method == "POST":
            updated_values = {}
            for key, _, _ in ENV_FIELDS:
                current = _read_env_value(file_values, key)
                if key in SENSITIVE_KEYS:
                    submitted = request.form.get(key, "")
                    updated_values[key] = submitted if submitted else current
                else:
                    updated_values[key] = request.form.get(key, "").strip()
            updated_values = _normalize_env_updates(updated_values)

            validation_errors = _validate_env_updates(updated_values)
            if validation_errors:
                for entry in validation_errors:
                    flash(entry, "error")
            else:
                final_values = dict(file_values)
                for key, value in updated_values.items():
                    if value == "":
                        final_values.pop(key, None)
                    else:
                        final_values[key] = value
                for legacy_keys in ENV_KEY_ALIASES.values():
                    for legacy_key in legacy_keys:
                        final_values.pop(legacy_key, None)
                saved, save_error, saved_env_file, skipped_keys = _try_write_env_file_with_fallback(
                    env_file,
                    fallback_env_file,
                    final_values,
                )
                if not saved:
                    file_values = final_values
                    flash(save_error, "error")
                else:
                    os.environ["WEB_ENV_FILE"] = str(saved_env_file)
                    for key, value in updated_values.items():
                        if key in skipped_keys:
                            continue
                        if value == "":
                            os.environ.pop(key, None)
                        else:
                            os.environ[key] = value
                    for legacy_keys in ENV_KEY_ALIASES.values():
                        for legacy_key in legacy_keys:
                            os.environ.pop(legacy_key, None)
                    effective_timeout_minutes = _normalize_session_timeout_minutes(
                        final_values.get(
                            "WEB_SESSION_TIMEOUT_MINUTES",
                            os.getenv(
                                "WEB_SESSION_TIMEOUT_MINUTES",
                                str(WEB_INACTIVITY_TIMEOUT_MINUTES),
                            ),
                        ),
                        default_value=WEB_INACTIVITY_TIMEOUT_MINUTES,
                    )
                    session_timeout_state["minutes"] = effective_timeout_minutes
                    if callable(on_env_settings_saved):
                        applied_updates = {key: value for key, value in updated_values.items() if key not in skipped_keys}
                        on_env_settings_saved({**applied_updates, "WEB_ENV_FILE": str(saved_env_file)})
                    if saved_env_file != env_file:
                        flash(
                            f"Settings saved to fallback env file {saved_env_file} and applied where supported.",
                            "success",
                        )
                    else:
                        flash(f"Settings saved to {env_file} and applied where supported.", "success")
                    file_values = _load_effective_env_values(env_file, fallback_env_file)

        grouped_rows: dict[str, list[str]] = {section_title: [] for section_title, _section_description, _field_keys in ENV_FIELD_SECTIONS}
        for key, label, description in ENV_FIELDS:
            value = _read_env_value(file_values, key)
            safe_value = "" if key in SENSITIVE_KEYS else value
            placeholder = "•••••• (unchanged if blank)" if key in SENSITIVE_KEYS else ""
            input_type = "password" if key in SENSITIVE_KEYS else "text"
            static_select_options = []
            select_options = []
            select_placeholder = "Select..."
            if key == "WEB_SESSION_TIMEOUT_MINUTES":
                safe_value = str(
                    _normalize_session_timeout_minutes(
                        safe_value or str(WEB_INACTIVITY_TIMEOUT_MINUTES),
                        default_value=WEB_INACTIVITY_TIMEOUT_MINUTES,
                    )
                )
                static_select_options = [
                    {"value": str(minutes), "label": f"{minutes} minutes"} for minutes in SESSION_TIMEOUT_MINUTE_OPTIONS
                ]
                select_placeholder = "Select auto logout timeout..."
            elif key == "REDDIT_FEED_CHECK_SCHEDULE":
                static_select_options = [{"value": value, "label": label} for value, label in REDDIT_FEED_SCHEDULE_OPTIONS]
                select_placeholder = "Select Reddit polling interval..."
            elif key in {
                "ENABLE_MEMBERS_INTENT",
                "COMMAND_RESPONSES_EPHEMERAL",
                "SHORTENER_ENABLED",
                "FIRMWARE_MONITOR_ENABLED",
                "REDDIT_FEED_NOTIFY_ENABLED",
                "YOUTUBE_NOTIFY_ENABLED",
                "LINKEDIN_NOTIFY_ENABLED",
                "BETA_PROGRAM_NOTIFY_ENABLED",
                "SERVICE_MONITOR_ENABLED",
                "UPTIME_STATUS_ENABLED",
                "UPTIME_STATUS_NOTIFY_ENABLED",
                "UPTIME_STATUS_VERIFY_TLS",
            }:
                safe_value = "true" if _is_truthy_env_value(safe_value) else "false"
                static_select_options = [
                    {"value": "false", "label": "false"},
                    {"value": "true", "label": "true"},
                ]
                select_placeholder = "Select true/false..."
            elif key in {"LOG_LEVEL", "CONTAINER_LOG_LEVEL", "DISCORD_LOG_LEVEL"}:
                safe_level = str(safe_value or "INFO").strip().upper()
                if safe_level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
                    safe_level = "INFO"
                safe_value = safe_level
                static_select_options = [
                    {"value": "DEBUG", "label": "DEBUG"},
                    {"value": "INFO", "label": "INFO"},
                    {"value": "WARNING", "label": "WARNING"},
                    {"value": "ERROR", "label": "ERROR"},
                    {"value": "CRITICAL", "label": "CRITICAL"},
                ]
                select_placeholder = "Select log level..."
            elif key == "WEB_SESSION_COOKIE_SAMESITE":
                safe_value = _normalize_session_cookie_samesite(safe_value or "Lax", default_value="Lax")
                static_select_options = [
                    {"value": "Lax", "label": "Lax (recommended)"},
                    {"value": "Strict", "label": "Strict"},
                    {"value": "None", "label": "None (requires HTTPS secure cookie)"},
                ]
                select_placeholder = "Select SameSite policy..."
            elif key == "PUPPY_IMAGE_TIMEOUT_SECONDS":
                static_select_options = [{"value": value, "label": f"{value}s"} for value in ("5", "8", "10", "15", "30")]
                select_placeholder = "Select puppy API timeout..."
            elif key == "SHORTENER_TIMEOUT_SECONDS":
                static_select_options = [{"value": value, "label": f"{value}s"} for value in ("5", "8", "10", "15", "30")]
                select_placeholder = "Select shortener timeout..."
            elif key == "YOUTUBE_POLL_INTERVAL_SECONDS":
                static_select_options = [
                    {"value": value, "label": label}
                    for value, label in (
                        ("60", "Every 1 minute"),
                        ("120", "Every 2 minutes"),
                        ("300", "Every 5 minutes"),
                        ("600", "Every 10 minutes"),
                        ("900", "Every 15 minutes"),
                        ("1800", "Every 30 minutes"),
                    )
                ]
                select_placeholder = "Select YouTube polling interval..."
            elif key == "LINKEDIN_POLL_INTERVAL_SECONDS":
                static_select_options = [
                    {"value": value, "label": label}
                    for value, label in (
                        ("300", "Every 5 minutes"),
                        ("600", "Every 10 minutes"),
                        ("900", "Every 15 minutes"),
                        ("1800", "Every 30 minutes"),
                        ("3600", "Every 60 minutes"),
                    )
                ]
                select_placeholder = "Select LinkedIn polling interval..."
            elif key == "UPTIME_STATUS_CHECK_SCHEDULE":
                static_select_options = [
                    {"value": value, "label": label}
                    for value, label in (
                        ("*/1 * * * *", "Every 1 minute"),
                        ("*/5 * * * *", "Every 5 minutes"),
                        ("*/10 * * * *", "Every 10 minutes"),
                        ("*/15 * * * *", "Every 15 minutes"),
                        ("*/30 * * * *", "Every 30 minutes"),
                    )
                ]
                select_placeholder = "Select Uptime Kuma polling interval..."
            elif key in {"YOUTUBE_REQUEST_TIMEOUT_SECONDS", "LINKEDIN_REQUEST_TIMEOUT_SECONDS", "UPTIME_STATUS_TIMEOUT_SECONDS"}:
                static_select_options = [{"value": value, "label": f"{value}s"} for value in ("5", "8", "10", "12", "15", "30")]
                select_placeholder = "Select timeout..."
            if key == "firmware_notification_channel" or key.endswith("_CHANNEL_ID"):
                select_options = channel_options
            elif key.endswith("_ROLE_ID"):
                select_options = role_options

            if key == "SERVICE_MONITOR_TARGETS_JSON":
                input_html = (
                    f"<textarea name='{escape(key, quote=True)}' "
                    "placeholder='[{&quot;name&quot;:&quot;Discord Status&quot;,&quot;url&quot;:&quot;https://discordstatus.com&quot;}]'>"
                    f"{escape(str(safe_value or ''))}</textarea>"
                )
            elif static_select_options:
                input_html = _render_fixed_select_input(
                    name=key,
                    selected_value=safe_value,
                    options=static_select_options,
                    placeholder=select_placeholder,
                )
            elif select_options:
                input_html = _render_select_input(
                    name=key,
                    selected_value=safe_value,
                    options=select_options,
                    placeholder=select_placeholder if select_placeholder != "Select..." else "Select from Discord...",
                )
            else:
                input_html = (
                    f"<input type='{escape(input_type)}' name='{escape(key)}' "
                    f"value='{escape(safe_value, quote=True)}' placeholder='{escape(placeholder, quote=True)}' />"
                )
            section_title, _section_description = ENV_FIELD_SECTION_LOOKUP.get(key, ("Other Settings", ""))
            grouped_rows.setdefault(section_title, []).append(
                f"""
                <tr>
                  <td><strong>{escape(label)}</strong><div class="muted mono">{escape(key)}</div></td>
                  <td>{input_html}</td>
                  <td class="muted">{escape(description)}</td>
                </tr>
                """
            )
        catalog_note = ""
        if channel_options or role_options:
            catalog_note = (
                f"<p class='muted'>Loaded live Discord options from {escape(str(selected_guild.get('name') or 'unknown'))} "
                f"({escape(selected_guild_id or 'unknown')}). Channels: {len(channel_options)}; Roles: {len(role_options)}.</p>"
            )
        elif catalog_error:
            catalog_note = f"<p class='muted'>Could not load Discord options: {escape(catalog_error)}</p>"

        section_cards = []
        for section_title, section_description, _field_keys in ENV_FIELD_SECTIONS:
            section_rows = grouped_rows.get(section_title, [])
            if not section_rows:
                continue
            section_cards.append(
                "<div class='card'>"
                f"<h3>{escape(section_title)}</h3>"
                f"<p class='muted'>{escape(section_description)}</p>"
                "<table><thead><tr><th>Setting</th><th>Value</th><th>Description</th></tr></thead>"
                f"<tbody>{''.join(section_rows)}</tbody></table>"
                "</div>"
            )

        body = (
            "<div class='card'><h2>Global Environment Settings</h2>"
            "<p class='muted'>These settings are shared across all Discord servers managed by this bot. Use this page for global defaults and runtime behavior.</p>"
            "<p class='muted'>If a setting also exists in <span class='mono'>/admin/guild-settings</span>, the guild value overrides the global default for that one server.</p>"
            + (
                f"<p class='muted'>Discord dropdown data is loaded from the selected server: <strong>{escape(str(selected_guild.get('name') or 'Unknown'))}</strong>.</p>"
                if selected_guild_id
                else "<p class='muted'>Select a Discord server to populate live channel and role dropdowns.</p>"
            )
            + f"{catalog_note}"
            + "<form method='post'>"
            + "".join(section_cards)
            + "<div class='card'><div style='margin-top:4px;'><button class='btn' type='submit'>Save Global Settings</button></div></div></form>"
        )
        return _render_page("Global Settings", body, user["email"], bool(user.get("is_admin")))

    @app.route("/admin/tag-responses", methods=["GET", "POST"])
    @login_required
    def tag_responses():
        user = _current_user()
        selection_redirect = _require_selected_guild_redirect()
        if selection_redirect is not None:
            return selection_redirect
        selected_guild = _selected_guild() or {}
        selected_guild_id = str(selected_guild.get("id") or "")
        selected_guild_name = str(selected_guild.get("name") or "Unknown")
        path = Path(tag_responses_file)
        path.parent.mkdir(parents=True, exist_ok=True)

        if request.method == "POST":
            raw = request.form.get("tag_json", "")
            try:
                parsed = json.loads(raw)
                if not isinstance(parsed, dict):
                    raise ValueError("JSON must be an object")
                for key, value in parsed.items():
                    if not isinstance(key, str) or not isinstance(value, str):
                        raise ValueError("All tag keys/values must be strings")
                if callable(on_save_tag_responses):
                    response = on_save_tag_responses(parsed, user["email"], selected_guild_id)
                    if not isinstance(response, dict):
                        raise ValueError("Invalid response from tag response save handler")
                    if not response.get("ok"):
                        raise ValueError(str(response.get("error") or "Failed to save tag responses"))
                else:
                    path.write_text(json.dumps(parsed, indent=2) + "\n")
                if callable(on_tag_responses_saved):
                    on_tag_responses_saved(selected_guild_id)
                flash("Tag responses updated.", "success")
            except Exception as exc:
                flash(f"Invalid tag JSON: {exc}", "error")

        if callable(on_get_tag_responses):
            response = on_get_tag_responses(selected_guild_id)
            if isinstance(response, dict) and response.get("ok"):
                current_mapping = response.get("mapping", {}) or {}
                current = json.dumps(current_mapping, indent=2) + "\n"
            else:
                error_text = response.get("error") if isinstance(response, dict) else "Unknown error"
                flash(f"Could not load tag responses from storage: {error_text}", "error")
                current = "{}\n"
        else:
            if not path.exists():
                path.write_text("{}\n")
            current = path.read_text()

        escaped_current = escape(current)
        body = f"""
        <div class="card">
          <h2>Tag Responses</h2>
          <p class="muted">Edit the tag-to-response JSON mapping used by slash and message tag commands in <strong>{escape(selected_guild_name)}</strong>.</p>
          <form method="post">
            <textarea name="tag_json">{escaped_current}</textarea>
            <div style="margin-top:14px;">
              <button class="btn" type="submit">Save Tag Responses</button>
            </div>
          </form>
        </div>
        """
        return _render_page("Tag Responses", body, user["email"], bool(user.get("is_admin")))

    @app.route("/admin/bulk-role-csv", methods=["GET", "POST"])
    @login_required
    def bulk_role_csv():
        user = _current_user()
        selection_redirect = _require_selected_guild_redirect()
        if selection_redirect is not None:
            return selection_redirect
        selected_guild = _selected_guild() or {}
        selected_guild_id = str(selected_guild.get("id") or "")
        operation_result = None
        max_upload_bytes = _get_int_env("WEB_BULK_ASSIGN_MAX_UPLOAD_BYTES", 2 * 1024 * 1024, minimum=1024)
        report_list_limit = _get_int_env("WEB_BULK_ASSIGN_REPORT_LIST_LIMIT", 50, minimum=1)
        _channel_options, role_options, catalog_error = _load_discord_catalog_options(selected_guild_id)

        if request.method == "POST":
            selected_role_input = request.form.get("role_id_select", "").strip()
            manual_role_input = request.form.get("role_id", "").strip()
            role_input = selected_role_input if role_options else (manual_role_input or selected_role_input)
            uploaded_file = request.files.get("csv_file")
            if not role_input:
                flash("Role selection is required.", "error")
            elif uploaded_file is None or not uploaded_file.filename:
                flash("CSV file is required.", "error")
            elif not uploaded_file.filename.lower().endswith(".csv"):
                flash("Uploaded file must be a .csv file.", "error")
            elif not callable(on_bulk_assign_role_csv):
                flash("Bulk CSV assignment is not configured in this runtime.", "error")
            else:
                payload = uploaded_file.read()
                if not payload:
                    flash("Uploaded CSV is empty.", "error")
                elif len(payload) > max_upload_bytes:
                    flash(
                        f"CSV file is too large ({len(payload)} bytes). Max allowed is {max_upload_bytes} bytes.",
                        "error",
                    )
                else:
                    response = on_bulk_assign_role_csv(
                        selected_guild_id,
                        role_input,
                        payload,
                        uploaded_file.filename,
                        user["email"],
                    )
                    if not isinstance(response, dict):
                        flash("Invalid response from bulk assignment handler.", "error")
                    elif not response.get("ok"):
                        flash(response.get("error", "Bulk assignment failed."), "error")
                    else:
                        operation_result = response
                        flash("Bulk assignment completed.", "success")

        summary_html = ""
        details_html = ""
        report_html = ""
        if operation_result:
            summary_lines = operation_result.get("summary_lines", [])
            summary_rows = "".join(f"<div class='mono'>{escape(line)}</div>" for line in summary_lines)
            summary_html = f"""
            <div class="card">
              <h3>Result Summary</h3>
              {summary_rows}
            </div>
            """

            result_data = operation_result.get("result", {})

            def build_list_section(title: str, key: str, limit: int):
                values = result_data.get(key, []) or []
                if not values:
                    return f"<div><h4>{escape(title)} (0)</h4><p class='muted'>None</p></div>"
                items = "".join(f"<li class='mono'>{escape(value)}</li>" for value in values[:limit])
                overflow = len(values) - limit
                overflow_note = f"<p class='muted'>... and {overflow} more</p>" if overflow > 0 else ""
                return f"<div><h4>{escape(title)} ({len(values)})</h4><ul>{items}</ul>{overflow_note}</div>"

            details_html = f"""
            <div class="card">
              <h3>Missing / Errors</h3>
              {build_list_section("Unmatched", "unmatched_names", report_list_limit)}
              {build_list_section("Ambiguous", "ambiguous_names", report_list_limit)}
              {build_list_section("Failed", "assignment_failures", report_list_limit)}
            </div>
            """

            report_html = f"""
            <div class="card">
              <h3>Full Report</h3>
              <textarea readonly>{escape(operation_result.get("report_text", ""))}</textarea>
            </div>
            """

        role_picker_html = ""
        if role_options:
            role_picker_html = (
                "<label>Role (Discord list)</label>"
                + _render_select_input("role_id_select", "", role_options, "Choose role...")
                + "<p class='muted'>Choose the target role using the current guild role list.</p>"
            )
        elif catalog_error:
            role_picker_html = f"<p class='muted'>Could not load role dropdown: {escape(catalog_error)}</p>"
        else:
            role_picker_html = "<p class='muted'>Role dropdown is unavailable. Use manual Role ID input.</p>"

        body = f"""
        <div class="card">
          <h2>Bulk Assign Role from CSV</h2>
          <p class="muted">Selected server: <strong>{escape(str(selected_guild.get("name") or "Unknown"))}</strong></p>
          <p class="muted">Upload a CSV of Discord names (comma-separated or one-per-line), and assign all matched members to the specified role.</p>
          <p class="muted">Current upload limit: {max_upload_bytes} bytes. Current per-section display limit: {report_list_limit} entries.</p>
          <form method="post" enctype="multipart/form-data">
            {role_picker_html}
            {"<label>Role ID (or role mention like &lt;@&amp;123&gt;)</label><input type='text' name='role_id' placeholder='123456789012345678' />" if not role_options else ""}
            <label style="margin-top:10px;display:block;">CSV file</label>
            <input type="file" name="csv_file" accept=".csv,text/csv" required />
            <div style="margin-top:14px;">
              <button class="btn" type="submit">Run Bulk Assignment</button>
            </div>
          </form>
        </div>
        {summary_html}
        {details_html}
        {report_html}
        """
        return _render_page("Bulk Role CSV", body, user["email"], bool(user.get("is_admin")))

    @app.route("/admin/users", methods=["GET", "POST"])
    @admin_required
    def users():
        user = _current_user()
        users_data = _read_users(users_file)
        groups_data = _read_guild_groups(users_file)
        all_guilds, guild_error = _load_all_guilds()
        guild_options = [
            {
                "id": str(entry.get("id") or "").strip(),
                "label": str(entry.get("name") or entry.get("id") or "Unknown"),
            }
            for entry in all_guilds
            if str(entry.get("id") or "").strip()
        ]
        group_options = [
            {
                "id": str(entry.get("id") or "").strip(),
                "label": str(entry.get("name") or entry.get("id") or "Unknown"),
            }
            for entry in groups_data
            if str(entry.get("id") or "").strip()
        ]
        group_name_by_id = {
            str(entry.get("id") or "").strip(): str(entry.get("name") or "").strip()
            for entry in groups_data
            if str(entry.get("id") or "").strip()
        }

        if request.method == "POST":
            action = request.form.get("action", "").strip()
            if action == "create_group":
                group_name = _normalize_guild_group_name(request.form.get("group_name", ""))
                guild_ids = _normalize_id_string_list(request.form.getlist("guild_ids"))
                if not group_name:
                    flash("Guild group name is required.", "error")
                elif not guild_ids:
                    flash("Choose at least one Discord server for the guild group.", "error")
                elif any(str(entry.get("name") or "").strip().casefold() == group_name.casefold() for entry in groups_data):
                    flash("A guild group with that name already exists.", "error")
                else:
                    groups_data.append(
                        {
                            "id": secrets.token_hex(8),
                            "name": group_name,
                            "guild_ids": guild_ids,
                            "created_at": _now_iso(),
                        }
                    )
                    _save_guild_groups(users_file, groups_data)
                    flash(f"Created guild group {group_name}.", "success")
                    groups_data = _read_guild_groups(users_file)
            elif action == "edit_group":
                group_id = str(request.form.get("group_id", "") or "").strip()
                group_name = _normalize_guild_group_name(request.form.get("group_name", ""))
                guild_ids = _normalize_id_string_list(request.form.getlist("guild_ids"))
                if not group_id:
                    flash("Guild group ID is required.", "error")
                elif not group_name:
                    flash("Guild group name is required.", "error")
                elif not guild_ids:
                    flash("Choose at least one Discord server for the guild group.", "error")
                elif any(
                    str(entry.get("name") or "").strip().casefold() == group_name.casefold()
                    and str(entry.get("id") or "").strip() != group_id
                    for entry in groups_data
                ):
                    flash("Another guild group already uses that name.", "error")
                else:
                    changed = False
                    for entry in groups_data:
                        if str(entry.get("id") or "").strip() == group_id:
                            entry["name"] = group_name
                            entry["guild_ids"] = guild_ids
                            changed = True
                            break
                    if changed:
                        _save_guild_groups(users_file, groups_data)
                        flash(f"Updated guild group {group_name}.", "success")
                        groups_data = _read_guild_groups(users_file)
                    else:
                        flash("Guild group not found.", "error")
            elif action == "delete_group":
                group_id = str(request.form.get("group_id", "") or "").strip()
                next_groups = [entry for entry in groups_data if str(entry.get("id") or "").strip() != group_id]
                if len(next_groups) == len(groups_data):
                    flash("Guild group not found.", "error")
                else:
                    for entry in users_data:
                        entry["guild_group_ids"] = [
                            value
                            for value in _normalize_string_id_list(entry.get("guild_group_ids", []))
                            if value != group_id
                        ]
                    _save_guild_groups(users_file, next_groups)
                    _save_users(users_file, users_data)
                    flash("Deleted guild group.", "success")
                    groups_data = _read_guild_groups(users_file)
                    users_data = _read_users(users_file)
            elif action == "create":
                email = _normalize_email(request.form.get("email", ""))
                password = request.form.get("password", "")
                confirm_password = request.form.get("confirm_password", "")
                first_name = _clean_profile_text(request.form.get("first_name", ""), max_length=80)
                last_name = _clean_profile_text(request.form.get("last_name", ""), max_length=80)
                display_name = _clean_profile_text(request.form.get("display_name", ""), max_length=80)
                requested_role = _normalize_web_user_role(request.form.get("role", "read_only"))
                guild_group_ids = _normalize_string_id_list(request.form.getlist("guild_group_ids"))
                is_admin = requested_role == "admin"
                if not _is_valid_email(email):
                    flash("Enter a valid email.", "error")
                elif not first_name:
                    flash("First name is required.", "error")
                elif not last_name:
                    flash("Last name is required.", "error")
                elif not display_name:
                    flash("Display name is required.", "error")
                elif any(entry["email"] == email for entry in users_data):
                    flash("A user with that email already exists.", "error")
                elif requested_role == "guild_admin" and not guild_group_ids:
                    flash("Guild Admin users must be assigned at least one guild group.", "error")
                elif password != confirm_password:
                    flash("Password and confirmation must match.", "error")
                else:
                    password_errors = _password_policy_errors(password)
                    if password_errors:
                        for message in password_errors:
                            flash(message, "error")
                    else:
                        users_data.append(
                            {
                                "email": email,
                                "password_hash": _hash_password(password),
                                "role": requested_role,
                                "is_admin": is_admin,
                                "first_name": first_name,
                                "last_name": last_name,
                                "display_name": display_name,
                                "guild_group_ids": guild_group_ids,
                                "password_changed_at": _now_iso(),
                                "email_changed_at": _now_iso(),
                                "created_at": _now_iso(),
                            }
                        )
                        _save_users(users_file, users_data)
                        flash(f"Created user {email}.", "success")
                        users_data = _read_users(users_file)

            elif action == "delete":
                target_email = _normalize_email(request.form.get("email", ""))
                candidate = [entry for entry in users_data if entry["email"] != target_email]
                admin_count = sum(1 for entry in candidate if entry.get("is_admin"))
                if target_email == user["email"]:
                    flash("You cannot delete your own account.", "error")
                elif admin_count < 1:
                    flash("At least one admin account must remain.", "error")
                elif len(candidate) == len(users_data):
                    flash("User not found.", "error")
                else:
                    _save_users(users_file, candidate)
                    flash(f"Deleted user {target_email}.", "success")
                    users_data = _read_users(users_file)

            elif action == "password":
                target_email = _normalize_email(request.form.get("email", ""))
                new_password = request.form.get("password", "")
                confirm_password = request.form.get("confirm_password", "")
                if new_password != confirm_password:
                    flash("Password and confirmation must match.", "error")
                else:
                    password_errors = _password_policy_errors(new_password)
                    if password_errors:
                        for message in password_errors:
                            flash(message, "error")
                    else:
                        changed = False
                        for entry in users_data:
                            if entry["email"] == target_email:
                                entry["password_hash"] = _hash_password(new_password)
                                entry["password_changed_at"] = _now_iso()
                                changed = True
                                break
                        if changed:
                            _save_users(users_file, users_data)
                            flash(f"Password updated for {target_email}.", "success")
                            users_data = _read_users(users_file)
                        else:
                            flash("User not found.", "error")

            elif action == "edit_user":
                target_email = _normalize_email(request.form.get("email", ""))
                updated_email = _normalize_email(request.form.get("updated_email", ""))
                first_name = _clean_profile_text(request.form.get("first_name", ""), max_length=80)
                last_name = _clean_profile_text(request.form.get("last_name", ""), max_length=80)
                display_name = _clean_profile_text(request.form.get("display_name", ""), max_length=80)
                guild_group_ids = _normalize_string_id_list(request.form.getlist("guild_group_ids"))
                if not target_email:
                    flash("User email is required.", "error")
                elif not _is_valid_email(updated_email):
                    flash("Enter a valid updated email.", "error")
                elif not first_name:
                    flash("First name is required.", "error")
                elif not last_name:
                    flash("Last name is required.", "error")
                elif not display_name:
                    flash("Display name is required.", "error")
                elif updated_email != target_email and any(entry["email"] == updated_email for entry in users_data):
                    flash("Another user already has that email.", "error")
                else:
                    changed = False
                    now_iso = _now_iso()
                    for entry in users_data:
                        if entry["email"] == target_email:
                            original_email = entry["email"]
                            entry["email"] = updated_email
                            entry["first_name"] = first_name
                            entry["last_name"] = last_name
                            entry["display_name"] = display_name
                            if _normalize_web_user_role(entry.get("role", ""), is_admin=bool(entry.get("is_admin"))) == "guild_admin":
                                entry["guild_group_ids"] = guild_group_ids
                            if updated_email != original_email:
                                entry["email_changed_at"] = now_iso
                            changed = True
                            break
                    if changed:
                        _save_users(users_file, users_data)
                        flash(f"Updated user profile for {updated_email}.", "success")
                        users_data = _read_users(users_file)
                    else:
                        flash("User not found.", "error")

            elif action == "set_role":
                target_email = _normalize_email(request.form.get("email", ""))
                requested_role = _normalize_web_user_role(request.form.get("role", "read_only"))
                target_is_admin = requested_role == "admin"
                if target_email == user["email"] and not target_is_admin:
                    flash(
                        "You cannot set your own account to a non-admin role. Another admin must do this.",
                        "error",
                    )
                else:
                    changed = False
                    for entry in users_data:
                        if entry["email"] == target_email:
                            if requested_role == "guild_admin" and not _normalize_string_id_list(entry.get("guild_group_ids", [])):
                                flash("Assign at least one guild group before changing this user to Guild Admin.", "error")
                                changed = False
                                break
                            entry["role"] = requested_role
                            entry["is_admin"] = target_is_admin
                            changed = True
                            break
                    if changed:
                        if sum(1 for entry in users_data if entry.get("is_admin")) < 1:
                            flash("At least one admin account must remain.", "error")
                        else:
                            _save_users(users_file, users_data)
                            flash(
                                f"Updated role for {target_email} to {_user_role_label(requested_role, is_admin=target_is_admin)}.",
                                "success",
                            )
                            users_data = _read_users(users_file)
                    else:
                        flash("User not found.", "error")

        user_rows = []
        for entry in users_data:
            email = entry["email"]
            current_role_value = _normalize_web_user_role(entry.get("role", ""), is_admin=bool(entry.get("is_admin")))
            is_admin_entry = current_role_value == "admin"
            role_label = _user_role_label(current_role_value, is_admin=is_admin_entry)
            assigned_group_ids = _normalize_string_id_list(entry.get("guild_group_ids", []))
            assigned_group_names = [group_name_by_id.get(group_id, f"Unknown ({group_id})") for group_id in assigned_group_ids]
            group_scope_label = ", ".join(assigned_group_names) if assigned_group_names else ("All guilds" if current_role_value in {"admin", "read_only"} else "Primary GL.iNet guild only" if current_role_value in {"glinet_read_only", "glinet_rw"} else "No guild groups")
            role_select_html = _render_fixed_select_input(
                f"role__{email}",
                current_role_value,
                [
                    {"value": "read_only", "label": "Read-only"},
                    {"value": "guild_admin", "label": "Guild Admin"},
                    {"value": "glinet_read_only", "label": "Glinet-Read-Only"},
                    {"value": "glinet_rw", "label": "Glinet-RW"},
                    {"value": "admin", "label": "Admin"},
                ],
                "Select role...",
            ).replace("<select ", "<select style='min-width:150px;' ")
            display_name = str(entry.get("display_name") or _default_display_name(email))
            first_name = str(entry.get("first_name") or "")
            last_name = str(entry.get("last_name") or "")
            full_name = _clean_profile_text(
                f"{first_name} {last_name}",
                max_length=160,
            )
            user_rows.append(
                f"""
                <tr>
                  <td>{escape(display_name)}</td>
                  <td>{escape(full_name or "n/a")}</td>
                  <td class="mono">{escape(email)}</td>
                  <td>{escape(role_label)}</td>
                  <td>{escape(group_scope_label)}</td>
                  <td class="mono">{escape(format_timestamp_display(entry.get("password_changed_at"), blank="n/a"))}</td>
                  <td class="mono">{escape(format_timestamp_display(entry.get("created_at"), blank="n/a"))}</td>
                  <td>
                    <form method="post" style="display:inline;">
                      <input type="hidden" name="action" value="set_role" />
                      <input type="hidden" name="email" value="{escape(email, quote=True)}" />
                      {role_select_html.replace(f"name='{escape(f'role__{email}', quote=True)}'", "name='role'")}
                      <button class="btn secondary" type="submit">Set Role</button>
                    </form>
                    <a class="btn secondary" style="margin-left:6px;" href="#edit-user-{escape(email, quote=True)}">Edit</a>
                    <form method="post" style="display:inline;margin-left:6px;">
                      <input type="hidden" name="action" value="delete" />
                      <input type="hidden" name="email" value="{escape(email, quote=True)}" />
                      <button class="btn secondary" type="submit">Delete</button>
                    </form>
                  </td>
                </tr>
                <tr>
                  <td colspan="7">
                    <details id="edit-user-{escape(email, quote=True)}">
                      <summary>Edit {escape(display_name)}</summary>
                      <div class="grid" style="margin-top:12px;">
                        <div class="card">
                          <h3>Edit Profile</h3>
                          <form method="post">
                            <input type="hidden" name="action" value="edit_user" />
                            <input type="hidden" name="email" value="{escape(email, quote=True)}" />
                            <label>First Name</label>
                            <input type="text" name="first_name" autocomplete="given-name" value="{escape(first_name, quote=True)}" required />
                            <label style="margin-top:10px;display:block;">Last Name</label>
                            <input type="text" name="last_name" autocomplete="family-name" value="{escape(last_name, quote=True)}" required />
                            <label style="margin-top:10px;display:block;">Display Name</label>
                            <input type="text" name="display_name" autocomplete="nickname" value="{escape(display_name, quote=True)}" required />
                            <label style="margin-top:10px;display:block;">Email</label>
                            <input type="email" name="updated_email" autocomplete="email" autocapitalize="none" spellcheck="false" value="{escape(email, quote=True)}" required />
                            <label style="margin-top:10px;display:block;">Guild Groups</label>
                            {_render_multi_select_input("guild_group_ids", assigned_group_ids, group_options, size=6)}
                            <p class="muted">Only used when this account role is <strong>Guild Admin</strong>. Use Ctrl/Cmd-click to select multiple groups.</p>
                            <button class="btn" type="submit" style="margin-top:14px;">Save User Changes</button>
                          </form>
                        </div>
                        <div class="card">
                          <h3>Reset Password</h3>
                          <form method="post">
                            <input type="hidden" name="action" value="password" />
                            <input type="hidden" name="email" value="{escape(email, quote=True)}" />
                            <label>New Password</label>
                            <input id="reset_user_password_{escape(email, quote=True)}" type="password" name="password" autocomplete="new-password" required />
                            <label style="margin-top:10px;display:block;">Confirm Password</label>
                            <input id="reset_user_password_confirm_{escape(email, quote=True)}" type="password" name="confirm_password" autocomplete="new-password" required />
                            <label style="margin-top:8px;display:block;">
                              <input type="checkbox"
                                onchange="document.getElementById('reset_user_password_{escape(email, quote=True)}').type=this.checked?'text':'password';document.getElementById('reset_user_password_confirm_{escape(email, quote=True)}').type=this.checked?'text':'password';" />
                              Show password
                            </label>
                            <button class="btn" type="submit" style="margin-top:14px;">Update Password</button>
                          </form>
                        </div>
                      </div>
                    </details>
                  </td>
                </tr>
                """
            )

        body = f"""
        <div class="grid">
          <div class="card">
            <h2>Create User</h2>
            <p class="muted">No public signup exists. Admins create accounts here.</p>
            <form method="post">
              <input type="hidden" name="action" value="create" />
              <label>First Name</label>
              <input type="text" name="first_name" autocomplete="given-name" required />
              <label style="margin-top:10px;display:block;">Last Name</label>
              <input type="text" name="last_name" autocomplete="family-name" required />
              <label style="margin-top:10px;display:block;">Display Name</label>
              <input type="text" name="display_name" autocomplete="nickname" required />
              <label style="margin-top:10px;display:block;">Email</label>
              <input type="email" name="email" autocomplete="email" autocapitalize="none" spellcheck="false" required />
              <label style="margin-top:10px;display:block;">Password</label>
              <input id="create_user_password" type="password" name="password" autocomplete="new-password" required />
              <label style="margin-top:10px;display:block;">Confirm Password</label>
              <input id="create_user_password_confirm" type="password" name="confirm_password" autocomplete="new-password" required />
              <label style="margin-top:8px;display:block;">
                <input type="checkbox"
                  onchange="document.getElementById('create_user_password').type=this.checked?'text':'password';document.getElementById('create_user_password_confirm').type=this.checked?'text':'password';" />
                Show password
              </label>
              <label style="margin-top:10px;display:block;">Role</label>
              <select name="role">
                <option value="read_only">Read-only</option>
                <option value="guild_admin">Guild Admin</option>
                <option value="glinet_read_only">Glinet-Read-Only</option>
                <option value="glinet_rw">Glinet-RW</option>
                <option value="admin">Admin</option>
              </select>
              <label style="margin-top:10px;display:block;">Guild Groups</label>
              {_render_multi_select_input("guild_group_ids", [], group_options, size=6)}
              <p class="muted">Guild Groups only apply to the <strong>Guild Admin</strong> role. Use Ctrl/Cmd-click to select multiple groups.</p>
              <p class="muted">Password policy: 6-16 characters, at least 2 numbers, 1 uppercase letter, and 1 symbol.</p>
              <button class="btn" type="submit">Create User</button>
            </form>
          </div>
          <div class="card">
            <h2>Guild Groups</h2>
            <p class="muted">Guild Groups define which Discord servers a <strong>Guild Admin</strong> can manage. Assign one or more groups to a user to scope their server access.</p>
            {f"<p class='muted'>Could not load Discord guild list: {escape(guild_error)}</p>" if guild_error else ""}
            <form method="post">
              <input type="hidden" name="action" value="create_group" />
              <label>Group Name</label>
              <input type="text" name="group_name" placeholder="Support Servers" required />
              <label style="margin-top:10px;display:block;">Discord Servers</label>
              {_render_multi_select_input("guild_ids", [], guild_options, size=6)}
              <p class="muted">Use Ctrl/Cmd-click to select multiple servers.</p>
              <button class="btn" type="submit">Create Guild Group</button>
            </form>
            {"".join(
                f'''
                <details style="margin-top:12px;">
                  <summary>{escape(str(group.get("name") or "Guild Group"))}</summary>
                  <form method="post" style="margin-top:12px;">
                    <input type="hidden" name="action" value="edit_group" />
                    <input type="hidden" name="group_id" value="{escape(str(group.get("id") or ""), quote=True)}" />
                    <label>Group Name</label>
                    <input type="text" name="group_name" value="{escape(str(group.get("name") or ""), quote=True)}" required />
                    <label style="margin-top:10px;display:block;">Discord Servers</label>
                    {_render_multi_select_input("guild_ids", group.get("guild_ids", []), guild_options, size=6)}
                    <div style="margin-top:12px;display:flex;gap:8px;flex-wrap:wrap;">
                      <button class="btn" type="submit">Save Group</button>
                    </div>
                  </form>
                  <form method="post" style="margin-top:8px;" onsubmit="return confirm('Delete this guild group? Assigned users will lose access from this group.');">
                    <input type="hidden" name="action" value="delete_group" />
                    <input type="hidden" name="group_id" value="{escape(str(group.get("id") or ""), quote=True)}" />
                    <button class="btn danger" type="submit">Delete Group</button>
                  </form>
                </details>
                '''
                for group in groups_data
            ) if groups_data else "<p class='muted'>No guild groups created yet.</p>"}
          </div>
        </div>
        <div class="card">
          <h2>Existing Users</h2>
          <table>
            <thead><tr><th>Display</th><th>Name</th><th>Email</th><th>Role</th><th>Guild Scope</th><th>Password Changed</th><th>Created</th><th>Actions</th></tr></thead>
            <tbody>{"".join(user_rows)}</tbody>
          </table>
        </div>
        """
        return _render_page("Users", body, user["email"], bool(user.get("is_admin")))

    return app


def start_web_admin_interface(
    host: str,
    port: int,
    https_port: int,
    https_enabled: bool,
    data_dir: str,
    env_file_path: str,
    tag_responses_file: str,
    default_admin_email: str,
    default_admin_password: str,
    on_get_guilds=None,
    on_get_guild_settings=None,
    on_save_guild_settings=None,
    on_env_settings_saved=None,
    on_get_tag_responses=None,
    on_save_tag_responses=None,
    on_tag_responses_saved=None,
    on_bulk_assign_role_csv=None,
    on_get_discord_catalog=None,
    on_get_command_permissions=None,
    on_save_command_permissions=None,
    on_get_actions=None,
    on_get_member_activity=None,
    on_export_member_activity=None,
    on_get_reddit_feeds=None,
    on_manage_reddit_feeds=None,
    on_get_youtube_subscriptions=None,
    on_manage_youtube_subscriptions=None,
    on_get_linkedin_subscriptions=None,
    on_manage_linkedin_subscriptions=None,
    on_get_beta_program_subscriptions=None,
    on_manage_beta_program_subscriptions=None,
    on_get_role_access_mappings=None,
    on_manage_role_access_mappings=None,
    on_get_bot_profile=None,
    on_update_bot_profile=None,
    on_update_bot_avatar=None,
    on_request_restart=None,
    on_leave_guild=None,
    logger=None,
):
    app = create_web_app(
        data_dir=data_dir,
        env_file_path=env_file_path,
        tag_responses_file=tag_responses_file,
        default_admin_email=default_admin_email,
        default_admin_password=default_admin_password,
        on_get_guilds=on_get_guilds,
        on_get_guild_settings=on_get_guild_settings,
        on_save_guild_settings=on_save_guild_settings,
        on_env_settings_saved=on_env_settings_saved,
        on_get_tag_responses=on_get_tag_responses,
        on_save_tag_responses=on_save_tag_responses,
        on_tag_responses_saved=on_tag_responses_saved,
        on_bulk_assign_role_csv=on_bulk_assign_role_csv,
        on_get_discord_catalog=on_get_discord_catalog,
        on_get_command_permissions=on_get_command_permissions,
        on_save_command_permissions=on_save_command_permissions,
        on_get_actions=on_get_actions,
        on_get_member_activity=on_get_member_activity,
        on_export_member_activity=on_export_member_activity,
        on_get_reddit_feeds=on_get_reddit_feeds,
        on_manage_reddit_feeds=on_manage_reddit_feeds,
        on_get_youtube_subscriptions=on_get_youtube_subscriptions,
        on_manage_youtube_subscriptions=on_manage_youtube_subscriptions,
        on_get_linkedin_subscriptions=on_get_linkedin_subscriptions,
        on_manage_linkedin_subscriptions=on_manage_linkedin_subscriptions,
        on_get_beta_program_subscriptions=on_get_beta_program_subscriptions,
        on_manage_beta_program_subscriptions=on_manage_beta_program_subscriptions,
        on_get_role_access_mappings=on_get_role_access_mappings,
        on_manage_role_access_mappings=on_manage_role_access_mappings,
        on_get_bot_profile=on_get_bot_profile,
        on_update_bot_profile=on_update_bot_profile,
        on_update_bot_avatar=on_update_bot_avatar,
        on_request_restart=on_request_restart,
        on_leave_guild=on_leave_guild,
        logger=logger,
    )
    servers = []
    threads = []

    def _serve_forever(server, name: str):
        thread = threading.Thread(target=server.serve_forever, name=name, daemon=True)
        thread.start()
        threads.append(thread)
        return thread

    http_server = make_server(host, port, app, threaded=True)
    servers.append(http_server)
    _serve_forever(http_server, "web_admin_http")
    if logger:
        logger.info("Starting web admin interface on http://%s:%s", host, port)

    if https_enabled:
        ssl_context, cert_path, key_path, generated = _ensure_https_ssl_context(
            data_dir=data_dir,
            harden_file_permissions=_is_truthy_env_value(os.getenv("WEB_HARDEN_FILE_PERMISSIONS", "true")),
            logger=logger,
        )
        https_server = make_server(host, https_port, app, threaded=True, ssl_context=ssl_context)
        servers.append(https_server)
        _serve_forever(https_server, "web_admin_https")
        if logger:
            logger.info(
                "Starting web admin interface on https://%s:%s using cert=%s key=%s%s",
                host,
                https_port,
                cert_path,
                key_path,
                " (generated self-signed)" if generated else "",
            )

    try:
        while True:
            for thread in threads:
                if not thread.is_alive():
                    raise RuntimeError(f"Web admin listener thread stopped unexpectedly: {thread.name}")
            time.sleep(1)
    finally:
        for server in servers:
            try:
                server.shutdown()
            except Exception as exc:
                if logger:
                    logger.debug("Web admin server shutdown raised %s", exc)

import discord
from discord.ext import commands
from discord import app_commands
import logging
import os
import json
import asyncio
import concurrent.futures
import re
import time
import csv
import io
import threading
import sqlite3
import secrets
import sys
import hashlib
from collections import deque
from difflib import SequenceMatcher
from datetime import timedelta, datetime, timezone
from html import unescape
from urllib.parse import urljoin
from dotenv import load_dotenv
import requests
from bs4 import BeautifulSoup
from croniter import croniter
from web_admin import start_web_admin_interface

load_dotenv()

# Directory to persist data files. This folder is mounted as a Docker volume
# so codes and invites survive container rebuilds.
DATA_DIR = os.getenv("DATA_DIR", "data")
os.makedirs(DATA_DIR, exist_ok=True)

VALID_LOG_LEVELS = {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"}
SENSITIVE_LOG_VALUE_PATTERN = re.compile(
    r"(?i)\b(password|token|secret|authorization|cookie)\b\s*[:=]\s*([^\s,;]+)"
)
REDDIT_SUBREDDIT_PATTERN = re.compile(r"^[A-Za-z0-9_]{2,21}$")
TRUTHY_ENV_VALUES = {"1", "true", "yes", "on"}
FALSY_ENV_VALUES = {"0", "false", "no", "off"}


def normalize_log_level(raw_value: str, fallback: str = "INFO"):
    candidate = str(raw_value or "").strip().upper()
    fallback_level = str(fallback or "INFO").strip().upper()
    if fallback_level not in VALID_LOG_LEVELS:
        fallback_level = "INFO"
    if candidate in VALID_LOG_LEVELS:
        return candidate
    return fallback_level


def to_logging_level(level_name: str):
    return getattr(logging, str(level_name or "INFO").upper(), logging.INFO)


def is_truthy_env_value(raw_value, default_value: bool = True):
    value = str(raw_value or "").strip().lower()
    if not value:
        return bool(default_value)
    if value in TRUTHY_ENV_VALUES:
        return True
    if value in FALSY_ENV_VALUES:
        return False
    return bool(default_value)


def normalize_http_url_setting(raw_value: str, fallback_value: str, setting_name: str):
    candidate = str(raw_value or "").strip()
    fallback = str(fallback_value or "").strip()
    if not candidate:
        return fallback
    if candidate.startswith(("http://", "https://")):
        return candidate
    normalized = f"https://{candidate.lstrip('/')}"
    logger.warning(
        "%s is missing URL scheme; normalizing to %s", setting_name, normalized
    )
    return normalized


def normalize_reddit_subreddit_setting(
    raw_value: str, fallback_value: str = "GlInet", setting_name: str = "REDDIT_SUBREDDIT"
):
    fallback = str(fallback_value or "GlInet").strip() or "GlInet"
    candidate = str(raw_value or "").strip()
    if not candidate:
        return fallback

    lower_candidate = candidate.lower()
    if lower_candidate.startswith(("http://", "https://")):
        match = re.search(r"/r/([A-Za-z0-9_]{2,21})(?:/|$)", candidate)
        candidate = match.group(1) if match else ""
    elif lower_candidate.startswith("r/"):
        candidate = candidate[2:]
    elif lower_candidate.startswith("/r/"):
        candidate = candidate[3:]

    candidate = candidate.strip().strip("/")
    if REDDIT_SUBREDDIT_PATTERN.fullmatch(candidate):
        return candidate

    logger.warning(
        "Invalid %s value '%s'; using fallback subreddit '%s'",
        setting_name,
        raw_value,
        fallback,
    )
    return fallback


def resolve_log_dir(preferred_value: str):
    preferred = str(preferred_value or "").strip()
    candidates = ["/logs", preferred, os.path.join(DATA_DIR, "logs"), DATA_DIR]
    seen = set()
    for candidate in candidates:
        if not candidate:
            continue
        if candidate in seen:
            continue
        seen.add(candidate)
        try:
            os.makedirs(candidate, exist_ok=True)
            return candidate
        except OSError:
            continue
    return DATA_DIR


def _chmod_if_possible(path: str, mode: int):
    try:
        os.chmod(path, mode)
        return True
    except (PermissionError, OSError):
        return False


def ensure_log_storage_security(log_dir: str, log_files, enabled: bool):
    notices = []
    if not enabled:
        return notices
    if not log_dir:
        notices.append("LOG_DIR is empty; skipped secure log permission hardening.")
        return notices
    try:
        os.makedirs(log_dir, exist_ok=True)
    except OSError as exc:
        notices.append(f"Unable to create LOG_DIR '{log_dir}': {exc}")
        return notices
    if not _chmod_if_possible(log_dir, 0o700):
        notices.append(
            f"Unable to set secure directory mode on LOG_DIR '{log_dir}' (expected 0700)."
        )
    for file_path in log_files:
        if not file_path:
            continue
        if not os.path.exists(file_path):
            continue
        if not _chmod_if_possible(file_path, 0o600):
            notices.append(
                f"Unable to set secure file mode on log file '{file_path}' (expected 0600)."
            )
    return notices


# Set up logging to console and persistent file
LOG_LEVEL = normalize_log_level(os.getenv("LOG_LEVEL", "INFO"))
CONTAINER_LOG_LEVEL = normalize_log_level(
    os.getenv("CONTAINER_LOG_LEVEL", "ERROR"), fallback="ERROR"
)
DISCORD_LOG_LEVEL = normalize_log_level(
    os.getenv("DISCORD_LOG_LEVEL", "INFO"), fallback="INFO"
)
LOG_HARDEN_FILE_PERMISSIONS = is_truthy_env_value(
    os.getenv("LOG_HARDEN_FILE_PERMISSIONS", "true"),
    default_value=True,
)
LOG_DIR = resolve_log_dir(os.getenv("LOG_DIR", "/logs"))
BOT_LOG_FILE = os.path.join(LOG_DIR, "bot.log")
CONTAINER_ERROR_LOG_FILE = os.path.join(LOG_DIR, "container_errors.log")
WEB_GUI_AUDIT_LOG_FILE = os.path.join(LOG_DIR, "web_gui_audit.log")
log_permission_notices = ensure_log_storage_security(
    LOG_DIR,
    [],
    LOG_HARDEN_FILE_PERMISSIONS,
)
logger = logging.getLogger("invite_bot")
logger.setLevel(to_logging_level(LOG_LEVEL))
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

console_handler = logging.StreamHandler()
console_handler.setLevel(to_logging_level(LOG_LEVEL))
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

file_handler = logging.FileHandler(BOT_LOG_FILE, encoding="utf-8")
file_handler.setLevel(to_logging_level(LOG_LEVEL))
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


class WebGuiAuditFilter(logging.Filter):
    def filter(self, record: logging.LogRecord):
        try:
            message = str(record.getMessage() or "")
        except Exception:
            return False
        return message.startswith("WEB_AUDIT ")


web_gui_audit_handler = logging.FileHandler(WEB_GUI_AUDIT_LOG_FILE, encoding="utf-8")
web_gui_audit_handler.setLevel(logging.INFO)
web_gui_audit_handler.setFormatter(formatter)
web_gui_audit_handler.addFilter(WebGuiAuditFilter())
logger.addHandler(web_gui_audit_handler)

container_error_handler = logging.FileHandler(CONTAINER_ERROR_LOG_FILE, encoding="utf-8")
container_error_handler.setLevel(to_logging_level(CONTAINER_LOG_LEVEL))
container_error_handler.setFormatter(formatter)
root_logger = logging.getLogger()
root_logger.addHandler(container_error_handler)
log_permission_notices.extend(
    ensure_log_storage_security(
        LOG_DIR,
        [BOT_LOG_FILE, CONTAINER_ERROR_LOG_FILE, WEB_GUI_AUDIT_LOG_FILE],
        LOG_HARDEN_FILE_PERMISSIONS,
    )
)


def apply_external_logger_levels():
    logging.getLogger("discord").setLevel(to_logging_level(DISCORD_LOG_LEVEL))
    logging.getLogger("werkzeug").setLevel(to_logging_level(DISCORD_LOG_LEVEL))


apply_external_logger_levels()
logger.info(
    "Runtime log files: %s | %s | %s",
    BOT_LOG_FILE,
    CONTAINER_ERROR_LOG_FILE,
    WEB_GUI_AUDIT_LOG_FILE,
)
if LOG_DIR != "/logs":
    logger.warning(
        "LOG_DIR resolved to %s (not /logs). Set LOG_DIR=/logs to keep logs in /logs.",
        LOG_DIR,
    )
if LOG_HARDEN_FILE_PERMISSIONS:
    logger.info(
        "Log permission hardening enabled for LOG_DIR=%s (expected modes: dir 0700, files 0600).",
        LOG_DIR,
    )
else:
    logger.warning(
        "Log permission hardening is disabled via LOG_HARDEN_FILE_PERMISSIONS=false."
    )
for notice in log_permission_notices:
    logger.warning("Log storage security: %s", notice)


def install_global_exception_logging():
    def _sys_excepthook(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            return
        logger.critical(
            "Unhandled exception reached sys.excepthook",
            exc_info=(exc_type, exc_value, exc_traceback),
        )

    def _thread_excepthook(args):
        if args.exc_type and issubclass(args.exc_type, KeyboardInterrupt):
            return
        thread_name = args.thread.name if args.thread else "unknown"
        logger.critical(
            "Unhandled exception in thread %s",
            thread_name,
            exc_info=(args.exc_type, args.exc_value, args.exc_traceback),
        )

    sys.excepthook = _sys_excepthook
    threading.excepthook = _thread_excepthook


install_global_exception_logging()


def install_asyncio_exception_logging(loop: asyncio.AbstractEventLoop):
    if loop is None or getattr(loop, "_invite_bot_exception_logging", False):
        return

    def _asyncio_exception_handler(active_loop, context):
        message = str(context.get("message") or "Unhandled asyncio exception")
        exception = context.get("exception")
        if exception is not None:
            logger.error(
                "Asyncio exception: %s",
                message,
                exc_info=(type(exception), exception, exception.__traceback__),
            )
        else:
            logger.error("Asyncio exception: %s | context=%s", message, context)

    loop.set_exception_handler(_asyncio_exception_handler)
    setattr(loop, "_invite_bot_exception_logging", True)


def get_required_env(name: str):
    value = os.getenv(name)
    if value is None or str(value).strip() == "":
        message = (
            f"Missing required environment variable: {name}. "
            "Ensure it is set via env_file/environment in your container runtime."
        )
        logger.critical(message)
        raise RuntimeError(message)
    return str(value).strip()


def get_required_int_env(name: str):
    raw_value = get_required_env(name)
    try:
        return int(raw_value)
    except ValueError as exc:
        message = f"Invalid integer value for required environment variable {name}: {raw_value!r}"
        logger.critical(message)
        raise RuntimeError(message) from exc


TOKEN = get_required_env("DISCORD_TOKEN")
GUILD_ID = get_required_int_env("GUILD_ID")
GENERAL_CHANNEL_ID = int(os.getenv("GENERAL_CHANNEL_ID", "0"))
FORUM_BASE_URL = os.getenv("FORUM_BASE_URL", "https://forum.gl-inet.com").rstrip("/")
FORUM_MAX_RESULTS = int(os.getenv("FORUM_MAX_RESULTS", "5"))
REDDIT_BASE_URL = "https://www.reddit.com"
REDDIT_SUBREDDIT = normalize_reddit_subreddit_setting(
    os.getenv("REDDIT_SUBREDDIT", "GlInet"), fallback_value="GlInet"
)
REDDIT_MAX_RESULTS = 5
DOCS_MAX_RESULTS_PER_SITE = int(os.getenv("DOCS_MAX_RESULTS_PER_SITE", "2"))
DOCS_INDEX_TTL_SECONDS = int(os.getenv("DOCS_INDEX_TTL_SECONDS", "3600"))
SEARCH_RESPONSE_MAX_CHARS = int(os.getenv("SEARCH_RESPONSE_MAX_CHARS", "1900"))
DISCORD_MESSAGE_SAFE_MAX_CHARS = 1900
FIRMWARE_FEED_URL = normalize_http_url_setting(
    os.getenv("FIRMWARE_FEED_URL", ""),
    "https://gl-fw.remotetohome.io/",
    "FIRMWARE_FEED_URL",
)
FIRMWARE_NOTIFICATION_CHANNEL_RAW = os.getenv(
    "firmware_notification_channel",
    os.getenv(
        "FIRMWARE_NOTIFICATION_CHANNEL",
        os.getenv("FIRMWARE_NOTIFY_CHANNEL_ID", ""),
    ),
).strip()
if FIRMWARE_NOTIFICATION_CHANNEL_RAW.startswith(
    "<#"
) and FIRMWARE_NOTIFICATION_CHANNEL_RAW.endswith(">"):
    FIRMWARE_NOTIFICATION_CHANNEL_RAW = FIRMWARE_NOTIFICATION_CHANNEL_RAW[2:-1]
try:
    FIRMWARE_NOTIFY_CHANNEL_ID = (
        int(FIRMWARE_NOTIFICATION_CHANNEL_RAW)
        if FIRMWARE_NOTIFICATION_CHANNEL_RAW
        else 0
    )
except ValueError:
    logger.warning(
        "Invalid firmware_notification_channel value: %s",
        FIRMWARE_NOTIFICATION_CHANNEL_RAW,
    )
    FIRMWARE_NOTIFY_CHANNEL_ID = 0

FIRMWARE_CHECK_SCHEDULE = os.getenv(
    "firmware_check_schedule",
    os.getenv("FIRMWARE_CHECK_SCHEDULE", ""),
).strip()
if not FIRMWARE_CHECK_SCHEDULE:
    legacy_interval_raw = os.getenv("FIRMWARE_CHECK_INTERVAL_SECONDS", "").strip()
    if legacy_interval_raw:
        try:
            interval_seconds = max(60, int(legacy_interval_raw))
            interval_minutes = max(1, interval_seconds // 60)
            FIRMWARE_CHECK_SCHEDULE = f"*/{interval_minutes} * * * *"
        except ValueError:
            logger.warning(
                "Invalid FIRMWARE_CHECK_INTERVAL_SECONDS value: %s", legacy_interval_raw
            )
if not FIRMWARE_CHECK_SCHEDULE:
    FIRMWARE_CHECK_SCHEDULE = "*/30 * * * *"

FIRMWARE_REQUEST_TIMEOUT_SECONDS = int(
    os.getenv("FIRMWARE_REQUEST_TIMEOUT_SECONDS", "30")
)
FIRMWARE_RELEASE_NOTES_MAX_CHARS = max(
    200, int(os.getenv("FIRMWARE_RELEASE_NOTES_MAX_CHARS", "900"))
)
WEB_ENABLED = os.getenv("WEB_ENABLED", "true").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
WEB_BIND_HOST = os.getenv("WEB_BIND_HOST", "127.0.0.1").strip() or "127.0.0.1"
try:
    WEB_PORT = int(os.getenv("WEB_PORT", "8080"))
except ValueError:
    WEB_PORT = 8080
WEB_ENV_FILE = os.getenv("WEB_ENV_FILE", ".env").strip() or ".env"
WEB_ADMIN_DEFAULT_EMAIL = os.getenv(
    "WEB_ADMIN_DEFAULT_EMAIL",
    os.getenv("WEB_ADMIN_DEFAULT_USERNAME", "admin@example.com"),
).strip()
WEB_ADMIN_DEFAULT_PASSWORD = os.getenv("WEB_ADMIN_DEFAULT_PASSWORD", "")
try:
    WEB_DISCORD_CATALOG_TTL_SECONDS = max(
        15, int(os.getenv("WEB_DISCORD_CATALOG_TTL_SECONDS", "120"))
    )
except ValueError:
    WEB_DISCORD_CATALOG_TTL_SECONDS = 120
try:
    WEB_DISCORD_CATALOG_FETCH_TIMEOUT_SECONDS = max(
        5,
        int(os.getenv("WEB_DISCORD_CATALOG_FETCH_TIMEOUT_SECONDS", "20")),
    )
except ValueError:
    WEB_DISCORD_CATALOG_FETCH_TIMEOUT_SECONDS = 20
try:
    WEB_BULK_ASSIGN_TIMEOUT_SECONDS = max(
        30, int(os.getenv("WEB_BULK_ASSIGN_TIMEOUT_SECONDS", "300"))
    )
except ValueError:
    WEB_BULK_ASSIGN_TIMEOUT_SECONDS = 300
try:
    WEB_BOT_PROFILE_TIMEOUT_SECONDS = max(
        5, int(os.getenv("WEB_BOT_PROFILE_TIMEOUT_SECONDS", "20"))
    )
except ValueError:
    WEB_BOT_PROFILE_TIMEOUT_SECONDS = 20
try:
    WEB_AVATAR_MAX_UPLOAD_BYTES = max(
        1024, int(os.getenv("WEB_AVATAR_MAX_UPLOAD_BYTES", str(2 * 1024 * 1024)))
    )
except ValueError:
    WEB_AVATAR_MAX_UPLOAD_BYTES = 2 * 1024 * 1024
COUNTRY_CODE_PATTERN = re.compile(r"^[A-Za-z]{2}$")
COUNTRY_LEGACY_SUFFIX_PATTERN = re.compile(r"_[A-Z]{2}$")
COUNTRY_FLAG_SUFFIX_PATTERN = re.compile(r"\s*-\s*[\U0001F1E6-\U0001F1FF]{2}$")
COUNTRY_CODE_SUFFIX_PATTERN = re.compile(r"\s*-\s*[A-Z]{2}$")
TIMEOUT_DURATION_PATTERN = re.compile(r"^\s*(\d+)\s*([mhd]?)\s*$", re.IGNORECASE)
HEX_COLOR_PATTERN = re.compile(r"^[0-9a-fA-F]{6}$")
MODERATOR_ROLE_IDS = {
    int(os.getenv("MODERATOR_ROLE_ID", "1294957416294645771")),
    int(os.getenv("ADMIN_ROLE_ID", "1138302148292116551")),
}
MOD_LOG_CHANNEL_ID = int(os.getenv("MOD_LOG_CHANNEL_ID", "1311820410269995009"))
KICK_PRUNE_HOURS = int(os.getenv("KICK_PRUNE_HOURS", "72"))
TIMEOUT_MAX_MINUTES = 28 * 24 * 60
ROLE_NAME_MAX_LENGTH = 100
CSV_ROLE_ASSIGN_MAX_NAMES = int(os.getenv("CSV_ROLE_ASSIGN_MAX_NAMES", "500"))
BOT_USERNAME_MIN_LENGTH = 2
BOT_USERNAME_MAX_LENGTH = 32
BOT_NICKNAME_MAX_LENGTH = 32
DOCS_SITE_MAP = {
    "kvm": ("KVM Docs", "https://docs.gl-inet.com/kvm/en"),
    "iot": ("IoT Docs", "https://docs.gl-inet.com/iot/en"),
    "router": ("Router Docs v4", "https://docs.gl-inet.com/router/en/4"),
}
DOCS_SOURCES = [DOCS_SITE_MAP["kvm"], DOCS_SITE_MAP["iot"], DOCS_SITE_MAP["router"]]

ROLE_FILE = os.path.join(DATA_DIR, "access_role.txt")
INVITE_FILE = os.path.join(DATA_DIR, "permanent_invite.txt")
CODES_FILE = os.path.join(DATA_DIR, "role_codes.txt")
INVITE_ROLE_FILE = os.path.join(DATA_DIR, "invite_roles.json")
TAG_RESPONSES_FILE = os.path.join(DATA_DIR, "tag_responses.json")
FIRMWARE_STATE_FILE = os.path.join(DATA_DIR, "firmware_seen.json")
COMMAND_PERMISSIONS_FILE = os.path.join(DATA_DIR, "command_permissions.json")
DB_FILE = os.path.join(DATA_DIR, "bot_data.db")
WEB_USERS_FILE = os.path.join(DATA_DIR, "web_users.json")

OLD_DEFAULT_TAG_RESPONSES = {
    "!betatest": "✅ Thanks for your interest in the beta! We'll share more details soon.",
    "!support": "🛠️ Need help? Please open a ticket or message a moderator.",
}
DEFAULT_TAG_RESPONSES = {
    "!betatest": (
        "✅ Thanks for your interest in the beta! We'll share more details soon.\n"
        "🔗 Beta updates: https://forum.gl-inet.com/"
    ),
    "!support": (
        "🛠️ Need help? Please open a ticket or message a moderator.\n"
        "🔗 Support Discord: https://discord.gg/m6UjX6UhKe"
    ),
}
DEFAULT_ALLOWED_ROLE_NAMES = {"Employee", "Admin", "Gl.iNet Moderator"}
COMMAND_PERMISSION_MODE_DEFAULT = "default"
COMMAND_PERMISSION_MODE_PUBLIC = "public"
COMMAND_PERMISSION_MODE_CUSTOM_ROLES = "custom_roles"
COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC = "public"
COMMAND_PERMISSION_DEFAULT_POLICY_ALLOWED_NAMES = "allowed_role_names"
COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS = "moderator_role_ids"
MODERATOR_ONLY_COMMAND_KEYS = {
    "add_role_member",
    "ban_member",
    "bulk_assign_role_csv",
    "create_role",
    "delete_role",
    "edit_role",
    "kick_member",
    "remove_role_member",
    "timeout_member",
    "unban_member",
    "untimeout_member",
}
COMMAND_PERMISSION_DEFAULTS = {
    "list": COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC,
    "tag_commands": COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC,
    "submitrole": COMMAND_PERMISSION_DEFAULT_POLICY_ALLOWED_NAMES,
    "bulk_assign_role_csv": COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS,
    "enter_role": COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC,
    "getaccess": COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC,
    "country": COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC,
    "clear_country": COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC,
    "create_role": COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS,
    "delete_role": COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS,
    "edit_role": COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS,
    "modlog_test": COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS,
    "ban_member": COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS,
    "kick_member": COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS,
    "timeout_member": COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS,
    "untimeout_member": COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS,
    "unban_member": COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS,
    "add_role_member": COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS,
    "remove_role_member": COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS,
    "prune_messages": COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS,
    "logs": COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS,
    "search": COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC,
    "search_reddit": COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC,
    "search_forum": COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC,
    "search_kvm": COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC,
    "search_iot": COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC,
    "search_router": COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC,
}
for _command_key in MODERATOR_ONLY_COMMAND_KEYS:
    COMMAND_PERMISSION_DEFAULTS[_command_key] = (
        COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS
    )
COMMAND_PERMISSION_METADATA = {
    "list": {
        "label": "!list",
        "description": "Show available tag commands.",
    },
    "tag_commands": {
        "label": "Dynamic Tag Commands",
        "description": "Slash/message tag responses loaded from persistent storage.",
    },
    "submitrole": {
        "label": "/submitrole",
        "description": "Create role invite + code mapping.",
    },
    "bulk_assign_role_csv": {
        "label": "/bulk_assign_role_csv",
        "description": "Bulk-assign a role from CSV.",
    },
    "enter_role": {
        "label": "/enter_role",
        "description": "Enter a code to receive a role.",
    },
    "getaccess": {
        "label": "/getaccess",
        "description": "Assign the configured default access role.",
    },
    "country": {
        "label": "/country, !country",
        "description": "Set country suffix on nickname.",
    },
    "clear_country": {
        "label": "/clear_country, !clearcountry",
        "description": "Remove country suffix from nickname.",
    },
    "create_role": {
        "label": "/create_role",
        "description": "Create a guild role.",
    },
    "delete_role": {
        "label": "/delete_role",
        "description": "Delete a guild role.",
    },
    "edit_role": {
        "label": "/edit_role",
        "description": "Edit role name/color/flags.",
    },
    "modlog_test": {
        "label": "/modlog_test, !modlogtest",
        "description": "Send a moderation log test event.",
    },
    "ban_member": {
        "label": "/ban_member, !banmember",
        "description": "Ban a member.",
    },
    "kick_member": {
        "label": "/kick_member, !kickmember",
        "description": "Kick a member and prune recent messages.",
    },
    "timeout_member": {
        "label": "/timeout_member, !timeoutmember",
        "description": "Apply a timeout to a member.",
    },
    "untimeout_member": {
        "label": "/untimeout_member, !untimeoutmember",
        "description": "Remove timeout from a member.",
    },
    "unban_member": {
        "label": "/unban_member, !unbanmember",
        "description": "Unban a user by ID.",
    },
    "add_role_member": {
        "label": "/add_role_member, !addrolemember",
        "description": "Assign a role to a member.",
    },
    "remove_role_member": {
        "label": "/remove_role_member, !removerolemember",
        "description": "Remove a role from a member.",
    },
    "prune_messages": {
        "label": "/prune_messages, !prune",
        "description": "Prune recent messages in the current channel.",
    },
    "logs": {
        "label": "/logs",
        "description": "View recent container error log entries.",
    },
    "search": {
        "label": "/search, !search",
        "description": "Search forum + docs.",
    },
    "search_reddit": {
        "label": "/search_reddit, !searchreddit",
        "description": "Search configured subreddit on Reddit.",
    },
    "search_forum": {
        "label": "/search_forum, !searchforum",
        "description": "Search forum only.",
    },
    "search_kvm": {
        "label": "/search_kvm, !searchkvm",
        "description": "Search KVM docs only.",
    },
    "search_iot": {
        "label": "/search_iot, !searchiot",
        "description": "Search IoT docs only.",
    },
    "search_router": {
        "label": "/search_router, !searchrouter",
        "description": "Search Router docs only.",
    },
}
COMMAND_PERMISSION_POLICY_LABELS = {
    COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC: "Public (any member)",
    COMMAND_PERMISSION_DEFAULT_POLICY_ALLOWED_NAMES: "Named roles: Employee/Admin/Gl.iNet Moderator",
    COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS: "Moderator/Admin role IDs (env)",
}

intents = discord.Intents.default()
intents.members = True
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents, case_insensitive=True)
tree = bot.tree

tag_responses = {}
tag_responses_mtime = None
tag_command_names = set()
docs_index_cache = {}
firmware_monitor_task = None
web_admin_thread = None
discord_catalog_cache = {"fetched_at": 0.0, "data": None}
BOT_SERVER_NICKNAME_UNSET = object()
command_permissions_lock = threading.Lock()
command_permissions_cache = {"mtime": None, "rules": {}}
db_lock = threading.RLock()
db_connection = None
FIRMWARE_CHANNEL_WARNING_COOLDOWN_SECONDS = 3600
FIRMWARE_NOTIFICATION_ITEM_LIMIT = 12
firmware_channel_warning_state = {"reason": "", "last_logged_at": 0.0}


def normalize_tag(tag: str) -> str:
    return tag.strip().lower()


def parse_int_setting(raw_value, default_value, minimum=None):
    try:
        parsed = int(str(raw_value).strip())
    except (TypeError, ValueError):
        return default_value
    if minimum is not None and parsed < minimum:
        return default_value
    return parsed


def parse_firmware_channel_id(raw_value, default_value):
    value = str(raw_value or "").strip()
    if value.startswith("<#") and value.endswith(">"):
        value = value[2:-1]
    try:
        return int(value) if value else default_value
    except ValueError:
        return default_value


def get_db_connection():
    global db_connection
    with db_lock:
        if db_connection is not None:
            return db_connection

        conn = sqlite3.connect(DB_FILE, check_same_thread=False, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA temp_store=MEMORY;")
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("PRAGMA cache_size=-20000;")
        db_connection = conn
        return conn


def ensure_db_schema():
    conn = get_db_connection()
    with db_lock:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS role_codes (
                code TEXT PRIMARY KEY,
                role_id INTEGER NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS invite_roles (
                invite_code TEXT PRIMARY KEY,
                role_id INTEGER NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS tag_responses (
                tag TEXT PRIMARY KEY,
                response TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS firmware_seen (
                entry_id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS command_permissions (
                command_key TEXT PRIMARY KEY,
                mode TEXT NOT NULL,
                role_ids_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS web_users (
                email TEXT PRIMARY KEY,
                password_hash TEXT NOT NULL,
                is_admin INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS kv_store (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_role_codes_role_id ON role_codes(role_id);
            CREATE INDEX IF NOT EXISTS idx_invite_roles_role_id ON invite_roles(role_id);
            """
        )
        conn.commit()


def db_kv_get(key: str):
    conn = get_db_connection()
    with db_lock:
        row = conn.execute(
            "SELECT value FROM kv_store WHERE key = ?",
            (key,),
        ).fetchone()
    return row["value"] if row else None


def db_kv_set(key: str, value: str):
    conn = get_db_connection()
    now_iso = datetime.now(timezone.utc).isoformat()
    with db_lock:
        conn.execute(
            """
            INSERT INTO kv_store (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
            """,
            (key, value, now_iso),
        )
        conn.commit()


def db_kv_delete(key: str):
    conn = get_db_connection()
    with db_lock:
        conn.execute("DELETE FROM kv_store WHERE key = ?", (key,))
        conn.commit()


def migrate_legacy_files_to_db():
    conn = get_db_connection()
    now_iso = datetime.now(timezone.utc).isoformat()

    with db_lock:

        def kv_exists(key: str):
            row = conn.execute(
                "SELECT 1 FROM kv_store WHERE key = ?", (key,)
            ).fetchone()
            return row is not None

        def kv_insert_if_missing(key: str, value: str):
            if kv_exists(key):
                return
            conn.execute(
                """
                INSERT INTO kv_store (key, value, updated_at)
                VALUES (?, ?, ?)
                """,
                (key, str(value), now_iso),
            )

        if os.path.exists(CODES_FILE):
            try:
                with open(CODES_FILE, "r") as f:
                    for raw_line in f:
                        line = raw_line.strip()
                        if ":" not in line:
                            continue
                        code, raw_role_id = line.split(":", 1)
                        code = code.strip()
                        try:
                            role_id = int(raw_role_id.strip())
                        except ValueError:
                            continue
                        if code and role_id > 0:
                            conn.execute(
                                """
                                INSERT OR IGNORE INTO role_codes (code, role_id, created_at)
                                VALUES (?, ?, ?)
                                """,
                                (code, role_id, now_iso),
                            )
            except Exception:
                logger.exception(
                    "Failed migrating legacy role codes from %s", CODES_FILE
                )

        if os.path.exists(INVITE_ROLE_FILE):
            try:
                with open(INVITE_ROLE_FILE, "r") as f:
                    mapping = json.load(f)
                if isinstance(mapping, dict):
                    for invite_code, raw_role_id in mapping.items():
                        try:
                            role_id = int(raw_role_id)
                        except (TypeError, ValueError):
                            continue
                        code = str(invite_code or "").strip()
                        if code and role_id > 0:
                            conn.execute(
                                """
                                INSERT OR IGNORE INTO invite_roles (invite_code, role_id, created_at)
                                VALUES (?, ?, ?)
                                """,
                                (code, role_id, now_iso),
                            )
            except Exception:
                logger.exception(
                    "Failed migrating legacy invite-role mapping from %s",
                    INVITE_ROLE_FILE,
                )

        tag_mapping_loaded = False
        if os.path.exists(TAG_RESPONSES_FILE):
            try:
                with open(TAG_RESPONSES_FILE, "r") as f:
                    payload = json.load(f)
                if isinstance(payload, dict):
                    tag_mapping_loaded = True
                    for raw_tag, raw_response in payload.items():
                        tag = normalize_tag(str(raw_tag))
                        if not tag:
                            continue
                        conn.execute(
                            """
                            INSERT OR IGNORE INTO tag_responses (tag, response, updated_at)
                            VALUES (?, ?, ?)
                            """,
                            (tag, str(raw_response), now_iso),
                        )
            except Exception:
                logger.exception(
                    "Failed migrating legacy tag responses from %s", TAG_RESPONSES_FILE
                )

        tag_count = conn.execute("SELECT COUNT(*) AS c FROM tag_responses").fetchone()[
            "c"
        ]
        if tag_count == 0 and not tag_mapping_loaded:
            for raw_tag, raw_response in DEFAULT_TAG_RESPONSES.items():
                conn.execute(
                    """
                    INSERT OR IGNORE INTO tag_responses (tag, response, updated_at)
                    VALUES (?, ?, ?)
                    """,
                    (normalize_tag(raw_tag), str(raw_response), now_iso),
                )

        if os.path.exists(FIRMWARE_STATE_FILE):
            try:
                with open(FIRMWARE_STATE_FILE, "r") as f:
                    payload = json.load(f)
                seen_ids = payload.get("seen_ids", [])
                if isinstance(seen_ids, list):
                    for item in seen_ids:
                        entry_id = str(item or "").strip()
                        if not entry_id:
                            continue
                        conn.execute(
                            """
                            INSERT OR IGNORE INTO firmware_seen (entry_id, created_at)
                            VALUES (?, ?)
                            """,
                            (entry_id, now_iso),
                        )
                    kv_insert_if_missing("firmware_seen_initialized", "1")
                    kv_insert_if_missing("firmware_source_url", FIRMWARE_FEED_URL)
                    sync_value = str(payload.get("last_synced") or "").strip()
                    if sync_value:
                        kv_insert_if_missing("firmware_last_synced", sync_value)
            except Exception:
                logger.exception(
                    "Failed migrating legacy firmware state from %s",
                    FIRMWARE_STATE_FILE,
                )

        firmware_seen_count = conn.execute(
            "SELECT COUNT(*) AS c FROM firmware_seen"
        ).fetchone()["c"]
        if firmware_seen_count > 0:
            kv_insert_if_missing("firmware_seen_initialized", "1")
            kv_insert_if_missing("firmware_source_url", FIRMWARE_FEED_URL)

        if os.path.exists(COMMAND_PERMISSIONS_FILE):
            try:
                with open(COMMAND_PERMISSIONS_FILE, "r") as f:
                    payload = json.load(f)
                raw_rules = (
                    payload.get("rules", {}) if isinstance(payload, dict) else {}
                )
                if isinstance(raw_rules, dict):
                    for command_key, raw_rule in raw_rules.items():
                        if command_key not in COMMAND_PERMISSION_DEFAULTS:
                            continue
                        normalized_rule = normalize_command_permission_rule(raw_rule)
                        if normalized_rule["mode"] == COMMAND_PERMISSION_MODE_DEFAULT:
                            continue
                        conn.execute(
                            """
                            INSERT OR IGNORE INTO command_permissions (command_key, mode, role_ids_json, updated_at)
                            VALUES (?, ?, ?, ?)
                            """,
                            (
                                command_key,
                                normalized_rule["mode"],
                                json.dumps(normalized_rule["role_ids"]),
                                now_iso,
                            ),
                        )
            except Exception:
                logger.exception(
                    "Failed migrating legacy command permissions from %s",
                    COMMAND_PERMISSIONS_FILE,
                )

        if os.path.exists(WEB_USERS_FILE):
            try:
                with open(WEB_USERS_FILE, "r") as f:
                    payload = json.load(f)
                users = payload.get("users", []) if isinstance(payload, dict) else []
                if isinstance(users, list):
                    for entry in users:
                        if not isinstance(entry, dict):
                            continue
                        email = str(entry.get("email", "")).strip().lower()
                        password_hash = str(entry.get("password_hash", "")).strip()
                        if not email or not password_hash:
                            continue
                        is_admin = 1 if bool(entry.get("is_admin", False)) else 0
                        created_at = str(entry.get("created_at") or now_iso)
                        conn.execute(
                            """
                            INSERT OR IGNORE INTO web_users (email, password_hash, is_admin, created_at, updated_at)
                            VALUES (?, ?, ?, ?, ?)
                            """,
                            (email, password_hash, is_admin, created_at, now_iso),
                        )
            except Exception:
                logger.exception(
                    "Failed migrating legacy web users from %s", WEB_USERS_FILE
                )

        if not kv_exists("access_role_id") and os.path.exists(ROLE_FILE):
            try:
                with open(ROLE_FILE, "r") as f:
                    raw_value = f.read().strip()
                role_id = int(raw_value)
                if role_id > 0:
                    kv_insert_if_missing("access_role_id", str(role_id))
            except Exception:
                logger.exception(
                    "Failed migrating legacy access role from %s", ROLE_FILE
                )

        conn.commit()


def initialize_storage():
    ensure_db_schema()
    migrate_legacy_files_to_db()


def tag_to_command_name(tag: str) -> str:
    normalized = normalize_tag(tag)
    if normalized.startswith("!"):
        normalized = normalized[1:]
    if normalized.startswith("/"):
        normalized = normalized[1:]
    return normalized.replace(" ", "_")


def load_tag_responses():
    conn = get_db_connection()
    with db_lock:
        rows = conn.execute("SELECT tag, response FROM tag_responses").fetchall()
    if not rows:
        save_tag_responses(DEFAULT_TAG_RESPONSES)
        return {normalize_tag(k): str(v) for k, v in DEFAULT_TAG_RESPONSES.items()}
    return {normalize_tag(row["tag"]): str(row["response"]) for row in rows}


def save_tag_responses(mapping):
    conn = get_db_connection()
    now_iso = datetime.now(timezone.utc).isoformat()
    normalized = {
        normalize_tag(k): str(v) for k, v in (mapping or {}).items() if normalize_tag(k)
    }
    with db_lock:
        conn.execute("DELETE FROM tag_responses")
        for tag, response in normalized.items():
            conn.execute(
                """
                INSERT INTO tag_responses (tag, response, updated_at)
                VALUES (?, ?, ?)
                """,
                (tag, response, now_iso),
            )
        conn.commit()
    db_kv_set("tag_responses_updated_at", now_iso)


def get_tag_responses():
    global tag_responses, tag_responses_mtime
    current_version = db_kv_get("tag_responses_updated_at") or "bootstrap"
    if tag_responses_mtime != current_version:
        tag_responses = load_tag_responses()
        tag_responses_mtime = current_version
    return tag_responses


def upgrade_legacy_default_tag_responses():
    current = dict(get_tag_responses())
    changed = False
    for raw_tag, old_response in OLD_DEFAULT_TAG_RESPONSES.items():
        tag = normalize_tag(raw_tag)
        new_response = DEFAULT_TAG_RESPONSES.get(tag)
        if not tag or not new_response:
            continue
        if str(current.get(tag, "")) == str(old_response):
            current[tag] = new_response
            changed = True
    if not changed:
        return
    save_tag_responses(current)
    logger.info("Upgraded default tag responses to include support links.")


def build_command_list():
    tags = sorted(get_tag_responses().keys())
    if not tags:
        return "No tag commands are available yet."
    return "Tag commands:\n" + "\n".join(tags)


def register_tag_commands():
    global tag_command_names
    tag_commands = {}
    for tag in get_tag_responses().keys():
        command_name = tag_to_command_name(tag)
        if not command_name:
            continue
        tag_commands[command_name] = tag

    existing_names = {cmd.name for cmd in tree.get_commands()}
    for command_name, tag in tag_commands.items():
        if command_name in existing_names:
            logger.warning(
                "Skipping tag slash command /%s due to name conflict", command_name
            )
            continue

        def make_tag_reply(tag_key: str):
            async def tag_reply(interaction: discord.Interaction):
                if not can_use_command(interaction.user, "tag_commands"):
                    await interaction.response.send_message(
                        build_command_permission_denied_message(
                            "tag_commands", interaction.guild
                        ),
                        ephemeral=True,
                    )
                    return
                tag_response = str(get_tag_responses().get(tag_key, "")).strip()
                if not tag_response:
                    await interaction.response.send_message(
                        "❌ This tag response is not configured.", ephemeral=True
                    )
                    return
                await interaction.response.send_message(tag_response)

            return tag_reply

        try:
            tree.add_command(
                app_commands.Command(
                    name=command_name,
                    description=f"Tag response for {command_name}",
                    callback=make_tag_reply(tag),
                ),
                guild=discord.Object(id=GUILD_ID),
            )
            tag_command_names.add(command_name)
        except app_commands.errors.CommandAlreadyRegistered:
            logger.info("Tag slash command /%s already registered", command_name)
        except TypeError:
            logger.exception("Failed to register tag slash command /%s", command_name)


async def reload_tag_commands_runtime():
    global tag_command_names
    try:
        guild_obj = discord.Object(id=GUILD_ID)

        removed_count = 0
        for command_name in list(tag_command_names):
            removed_command = tree.remove_command(command_name, guild=guild_obj)
            if removed_command is not None:
                removed_count += 1
        tag_command_names.clear()

        register_tag_commands()
        synced = await tree.sync(guild=guild_obj)
        logger.info(
            "Tag commands reloaded: removed=%s registered=%s synced=%s",
            removed_count,
            len(tag_command_names),
            len(synced),
        )
    except Exception:
        logger.exception("Failed to reload tag slash commands")


def schedule_tag_command_refresh():
    loop = getattr(bot, "loop", None)
    if loop is None or not loop.is_running():
        logger.warning("Cannot refresh tag slash commands yet: bot loop is not running")
        return False

    def _start_refresh():
        asyncio.create_task(reload_tag_commands_runtime(), name="tag_commands_refresh")

    loop.call_soon_threadsafe(_start_refresh)
    return True


def generate_code():
    while True:
        code = ""
        last_digit = None
        streak = 1
        for _ in range(6):
            digit = str(secrets.randbelow(10))
            if digit == last_digit:
                streak += 1
            else:
                streak = 1
            if streak > 2:
                break
            code += digit
            last_digit = digit
        if len(code) == 6:
            logger.debug("Generated code %s", code)
            return code


def save_role_code(code, role_id):
    now_iso = datetime.now(timezone.utc).isoformat()
    conn = get_db_connection()
    with db_lock:
        conn.execute(
            """
            INSERT OR REPLACE INTO role_codes (code, role_id, created_at)
            VALUES (?, ?, ?)
            """,
            (str(code), int(role_id), now_iso),
        )
        conn.commit()
    logger.info("Saved code %s for role %s", code, role_id)


def get_role_id_by_code(code):
    conn = get_db_connection()
    with db_lock:
        row = conn.execute(
            "SELECT role_id FROM role_codes WHERE code = ?",
            (str(code),),
        ).fetchone()
    if row is None:
        return None
    role_id = int(row["role_id"])
    logger.info("Code %s matched role %s", code, role_id)
    return role_id


def load_invite_roles():
    conn = get_db_connection()
    with db_lock:
        rows = conn.execute("SELECT invite_code, role_id FROM invite_roles").fetchall()
    return {row["invite_code"]: int(row["role_id"]) for row in rows}


def save_invite_role(invite_code, role_id):
    now_iso = datetime.now(timezone.utc).isoformat()
    conn = get_db_connection()
    with db_lock:
        conn.execute(
            """
            INSERT OR REPLACE INTO invite_roles (invite_code, role_id, created_at)
            VALUES (?, ?, ?)
            """,
            (str(invite_code), int(role_id), now_iso),
        )
        conn.commit()
    logger.info("Saved invite %s for role %s", invite_code, role_id)


def has_allowed_role(member: discord.Member):
    has_role = any(role.name in DEFAULT_ALLOWED_ROLE_NAMES for role in member.roles)
    logger.debug("User %s allowed: %s", member, has_role)
    return has_role


def has_moderator_access(member: discord.Member):
    return any(role.id in MODERATOR_ROLE_IDS for role in member.roles)


def normalize_permission_mode(value: str | None):
    raw = (value or "").strip().lower()
    if raw in {
        COMMAND_PERMISSION_MODE_DEFAULT,
        COMMAND_PERMISSION_MODE_PUBLIC,
        COMMAND_PERMISSION_MODE_CUSTOM_ROLES,
    }:
        return raw
    return COMMAND_PERMISSION_MODE_DEFAULT


def normalize_role_ids(values):
    normalized = []
    seen = set()
    if isinstance(values, str):
        values = re.split(r"[\s,]+", values.strip()) if values.strip() else []
    if not isinstance(values, list):
        values = []
    for value in values:
        cleaned = str(value or "").strip()
        if cleaned.startswith("<@&") and cleaned.endswith(">"):
            cleaned = cleaned[3:-1]
        try:
            role_id = int(cleaned)
        except (TypeError, ValueError):
            continue
        if role_id <= 0 or role_id in seen:
            continue
        seen.add(role_id)
        normalized.append(role_id)
    return normalized


def normalize_command_permission_rule(raw_rule):
    if not isinstance(raw_rule, dict):
        return {
            "mode": COMMAND_PERMISSION_MODE_DEFAULT,
            "role_ids": [],
        }
    mode = normalize_permission_mode(raw_rule.get("mode"))
    role_ids = normalize_role_ids(raw_rule.get("role_ids", []))
    if mode != COMMAND_PERMISSION_MODE_CUSTOM_ROLES:
        role_ids = []
    return {
        "mode": mode,
        "role_ids": role_ids,
    }


def load_command_permission_rules():
    global command_permissions_cache
    with command_permissions_lock:
        version = db_kv_get("command_permissions_updated_at") or "bootstrap"
        if command_permissions_cache.get("mtime") == version:
            return command_permissions_cache.get("rules", {})

        conn = get_db_connection()
        with db_lock:
            rows = conn.execute(
                "SELECT command_key, mode, role_ids_json FROM command_permissions"
            ).fetchall()
        normalized_rules = {}
        for row in rows:
            command_key = str(row["command_key"])
            if command_key not in COMMAND_PERMISSION_DEFAULTS:
                continue
            try:
                role_ids_payload = json.loads(row["role_ids_json"] or "[]")
            except Exception:
                role_ids_payload = []
            normalized_rules[command_key] = normalize_command_permission_rule(
                {"mode": row["mode"], "role_ids": role_ids_payload}
            )

        command_permissions_cache = {"mtime": version, "rules": normalized_rules}
        return normalized_rules


def save_command_permission_rules(rules: dict, actor_email: str = ""):
    safe_rules = {}
    for command_key, raw_rule in (rules or {}).items():
        if command_key not in COMMAND_PERMISSION_DEFAULTS:
            continue
        normalized_rule = normalize_command_permission_rule(raw_rule)
        if normalized_rule["mode"] == COMMAND_PERMISSION_MODE_DEFAULT:
            continue
        safe_rules[command_key] = normalized_rule

    updated_at = datetime.now(timezone.utc).isoformat()
    conn = get_db_connection()
    with command_permissions_lock:
        with db_lock:
            conn.execute("DELETE FROM command_permissions")
            for command_key, normalized_rule in safe_rules.items():
                conn.execute(
                    """
                    INSERT INTO command_permissions (command_key, mode, role_ids_json, updated_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        command_key,
                        normalized_rule["mode"],
                        json.dumps(normalized_rule["role_ids"]),
                        updated_at,
                    ),
                )
            conn.commit()
        db_kv_set("command_permissions_updated_at", updated_at)
        db_kv_set("command_permissions_updated_by", actor_email or "unknown")
        command_permissions_cache["mtime"] = updated_at
        command_permissions_cache["rules"] = safe_rules
    return safe_rules


def resolve_command_permission_state(command_key: str):
    default_policy = COMMAND_PERMISSION_DEFAULTS.get(
        command_key, COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC
    )
    rule = load_command_permission_rules().get(
        command_key,
        {"mode": COMMAND_PERMISSION_MODE_DEFAULT, "role_ids": []},
    )
    mode = rule.get("mode", COMMAND_PERMISSION_MODE_DEFAULT)
    role_ids = normalize_role_ids(rule.get("role_ids", []))
    return default_policy, mode, role_ids


def member_has_any_role_id(member: discord.Member | discord.User, role_ids: list[int]):
    if not isinstance(member, discord.Member):
        return False
    if not role_ids:
        return False
    member_role_ids = {role.id for role in member.roles}
    return any(role_id in member_role_ids for role_id in role_ids)


def can_use_command(member: discord.Member | discord.User, command_key: str):
    default_policy, mode, role_ids = resolve_command_permission_state(command_key)

    if mode == COMMAND_PERMISSION_MODE_PUBLIC:
        return True
    if mode == COMMAND_PERMISSION_MODE_CUSTOM_ROLES:
        return member_has_any_role_id(member, role_ids)

    if default_policy == COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC:
        return True
    if default_policy == COMMAND_PERMISSION_DEFAULT_POLICY_ALLOWED_NAMES:
        return isinstance(member, discord.Member) and has_allowed_role(member)
    if default_policy == COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS:
        return isinstance(member, discord.Member) and has_moderator_access(member)
    return False


def build_command_permission_denied_message(
    command_key: str, guild: discord.Guild | None = None
):
    default_policy, mode, role_ids = resolve_command_permission_state(command_key)
    if mode == COMMAND_PERMISSION_MODE_CUSTOM_ROLES:
        if guild is None or not role_ids:
            return "❌ You do not have one of the roles allowed to use this command."
        mentions = []
        for role_id in role_ids:
            role = guild.get_role(role_id)
            mentions.append(role.mention if role else f"`{role_id}`")
        return f"❌ You need one of these roles: {', '.join(mentions)}."

    if default_policy == COMMAND_PERMISSION_DEFAULT_POLICY_ALLOWED_NAMES:
        names = ", ".join(sorted(DEFAULT_ALLOWED_ROLE_NAMES))
        return f"❌ You need one of these roles: `{names}`."
    if default_policy == COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS:
        return "❌ Only moderators can use this command."
    return "❌ You do not have permission to use this command."


async def ensure_interaction_command_access(
    interaction: discord.Interaction, command_key: str
):
    if can_use_command(interaction.user, command_key):
        return True
    await interaction.response.send_message(
        build_command_permission_denied_message(command_key, interaction.guild),
        ephemeral=True,
    )
    return False


async def ensure_prefix_command_access(ctx: commands.Context, command_key: str):
    if can_use_command(ctx.author, command_key):
        return True
    await ctx.send(build_command_permission_denied_message(command_key, ctx.guild))
    return False


async def send_safe_interaction_message(
    interaction: discord.Interaction, message_text: str, ephemeral: bool = True
):
    try:
        if interaction.response.is_done():
            await interaction.followup.send(message_text, ephemeral=ephemeral)
        else:
            await interaction.response.send_message(message_text, ephemeral=ephemeral)
        return True
    except discord.HTTPException:
        logger.exception(
            "Failed sending interaction response message for user=%s command=%s",
            interaction.user,
            interaction.command.name if interaction.command else "unknown",
        )
        return False


def build_command_permissions_web_payload():
    rules = load_command_permission_rules()
    commands_payload = []
    for command_key, metadata in COMMAND_PERMISSION_METADATA.items():
        default_policy = COMMAND_PERMISSION_DEFAULTS.get(
            command_key, COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC
        )
        rule = normalize_command_permission_rule(rules.get(command_key, {}))
        commands_payload.append(
            {
                "key": command_key,
                "label": metadata.get("label", command_key),
                "description": metadata.get("description", ""),
                "default_policy": default_policy,
                "default_policy_label": COMMAND_PERMISSION_POLICY_LABELS.get(
                    default_policy, default_policy
                ),
                "mode": rule["mode"],
                "role_ids": rule["role_ids"],
            }
        )
    return {
        "ok": True,
        "commands": commands_payload,
        "allowed_role_names": sorted(DEFAULT_ALLOWED_ROLE_NAMES),
        "moderator_role_ids": sorted(MODERATOR_ROLE_IDS),
    }


def run_web_get_command_permissions():
    try:
        return build_command_permissions_web_payload()
    except Exception:
        logger.exception("Failed to build command permissions payload for web admin")
        return {
            "ok": False,
            "error": "Unexpected error while loading command permissions.",
        }


def run_web_update_command_permissions(payload: dict, actor_email: str):
    if not isinstance(payload, dict):
        return {
            "ok": False,
            "error": "Invalid payload type for command permissions update.",
        }
    commands_payload = payload.get("commands", {})
    if not isinstance(commands_payload, dict):
        return {"ok": False, "error": "Payload is missing a commands object."}

    updated_rules = {}
    validation_errors = []
    for command_key in COMMAND_PERMISSION_METADATA.keys():
        raw_rule = commands_payload.get(command_key, {})
        if not isinstance(raw_rule, dict):
            raw_rule = {}

        mode = normalize_permission_mode(raw_rule.get("mode"))
        role_ids = normalize_role_ids(raw_rule.get("role_ids", []))
        if mode == COMMAND_PERMISSION_MODE_CUSTOM_ROLES and not role_ids:
            validation_errors.append(
                f"{command_key}: mode `custom_roles` requires at least one role ID."
            )
        updated_rules[command_key] = {"mode": mode, "role_ids": role_ids}

    if validation_errors:
        return {"ok": False, "error": " ".join(validation_errors)}

    try:
        save_command_permission_rules(updated_rules, actor_email=actor_email)
    except Exception:
        logger.exception("Failed to save command permissions from web admin")
        return {"ok": False, "error": "Failed to save command permissions."}

    response = build_command_permissions_web_payload()
    response["message"] = "Command permissions updated."
    logger.info("Command permissions updated via web admin")
    return response


def run_web_get_tag_responses():
    try:
        mapping = get_tag_responses()
    except Exception:
        logger.exception("Failed loading tag responses for web admin")
        return {"ok": False, "error": "Unexpected error while loading tag responses."}
    return {"ok": True, "mapping": mapping}


def run_web_save_tag_responses(mapping: dict, actor_email: str):
    if not isinstance(mapping, dict):
        return {"ok": False, "error": "Tag responses payload must be a JSON object."}

    normalized = {}
    for raw_tag, raw_response in mapping.items():
        if not isinstance(raw_tag, str) or not isinstance(raw_response, str):
            return {"ok": False, "error": "All tag keys and values must be strings."}
        tag = normalize_tag(raw_tag)
        if not tag:
            continue
        normalized[tag] = raw_response

    try:
        save_tag_responses(normalized)
    except Exception:
        logger.exception("Failed saving tag responses from web admin")
        return {"ok": False, "error": "Unexpected error while saving tag responses."}

    logger.info("Tag responses updated via web admin (%s entries)", len(normalized))
    return {"ok": True, "mapping": normalized, "message": "Tag responses updated."}


def validate_moderation_target(
    actor: discord.Member, target: discord.Member, bot_member: discord.Member
):
    if target.id == actor.id:
        return False, "❌ You cannot moderate yourself."
    if target.id == actor.guild.owner_id:
        return False, "❌ You cannot moderate the server owner."
    if target.id == bot_member.id:
        return False, "❌ You cannot moderate the bot."
    if actor.id != actor.guild.owner_id and actor.top_role <= target.top_role:
        return False, "❌ You can only moderate members below your top role."
    if bot_member.top_role <= target.top_role:
        return False, "❌ I can only moderate members below my top role."
    return True, None


def validate_manageable_role(
    actor: discord.Member, role: discord.Role, bot_member: discord.Member
):
    if role == actor.guild.default_role:
        return False, "❌ You cannot manage the @everyone role."
    if role.managed:
        return (
            False,
            "❌ That role is managed by an integration and cannot be changed here.",
        )
    if actor.id != actor.guild.owner_id and actor.top_role <= role:
        return False, "❌ You can only manage roles below your top role."
    if bot_member.top_role <= role:
        return False, "❌ I can only manage roles below my top role."
    return True, None


def parse_role_color(value: str | None):
    if value is None:
        return None, None

    cleaned = value.strip()
    if not cleaned:
        return None, "❌ Color cannot be blank."

    if cleaned.lower() in {"none", "default", "reset"}:
        return discord.Color.default(), None

    if cleaned.startswith("#"):
        cleaned = cleaned[1:]
    if cleaned.lower().startswith("0x"):
        cleaned = cleaned[2:]

    if not HEX_COLOR_PATTERN.fullmatch(cleaned):
        return None, "❌ Invalid color. Use hex like `#1ABC9C`, `1ABC9C`, or `none`."

    return discord.Color(int(cleaned, 16)), None


def normalize_member_lookup_name(value: str):
    if not value:
        return None
    normalized = re.sub(r"\s+", " ", value.strip().lstrip("@")).casefold()
    return normalized or None


def parse_member_names_from_csv_bytes(data: bytes):
    decoded = None
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            decoded = data.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    if decoded is None:
        return []

    names = []
    reader = csv.reader(io.StringIO(decoded))
    for row in reader:
        for cell in row:
            candidate = cell.strip()
            if candidate:
                names.append(candidate)
    return names


def build_member_name_lookup(guild: discord.Guild):
    lookup = {}
    for member in guild.members:
        candidates = {
            member.name,
            member.display_name,
            member.global_name,
            str(member),
        }
        if member.discriminator and member.discriminator != "0":
            candidates.add(f"{member.name}#{member.discriminator}")

        for candidate in candidates:
            key = normalize_member_lookup_name(candidate)
            if not key:
                continue
            lookup.setdefault(key, []).append(member)
    return lookup


def unique_member_names(values: list[str]):
    seen = set()
    unique = []
    for value in values:
        key = normalize_member_lookup_name(value)
        if not key or key in seen:
            continue
        seen.add(key)
        unique.append(value.strip())
    return unique


def parse_role_id_input(value: str):
    cleaned = (value or "").strip()
    if cleaned.startswith("<@&") and cleaned.endswith(">"):
        cleaned = cleaned[3:-1]
    try:
        role_id = int(cleaned)
    except (TypeError, ValueError):
        return None
    return role_id if role_id > 0 else None


def parse_user_id_input(value: str):
    cleaned = (value or "").strip()
    if cleaned.startswith("<@") and cleaned.endswith(">"):
        cleaned = cleaned[2:-1]
        if cleaned.startswith("!"):
            cleaned = cleaned[1:]
    try:
        user_id = int(cleaned)
    except (TypeError, ValueError):
        return None
    return user_id if user_id > 0 else None


def format_bulk_assignment_preview(title: str, values: list[str], limit: int = 20):
    if not values:
        return None
    preview = ", ".join(f"`{clip_text(value, 40)}`" for value in values[:limit])
    remaining = len(values) - limit
    if remaining > 0:
        preview = f"{preview} ... (+{remaining} more)"
    return f"**{title}:** {preview}"


def build_bulk_assignment_summary_lines(
    source_name: str, role_mention: str, result: dict
):
    summary_lines = [
        f"✅ Finished processing `{source_name}` for role {role_mention}.",
        f"- Unique names processed: `{result['unique_names_count']}`",
        f"- Matched members: `{result['matched_members_count']}`",
        f"- Assigned now: `{len(result['assigned'])}`",
        f"- Already had role: `{len(result['already_had_role'])}`",
        f"- Unmatched names: `{len(result['unmatched_names'])}`",
        f"- Ambiguous names: `{len(result['ambiguous_names'])}`",
        f"- Assignment failures: `{len(result['assignment_failures'])}`",
    ]
    for line in (
        format_bulk_assignment_preview("Unmatched", result["unmatched_names"]),
        format_bulk_assignment_preview("Ambiguous", result["ambiguous_names"]),
        format_bulk_assignment_preview("Failed", result["assignment_failures"]),
        format_bulk_assignment_preview(
            "Duplicate member inputs", result["duplicate_member_inputs"]
        ),
    ):
        if line:
            summary_lines.append(line)
    return summary_lines


def build_bulk_assignment_report_text(
    role: discord.Role, requested_by: str, source_name: str, result: dict
):
    def section_block(title: str, values: list[str]):
        lines = [f"{title}: {len(values)}"]
        if values:
            lines.extend(f"- {value}" for value in values)
        return "\n".join(lines)

    return "\n\n".join(
        [
            f"Bulk CSV Role Assignment Report\n"
            f"Role: {role.name} ({role.id})\n"
            f"Requested by: {requested_by}\n"
            f"File: {source_name}\n"
            f"Timestamp: {discord.utils.utcnow().isoformat()}",
            section_block("Assigned", result["assigned"]),
            section_block("Already had role", result["already_had_role"]),
            section_block("Unmatched", result["unmatched_names"]),
            section_block("Ambiguous", result["ambiguous_names"]),
            section_block("Failed", result["assignment_failures"]),
            section_block("Duplicate member inputs", result["duplicate_member_inputs"]),
        ]
    )


async def process_bulk_role_assignment_payload(
    guild: discord.Guild,
    role: discord.Role,
    payload: bytes,
    requested_by: str,
    reason_actor: str,
):
    raw_names = parse_member_names_from_csv_bytes(payload)
    unique_names = unique_member_names(raw_names)
    if not unique_names:
        return None, "❌ The uploaded file did not contain any names."
    if len(unique_names) > CSV_ROLE_ASSIGN_MAX_NAMES:
        return (
            None,
            f"❌ Too many names. Limit is `{CSV_ROLE_ASSIGN_MAX_NAMES}` unique names per file.",
        )

    member_lookup = build_member_name_lookup(guild)
    matched_members = {}
    duplicate_member_inputs = []
    ambiguous_names = []
    unmatched_names = []
    for raw_name in unique_names:
        key = normalize_member_lookup_name(raw_name)
        matches = member_lookup.get(key, [])
        if len(matches) == 1:
            member = matches[0]
            if member.id in matched_members:
                duplicate_member_inputs.append(raw_name)
                continue
            matched_members[member.id] = (member, raw_name)
        elif len(matches) > 1:
            ambiguous_names.append(f"{raw_name} ({len(matches)} matches)")
        else:
            unmatched_names.append(raw_name)

    assigned = []
    already_had_role = []
    assignment_failures = []
    for member, matched_name in matched_members.values():
        if role in member.roles:
            already_had_role.append(f"{matched_name} ({member})")
            continue
        try:
            await member.add_roles(role, reason=reason_actor)
            assigned.append(f"{matched_name} ({member})")
        except discord.Forbidden:
            assignment_failures.append(f"{matched_name} (permission denied)")
        except discord.HTTPException:
            assignment_failures.append(f"{matched_name} (Discord API error)")

    result = {
        "unique_names_count": len(unique_names),
        "matched_members_count": len(matched_members),
        "assigned": assigned,
        "already_had_role": already_had_role,
        "unmatched_names": unmatched_names,
        "ambiguous_names": ambiguous_names,
        "assignment_failures": assignment_failures,
        "duplicate_member_inputs": duplicate_member_inputs,
    }
    logger.info(
        "CSV role assignment role=%s processed=%s assigned=%s already=%s unmatched=%s ambiguous=%s failed=%s",
        role.id,
        result["unique_names_count"],
        len(assigned),
        len(already_had_role),
        len(unmatched_names),
        len(ambiguous_names),
        len(assignment_failures),
    )
    return result, None


async def run_web_bulk_role_assignment_async(
    role_input: str, payload: bytes, filename: str, actor_email: str
):
    guild = bot.get_guild(GUILD_ID)
    if guild is None:
        return {"ok": False, "error": "Guild is not currently available to the bot."}

    role_id = parse_role_id_input(role_input)
    if role_id is None:
        return {
            "ok": False,
            "error": "Role ID is invalid. Use a numeric role ID or role mention format.",
        }
    role = guild.get_role(role_id)
    if role is None:
        return {
            "ok": False,
            "error": f"Role `{role_id}` was not found in the configured guild.",
        }

    bot_user_id = bot.user.id if bot.user else None
    bot_member = guild.me or (guild.get_member(bot_user_id) if bot_user_id else None)
    if bot_member is None:
        return {"ok": False, "error": "Could not resolve bot member in this guild."}
    if role == guild.default_role:
        return {"ok": False, "error": "The @everyone role cannot be assigned this way."}
    if role.managed:
        return {
            "ok": False,
            "error": "That role is managed by an integration and cannot be assigned manually.",
        }
    if bot_member.top_role <= role:
        return {
            "ok": False,
            "error": "I cannot assign that role because it is above my top role.",
        }

    result, error = await process_bulk_role_assignment_payload(
        guild=guild,
        role=role,
        payload=payload,
        requested_by=f"web_admin:{actor_email}",
        reason_actor=f"Bulk CSV role assignment by web admin {actor_email}",
    )
    if error:
        return {"ok": False, "error": error}

    summary_lines = build_bulk_assignment_summary_lines(filename, role.mention, result)
    report_text = build_bulk_assignment_report_text(
        role, f"web admin {actor_email}", filename, result
    )
    return {
        "ok": True,
        "role_name": role.name,
        "role_id": role.id,
        "summary_lines": summary_lines,
        "report_text": report_text,
        "result": result,
    }


def run_web_bulk_role_assignment(
    role_input: str, payload: bytes, filename: str, actor_email: str
):
    loop = getattr(bot, "loop", None)
    if loop is None or not loop.is_running():
        return {
            "ok": False,
            "error": "Bot loop is not running yet. Try again in a few seconds.",
        }
    future = asyncio.run_coroutine_threadsafe(
        run_web_bulk_role_assignment_async(role_input, payload, filename, actor_email),
        loop,
    )
    try:
        return future.result(timeout=WEB_BULK_ASSIGN_TIMEOUT_SECONDS)
    except concurrent.futures.TimeoutError:
        future.cancel()
        return {
            "ok": False,
            "error": "Timed out while processing the CSV. Try a smaller file or retry.",
        }
    except Exception:
        logger.exception("Unexpected failure in web bulk role assignment")
        return {
            "ok": False,
            "error": "Unexpected error while assigning roles from CSV.",
        }


async def fetch_discord_catalog_async():
    guild = bot.get_guild(GUILD_ID)
    if guild is None:
        return {"ok": False, "error": "Configured guild is not available in bot cache."}

    channels = []
    source_channels = list(guild.channels)
    try:
        source_channels = await guild.fetch_channels()
    except Exception:
        logger.debug(
            "Falling back to cached guild channels for web catalog", exc_info=True
        )

    for channel in source_channels:
        if isinstance(channel, discord.CategoryChannel):
            channel_type = "category"
            label = f"{channel.name} [category]"
        elif isinstance(channel, discord.TextChannel):
            channel_type = "text"
            label = f"#{channel.name} [text]"
        elif isinstance(channel, discord.ForumChannel):
            channel_type = "forum"
            label = f"#{channel.name} [forum]"
        elif isinstance(channel, discord.VoiceChannel):
            channel_type = "voice"
            label = f"{channel.name} [voice]"
        elif isinstance(channel, discord.StageChannel):
            channel_type = "stage"
            label = f"{channel.name} [stage]"
        else:
            channel_type = str(channel.type)
            label = f"{channel.name} [{channel_type}]"

        channels.append(
            {
                "id": str(channel.id),
                "name": channel.name,
                "type": channel_type,
                "position": getattr(channel, "position", 0),
                "label": label,
            }
        )

    channels.sort(
        key=lambda item: (item["type"], item["position"], item["name"].casefold())
    )

    roles = []
    for role in guild.roles:
        if role == guild.default_role:
            continue
        roles.append(
            {
                "id": str(role.id),
                "name": role.name,
                "position": role.position,
                "label": f"@{role.name}",
            }
        )
    roles.sort(key=lambda item: (-item["position"], item["name"].casefold()))

    return {
        "ok": True,
        "guild": {"id": str(guild.id), "name": guild.name},
        "channels": channels,
        "roles": roles,
        "fetched_at": time.time(),
    }


def run_web_get_discord_catalog():
    now = time.time()
    cached = discord_catalog_cache.get("data")
    if (
        cached
        and now - discord_catalog_cache.get("fetched_at", 0.0)
        < WEB_DISCORD_CATALOG_TTL_SECONDS
    ):
        return cached

    loop = getattr(bot, "loop", None)
    if loop is None or not loop.is_running():
        return {"ok": False, "error": "Bot loop is not running yet."}

    future = asyncio.run_coroutine_threadsafe(fetch_discord_catalog_async(), loop)
    try:
        data = future.result(timeout=WEB_DISCORD_CATALOG_FETCH_TIMEOUT_SECONDS)
    except concurrent.futures.TimeoutError:
        future.cancel()
        return {"ok": False, "error": "Timed out fetching Discord channels/roles."}
    except Exception:
        logger.exception("Unexpected failure while fetching web Discord catalog")
        return {
            "ok": False,
            "error": "Unexpected error while fetching Discord channels/roles.",
        }

    if isinstance(data, dict) and data.get("ok"):
        discord_catalog_cache["fetched_at"] = now
        discord_catalog_cache["data"] = data
    return data


async def fetch_bot_profile_async():
    current_user = bot.user
    if current_user is None:
        return {"ok": False, "error": "Bot user is not ready yet."}

    guild = bot.get_guild(GUILD_ID)
    bot_member = None
    if guild is not None:
        bot_member = guild.me or guild.get_member(current_user.id)
        if bot_member is None:
            try:
                bot_member = await guild.fetch_member(current_user.id)
            except discord.HTTPException:
                logger.exception("Failed to fetch bot member for guild %s", guild.id)

    server_display_name = (
        bot_member.display_name if bot_member is not None else current_user.display_name
    )
    server_nickname = bot_member.nick if bot_member is not None else ""
    avatar_url = (
        str(current_user.display_avatar.url) if current_user.display_avatar else ""
    )
    return {
        "ok": True,
        "id": str(current_user.id),
        "name": current_user.name,
        "display_name": current_user.display_name,
        "global_name": current_user.global_name or "",
        "guild_id": str(guild.id) if guild is not None else "",
        "guild_name": guild.name if guild is not None else "",
        "server_display_name": server_display_name,
        "server_nickname": server_nickname or "",
        "avatar_url": avatar_url,
    }


def normalize_optional_text(value: str | None):
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def validate_bot_profile_change_request(
    username: str | None,
    server_nickname: str | None,
    clear_server_nickname: bool,
):
    normalized_username = normalize_optional_text(username)
    normalized_nickname = normalize_optional_text(server_nickname)

    if clear_server_nickname and normalized_nickname is not None:
        return (
            None,
            None,
            "Provide either `server_nickname` or `clear_server_nickname`, not both.",
        )
    if normalized_username is not None and not (
        BOT_USERNAME_MIN_LENGTH <= len(normalized_username) <= BOT_USERNAME_MAX_LENGTH
    ):
        return (
            None,
            None,
            f"Username must be between {BOT_USERNAME_MIN_LENGTH} and {BOT_USERNAME_MAX_LENGTH} characters.",
        )
    if (
        normalized_nickname is not None
        and len(normalized_nickname) > BOT_NICKNAME_MAX_LENGTH
    ):
        return (
            None,
            None,
            f"Server nickname must be {BOT_NICKNAME_MAX_LENGTH} characters or fewer.",
        )

    nickname_target = BOT_SERVER_NICKNAME_UNSET
    if clear_server_nickname:
        nickname_target = None
    elif normalized_nickname is not None:
        nickname_target = normalized_nickname

    if normalized_username is None and nickname_target is BOT_SERVER_NICKNAME_UNSET:
        return (
            None,
            None,
            "Provide at least one change: `username`, `server_nickname`, or `clear_server_nickname`.",
        )

    return normalized_username, nickname_target, None


async def resolve_configured_guild_bot_member():
    current_user = bot.user
    if current_user is None:
        return None, None, "Bot user is not ready yet."

    guild = bot.get_guild(GUILD_ID)
    if guild is None:
        return None, None, "Configured guild is not currently available to the bot."

    bot_member = guild.me or guild.get_member(current_user.id)
    if bot_member is None:
        try:
            bot_member = await guild.fetch_member(current_user.id)
        except discord.HTTPException:
            logger.exception(
                "Failed to fetch bot member for configured guild %s", guild.id
            )
            bot_member = None
    if bot_member is None:
        return guild, None, "Could not resolve the bot member in the configured guild."
    return guild, bot_member, None


async def apply_bot_profile_updates_async(
    username: str | None, server_nickname, actor_label: str
):
    current_user = bot.user
    if current_user is None:
        return {"ok": False, "error": "Bot user is not ready yet."}

    updated_username = False
    updated_server_nickname = False
    notes = []
    errors = []

    if username is not None:
        if username == current_user.name:
            notes.append("Username was already set to that value.")
        else:
            try:
                await current_user.edit(username=username)
                updated_username = True
            except discord.HTTPException:
                logger.exception("Failed to update bot username")
                errors.append(
                    "Failed to update username. Discord may enforce rename limits; try again later."
                )

    if server_nickname is not BOT_SERVER_NICKNAME_UNSET:
        _, bot_member, member_error = await resolve_configured_guild_bot_member()
        if member_error:
            errors.append(member_error)
        else:
            if bot_member.nick == server_nickname:
                if server_nickname is None:
                    notes.append("Server nickname was already cleared.")
                else:
                    notes.append("Server nickname was already set to that value.")
            else:
                try:
                    await bot_member.edit(
                        nick=server_nickname,
                        reason=f"Bot profile updated by {actor_label}",
                    )
                    updated_server_nickname = True
                except discord.Forbidden:
                    logger.exception(
                        "Missing permission to update bot server nickname"
                    )
                    errors.append(
                        "Missing permission to update server nickname. Check `Manage Nicknames` and role hierarchy."
                    )
                except discord.HTTPException:
                    logger.exception("Failed to update bot server nickname")
                    errors.append(
                        "Failed to update server nickname due to a Discord API error."
                    )

    profile = await fetch_bot_profile_async()
    result = {
        "ok": False,
        "updated_username": updated_username,
        "updated_server_nickname": updated_server_nickname,
    }
    if isinstance(profile, dict):
        for key, value in profile.items():
            if key != "ok":
                result[key] = value
        if not profile.get("ok"):
            errors.append(
                str(profile.get("error") or "Unable to refresh bot profile details.")
            )

    updated_parts = []
    if updated_username:
        updated_parts.append("username")
    if updated_server_nickname:
        updated_parts.append("server nickname")

    if updated_parts:
        message = f"Updated {' and '.join(updated_parts)}."
        if notes:
            message = f"{message} {' '.join(notes)}"
        if errors:
            message = f"{message} {' '.join(errors)}"
        result["ok"] = True
        result["message"] = message
        return result

    if errors:
        result["error"] = " ".join(errors)
        return result

    result["ok"] = True
    if notes:
        result["message"] = f"No changes were needed. {' '.join(notes)}"
    else:
        result["message"] = "No changes were needed."
    return result


def run_web_get_bot_profile():
    loop = getattr(bot, "loop", None)
    if loop is None or not loop.is_running():
        return {"ok": False, "error": "Bot loop is not running yet."}

    future = asyncio.run_coroutine_threadsafe(fetch_bot_profile_async(), loop)
    try:
        return future.result(timeout=WEB_BOT_PROFILE_TIMEOUT_SECONDS)
    except concurrent.futures.TimeoutError:
        future.cancel()
        return {"ok": False, "error": "Timed out while loading bot profile."}
    except Exception:
        logger.exception("Unexpected failure while loading bot profile")
        return {"ok": False, "error": "Unexpected error while loading bot profile."}


async def run_web_update_bot_avatar_async(payload: bytes, actor_email: str):
    current_user = bot.user
    if current_user is None:
        return {"ok": False, "error": "Bot user is not ready yet."}
    if not payload:
        return {"ok": False, "error": "Avatar image payload was empty."}
    if len(payload) > WEB_AVATAR_MAX_UPLOAD_BYTES:
        return {
            "ok": False,
            "error": f"Avatar file too large ({len(payload)} bytes). Max is {WEB_AVATAR_MAX_UPLOAD_BYTES} bytes.",
        }

    try:
        await current_user.edit(avatar=payload)
    except discord.HTTPException:
        logger.exception("Failed to update bot avatar via web admin")
        return {
            "ok": False,
            "error": "Discord rejected the avatar image. Use a valid PNG/JPG/WEBP/GIF image.",
        }

    profile = await fetch_bot_profile_async()
    if profile.get("ok"):
        logger.info("Bot avatar updated via web admin")
    return profile


async def run_web_update_bot_profile_async(
    username: str | None,
    server_nickname: str | None,
    clear_server_nickname: bool,
    actor_email: str,
):
    normalized_username, nickname_target, validation_error = (
        validate_bot_profile_change_request(
            username=username,
            server_nickname=server_nickname,
            clear_server_nickname=clear_server_nickname,
        )
    )
    if validation_error:
        return {"ok": False, "error": validation_error}

    result = await apply_bot_profile_updates_async(
        username=normalized_username,
        server_nickname=nickname_target,
        actor_label=f"web admin {actor_email}",
    )
    if result.get("ok"):
        logger.info(
            "Bot profile update via web admin (username=%s nickname_change=%s)",
            bool(normalized_username),
            nickname_target is not BOT_SERVER_NICKNAME_UNSET,
        )
    return result


def run_web_update_bot_profile(
    username: str | None,
    server_nickname: str | None,
    clear_server_nickname: bool,
    actor_email: str,
):
    loop = getattr(bot, "loop", None)
    if loop is None or not loop.is_running():
        return {
            "ok": False,
            "error": "Bot loop is not running yet. Try again in a few seconds.",
        }

    future = asyncio.run_coroutine_threadsafe(
        run_web_update_bot_profile_async(
            username, server_nickname, clear_server_nickname, actor_email
        ),
        loop,
    )
    try:
        return future.result(timeout=WEB_BOT_PROFILE_TIMEOUT_SECONDS)
    except concurrent.futures.TimeoutError:
        future.cancel()
        return {"ok": False, "error": "Timed out while updating bot profile."}
    except Exception:
        logger.exception("Unexpected failure while updating bot profile")
        return {"ok": False, "error": "Unexpected error while updating bot profile."}


def run_web_update_bot_avatar(payload: bytes, filename: str, actor_email: str):
    loop = getattr(bot, "loop", None)
    if loop is None or not loop.is_running():
        return {
            "ok": False,
            "error": "Bot loop is not running yet. Try again in a few seconds.",
        }

    future = asyncio.run_coroutine_threadsafe(
        run_web_update_bot_avatar_async(payload, actor_email), loop
    )
    try:
        return future.result(timeout=WEB_BOT_PROFILE_TIMEOUT_SECONDS)
    except concurrent.futures.TimeoutError:
        future.cancel()
        return {"ok": False, "error": "Timed out while updating bot avatar."}
    except Exception:
        logger.exception("Unexpected failure while updating bot avatar")
        return {"ok": False, "error": "Unexpected error while updating bot avatar."}


def run_web_request_restart(actor_email: str):
    logger.warning("Web admin container restart requested")

    def _exit_process():
        logger.warning("Exiting bot process due to web admin container restart request")
        os._exit(0)

    restart_timer = threading.Timer(1.0, _exit_process)
    restart_timer.daemon = True
    restart_timer.start()
    return {
        "ok": True,
        "message": "Restart requested. The container process will exit and restart shortly.",
    }


def parse_timeout_duration(value: str):
    match = TIMEOUT_DURATION_PATTERN.fullmatch(value or "")
    if not match:
        return None, None, "❌ Invalid duration. Use `30m`, `2h`, or `1d`."

    amount = int(match.group(1))
    unit = (match.group(2) or "m").lower()
    multiplier = {"m": 1, "h": 60, "d": 1440}
    total_minutes = amount * multiplier[unit]
    if total_minutes < 1:
        return None, None, "❌ Duration must be at least 1 minute."
    if total_minutes > TIMEOUT_MAX_MINUTES:
        return None, None, "❌ Duration cannot exceed 28 days."
    return timedelta(minutes=total_minutes), f"{amount}{unit}", None


def normalize_country_code(value: str):
    normalized = value.strip().upper()
    if COUNTRY_CODE_PATTERN.fullmatch(normalized):
        return normalized
    return None


def strip_country_suffix(name: str):
    without_flag = COUNTRY_FLAG_SUFFIX_PATTERN.sub("", name)
    without_code = COUNTRY_CODE_SUFFIX_PATTERN.sub("", without_flag)
    without_legacy = COUNTRY_LEGACY_SUFFIX_PATTERN.sub("", without_code)
    return without_legacy.rstrip(" _-")


def build_country_nickname(member: discord.Member, country_code: str):
    base_name = member.nick or member.display_name or member.name
    base_name = strip_country_suffix(base_name)
    if not base_name:
        base_name = member.name or "user"

    suffix = f" - {country_code}"
    max_base_length = 32 - len(suffix)
    trimmed_base = base_name[:max_base_length].rstrip() or base_name[:max_base_length]
    if not trimmed_base:
        trimmed_base = "user"[:max_base_length]
    return f"{trimmed_base}{suffix}"


async def set_member_country(member: discord.Member, country_code: str):
    nickname = build_country_nickname(member, country_code)
    if member.nick == nickname:
        return False, f"ℹ️ Your nickname already includes `{country_code}`."
    await member.edit(nick=nickname, reason=f"Set country code to {country_code}")
    return True, f"✅ Country updated. Your nickname is now `{nickname}`."


async def clear_member_country(member: discord.Member):
    if not member.nick:
        return False, "❌ You do not currently have a server nickname to update."

    stripped = strip_country_suffix(member.nick)
    if stripped == member.nick:
        return False, "❌ Your nickname does not end with a country code suffix."

    await member.edit(nick=stripped or None, reason="Clear country code suffix")
    if stripped:
        return True, f"✅ Country removed. Your nickname is now `{stripped}`."
    return True, "✅ Country removed. Your nickname has been reset."


async def prune_user_messages(guild: discord.Guild, user_id: int, hours: int):
    cutoff = discord.utils.utcnow() - timedelta(hours=hours)
    deleted_count = 0
    scanned_channels = 0
    reason = f"Prune last {hours}h messages for kicked member {user_id}"
    channels = list(guild.text_channels) + list(guild.threads)
    seen_channel_ids = set()

    for channel in channels:
        if channel.id in seen_channel_ids:
            continue
        seen_channel_ids.add(channel.id)

        perms = channel.permissions_for(guild.me)
        if not (
            perms.view_channel and perms.read_message_history and perms.manage_messages
        ):
            continue

        scanned_channels += 1
        try:
            deleted = await channel.purge(
                limit=None,
                after=cutoff,
                check=lambda message: message.author.id == user_id,
                bulk=True,
                reason=reason,
            )
            deleted_count += len(deleted)
        except discord.Forbidden:
            logger.warning(
                "Skipping channel %s while pruning: missing permissions", channel.id
            )
        except discord.HTTPException:
            logger.exception("Failed to prune messages in channel %s", channel.id)

    return deleted_count, scanned_channels


async def prune_channel_recent_messages(
    channel: discord.TextChannel | discord.Thread,
    amount: int,
    *,
    reason: str,
    skip_message_id: int | None = None,
):
    safe_amount = max(1, min(500, int(amount)))
    deleted_messages = await channel.purge(
        limit=safe_amount,
        check=lambda message: (
            not message.pinned and (skip_message_id is None or message.id != skip_message_id)
        ),
        bulk=True,
        reason=reason,
    )
    return len(deleted_messages)


async def resolve_mod_log_channel(guild: discord.Guild):
    channel = guild.get_channel(MOD_LOG_CHANNEL_ID)
    if channel is None:
        channel = bot.get_channel(MOD_LOG_CHANNEL_ID)
    if channel is None:
        try:
            channel = await bot.fetch_channel(MOD_LOG_CHANNEL_ID)
        except discord.NotFound:
            logger.warning("Moderation log channel %s not found", MOD_LOG_CHANNEL_ID)
            return None
        except discord.Forbidden:
            logger.warning(
                "No permission to access moderation log channel %s", MOD_LOG_CHANNEL_ID
            )
            return None
        except discord.HTTPException:
            logger.exception(
                "Failed to fetch moderation log channel %s", MOD_LOG_CHANNEL_ID
            )
            return None

    if isinstance(channel, (discord.TextChannel, discord.Thread)):
        return channel

    logger.warning(
        "Moderation log channel %s is not a text channel", MOD_LOG_CHANNEL_ID
    )
    return None


async def send_moderation_log(
    guild: discord.Guild,
    actor: discord.Member,
    action: str,
    target: discord.Member | None = None,
    reason: str | None = None,
    outcome: str = "success",
    details: str | None = None,
):
    channel = await resolve_mod_log_channel(guild)
    if channel is None:
        return False

    target_text = f"{target} (`{target.id}`)" if target else "N/A"
    reason_text = reason or "N/A"
    details_text = details or "N/A"
    message = (
        "🛡️ **Moderation Action**\n"
        f"**Moderator:** {actor.mention} (`{actor.id}`)\n"
        f"**Action:** `{action}`\n"
        f"**Target:** {target_text}\n"
        f"**Outcome:** `{outcome}`\n"
        f"**Reason:** {reason_text}\n"
        f"**Details:** {details_text}"
    )
    try:
        await channel.send(message)
        return True
    except discord.Forbidden:
        logger.warning(
            "No permission to send moderation logs to channel %s", MOD_LOG_CHANNEL_ID
        )
        return False
    except discord.HTTPException:
        logger.exception("Failed to send moderation log for action %s", action)
        return False


def clip_text(value: str, max_chars: int = 250):
    if not value:
        return "N/A"
    cleaned = value.strip().replace("\n", " ")
    if len(cleaned) <= max_chars:
        return cleaned
    return f"{cleaned[: max_chars - 3]}..."


def sanitize_log_text(value: str):
    text = str(value or "")
    if not text:
        return ""
    return SENSITIVE_LOG_VALUE_PATTERN.sub(r"\1=[REDACTED]", text)


def read_recent_log_lines(path: str, max_lines: int):
    try:
        line_limit = max(10, min(400, int(max_lines)))
    except (TypeError, ValueError):
        line_limit = 100

    with open(path, "r", encoding="utf-8", errors="replace") as handle:
        buffer = deque(handle, maxlen=line_limit)
    return sanitize_log_text("".join(buffer))


async def send_server_event_log(guild: discord.Guild, event_name: str, details: str):
    channel = await resolve_mod_log_channel(guild)
    if channel is None:
        return False

    message = f"📌 **Server Event:** `{event_name}`\n{details}"
    try:
        await channel.send(message)
        return True
    except discord.Forbidden:
        logger.warning(
            "No permission to send server event logs to channel %s", MOD_LOG_CHANNEL_ID
        )
        return False
    except discord.HTTPException:
        logger.exception("Failed to send server event log for %s", event_name)
        return False


def normalize_release_notes_text(value: str):
    lines = []
    for raw_line in (value or "").splitlines():
        cleaned = re.sub(r"\s+", " ", raw_line).strip()
        if cleaned:
            lines.append(cleaned)
    return "\n".join(lines)


def load_firmware_seen_ids():
    initialized = db_kv_get("firmware_seen_initialized")
    if initialized != "1":
        return None
    conn = get_db_connection()
    with db_lock:
        rows = conn.execute("SELECT entry_id FROM firmware_seen").fetchall()
    return {str(row["entry_id"]) for row in rows if row["entry_id"]}


def load_firmware_signature_map():
    raw_payload = db_kv_get("firmware_entry_signatures")
    if not raw_payload:
        return None
    try:
        parsed = json.loads(raw_payload)
    except (TypeError, ValueError):
        logger.warning("Firmware signature state is invalid JSON; rebuilding baseline.")
        return None
    if not isinstance(parsed, dict):
        logger.warning("Firmware signature state is invalid; rebuilding baseline.")
        return None
    signatures = {}
    for raw_key, raw_signature in parsed.items():
        key = str(raw_key or "").strip()
        signature = str(raw_signature or "").strip()
        if key and signature:
            signatures[key] = signature
    return signatures


def build_firmware_change_key(entry: dict):
    model_code = str(entry.get("model_code") or "unknown").strip().lower()
    stage = str(entry.get("stage") or "unknown").strip().lower()
    version = str(entry.get("version") or "unknown").strip()
    return f"{model_code}|{stage}|{version}"


def build_firmware_entry_signature(entry: dict):
    file_tokens = [
        f"{str(item.get('label') or '').strip()}|{str(item.get('url') or '').strip()}"
        for item in entry.get("files", [])
        if str(item.get("url") or "").strip()
    ]
    sha_tokens = [
        str(value).strip() for value in entry.get("sha256", []) if str(value).strip()
    ]
    payload = "|".join(
        [
            str(entry.get("published_date") or "").strip(),
            ",".join(sorted(file_tokens)),
            ",".join(sorted(sha_tokens)),
            normalize_release_notes_text(str(entry.get("release_notes") or "")),
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def build_firmware_signature_snapshot(entries: list[dict]):
    snapshot = {}
    for entry in entries:
        change_key = build_firmware_change_key(entry)
        entry["change_key"] = change_key
        snapshot[change_key] = build_firmware_entry_signature(entry)
    return snapshot


def save_firmware_state(
    seen_ids: set[str], signature_snapshot: dict[str, str], sync_label: str = ""
):
    now_iso = datetime.now(timezone.utc).isoformat()
    conn = get_db_connection()
    with db_lock:
        conn.execute("DELETE FROM firmware_seen")
        for entry_id in sorted(seen_ids):
            cleaned = str(entry_id or "").strip()
            if not cleaned:
                continue
            conn.execute(
                """
                INSERT INTO firmware_seen (entry_id, created_at)
                VALUES (?, ?)
                """,
                (cleaned, now_iso),
            )
        conn.commit()
    db_kv_set("firmware_seen_initialized", "1")
    db_kv_set("firmware_source_url", FIRMWARE_FEED_URL)
    db_kv_set(
        "firmware_entry_signatures",
        json.dumps(signature_snapshot, separators=(",", ":"), sort_keys=True),
    )
    if sync_label:
        db_kv_set("firmware_last_synced", sync_label)
    else:
        db_kv_delete("firmware_last_synced")


def parse_firmware_entries(page_html: str):
    soup = BeautifulSoup(page_html, "html.parser")
    sync_line = soup.select_one(".sync-line")
    sync_label = (
        clean_search_text(sync_line.get_text(" ", strip=True)) if sync_line else ""
    )
    entries = []

    for section in soup.select("section.model-section"):
        model_code = (section.get("id") or "").strip()
        model_name = model_code.upper() if model_code else "Unknown Model"
        heading = section.find("h2")
        if heading:
            code_tag = heading.find("span", class_="code")
            if code_tag is not None:
                code_tag.extract()
            model_name = (
                clean_search_text(heading.get_text(" ", strip=True)) or model_name
            )

        for row in section.find_all("div", class_="fw-row"):
            stage = (row.get("data-stage") or "unknown").strip().lower()
            version_tag = row.find("span", class_="fw-version")
            date_tag = row.find("span", class_="fw-date")
            version = (
                clean_search_text(version_tag.get_text(" ", strip=True))
                if version_tag
                else "unknown"
            )
            published_date = (
                clean_search_text(date_tag.get_text(" ", strip=True))
                if date_tag
                else "unknown"
            )

            files = []
            for link in row.select(".fw-files a[href]"):
                label = clean_search_text(link.get_text(" ", strip=True)) or "Download"
                url = link["href"].strip()
                if url:
                    files.append({"label": label, "url": url})

            sha256_values = []
            for badge in row.select(".fw-sha .sha-badge"):
                title = (badge.get("title") or "").strip()
                if title:
                    sha256_values.append(title.split(" —", 1)[0].strip())

            release_notes = ""
            notes_block = row.find_next_sibling()
            if (
                notes_block
                and notes_block.name == "details"
                and "release-notes" in (notes_block.get("class") or [])
            ):
                notes_content = notes_block.find("div", class_="content")
                if notes_content:
                    release_notes = normalize_release_notes_text(
                        notes_content.get_text("\n", strip=True)
                    )

            file_key = "|".join(sorted(file_info["url"] for file_info in files))
            sha_key = "|".join(sorted(value for value in sha256_values if value))
            entry_id = (
                f"{model_code}|{stage}|{version}|{published_date}|{file_key or sha_key}"
            )
            entries.append(
                {
                    "id": entry_id,
                    "model_code": model_code or "unknown",
                    "model_name": model_name,
                    "stage": stage,
                    "version": version,
                    "published_date": published_date,
                    "files": files,
                    "sha256": sha256_values,
                    "release_notes": release_notes,
                }
            )

    return entries, sync_label


def fetch_firmware_entries():
    response = requests.get(FIRMWARE_FEED_URL, timeout=FIRMWARE_REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    return parse_firmware_entries(response.text)


def trim_discord_message(message: str, max_chars: int = 1900):
    if len(message) <= max_chars:
        return message
    return message[: max_chars - 4].rstrip() + " ..."


def firmware_stage_label(raw_stage: str):
    stage = str(raw_stage or "").strip().lower()
    return (
        "Stable"
        if stage == "release"
        else "Testing"
        if stage == "testing"
        else stage.title() or "Unknown"
    )


def format_firmware_change_summary(
    new_entries: list[dict], changed_entries: list[dict], sync_label: str
):
    total_changes = len(new_entries) + len(changed_entries)
    lines = [
        "📡 **Firmware updates detected**",
        f"New: `{len(new_entries)}` | Changed: `{len(changed_entries)}`",
    ]
    combined_entries = [("🆕", entry) for entry in new_entries] + [
        ("🔄", entry) for entry in changed_entries
    ]
    combined_entries.sort(
        key=lambda item: (
            item[1].get("published_date", ""),
            item[1].get("model_code", ""),
            item[1].get("version", ""),
        )
    )
    for icon, entry in combined_entries[:FIRMWARE_NOTIFICATION_ITEM_LIMIT]:
        stage_text = firmware_stage_label(entry.get("stage", ""))
        model_code = str(entry.get("model_code") or "unknown").upper()
        version = str(entry.get("version") or "unknown")
        published_date = str(entry.get("published_date") or "unknown")
        lines.append(
            f"- {icon} `{model_code}` `{version}` ({stage_text}, {published_date})"
        )

    if total_changes > FIRMWARE_NOTIFICATION_ITEM_LIMIT:
        remaining = total_changes - FIRMWARE_NOTIFICATION_ITEM_LIMIT
        lines.append(f"- ... and `{remaining}` more update(s)")
    if sync_label:
        lines.append(f"`{sync_label}`")
    lines.append(f"Source: {FIRMWARE_FEED_URL}")
    return trim_discord_message("\n".join(lines))


async def resolve_firmware_notify_channel():
    if FIRMWARE_NOTIFY_CHANNEL_ID <= 0:
        return None, "channel_id_not_configured"

    channel = bot.get_channel(FIRMWARE_NOTIFY_CHANNEL_ID)
    if channel is None:
        try:
            channel = await bot.fetch_channel(FIRMWARE_NOTIFY_CHANNEL_ID)
        except discord.NotFound:
            return None, "channel_not_found"
        except discord.Forbidden:
            return None, "channel_access_forbidden"
        except discord.HTTPException:
            logger.exception(
                "Failed to fetch firmware notify channel %s", FIRMWARE_NOTIFY_CHANNEL_ID
            )
            return None, "channel_fetch_http_error"

    if isinstance(channel, (discord.TextChannel, discord.Thread)):
        return channel, ""

    return None, "channel_not_text"


def log_firmware_channel_unavailable(reason_key: str, pending_count: int):
    reason_messages = {
        "channel_id_not_configured": "firmware_notification_channel is not configured",
        "channel_not_found": "configured channel was not found",
        "channel_access_forbidden": "bot does not have access to the configured channel",
        "channel_fetch_http_error": "Discord API error while fetching configured channel",
        "channel_not_text": "configured channel is not a text/thread channel",
    }
    reason_text = reason_messages.get(reason_key, "configured channel is unavailable")
    now_ts = time.time()
    last_reason = firmware_channel_warning_state.get("reason", "")
    last_logged_at = float(firmware_channel_warning_state.get("last_logged_at", 0.0))
    if (
        reason_key == last_reason
        and (now_ts - last_logged_at) < FIRMWARE_CHANNEL_WARNING_COOLDOWN_SECONDS
    ):
        return

    firmware_channel_warning_state["reason"] = reason_key
    firmware_channel_warning_state["last_logged_at"] = now_ts
    logger.warning(
        "Firmware notifications paused: %s (channel_id=%s, guild_id=%s, pending_updates=%d). "
        "Update firmware_notification_channel to a valid text channel the bot can access.",
        reason_text,
        FIRMWARE_NOTIFY_CHANNEL_ID,
        GUILD_ID,
        pending_count,
    )


async def check_firmware_updates_once():
    try:
        entries, sync_label = await asyncio.to_thread(fetch_firmware_entries)
    except requests.RequestException:
        logger.exception("Firmware fetch failed from %s", FIRMWARE_FEED_URL)
        return
    except Exception:
        logger.exception("Unexpected firmware parsing failure")
        return

    if not entries:
        logger.warning("Firmware monitor parsed no entries from %s", FIRMWARE_FEED_URL)
        return

    current_ids = {entry["id"] for entry in entries}
    current_signatures = build_firmware_signature_snapshot(entries)
    seen_ids = load_firmware_seen_ids()
    if seen_ids is None:
        save_firmware_state(current_ids, current_signatures, sync_label)
        logger.info(
            "Firmware monitor baseline initialized with %d entries", len(current_ids)
        )
        return

    previous_signatures = load_firmware_signature_map()
    if previous_signatures is None:
        save_firmware_state(current_ids, current_signatures, sync_label)
        logger.info(
            "Firmware monitor signature baseline initialized with %d entries",
            len(current_ids),
        )
        return

    new_entries = []
    changed_entries = []
    for entry in entries:
        change_key = str(entry.get("change_key") or "").strip()
        if not change_key:
            continue
        previous_signature = previous_signatures.get(change_key)
        current_signature = current_signatures.get(change_key, "")
        if previous_signature is None:
            new_entries.append(entry)
            continue
        if previous_signature != current_signature:
            changed_entries.append(entry)

    if not new_entries and not changed_entries:
        save_firmware_state(current_ids, current_signatures, sync_label)
        return

    channel, channel_error = await resolve_firmware_notify_channel()
    if channel is None:
        log_firmware_channel_unavailable(
            channel_error, len(new_entries) + len(changed_entries)
        )
        return

    firmware_channel_warning_state["reason"] = ""
    firmware_channel_warning_state["last_logged_at"] = 0.0
    logger.info(
        "Firmware monitor detected %d new and %d changed entries",
        len(new_entries),
        len(changed_entries),
    )
    try:
        await channel.send(
            format_firmware_change_summary(new_entries, changed_entries, sync_label)
        )
    except discord.Forbidden:
        logger.warning(
            "No permission to post firmware notification in channel %s", channel.id
        )
        return
    except discord.HTTPException:
        logger.exception("Failed to post firmware summary notification")
        return

    save_firmware_state(current_ids, current_signatures, sync_label)


async def firmware_monitor_loop():
    if FIRMWARE_NOTIFY_CHANNEL_ID <= 0:
        logger.info(
            "Firmware monitor disabled: set firmware_notification_channel to enable it."
        )
        return

    if not croniter.is_valid(FIRMWARE_CHECK_SCHEDULE):
        logger.error(
            "Firmware monitor disabled: invalid firmware_check_schedule '%s'",
            FIRMWARE_CHECK_SCHEDULE,
        )
        return

    logger.info(
        "Firmware monitor active: checking %s on cron '%s' (UTC)",
        FIRMWARE_FEED_URL,
        FIRMWARE_CHECK_SCHEDULE,
    )
    await check_firmware_updates_once()

    while not bot.is_closed():
        now_utc = datetime.now(timezone.utc)
        next_run_utc = croniter(FIRMWARE_CHECK_SCHEDULE, now_utc).get_next(datetime)
        wait_seconds = max(1, int((next_run_utc - now_utc).total_seconds()))
        logger.debug(
            "Next firmware check scheduled for %s UTC", next_run_utc.isoformat()
        )
        await asyncio.sleep(wait_seconds)
        await check_firmware_updates_once()


def restart_firmware_monitor_task():
    global firmware_monitor_task
    if firmware_monitor_task is not None and not firmware_monitor_task.done():
        firmware_monitor_task.cancel()
    firmware_monitor_task = asyncio.create_task(
        firmware_monitor_loop(), name="firmware_monitor"
    )


def schedule_firmware_monitor_restart():
    loop = getattr(bot, "loop", None)
    if loop is None or not loop.is_running():
        return
    loop.call_soon_threadsafe(restart_firmware_monitor_task)


def refresh_runtime_settings_from_env(_updated_values=None):
    global LOG_LEVEL
    global CONTAINER_LOG_LEVEL
    global DISCORD_LOG_LEVEL
    global GENERAL_CHANNEL_ID
    global FORUM_BASE_URL
    global FORUM_MAX_RESULTS
    global REDDIT_SUBREDDIT
    global DOCS_MAX_RESULTS_PER_SITE
    global DOCS_INDEX_TTL_SECONDS
    global SEARCH_RESPONSE_MAX_CHARS
    global MODERATOR_ROLE_IDS
    global MOD_LOG_CHANNEL_ID
    global KICK_PRUNE_HOURS
    global CSV_ROLE_ASSIGN_MAX_NAMES
    global FIRMWARE_FEED_URL
    global FIRMWARE_NOTIFY_CHANNEL_ID
    global FIRMWARE_CHECK_SCHEDULE
    global FIRMWARE_REQUEST_TIMEOUT_SECONDS
    global FIRMWARE_RELEASE_NOTES_MAX_CHARS
    global WEB_DISCORD_CATALOG_TTL_SECONDS
    global WEB_DISCORD_CATALOG_FETCH_TIMEOUT_SECONDS
    global WEB_BULK_ASSIGN_TIMEOUT_SECONDS
    global WEB_BOT_PROFILE_TIMEOUT_SECONDS
    global WEB_AVATAR_MAX_UPLOAD_BYTES

    LOG_LEVEL = normalize_log_level(os.getenv("LOG_LEVEL", LOG_LEVEL), fallback=LOG_LEVEL)
    CONTAINER_LOG_LEVEL = normalize_log_level(
        os.getenv("CONTAINER_LOG_LEVEL", CONTAINER_LOG_LEVEL),
        fallback=CONTAINER_LOG_LEVEL,
    )
    DISCORD_LOG_LEVEL = normalize_log_level(
        os.getenv("DISCORD_LOG_LEVEL", DISCORD_LOG_LEVEL),
        fallback=DISCORD_LOG_LEVEL,
    )
    logger.setLevel(to_logging_level(LOG_LEVEL))
    console_handler.setLevel(to_logging_level(LOG_LEVEL))
    file_handler.setLevel(to_logging_level(LOG_LEVEL))
    container_error_handler.setLevel(to_logging_level(CONTAINER_LOG_LEVEL))
    apply_external_logger_levels()

    GENERAL_CHANNEL_ID = parse_int_setting(
        os.getenv("GENERAL_CHANNEL_ID", GENERAL_CHANNEL_ID),
        GENERAL_CHANNEL_ID,
        minimum=0,
    )
    FORUM_BASE_URL = os.getenv("FORUM_BASE_URL", FORUM_BASE_URL).rstrip("/")
    FORUM_MAX_RESULTS = parse_int_setting(
        os.getenv("FORUM_MAX_RESULTS", FORUM_MAX_RESULTS), FORUM_MAX_RESULTS, minimum=1
    )
    REDDIT_SUBREDDIT = normalize_reddit_subreddit_setting(
        os.getenv("REDDIT_SUBREDDIT", REDDIT_SUBREDDIT),
        fallback_value=REDDIT_SUBREDDIT,
    )
    DOCS_MAX_RESULTS_PER_SITE = parse_int_setting(
        os.getenv("DOCS_MAX_RESULTS_PER_SITE", DOCS_MAX_RESULTS_PER_SITE),
        DOCS_MAX_RESULTS_PER_SITE,
        minimum=1,
    )
    DOCS_INDEX_TTL_SECONDS = parse_int_setting(
        os.getenv("DOCS_INDEX_TTL_SECONDS", DOCS_INDEX_TTL_SECONDS),
        DOCS_INDEX_TTL_SECONDS,
        minimum=60,
    )
    SEARCH_RESPONSE_MAX_CHARS = parse_int_setting(
        os.getenv("SEARCH_RESPONSE_MAX_CHARS", SEARCH_RESPONSE_MAX_CHARS),
        SEARCH_RESPONSE_MAX_CHARS,
        minimum=200,
    )

    moderator_role_id = parse_int_setting(
        os.getenv("MODERATOR_ROLE_ID", next(iter(MODERATOR_ROLE_IDS))),
        next(iter(MODERATOR_ROLE_IDS)),
        minimum=1,
    )
    admin_role_id = parse_int_setting(
        os.getenv("ADMIN_ROLE_ID", moderator_role_id),
        moderator_role_id,
        minimum=1,
    )
    MODERATOR_ROLE_IDS = {moderator_role_id, admin_role_id}

    MOD_LOG_CHANNEL_ID = parse_int_setting(
        os.getenv("MOD_LOG_CHANNEL_ID", MOD_LOG_CHANNEL_ID),
        MOD_LOG_CHANNEL_ID,
        minimum=1,
    )
    KICK_PRUNE_HOURS = parse_int_setting(
        os.getenv("KICK_PRUNE_HOURS", KICK_PRUNE_HOURS), KICK_PRUNE_HOURS, minimum=1
    )
    CSV_ROLE_ASSIGN_MAX_NAMES = parse_int_setting(
        os.getenv("CSV_ROLE_ASSIGN_MAX_NAMES", CSV_ROLE_ASSIGN_MAX_NAMES),
        CSV_ROLE_ASSIGN_MAX_NAMES,
        minimum=1,
    )

    FIRMWARE_FEED_URL = normalize_http_url_setting(
        os.getenv("FIRMWARE_FEED_URL", FIRMWARE_FEED_URL),
        FIRMWARE_FEED_URL,
        "FIRMWARE_FEED_URL",
    )
    FIRMWARE_NOTIFY_CHANNEL_ID = parse_firmware_channel_id(
        os.getenv(
            "firmware_notification_channel",
            os.getenv("FIRMWARE_NOTIFICATION_CHANNEL", FIRMWARE_NOTIFY_CHANNEL_ID),
        ),
        FIRMWARE_NOTIFY_CHANNEL_ID,
    )
    candidate_schedule = (
        os.getenv(
            "firmware_check_schedule",
            os.getenv("FIRMWARE_CHECK_SCHEDULE", FIRMWARE_CHECK_SCHEDULE),
        ).strip()
        or FIRMWARE_CHECK_SCHEDULE
    )
    if croniter.is_valid(candidate_schedule):
        FIRMWARE_CHECK_SCHEDULE = candidate_schedule
    else:
        logger.warning(
            "Ignoring invalid firmware_check_schedule value: %s", candidate_schedule
        )
    FIRMWARE_REQUEST_TIMEOUT_SECONDS = parse_int_setting(
        os.getenv("FIRMWARE_REQUEST_TIMEOUT_SECONDS", FIRMWARE_REQUEST_TIMEOUT_SECONDS),
        FIRMWARE_REQUEST_TIMEOUT_SECONDS,
        minimum=5,
    )
    FIRMWARE_RELEASE_NOTES_MAX_CHARS = parse_int_setting(
        os.getenv("FIRMWARE_RELEASE_NOTES_MAX_CHARS", FIRMWARE_RELEASE_NOTES_MAX_CHARS),
        FIRMWARE_RELEASE_NOTES_MAX_CHARS,
        minimum=200,
    )
    WEB_DISCORD_CATALOG_TTL_SECONDS = parse_int_setting(
        os.getenv("WEB_DISCORD_CATALOG_TTL_SECONDS", WEB_DISCORD_CATALOG_TTL_SECONDS),
        WEB_DISCORD_CATALOG_TTL_SECONDS,
        minimum=15,
    )
    WEB_DISCORD_CATALOG_FETCH_TIMEOUT_SECONDS = parse_int_setting(
        os.getenv(
            "WEB_DISCORD_CATALOG_FETCH_TIMEOUT_SECONDS",
            WEB_DISCORD_CATALOG_FETCH_TIMEOUT_SECONDS,
        ),
        WEB_DISCORD_CATALOG_FETCH_TIMEOUT_SECONDS,
        minimum=5,
    )
    WEB_BULK_ASSIGN_TIMEOUT_SECONDS = parse_int_setting(
        os.getenv("WEB_BULK_ASSIGN_TIMEOUT_SECONDS", WEB_BULK_ASSIGN_TIMEOUT_SECONDS),
        WEB_BULK_ASSIGN_TIMEOUT_SECONDS,
        minimum=30,
    )
    WEB_BOT_PROFILE_TIMEOUT_SECONDS = parse_int_setting(
        os.getenv("WEB_BOT_PROFILE_TIMEOUT_SECONDS", WEB_BOT_PROFILE_TIMEOUT_SECONDS),
        WEB_BOT_PROFILE_TIMEOUT_SECONDS,
        minimum=5,
    )
    WEB_AVATAR_MAX_UPLOAD_BYTES = parse_int_setting(
        os.getenv("WEB_AVATAR_MAX_UPLOAD_BYTES", WEB_AVATAR_MAX_UPLOAD_BYTES),
        WEB_AVATAR_MAX_UPLOAD_BYTES,
        minimum=1024,
    )

    docs_index_cache.clear()
    discord_catalog_cache["fetched_at"] = 0.0
    discord_catalog_cache["data"] = None
    schedule_firmware_monitor_restart()
    logger.info("Runtime settings refreshed from environment")


def refresh_tag_responses_from_web():
    get_tag_responses()
    if schedule_tag_command_refresh():
        logger.info(
            "Tag responses refreshed from storage; slash command refresh scheduled"
        )
    else:
        logger.info(
            "Tag responses refreshed from storage; slash command refresh deferred until bot loop is ready"
        )


def start_web_admin_server():
    global web_admin_thread
    if not WEB_ENABLED:
        logger.info("Web admin interface disabled via WEB_ENABLED")
        return
    if web_admin_thread is not None and web_admin_thread.is_alive():
        return

    def runner():
        try:
            start_web_admin_interface(
                host=WEB_BIND_HOST,
                port=WEB_PORT,
                data_dir=DATA_DIR,
                env_file_path=WEB_ENV_FILE,
                tag_responses_file=TAG_RESPONSES_FILE,
                default_admin_email=WEB_ADMIN_DEFAULT_EMAIL,
                default_admin_password=WEB_ADMIN_DEFAULT_PASSWORD,
                on_env_settings_saved=refresh_runtime_settings_from_env,
                on_tag_responses_saved=refresh_tag_responses_from_web,
                on_get_tag_responses=run_web_get_tag_responses,
                on_save_tag_responses=run_web_save_tag_responses,
                on_bulk_assign_role_csv=run_web_bulk_role_assignment,
                on_get_discord_catalog=run_web_get_discord_catalog,
                on_get_command_permissions=run_web_get_command_permissions,
                on_save_command_permissions=run_web_update_command_permissions,
                on_get_bot_profile=run_web_get_bot_profile,
                on_update_bot_profile=run_web_update_bot_profile,
                on_update_bot_avatar=run_web_update_bot_avatar,
                on_request_restart=run_web_request_restart,
                logger=logger,
            )
        except Exception:
            logger.exception("Web admin interface stopped unexpectedly")

    web_admin_thread = threading.Thread(target=runner, name="web_admin", daemon=True)
    web_admin_thread.start()


def search_forum_links(query: str):
    search_url = f"{FORUM_BASE_URL}/search.json"
    request_headers = {
        "Accept": "application/json,text/plain,*/*",
        "User-Agent": "GlinetDiscordBot/1.0 (+https://github.com/wickedyoda/Glinet_discord_bot)",
    }

    def extract_topic_links(payload: dict):
        links = []
        seen_topic_ids = set()

        topics = payload.get("topics", [])
        if not isinstance(topics, list):
            topics = []
        for topic in topics:
            topic_id = topic.get("id")
            if not topic_id or topic_id in seen_topic_ids:
                continue
            slug = topic.get("slug")
            if slug:
                links.append(f"{FORUM_BASE_URL}/t/{slug}/{topic_id}")
            else:
                links.append(f"{FORUM_BASE_URL}/t/{topic_id}")
            seen_topic_ids.add(topic_id)
            if len(links) >= FORUM_MAX_RESULTS:
                return links

        # Some responses may include posts but omit topic metadata.
        posts = payload.get("posts", [])
        if not isinstance(posts, list):
            posts = []
        for post in posts:
            topic_id = post.get("topic_id")
            if not topic_id or topic_id in seen_topic_ids:
                continue
            links.append(f"{FORUM_BASE_URL}/t/{topic_id}")
            seen_topic_ids.add(topic_id)
            if len(links) >= FORUM_MAX_RESULTS:
                break
        return links

    try:
        response = requests.get(
            search_url, params={"q": query}, timeout=10, headers=request_headers
        )
        response.raise_for_status()
        data = response.json()
    except requests.HTTPError as exc:
        status_code = getattr(exc.response, "status_code", None)
        if status_code == 429:
            logger.warning("Forum search rate limited for query: %s", query)
            return [
                "❌ Forum search is rate-limited right now. Please try again in a minute."
            ]
        logger.exception("Forum search HTTP failure for query: %s", query)
        return ["❌ Failed to fetch forum results."]
    except requests.RequestException:
        logger.exception("Forum search request failed for query: %s", query)
        return ["❌ Failed to fetch forum results."]
    except ValueError:
        logger.exception("Forum search returned invalid JSON for query: %s", query)
        return ["❌ Forum returned an invalid response."]

    links = extract_topic_links(data)

    return links if links else ["No results found."]


def search_reddit_posts(query: str):
    request_headers = {
        "Accept": "application/json,text/plain,*/*",
        "User-Agent": "GlinetDiscordBot/1.0 (+https://github.com/wickedyoda/Glinet_discord_bot)",
    }
    try:
        search_endpoint = f"{REDDIT_BASE_URL}/r/{REDDIT_SUBREDDIT}/search.json"
        response = requests.get(
            search_endpoint,
            params={
                "q": query,
                "sort": "relevance",
                "limit": REDDIT_MAX_RESULTS,
                "t": "all",
                "raw_json": 1,
                "restrict_sr": 1,
            },
            timeout=10,
            headers=request_headers,
        )
        response.raise_for_status()
        data = response.json()
    except requests.HTTPError as exc:
        status_code = getattr(exc.response, "status_code", None)
        if status_code == 429:
            logger.warning("Reddit search rate limited for query: %s", query)
            return [], "❌ Reddit search is rate-limited right now. Please try again soon."
        logger.exception("Reddit search HTTP failure for query: %s", query)
        return [], "❌ Failed to fetch Reddit results."
    except requests.RequestException:
        logger.exception("Reddit search request failed for query: %s", query)
        return [], "❌ Failed to fetch Reddit results."
    except ValueError:
        logger.exception("Reddit search returned invalid JSON for query: %s", query)
        return [], "❌ Reddit returned an invalid response."

    children = ((data or {}).get("data") or {}).get("children", [])
    if not isinstance(children, list):
        children = []

    posts = []
    seen_links = set()
    for item in children:
        if not isinstance(item, dict):
            continue
        payload = item.get("data", {})
        if not isinstance(payload, dict):
            continue
        permalink = str(payload.get("permalink") or "").strip()
        if not permalink:
            continue
        link = urljoin(REDDIT_BASE_URL, permalink)
        if link in seen_links:
            continue
        title = clean_search_text(str(payload.get("title") or "")).strip()
        posts.append((make_discord_safe_text(title or "Untitled post"), link))
        seen_links.add(link)
        if len(posts) >= REDDIT_MAX_RESULTS:
            break

    return posts, ""


def normalize_search_terms(query: str):
    raw_terms = [term.lower() for term in re.findall(r"[a-zA-Z0-9]+", query)]
    expanded_terms = []
    for term in raw_terms:
        if not term:
            continue
        expanded_terms.append(term)
        # Handle compact alpha+numeric queries like "flint3" by also indexing
        # split components ("flint", "3") for better document matches.
        if re.search(r"[a-z]", term) and re.search(r"\d", term):
            for piece in re.findall(r"[a-z]+|\d+", term):
                if not piece:
                    continue
                # Avoid overly-broad fragments like "mt" from "mt6000".
                if piece.isalpha() and len(piece) < 3:
                    continue
                expanded_terms.append(piece)

    normalized = []
    for term in expanded_terms:
        # Ignore single-digit tokens because they cause broad false positives
        # (for example "step 3" pages) in docs search scoring.
        if len(term) == 1 and term.isdigit():
            continue
        normalized.append(term)

    return list(dict.fromkeys(normalized))


def clean_search_text(value: str):
    no_html = re.sub(r"<[^>]+>", " ", value or "")
    return re.sub(r"\s+", " ", unescape(no_html)).strip()


def make_discord_safe_text(value: str):
    return str(value or "").encode("utf-8", errors="replace").decode("utf-8")


def load_docs_index(base_url: str):
    now = time.time()
    cached = docs_index_cache.get(base_url)
    if cached and now - cached["fetched_at"] < DOCS_INDEX_TTL_SECONDS:
        return cached["docs"]

    index_url = f"{base_url}/search/search_index.json"
    try:
        response = requests.get(index_url, timeout=15)
        response.raise_for_status()
        data = response.json()
        docs = data.get("docs", [])
        if not isinstance(docs, list):
            docs = []
        docs_index_cache[base_url] = {"fetched_at": now, "docs": docs}
        return docs
    except requests.RequestException:
        logger.exception("Docs index request failed for %s", base_url)
        return []
    except ValueError:
        logger.exception("Docs index returned invalid JSON for %s", base_url)
        return []


def score_document(title: str, text: str, terms, query: str):
    title_lc = title.lower()
    text_lc = text.lower()
    query_lc = clean_search_text(query).lower()
    title_tokens = re.findall(r"[a-z0-9]+", title_lc)

    score = 0
    if query_lc:
        if query_lc in title_lc:
            score += 24
        elif query_lc in text_lc:
            score += 10

    significant_terms = [term for term in terms if term]
    for idx, term in enumerate(significant_terms):
        direct_match = False
        if term in title_lc:
            score += 10
            direct_match = True
        if term in text_lc:
            score += min(5, text_lc.count(term))
            direct_match = True

        # Catch small typos like "flitn" => "flint" using title-token fuzziness.
        if not direct_match and len(term) >= 4 and title_tokens:
            for token in title_tokens:
                if abs(len(token) - len(term)) > 2:
                    continue
                if SequenceMatcher(None, token, term).ratio() >= 0.80:
                    score += 5
                    break

        if idx < len(significant_terms) - 1:
            phrase = f"{term} {significant_terms[idx + 1]}"
            if phrase in title_lc:
                score += 6
            elif phrase in text_lc:
                score += 2

    return score


def search_docs_site_links(query: str, base_url: str):
    terms = normalize_search_terms(query)
    if not terms:
        return []
    docs = load_docs_index(base_url)
    ranked = []
    for doc in docs:
        location = doc.get("location")
        if not location:
            continue
        title = clean_search_text(str(doc.get("title", "")))
        text = clean_search_text(str(doc.get("text", "")))
        score = score_document(title, text, terms, query)
        if score <= 0:
            continue
        resolved_url = urljoin(f"{base_url}/", location)
        ranked.append((score, title or location, resolved_url))
    ranked.sort(key=lambda item: item[0], reverse=True)
    return ranked[:DOCS_MAX_RESULTS_PER_SITE]


def search_docs_links(query: str):
    by_site = {}
    for site_name, base_url in DOCS_SOURCES:
        by_site[site_name] = search_docs_site_links(query, base_url)
    return by_site


def trim_search_message(message: str):
    safe_limit = min(
        DISCORD_MESSAGE_SAFE_MAX_CHARS,
        max(200, int(SEARCH_RESPONSE_MAX_CHARS)),
    )
    if len(message) <= safe_limit:
        return message
    trimmed = message[: safe_limit - 24].rsplit("\n", 1)[0]
    return f"{trimmed}\n...results truncated."


def build_search_message(query: str):
    forum_results = search_forum_links(query)
    docs_results = search_docs_links(query)

    lines = [f"🔎 Results for: `{query}`", "", "**Forum**"]
    forum_links = [item for item in forum_results if item.startswith("http")]
    if forum_links:
        lines.extend([f"- {link}" for link in forum_links])
    else:
        lines.append(f"- {forum_results[0]}")

    lines.append("")
    lines.append("**Docs**")
    docs_found = 0
    for site_name, _ in DOCS_SOURCES:
        site_results = docs_results.get(site_name, [])
        if not site_results:
            continue
        docs_found += len(site_results)
        lines.append(f"{site_name}:")
        for _, title, link in site_results:
            lines.append(f"- {title} - {link}")

    if docs_found == 0:
        lines.append("No matching docs results found.")

    return trim_search_message("\n".join(lines))


def build_forum_search_message(query: str):
    forum_results = search_forum_links(query)
    lines = [f"🔎 Forum results for: `{query}`", "", "**Forum**"]
    forum_links = [item for item in forum_results if item.startswith("http")]
    if forum_links:
        lines.extend([f"- {link}" for link in forum_links])
    else:
        lines.append(f"- {forum_results[0]}")
    return trim_search_message("\n".join(lines))


def build_reddit_search_message(query: str):
    safe_query = make_discord_safe_text(query)
    posts, error_message = search_reddit_posts(query)
    lines = [
        f"🔎 Reddit results for: `{safe_query}`",
        "",
        f"**Top {REDDIT_MAX_RESULTS} posts in r/{REDDIT_SUBREDDIT}**",
    ]
    if error_message:
        lines.append(f"- {error_message}")
        return trim_search_message("\n".join(lines))
    if posts:
        for index, (title, link) in enumerate(posts, start=1):
            lines.append(f"{index}. {title} - {link}")
    else:
        lines.append("- No Reddit results found.")
    return trim_search_message("\n".join(lines))


def build_docs_site_search_message(query: str, site_key: str):
    site_info = DOCS_SITE_MAP.get(site_key)
    if not site_info:
        return "❌ Invalid documentation site."

    site_name, base_url = site_info
    site_results = search_docs_site_links(query, base_url)
    lines = [f"🔎 {site_name} results for: `{query}`", "", f"**{site_name}**"]
    if site_results:
        for _, title, link in site_results:
            lines.append(f"- {title} - {link}")
    else:
        lines.append("- No matching docs results found.")
    return trim_search_message("\n".join(lines))


# Initialize SQLite storage before any runtime cache is loaded.
initialize_storage()
upgrade_legacy_default_tag_responses()

# Runtime caches for invite tracking
invite_roles = load_invite_roles()
invite_uses = {}


@bot.event
async def on_ready():
    global firmware_monitor_task
    install_asyncio_exception_logging(asyncio.get_running_loop())
    logger.info("Logged in as %s", bot.user.name)
    guild = bot.get_guild(GUILD_ID)
    if callable(globals().get("register_tag_commands")):
        register_tag_commands()
    else:
        logger.warning(
            "Tag slash commands not registered: register_tag_commands missing"
        )
    synced = await tree.sync(guild=guild)
    logger.info("Synced %d command(s) to guild %s", len(synced), GUILD_ID)
    get_tag_responses()

    # Cache invite uses for tracking
    try:
        invites = await guild.invites()
        for inv in invites:
            invite_uses[inv.code] = inv.uses
    except Exception:
        logger.exception("Failed to cache invites on startup")

    if firmware_monitor_task is None or firmware_monitor_task.done():
        firmware_monitor_task = asyncio.create_task(
            firmware_monitor_loop(), name="firmware_monitor"
        )


@bot.event
async def on_error(event_method: str, *args, **kwargs):
    logger.exception("Unhandled exception in event '%s'", event_method)


@tree.error
async def on_tree_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    command_name = interaction.command.name if interaction.command else "unknown"
    logger.error(
        "Unhandled app command error in /%s invoked by %s",
        command_name,
        interaction.user,
        exc_info=(type(error), error, error.__traceback__),
    )
    if isinstance(error, app_commands.CommandNotFound):
        await send_safe_interaction_message(
            interaction,
            "❌ This command is still syncing. Please wait 30-60 seconds and try again.",
            ephemeral=True,
        )
        return
    await send_safe_interaction_message(
        interaction,
        "❌ An unexpected error occurred while processing that command.",
        ephemeral=True,
    )


@bot.event
async def on_member_join(member: discord.Member):
    """Assign role based on the invite used to join."""
    guild = member.guild
    if guild.id != GUILD_ID:
        return

    used_invite = None
    try:
        invites = await guild.invites()
        for inv in invites:
            if inv.code in invite_roles and inv.uses > invite_uses.get(inv.code, 0):
                invite_uses[inv.code] = inv.uses
                used_invite = inv
                break
    except Exception:
        logger.exception("Failed to fetch invites on member join")

    if used_invite:
        role_id = invite_roles.get(used_invite.code)
        role = guild.get_role(role_id)
        if role:
            try:
                await member.add_roles(role)
                logger.info(
                    "Assigned role %s to %s via invite %s",
                    role.id,
                    member,
                    used_invite.code,
                )
            except Exception:
                logger.exception("Failed to assign role on join for %s", member)

    join_details = (
        f"**Member:** {member.mention} (`{member.id}`)\n"
        f"**Created:** <t:{int(member.created_at.timestamp())}:f>\n"
    )
    if used_invite:
        join_details += f"**Invite:** `{used_invite.code}`\n"
    await send_server_event_log(guild, "member_join", join_details)


@bot.event
async def on_member_remove(member: discord.Member):
    guild = member.guild
    if guild.id != GUILD_ID:
        return

    details = (
        f"**Member:** {member} (`{member.id}`)\n"
        f"**Nickname:** {clip_text(member.nick or 'N/A')}\n"
    )
    await send_server_event_log(guild, "member_leave", details)


@bot.event
async def on_message_delete(message: discord.Message):
    guild = message.guild
    if guild is None or guild.id != GUILD_ID:
        return

    channel_name = (
        message.channel.mention
        if hasattr(message.channel, "mention")
        else f"`{message.channel.id}`"
    )
    details = (
        f"**Author:** {message.author} (`{message.author.id}`)\n"
        f"**Channel:** {channel_name}\n"
        f"**Message ID:** `{message.id}`\n"
        f"**Content:** {clip_text(message.content)}\n"
        f"**Attachments:** `{len(message.attachments)}`\n"
    )
    await send_server_event_log(guild, "message_delete", details)


@bot.event
async def on_bulk_message_delete(messages: list[discord.Message]):
    if not messages:
        return
    guild = messages[0].guild
    if guild is None or guild.id != GUILD_ID:
        return

    channel = messages[0].channel
    channel_name = channel.mention if hasattr(channel, "mention") else f"`{channel.id}`"
    details = f"**Channel:** {channel_name}\n**Messages Deleted:** `{len(messages)}`\n"
    await send_server_event_log(guild, "bulk_message_delete", details)


@bot.event
async def on_user_update(before: discord.User, after: discord.User):
    guild = bot.get_guild(GUILD_ID)
    if guild is None:
        return
    member = guild.get_member(after.id)
    if member is None:
        return

    if before.name != after.name or before.global_name != after.global_name:
        details = (
            f"**User:** {member.mention} (`{after.id}`)\n"
            f"**Username:** {clip_text(before.name)} -> {clip_text(after.name)}\n"
            f"**Global Name:** {clip_text(before.global_name or 'N/A')} -> {clip_text(after.global_name or 'N/A')}\n"
        )
        await send_server_event_log(guild, "user_name_change", details)

    if before.display_avatar != after.display_avatar:
        details = (
            f"**User:** {member.mention} (`{after.id}`)\n"
            f"**New Avatar:** {after.display_avatar.url}\n"
        )
        await send_server_event_log(guild, "user_avatar_change", details)


@bot.event
async def on_member_update(before: discord.Member, after: discord.Member):
    guild = after.guild
    if guild.id != GUILD_ID:
        return

    if before.nick != after.nick:
        details = (
            f"**Member:** {after.mention} (`{after.id}`)\n"
            f"**Nickname:** {clip_text(before.nick or 'N/A')} -> {clip_text(after.nick or 'N/A')}\n"
        )
        await send_server_event_log(guild, "member_nickname_change", details)

    before_role_map = {role.id: role for role in before.roles}
    after_role_map = {role.id: role for role in after.roles}
    added_role_ids = sorted(set(after_role_map) - set(before_role_map))
    removed_role_ids = sorted(set(before_role_map) - set(after_role_map))

    for role_id in added_role_ids:
        role = after_role_map[role_id]
        details = (
            f"**Member:** {after.mention} (`{after.id}`)\n"
            f"**Role Added:** {role.mention} (`{role.id}`)\n"
        )
        await send_server_event_log(guild, "member_role_added", details)

    for role_id in removed_role_ids:
        role = before_role_map[role_id]
        details = (
            f"**Member:** {after.mention} (`{after.id}`)\n"
            f"**Role Removed:** {role.name} (`{role.id}`)\n"
        )
        await send_server_event_log(guild, "member_role_removed", details)


@bot.event
async def on_invite_create(invite: discord.Invite):
    guild = invite.guild
    if guild is None or guild.id != GUILD_ID:
        return

    inviter_text = (
        f"{invite.inviter} (`{invite.inviter.id}`)" if invite.inviter else "Unknown"
    )
    channel_text = invite.channel.mention if getattr(invite, "channel", None) else "N/A"
    details = (
        f"**Invite Code:** `{invite.code}`\n"
        f"**Inviter:** {inviter_text}\n"
        f"**Channel:** {channel_text}\n"
        f"**Max Uses:** `{invite.max_uses}`\n"
        f"**Max Age:** `{invite.max_age}`\n"
    )
    await send_server_event_log(guild, "invite_created", details)


@bot.event
async def on_guild_channel_create(channel: discord.abc.GuildChannel):
    guild = channel.guild
    if guild.id != GUILD_ID:
        return

    if isinstance(channel, discord.CategoryChannel):
        event_name = "category_created"
    else:
        event_name = "channel_created"

    parent_name = channel.category.name if channel.category else "N/A"
    details = (
        f"**Name:** {clip_text(channel.name)}\n"
        f"**ID:** `{channel.id}`\n"
        f"**Type:** `{channel.type}`\n"
        f"**Category:** {clip_text(parent_name)}\n"
    )
    await send_server_event_log(guild, event_name, details)


@bot.event
async def on_guild_channel_delete(channel: discord.abc.GuildChannel):
    guild = channel.guild
    if guild.id != GUILD_ID:
        return

    if isinstance(channel, discord.CategoryChannel):
        event_name = "category_deleted"
    else:
        event_name = "channel_deleted"

    parent_name = channel.category.name if channel.category else "N/A"
    details = (
        f"**Name:** {clip_text(channel.name)}\n"
        f"**ID:** `{channel.id}`\n"
        f"**Type:** `{channel.type}`\n"
        f"**Category:** {clip_text(parent_name)}\n"
    )
    await send_server_event_log(guild, event_name, details)


@bot.event
async def on_guild_role_create(role: discord.Role):
    guild = role.guild
    if guild.id != GUILD_ID:
        return

    details = (
        f"**Role:** {role.mention} (`{role.id}`)\n"
        f"**Color:** `{role.color}`\n"
        f"**Position:** `{role.position}`\n"
    )
    await send_server_event_log(guild, "role_created", details)


@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return
    if message.content:
        tag = normalize_tag(message.content.strip().split()[0])
        if tag == "!list":
            await bot.process_commands(message)
            return
        response = get_tag_responses().get(tag)
        if response:
            if can_use_command(message.author, "tag_commands"):
                await message.channel.send(response)
    await bot.process_commands(message)


@bot.command(name="list")
async def list_commands(ctx: commands.Context):
    if not await ensure_prefix_command_access(ctx, "list"):
        return
    await ctx.send(build_command_list())


@tree.command(
    name="submitrole",
    description="Submit a role for invite/code linking",
    guild=discord.Object(id=GUILD_ID),
)
async def submitrole(interaction: discord.Interaction):
    logger.info("/submitrole invoked by %s", interaction.user)
    if not await ensure_interaction_command_access(interaction, "submitrole"):
        return

    await interaction.response.send_message(
        "Please mention the role you want to assign.", ephemeral=True
    )

    def check(m):
        return (
            m.author.id == interaction.user.id
            and m.channel.id == interaction.channel.id
        )

    try:
        msg = await bot.wait_for("message", timeout=30.0, check=check)
        if not msg.role_mentions:
            await interaction.followup.send("❌ No role mentioned.", ephemeral=True)
            return

        role = msg.role_mentions[0]
        channel = bot.get_channel(GENERAL_CHANNEL_ID) or interaction.channel
        invite = await channel.create_invite(max_age=0, max_uses=0, unique=True)
        code = generate_code()
        save_role_code(code, role.id)
        save_invite_role(invite.code, role.id)
        invite_roles[invite.code] = role.id
        invite_uses[invite.code] = invite.uses

        logger.info(
            "Generated invite %s and code %s for role %s using channel %s",
            invite.url,
            code,
            role.id,
            channel.id,
        )

        await interaction.followup.send(
            f"✅ Invite link: {invite.url}\n🔢 6-digit code: `{code}`", ephemeral=True
        )
    except Exception:
        logger.exception("Error in /submitrole")
        await interaction.followup.send(
            "❌ Something went wrong. Try again.", ephemeral=True
        )


@tree.command(
    name="bulk_assign_role_csv",
    description="Assign a role to members listed in an uploaded CSV file",
    guild=discord.Object(id=GUILD_ID),
)
@app_commands.describe(
    role="Role to assign",
    csv_file="Upload a .csv containing Discord names (comma-separated or one-per-line)",
)
async def bulk_assign_role_csv(
    interaction: discord.Interaction, role: discord.Role, csv_file: discord.Attachment
):
    logger.info("/bulk_assign_role_csv invoked by %s", interaction.user)
    if not await ensure_interaction_command_access(interaction, "bulk_assign_role_csv"):
        return

    if interaction.guild is None:
        await interaction.response.send_message(
            "❌ This command can only be used in a server channel.", ephemeral=True
        )
        return

    bot_user_id = bot.user.id if bot.user else None
    bot_member = interaction.guild.me or (
        interaction.guild.get_member(bot_user_id) if bot_user_id else None
    )
    actor = interaction.user if isinstance(interaction.user, discord.Member) else None
    if bot_member is None:
        await interaction.response.send_message(
            "❌ Could not resolve bot member in this guild.", ephemeral=True
        )
        return
    if role == interaction.guild.default_role:
        await interaction.response.send_message(
            "❌ The @everyone role cannot be assigned this way.", ephemeral=True
        )
        return
    if role.managed:
        await interaction.response.send_message(
            "❌ That role is managed by an integration and cannot be assigned manually.",
            ephemeral=True,
        )
        return
    if bot_member.top_role <= role:
        await interaction.response.send_message(
            "❌ I can't assign that role because it's above my top role.",
            ephemeral=True,
        )
        return
    if actor and actor.id != interaction.guild.owner_id and actor.top_role <= role:
        await interaction.response.send_message(
            "❌ You can only bulk-assign roles below your top role.",
            ephemeral=True,
        )
        return

    if not csv_file.filename.lower().endswith(".csv"):
        await interaction.response.send_message(
            "❌ The uploaded file must be a `.csv` file.",
            ephemeral=True,
        )
        return

    await interaction.response.defer(ephemeral=True, thinking=True)

    try:
        payload = await csv_file.read()
    except Exception:
        logger.exception("Failed reading CSV attachment for /bulk_assign_role_csv")
        await interaction.followup.send(
            "❌ Could not read that file. Please try again.", ephemeral=True
        )
        return

    result, error = await process_bulk_role_assignment_payload(
        guild=interaction.guild,
        role=role,
        payload=payload,
        requested_by=str(interaction.user),
        reason_actor=f"Bulk CSV role assignment by {interaction.user} ({interaction.user.id})",
    )
    if error:
        await interaction.followup.send(error, ephemeral=True)
        return

    summary_lines = build_bulk_assignment_summary_lines(
        csv_file.filename, role.mention, result
    )
    report_text = build_bulk_assignment_report_text(
        role=role,
        requested_by=f"{interaction.user} ({interaction.user.id})",
        source_name=csv_file.filename,
        result=result,
    )
    report_filename = f"bulk_assign_report_{role.id}_{int(time.time())}.txt"

    await interaction.followup.send(
        "\n".join(summary_lines),
        ephemeral=True,
        file=discord.File(
            io.BytesIO(report_text.encode("utf-8")), filename=report_filename
        ),
    )


class CodeEntryModal(discord.ui.Modal):
    def __init__(self):
        super().__init__(title="Enter Role Code")
        self.code = discord.ui.TextInput(
            label="6-digit code", min_length=6, max_length=6
        )
        self.add_item(self.code)

    async def on_submit(self, interaction: discord.Interaction):
        role_id = get_role_id_by_code(self.code.value.strip())
        if not role_id:
            await interaction.response.send_message("❌ Invalid code.", ephemeral=True)
            return

        role = interaction.guild.get_role(role_id)
        if not role:
            await interaction.response.send_message(
                "❌ Role not found.", ephemeral=True
            )
            return

        await interaction.user.add_roles(role)
        await interaction.response.send_message(
            f"✅ You've been given the **{role.name}** role!", ephemeral=True
        )


@tree.command(
    name="enter_role",
    description="Enter a 6-digit code to receive a role",
    guild=discord.Object(id=GUILD_ID),
)
async def enter_role(interaction: discord.Interaction):
    """Prompt the user to enter their code via a modal."""
    logger.info("/enter_role invoked by %s", interaction.user)
    if not await ensure_interaction_command_access(interaction, "enter_role"):
        return
    await interaction.response.send_modal(CodeEntryModal())


@tree.command(
    name="getaccess",
    description="Assign yourself the protected role",
    guild=discord.Object(id=GUILD_ID),
)
async def getaccess(interaction: discord.Interaction):
    logger.info("/getaccess invoked by %s", interaction.user)
    if not await ensure_interaction_command_access(interaction, "getaccess"):
        return
    try:
        role_id_raw = db_kv_get("access_role_id")
        if role_id_raw is None and os.path.exists(ROLE_FILE):
            with open(ROLE_FILE, "r") as f:
                role_id_raw = f.read().strip()
        role_id = int(str(role_id_raw).strip())
        role = interaction.guild.get_role(role_id)
        await interaction.user.add_roles(role)
        logger.info("Assigned default role %s to user %s", role.id, interaction.user)
        await interaction.response.send_message(
            f"✅ You've been given the **{role.name}** role!", ephemeral=True
        )
    except Exception:
        logger.exception("Error in /getaccess")
        await interaction.response.send_message(
            "❌ Could not assign role. Contact an admin.", ephemeral=True
        )


@tree.command(
    name="country",
    description="Add your country code to your nickname",
    guild=discord.Object(id=GUILD_ID),
)
@app_commands.describe(code="2-letter country code (e.g. US, CA, DE)")
async def country_slash(interaction: discord.Interaction, code: str):
    logger.info("/country invoked by %s with code %s", interaction.user, code)
    if not await ensure_interaction_command_access(interaction, "country"):
        return
    normalized = normalize_country_code(code)
    if not normalized:
        await interaction.response.send_message(
            "❌ Please provide a valid 2-letter country code (A-Z).",
            ephemeral=True,
        )
        return

    try:
        success, message = await set_member_country(interaction.user, normalized)
        await interaction.response.send_message(message, ephemeral=True)
        logger.info("/country result for %s success=%s", interaction.user, success)
    except discord.Forbidden:
        logger.exception("Missing permission to edit nickname for %s", interaction.user)
        await interaction.response.send_message(
            "❌ I can't edit your nickname. Check role hierarchy and nickname permissions.",
            ephemeral=True,
        )
    except discord.HTTPException:
        logger.exception("Failed to update nickname for %s", interaction.user)
        await interaction.response.send_message(
            "❌ Could not update your nickname right now. Try again.",
            ephemeral=True,
        )


@bot.command(name="country")
async def country_prefix(ctx: commands.Context, code: str):
    logger.info("!country invoked by %s with code %s", ctx.author, code)
    if not await ensure_prefix_command_access(ctx, "country"):
        return
    normalized = normalize_country_code(code)
    if not normalized:
        await ctx.send("❌ Please provide a valid 2-letter country code (A-Z).")
        return

    try:
        _, message = await set_member_country(ctx.author, normalized)
        await ctx.send(message)
    except discord.Forbidden:
        logger.exception("Missing permission to edit nickname for %s", ctx.author)
        await ctx.send(
            "❌ I can't edit your nickname. Check role hierarchy and nickname permissions."
        )
    except discord.HTTPException:
        logger.exception("Failed to update nickname for %s", ctx.author)
        await ctx.send("❌ Could not update your nickname right now. Try again.")


@tree.command(
    name="clear_country",
    description="Remove country code suffix from your nickname",
    guild=discord.Object(id=GUILD_ID),
)
async def clear_country_slash(interaction: discord.Interaction):
    logger.info("/clear_country invoked by %s", interaction.user)
    if not await ensure_interaction_command_access(interaction, "clear_country"):
        return
    try:
        _, message = await clear_member_country(interaction.user)
        await interaction.response.send_message(message, ephemeral=True)
    except discord.Forbidden:
        logger.exception("Missing permission to edit nickname for %s", interaction.user)
        await interaction.response.send_message(
            "❌ I can't edit your nickname. Check role hierarchy and nickname permissions.",
            ephemeral=True,
        )
    except discord.HTTPException:
        logger.exception("Failed to clear nickname suffix for %s", interaction.user)
        await interaction.response.send_message(
            "❌ Could not update your nickname right now. Try again.",
            ephemeral=True,
        )


@bot.command(name="clearcountry")
async def clear_country_prefix(ctx: commands.Context):
    logger.info("!clearcountry invoked by %s", ctx.author)
    if not await ensure_prefix_command_access(ctx, "clear_country"):
        return
    try:
        _, message = await clear_member_country(ctx.author)
        await ctx.send(message)
    except discord.Forbidden:
        logger.exception("Missing permission to edit nickname for %s", ctx.author)
        await ctx.send(
            "❌ I can't edit your nickname. Check role hierarchy and nickname permissions."
        )
    except discord.HTTPException:
        logger.exception("Failed to clear nickname suffix for %s", ctx.author)
        await ctx.send("❌ Could not update your nickname right now. Try again.")


@tree.command(
    name="create_role",
    description="Create a new role",
    guild=discord.Object(id=GUILD_ID),
)
@app_commands.describe(
    name="Name for the new role",
    color="Optional role color like #1ABC9C",
    hoist="Display this role separately in the member list",
    mentionable="Allow members to mention this role",
)
async def create_role_slash(
    interaction: discord.Interaction,
    name: str,
    color: str | None = None,
    hoist: bool = False,
    mentionable: bool = False,
):
    logger.info("/create_role invoked by %s for role name %s", interaction.user, name)
    if not await ensure_interaction_command_access(interaction, "create_role"):
        return
    if interaction.guild is None:
        await interaction.response.send_message(
            "❌ This command can only be used in a server.", ephemeral=True
        )
        return

    normalized_name = name.strip()
    if not normalized_name:
        await interaction.response.send_message(
            "❌ Role name cannot be empty.", ephemeral=True
        )
        return
    if len(normalized_name) > ROLE_NAME_MAX_LENGTH:
        await interaction.response.send_message(
            f"❌ Role name must be {ROLE_NAME_MAX_LENGTH} characters or fewer.",
            ephemeral=True,
        )
        return

    parsed_color, color_error = parse_role_color(color)
    if color_error:
        await interaction.response.send_message(color_error, ephemeral=True)
        return

    bot_user_id = bot.user.id if bot.user else None
    bot_member = interaction.guild.me or (
        interaction.guild.get_member(bot_user_id) if bot_user_id else None
    )
    if bot_member is None:
        await interaction.response.send_message(
            "❌ Could not resolve bot member in this guild.", ephemeral=True
        )
        return
    if not bot_member.guild_permissions.manage_roles:
        await interaction.response.send_message(
            "❌ I need the `Manage Roles` permission to create roles.",
            ephemeral=True,
        )
        return

    create_kwargs = {
        "name": normalized_name,
        "hoist": hoist,
        "mentionable": mentionable,
    }
    if parsed_color is not None:
        create_kwargs["color"] = parsed_color

    action_reason = (
        f"Role created by {interaction.user} ({interaction.user.id}) via bot"
    )
    try:
        role = await interaction.guild.create_role(
            reason=action_reason, **create_kwargs
        )
    except discord.Forbidden:
        logger.exception("Missing permission to create role %s", normalized_name)
        await send_moderation_log(
            interaction.guild,
            interaction.user,
            "create_role",
            reason=action_reason,
            outcome="failed",
            details="Bot missing `Manage Roles` permission or role hierarchy block.",
        )
        await interaction.response.send_message(
            "❌ I can't create roles. Check `Manage Roles` permission and role hierarchy.",
            ephemeral=True,
        )
        return
    except discord.HTTPException:
        logger.exception("Failed to create role %s", normalized_name)
        await send_moderation_log(
            interaction.guild,
            interaction.user,
            "create_role",
            reason=action_reason,
            outcome="failed",
            details="Discord API error while creating role.",
        )
        await interaction.response.send_message(
            "❌ Failed to create role. Try again.", ephemeral=True
        )
        return

    await send_moderation_log(
        interaction.guild,
        interaction.user,
        "create_role",
        reason=action_reason,
        details=f"Created role {role.mention} (`{role.id}`).",
    )
    await interaction.response.send_message(
        f"✅ Created role {role.mention} (`{role.id}`).",
        ephemeral=True,
    )


@tree.command(
    name="delete_role",
    description="Delete a role",
    guild=discord.Object(id=GUILD_ID),
)
@app_commands.describe(role="Role to delete", reason="Reason for deletion")
async def delete_role_slash(
    interaction: discord.Interaction,
    role: discord.Role,
    reason: str | None = None,
):
    logger.info("/delete_role invoked by %s for role %s", interaction.user, role)
    if not await ensure_interaction_command_access(interaction, "delete_role"):
        return
    if interaction.guild is None:
        await interaction.response.send_message(
            "❌ This command can only be used in a server.", ephemeral=True
        )
        return

    bot_user_id = bot.user.id if bot.user else None
    bot_member = interaction.guild.me or (
        interaction.guild.get_member(bot_user_id) if bot_user_id else None
    )
    if bot_member is None:
        await interaction.response.send_message(
            "❌ Could not resolve bot member in this guild.", ephemeral=True
        )
        return
    if not bot_member.guild_permissions.manage_roles:
        await interaction.response.send_message(
            "❌ I need the `Manage Roles` permission to delete roles.",
            ephemeral=True,
        )
        return

    can_manage, error_message = validate_manageable_role(
        interaction.user, role, bot_member
    )
    action_reason = (
        reason or ""
    ).strip() or f"Role deleted by {interaction.user} via bot"
    if not can_manage:
        await send_moderation_log(
            interaction.guild,
            interaction.user,
            "delete_role",
            reason=action_reason,
            outcome="blocked",
            details=error_message,
        )
        await interaction.response.send_message(error_message, ephemeral=True)
        return

    role_name = role.name
    role_id = role.id
    try:
        await role.delete(reason=action_reason)
    except discord.Forbidden:
        logger.exception("Missing permission to delete role %s", role)
        await send_moderation_log(
            interaction.guild,
            interaction.user,
            "delete_role",
            reason=action_reason,
            outcome="failed",
            details="Bot missing `Manage Roles` permission or role hierarchy block.",
        )
        await interaction.response.send_message(
            "❌ I can't delete that role. Check `Manage Roles` permission and role hierarchy.",
            ephemeral=True,
        )
        return
    except discord.HTTPException:
        logger.exception("Failed to delete role %s", role)
        await send_moderation_log(
            interaction.guild,
            interaction.user,
            "delete_role",
            reason=action_reason,
            outcome="failed",
            details="Discord API error while deleting role.",
        )
        await interaction.response.send_message(
            "❌ Failed to delete that role. Try again.", ephemeral=True
        )
        return

    await send_moderation_log(
        interaction.guild,
        interaction.user,
        "delete_role",
        reason=action_reason,
        details=f"Deleted role `{role_name}` (`{role_id}`).",
    )
    await interaction.response.send_message(
        f"✅ Deleted role `{role_name}` (`{role_id}`).",
        ephemeral=True,
    )


@tree.command(
    name="edit_role",
    description="Edit role settings",
    guild=discord.Object(id=GUILD_ID),
)
@app_commands.describe(
    role="Role to edit",
    name="New role name",
    color="New color like #1ABC9C, or `none` to reset",
    hoist="Display this role separately in the member list",
    mentionable="Allow members to mention this role",
    reason="Reason for the edit",
)
async def edit_role_slash(
    interaction: discord.Interaction,
    role: discord.Role,
    name: str | None = None,
    color: str | None = None,
    hoist: bool | None = None,
    mentionable: bool | None = None,
    reason: str | None = None,
):
    logger.info("/edit_role invoked by %s for role %s", interaction.user, role)
    if not await ensure_interaction_command_access(interaction, "edit_role"):
        return
    if interaction.guild is None:
        await interaction.response.send_message(
            "❌ This command can only be used in a server.", ephemeral=True
        )
        return

    bot_user_id = bot.user.id if bot.user else None
    bot_member = interaction.guild.me or (
        interaction.guild.get_member(bot_user_id) if bot_user_id else None
    )
    if bot_member is None:
        await interaction.response.send_message(
            "❌ Could not resolve bot member in this guild.", ephemeral=True
        )
        return
    if not bot_member.guild_permissions.manage_roles:
        await interaction.response.send_message(
            "❌ I need the `Manage Roles` permission to edit roles.",
            ephemeral=True,
        )
        return

    can_manage, error_message = validate_manageable_role(
        interaction.user, role, bot_member
    )
    action_reason = (
        reason or ""
    ).strip() or f"Role edited by {interaction.user} via bot"
    if not can_manage:
        await send_moderation_log(
            interaction.guild,
            interaction.user,
            "edit_role",
            reason=action_reason,
            outcome="blocked",
            details=error_message,
        )
        await interaction.response.send_message(error_message, ephemeral=True)
        return

    edit_kwargs = {}
    changed_fields = []
    if name is not None:
        normalized_name = name.strip()
        if not normalized_name:
            await interaction.response.send_message(
                "❌ Role name cannot be empty.", ephemeral=True
            )
            return
        if len(normalized_name) > ROLE_NAME_MAX_LENGTH:
            await interaction.response.send_message(
                f"❌ Role name must be {ROLE_NAME_MAX_LENGTH} characters or fewer.",
                ephemeral=True,
            )
            return
        edit_kwargs["name"] = normalized_name
        changed_fields.append(f"name=`{normalized_name}`")

    if color is not None:
        parsed_color, color_error = parse_role_color(color)
        if color_error:
            await interaction.response.send_message(color_error, ephemeral=True)
            return
        edit_kwargs["color"] = parsed_color
        if parsed_color.value == 0:
            changed_fields.append("color=`default`")
        else:
            changed_fields.append(f"color=`#{parsed_color.value:06X}`")

    if hoist is not None:
        edit_kwargs["hoist"] = hoist
        changed_fields.append(f"hoist=`{hoist}`")

    if mentionable is not None:
        edit_kwargs["mentionable"] = mentionable
        changed_fields.append(f"mentionable=`{mentionable}`")

    if not edit_kwargs:
        await interaction.response.send_message(
            "❌ Provide at least one field to edit (`name`, `color`, `hoist`, `mentionable`).",
            ephemeral=True,
        )
        return

    try:
        await role.edit(reason=action_reason, **edit_kwargs)
    except discord.Forbidden:
        logger.exception("Missing permission to edit role %s", role)
        await send_moderation_log(
            interaction.guild,
            interaction.user,
            "edit_role",
            reason=action_reason,
            outcome="failed",
            details="Bot missing `Manage Roles` permission or role hierarchy block.",
        )
        await interaction.response.send_message(
            "❌ I can't edit that role. Check `Manage Roles` permission and role hierarchy.",
            ephemeral=True,
        )
        return
    except discord.HTTPException:
        logger.exception("Failed to edit role %s", role)
        await send_moderation_log(
            interaction.guild,
            interaction.user,
            "edit_role",
            reason=action_reason,
            outcome="failed",
            details="Discord API error while editing role.",
        )
        await interaction.response.send_message(
            "❌ Failed to edit that role. Try again.", ephemeral=True
        )
        return

    details = f"Edited role {role.mention} (`{role.id}`): {', '.join(changed_fields)}."
    await send_moderation_log(
        interaction.guild,
        interaction.user,
        "edit_role",
        reason=action_reason,
        details=details,
    )
    await interaction.response.send_message(f"✅ {details}", ephemeral=True)


@tree.command(
    name="modlog_test",
    description="Send a test moderation log entry",
    guild=discord.Object(id=GUILD_ID),
)
async def modlog_test_slash(interaction: discord.Interaction):
    logger.info("/modlog_test invoked by %s", interaction.user)
    if not await ensure_interaction_command_access(interaction, "modlog_test"):
        return

    sent = await send_moderation_log(
        interaction.guild,
        interaction.user,
        action="modlog_test",
        target=interaction.user,
        reason="Manual moderation log test",
        outcome="success",
        details="Triggered via /modlog_test",
    )
    if sent:
        await interaction.response.send_message(
            f"✅ Test moderation log sent to <#{MOD_LOG_CHANNEL_ID}>.",
            ephemeral=True,
        )
    else:
        await interaction.response.send_message(
            f"❌ Could not send test log to channel ID `{MOD_LOG_CHANNEL_ID}`. "
            "Check channel ID and bot permissions.",
            ephemeral=True,
        )


@bot.command(name="modlogtest")
async def modlog_test_prefix(ctx: commands.Context):
    logger.info("!modlogtest invoked by %s", ctx.author)
    if not await ensure_prefix_command_access(ctx, "modlog_test"):
        return

    sent = await send_moderation_log(
        ctx.guild,
        ctx.author,
        action="modlog_test",
        target=ctx.author,
        reason="Manual moderation log test",
        outcome="success",
        details="Triggered via !modlogtest",
    )
    if sent:
        await ctx.send(f"✅ Test moderation log sent to <#{MOD_LOG_CHANNEL_ID}>.")
    else:
        await ctx.send(
            f"❌ Could not send test log to channel ID `{MOD_LOG_CHANNEL_ID}`. "
            "Check channel ID and bot permissions."
        )


@tree.command(
    name="logs",
    description="View recent container error log entries",
    guild=discord.Object(id=GUILD_ID),
)
@app_commands.describe(lines="How many recent lines to return (10-400)")
async def logs_slash(
    interaction: discord.Interaction,
    lines: app_commands.Range[int, 10, 400] = 120,
):
    logger.info("/logs invoked by %s", interaction.user)
    if not await ensure_interaction_command_access(interaction, "logs"):
        return

    if not os.path.exists(CONTAINER_ERROR_LOG_FILE):
        await interaction.response.send_message(
            "ℹ️ No container error logs have been written yet.",
            ephemeral=True,
        )
        return

    try:
        log_tail = read_recent_log_lines(CONTAINER_ERROR_LOG_FILE, lines)
    except Exception:
        logger.exception("Failed reading container error logs for /logs")
        await interaction.response.send_message(
            "❌ Could not read container logs right now. Try again.",
            ephemeral=True,
        )
        return

    if not log_tail.strip():
        await interaction.response.send_message(
            "ℹ️ No container error logs have been written yet.",
            ephemeral=True,
        )
        return

    response_header = (
        f"Showing last `{int(lines)}` lines from `{os.path.basename(CONTAINER_ERROR_LOG_FILE)}`."
    )
    if len(log_tail) <= 1700:
        await interaction.response.send_message(
            f"{response_header}\n```log\n{log_tail}\n```",
            ephemeral=True,
        )
        return

    report_name = f"container_errors_last_{int(lines)}.log"
    await interaction.response.send_message(
        response_header,
        ephemeral=True,
        file=discord.File(io.BytesIO(log_tail.encode("utf-8")), filename=report_name),
    )


@tree.command(
    name="prune_messages",
    description="Remove recent messages in the current channel",
    guild=discord.Object(id=GUILD_ID),
)
@app_commands.describe(amount="How many recent messages to remove (1-500)")
async def prune_messages_slash(
    interaction: discord.Interaction,
    amount: app_commands.Range[int, 1, 500],
):
    logger.info(
        "/prune_messages invoked by %s amount=%s", interaction.user, int(amount)
    )
    if not await ensure_interaction_command_access(interaction, "prune_messages"):
        return
    if interaction.guild is None:
        await interaction.response.send_message(
            "❌ This command can only be used in a server.", ephemeral=True
        )
        return
    if not isinstance(interaction.user, discord.Member):
        await interaction.response.send_message(
            "❌ Could not resolve your guild membership for this command.",
            ephemeral=True,
        )
        return
    if not isinstance(interaction.channel, (discord.TextChannel, discord.Thread)):
        await interaction.response.send_message(
            "❌ This command can only be used in text channels or threads.",
            ephemeral=True,
        )
        return

    bot_user_id = bot.user.id if bot.user else None
    bot_member = interaction.guild.me or (
        interaction.guild.get_member(bot_user_id) if bot_user_id else None
    )
    if bot_member is None:
        await interaction.response.send_message(
            "❌ Could not resolve bot member in this guild.", ephemeral=True
        )
        return

    channel = interaction.channel
    perms = channel.permissions_for(bot_member)
    if not (perms.view_channel and perms.read_message_history and perms.manage_messages):
        await interaction.response.send_message(
            "❌ I need `View Channel`, `Read Message History`, and `Manage Messages` permissions here.",
            ephemeral=True,
        )
        return

    await interaction.response.defer(thinking=True, ephemeral=True)
    action_reason = f"Pruned {int(amount)} messages by {interaction.user} via bot"
    try:
        deleted_count = await prune_channel_recent_messages(
            channel,
            int(amount),
            reason=action_reason,
        )
    except discord.Forbidden:
        logger.exception(
            "Missing permission to prune messages in channel %s", channel.id
        )
        await send_moderation_log(
            interaction.guild,
            interaction.user,
            "prune_messages",
            reason=action_reason,
            outcome="failed",
            details=f"Bot missing required message-manage permissions in <#{channel.id}>.",
        )
        await interaction.followup.send(
            "❌ I can't prune messages in this channel due to permission limits.",
            ephemeral=True,
        )
        return
    except discord.HTTPException:
        logger.exception("Failed to prune messages in channel %s", channel.id)
        await send_moderation_log(
            interaction.guild,
            interaction.user,
            "prune_messages",
            reason=action_reason,
            outcome="failed",
            details=f"Discord API error while pruning in <#{channel.id}>.",
        )
        await interaction.followup.send(
            "❌ Failed to prune messages. Try again.", ephemeral=True
        )
        return

    await send_moderation_log(
        interaction.guild,
        interaction.user,
        "prune_messages",
        reason=action_reason,
        details=(
            f"Pruned {deleted_count} messages in {channel.mention} "
            f"(requested {int(amount)}; pinned messages skipped)."
        ),
    )
    await interaction.followup.send(
        (
            f"✅ Removed **{deleted_count}** messages from {channel.mention}. "
            f"(Requested {int(amount)}; pinned messages were skipped.)"
        ),
        ephemeral=True,
    )


@bot.command(name="prune")
async def prune_messages_prefix(ctx: commands.Context, amount: str):
    logger.info("!prune invoked by %s amount=%s", ctx.author, amount)
    if not await ensure_prefix_command_access(ctx, "prune_messages"):
        return
    if ctx.guild is None:
        await ctx.send("❌ This command can only be used in a server.")
        return
    if not isinstance(ctx.channel, (discord.TextChannel, discord.Thread)):
        await ctx.send("❌ This command can only be used in text channels or threads.")
        return

    raw_amount = str(amount or "").strip()
    if not raw_amount.isdigit():
        await ctx.send("❌ Amount must be a whole number between 1 and 500.")
        return
    requested_amount = int(raw_amount)
    if requested_amount < 1 or requested_amount > 500:
        await ctx.send("❌ Amount must be between 1 and 500.")
        return

    bot_user_id = bot.user.id if bot.user else None
    bot_member = ctx.guild.me or (
        ctx.guild.get_member(bot_user_id) if bot_user_id else None
    )
    if bot_member is None:
        await ctx.send("❌ Could not resolve bot member in this guild.")
        return

    channel = ctx.channel
    perms = channel.permissions_for(bot_member)
    if not (perms.view_channel and perms.read_message_history and perms.manage_messages):
        await ctx.send(
            "❌ I need `View Channel`, `Read Message History`, and `Manage Messages` permissions here."
        )
        return

    action_reason = f"Pruned {requested_amount} messages by {ctx.author} via bot"
    try:
        deleted_count = await prune_channel_recent_messages(
            channel,
            requested_amount,
            reason=action_reason,
            skip_message_id=ctx.message.id,
        )
    except discord.Forbidden:
        logger.exception(
            "Missing permission to prune messages in channel %s", channel.id
        )
        await send_moderation_log(
            ctx.guild,
            ctx.author,
            "prune_messages",
            reason=action_reason,
            outcome="failed",
            details=f"Bot missing required message-manage permissions in <#{channel.id}>.",
        )
        await ctx.send("❌ I can't prune messages in this channel due to permission limits.")
        return
    except discord.HTTPException:
        logger.exception("Failed to prune messages in channel %s", channel.id)
        await send_moderation_log(
            ctx.guild,
            ctx.author,
            "prune_messages",
            reason=action_reason,
            outcome="failed",
            details=f"Discord API error while pruning in <#{channel.id}>.",
        )
        await ctx.send("❌ Failed to prune messages. Try again.")
        return

    await send_moderation_log(
        ctx.guild,
        ctx.author,
        "prune_messages",
        reason=action_reason,
        details=(
            f"Pruned {deleted_count} messages in {channel.mention} "
            f"(requested {requested_amount}; pinned messages skipped)."
        ),
    )
    await ctx.send(
        f"✅ Removed **{deleted_count}** messages from {channel.mention}. "
        f"(Requested {requested_amount}; pinned messages were skipped.)"
    )


@tree.command(
    name="ban_member",
    description="Ban a member from the server",
    guild=discord.Object(id=GUILD_ID),
)
@app_commands.describe(member="Member to ban", reason="Reason for ban")
async def ban_member_slash(
    interaction: discord.Interaction, member: discord.Member, reason: str | None = None
):
    logger.info("/ban_member invoked by %s targeting %s", interaction.user, member)
    if not await ensure_interaction_command_access(interaction, "ban_member"):
        return

    can_moderate, error_message = validate_moderation_target(
        interaction.user, member, interaction.guild.me
    )
    if not can_moderate:
        await send_moderation_log(
            interaction.guild,
            interaction.user,
            "ban_member",
            member,
            reason,
            outcome="blocked",
            details=error_message,
        )
        await interaction.response.send_message(error_message, ephemeral=True)
        return

    action_reason = (reason or "").strip() or f"Banned by {interaction.user} via bot"
    try:
        await member.ban(reason=action_reason, delete_message_seconds=0)
    except discord.Forbidden:
        logger.exception("Missing permission to ban member %s", member)
        await send_moderation_log(
            interaction.guild,
            interaction.user,
            "ban_member",
            member,
            action_reason,
            outcome="failed",
            details="Bot missing `Ban Members` permission or role hierarchy block.",
        )
        await interaction.response.send_message(
            "❌ I can't ban that member. Check role hierarchy and `Ban Members` permission.",
            ephemeral=True,
        )
        return
    except discord.HTTPException:
        logger.exception("Failed to ban member %s", member)
        await send_moderation_log(
            interaction.guild,
            interaction.user,
            "ban_member",
            member,
            action_reason,
            outcome="failed",
            details="Discord API error while banning member.",
        )
        await interaction.response.send_message(
            "❌ Failed to ban the member. Try again.", ephemeral=True
        )
        return

    await send_moderation_log(
        interaction.guild,
        interaction.user,
        "ban_member",
        target=member,
        reason=action_reason,
        details="Banned successfully.",
    )
    await interaction.response.send_message(f"✅ Banned **{member}**.", ephemeral=True)


@bot.command(name="banmember")
async def ban_member_prefix(
    ctx: commands.Context, member: discord.Member, *, reason: str = ""
):
    logger.info("!banmember invoked by %s targeting %s", ctx.author, member)
    if not await ensure_prefix_command_access(ctx, "ban_member"):
        return

    can_moderate, error_message = validate_moderation_target(
        ctx.author, member, ctx.guild.me
    )
    if not can_moderate:
        await send_moderation_log(
            ctx.guild,
            ctx.author,
            "ban_member",
            member,
            reason.strip() or None,
            outcome="blocked",
            details=error_message,
        )
        await ctx.send(error_message)
        return

    action_reason = reason.strip() or f"Banned by {ctx.author} via bot"
    try:
        await member.ban(reason=action_reason, delete_message_seconds=0)
    except discord.Forbidden:
        logger.exception("Missing permission to ban member %s", member)
        await send_moderation_log(
            ctx.guild,
            ctx.author,
            "ban_member",
            member,
            action_reason,
            outcome="failed",
            details="Bot missing `Ban Members` permission or role hierarchy block.",
        )
        await ctx.send(
            "❌ I can't ban that member. Check role hierarchy and `Ban Members` permission."
        )
        return
    except discord.HTTPException:
        logger.exception("Failed to ban member %s", member)
        await send_moderation_log(
            ctx.guild,
            ctx.author,
            "ban_member",
            member,
            action_reason,
            outcome="failed",
            details="Discord API error while banning member.",
        )
        await ctx.send("❌ Failed to ban the member. Try again.")
        return

    await send_moderation_log(
        ctx.guild,
        ctx.author,
        "ban_member",
        target=member,
        reason=action_reason,
        details="Banned successfully.",
    )
    await ctx.send(f"✅ Banned **{member}**.")


@tree.command(
    name="kick_member",
    description="Kick a member and prune their last 72 hours of messages",
    guild=discord.Object(id=GUILD_ID),
)
@app_commands.describe(member="Member to kick", reason="Reason for kicking")
async def kick_member_slash(
    interaction: discord.Interaction, member: discord.Member, reason: str | None = None
):
    logger.info("/kick_member invoked by %s targeting %s", interaction.user, member)
    if not await ensure_interaction_command_access(interaction, "kick_member"):
        return

    can_moderate, error_message = validate_moderation_target(
        interaction.user, member, interaction.guild.me
    )
    if not can_moderate:
        await send_moderation_log(
            interaction.guild,
            interaction.user,
            "kick_member",
            member,
            reason,
            outcome="blocked",
            details=error_message,
        )
        await interaction.response.send_message(error_message, ephemeral=True)
        return

    await interaction.response.defer(thinking=True, ephemeral=True)
    action_reason = (reason or "").strip() or f"Kicked by {interaction.user} via bot"
    target_id = member.id
    target_name = str(member)

    try:
        await member.kick(reason=action_reason)
    except discord.Forbidden:
        logger.exception("Missing permission to kick member %s", member)
        await send_moderation_log(
            interaction.guild,
            interaction.user,
            "kick_member",
            member,
            action_reason,
            outcome="failed",
            details="Bot missing `Kick Members` permission or role hierarchy block.",
        )
        await interaction.followup.send(
            "❌ I can't kick that member. Check role hierarchy and `Kick Members` permission.",
            ephemeral=True,
        )
        return
    except discord.HTTPException:
        logger.exception("Failed to kick member %s", member)
        await send_moderation_log(
            interaction.guild,
            interaction.user,
            "kick_member",
            member,
            action_reason,
            outcome="failed",
            details="Discord API error while kicking member.",
        )
        await interaction.followup.send(
            "❌ Failed to kick the member. Try again.", ephemeral=True
        )
        return

    deleted_count, scanned_channels = await prune_user_messages(
        interaction.guild, target_id, KICK_PRUNE_HOURS
    )
    await send_moderation_log(
        interaction.guild,
        interaction.user,
        "kick_member",
        target=member,
        reason=action_reason,
        details=(
            f"Kicked successfully; pruned {deleted_count} messages "
            f"from last {KICK_PRUNE_HOURS}h across {scanned_channels} channels."
        ),
    )
    await interaction.followup.send(
        f"✅ Kicked **{target_name}** and pruned **{deleted_count}** messages "
        f"from the last **{KICK_PRUNE_HOURS}** hours across **{scanned_channels}** channels.",
        ephemeral=True,
    )


@bot.command(name="kickmember")
async def kick_member_prefix(
    ctx: commands.Context, member: discord.Member, *, reason: str = ""
):
    logger.info("!kickmember invoked by %s targeting %s", ctx.author, member)
    if not await ensure_prefix_command_access(ctx, "kick_member"):
        return

    can_moderate, error_message = validate_moderation_target(
        ctx.author, member, ctx.guild.me
    )
    if not can_moderate:
        await send_moderation_log(
            ctx.guild,
            ctx.author,
            "kick_member",
            member,
            reason.strip() or None,
            outcome="blocked",
            details=error_message,
        )
        await ctx.send(error_message)
        return

    action_reason = reason.strip() or f"Kicked by {ctx.author} via bot"
    target_id = member.id
    target_name = str(member)
    try:
        await member.kick(reason=action_reason)
    except discord.Forbidden:
        logger.exception("Missing permission to kick member %s", member)
        await send_moderation_log(
            ctx.guild,
            ctx.author,
            "kick_member",
            member,
            action_reason,
            outcome="failed",
            details="Bot missing `Kick Members` permission or role hierarchy block.",
        )
        await ctx.send(
            "❌ I can't kick that member. Check role hierarchy and `Kick Members` permission."
        )
        return
    except discord.HTTPException:
        logger.exception("Failed to kick member %s", member)
        await send_moderation_log(
            ctx.guild,
            ctx.author,
            "kick_member",
            member,
            action_reason,
            outcome="failed",
            details="Discord API error while kicking member.",
        )
        await ctx.send("❌ Failed to kick the member. Try again.")
        return

    deleted_count, scanned_channels = await prune_user_messages(
        ctx.guild, target_id, KICK_PRUNE_HOURS
    )
    await send_moderation_log(
        ctx.guild,
        ctx.author,
        "kick_member",
        target=member,
        reason=action_reason,
        details=(
            f"Kicked successfully; pruned {deleted_count} messages "
            f"from last {KICK_PRUNE_HOURS}h across {scanned_channels} channels."
        ),
    )
    await ctx.send(
        f"✅ Kicked **{target_name}** and pruned **{deleted_count}** messages "
        f"from the last **{KICK_PRUNE_HOURS}** hours across **{scanned_channels}** channels."
    )


@tree.command(
    name="timeout_member",
    description="Timeout a member for a duration (e.g. 30m, 2h, 1d)",
    guild=discord.Object(id=GUILD_ID),
)
@app_commands.describe(
    member="Member to timeout",
    duration="Duration like 30m, 2h, or 1d",
    reason="Reason for timeout",
)
async def timeout_member_slash(
    interaction: discord.Interaction,
    member: discord.Member,
    duration: str,
    reason: str | None = None,
):
    logger.info(
        "/timeout_member invoked by %s targeting %s for %s",
        interaction.user,
        member,
        duration,
    )
    if not await ensure_interaction_command_access(interaction, "timeout_member"):
        return

    can_moderate, error_message = validate_moderation_target(
        interaction.user, member, interaction.guild.me
    )
    if not can_moderate:
        await send_moderation_log(
            interaction.guild,
            interaction.user,
            "timeout_member",
            member,
            reason,
            outcome="blocked",
            details=error_message,
        )
        await interaction.response.send_message(error_message, ephemeral=True)
        return

    timeout_delta, duration_text, parse_error = parse_timeout_duration(duration)
    if parse_error:
        await send_moderation_log(
            interaction.guild,
            interaction.user,
            "timeout_member",
            member,
            reason,
            outcome="blocked",
            details=parse_error,
        )
        await interaction.response.send_message(parse_error, ephemeral=True)
        return

    until = discord.utils.utcnow() + timeout_delta
    action_reason = (reason or "").strip() or f"Timed out by {interaction.user} via bot"
    try:
        await member.timeout(until, reason=action_reason)
    except discord.Forbidden:
        logger.exception("Missing permission to timeout member %s", member)
        await send_moderation_log(
            interaction.guild,
            interaction.user,
            "timeout_member",
            member,
            action_reason,
            outcome="failed",
            details="Bot missing `Moderate Members` permission or role hierarchy block.",
        )
        await interaction.response.send_message(
            "❌ I can't timeout that member. Check role hierarchy and `Moderate Members` permission.",
            ephemeral=True,
        )
        return
    except discord.HTTPException:
        logger.exception("Failed to timeout member %s", member)
        await send_moderation_log(
            interaction.guild,
            interaction.user,
            "timeout_member",
            member,
            action_reason,
            outcome="failed",
            details="Discord API error while applying timeout.",
        )
        await interaction.response.send_message(
            "❌ Failed to timeout the member. Try again.", ephemeral=True
        )
        return

    timestamp = int(until.timestamp())
    await send_moderation_log(
        interaction.guild,
        interaction.user,
        "timeout_member",
        target=member,
        reason=action_reason,
        details=f"Timed out for {duration_text} until <t:{timestamp}:f>.",
    )
    await interaction.response.send_message(
        f"✅ Timed out **{member}** for **{duration_text}** (until <t:{timestamp}:f>).",
        ephemeral=True,
    )


@bot.command(name="timeoutmember")
async def timeout_member_prefix(
    ctx: commands.Context, member: discord.Member, duration: str, *, reason: str = ""
):
    logger.info(
        "!timeoutmember invoked by %s targeting %s for %s", ctx.author, member, duration
    )
    if not await ensure_prefix_command_access(ctx, "timeout_member"):
        return

    can_moderate, error_message = validate_moderation_target(
        ctx.author, member, ctx.guild.me
    )
    if not can_moderate:
        await send_moderation_log(
            ctx.guild,
            ctx.author,
            "timeout_member",
            member,
            reason.strip() or None,
            outcome="blocked",
            details=error_message,
        )
        await ctx.send(error_message)
        return

    timeout_delta, duration_text, parse_error = parse_timeout_duration(duration)
    if parse_error:
        await send_moderation_log(
            ctx.guild,
            ctx.author,
            "timeout_member",
            member,
            reason.strip() or None,
            outcome="blocked",
            details=parse_error,
        )
        await ctx.send(parse_error)
        return

    until = discord.utils.utcnow() + timeout_delta
    action_reason = reason.strip() or f"Timed out by {ctx.author} via bot"
    try:
        await member.timeout(until, reason=action_reason)
    except discord.Forbidden:
        logger.exception("Missing permission to timeout member %s", member)
        await send_moderation_log(
            ctx.guild,
            ctx.author,
            "timeout_member",
            member,
            action_reason,
            outcome="failed",
            details="Bot missing `Moderate Members` permission or role hierarchy block.",
        )
        await ctx.send(
            "❌ I can't timeout that member. Check role hierarchy and `Moderate Members` permission."
        )
        return
    except discord.HTTPException:
        logger.exception("Failed to timeout member %s", member)
        await send_moderation_log(
            ctx.guild,
            ctx.author,
            "timeout_member",
            member,
            action_reason,
            outcome="failed",
            details="Discord API error while applying timeout.",
        )
        await ctx.send("❌ Failed to timeout the member. Try again.")
        return

    timestamp = int(until.timestamp())
    await send_moderation_log(
        ctx.guild,
        ctx.author,
        "timeout_member",
        target=member,
        reason=action_reason,
        details=f"Timed out for {duration_text} until <t:{timestamp}:f>.",
    )
    await ctx.send(
        f"✅ Timed out **{member}** for **{duration_text}** (until <t:{timestamp}:f>)."
    )


@tree.command(
    name="untimeout_member",
    description="Remove timeout from a member",
    guild=discord.Object(id=GUILD_ID),
)
@app_commands.describe(
    member="Member to remove timeout from", reason="Reason for removing timeout"
)
async def untimeout_member_slash(
    interaction: discord.Interaction, member: discord.Member, reason: str | None = None
):
    logger.info(
        "/untimeout_member invoked by %s targeting %s", interaction.user, member
    )
    if not await ensure_interaction_command_access(interaction, "untimeout_member"):
        return

    can_moderate, error_message = validate_moderation_target(
        interaction.user, member, interaction.guild.me
    )
    if not can_moderate:
        await send_moderation_log(
            interaction.guild,
            interaction.user,
            "untimeout_member",
            member,
            reason,
            outcome="blocked",
            details=error_message,
        )
        await interaction.response.send_message(error_message, ephemeral=True)
        return

    timed_out_until = member.timed_out_until
    if timed_out_until is None or timed_out_until <= discord.utils.utcnow():
        await interaction.response.send_message(
            "ℹ️ That member is not currently timed out.", ephemeral=True
        )
        return

    action_reason = (
        reason or ""
    ).strip() or f"Timeout removed by {interaction.user} via bot"
    try:
        await member.timeout(None, reason=action_reason)
    except discord.Forbidden:
        logger.exception("Missing permission to remove timeout for member %s", member)
        await send_moderation_log(
            interaction.guild,
            interaction.user,
            "untimeout_member",
            member,
            action_reason,
            outcome="failed",
            details="Bot missing `Moderate Members` permission or role hierarchy block.",
        )
        await interaction.response.send_message(
            "❌ I can't remove timeout from that member. Check role hierarchy and `Moderate Members` permission.",
            ephemeral=True,
        )
        return
    except discord.HTTPException:
        logger.exception("Failed to remove timeout for member %s", member)
        await send_moderation_log(
            interaction.guild,
            interaction.user,
            "untimeout_member",
            member,
            action_reason,
            outcome="failed",
            details="Discord API error while removing timeout.",
        )
        await interaction.response.send_message(
            "❌ Failed to remove timeout. Try again.", ephemeral=True
        )
        return

    await send_moderation_log(
        interaction.guild,
        interaction.user,
        "untimeout_member",
        target=member,
        reason=action_reason,
        details="Timeout removed successfully.",
    )
    await interaction.response.send_message(
        f"✅ Removed timeout for **{member}**.", ephemeral=True
    )


@bot.command(name="untimeoutmember")
async def untimeout_member_prefix(
    ctx: commands.Context, member: discord.Member, *, reason: str = ""
):
    logger.info("!untimeoutmember invoked by %s targeting %s", ctx.author, member)
    if not await ensure_prefix_command_access(ctx, "untimeout_member"):
        return

    can_moderate, error_message = validate_moderation_target(
        ctx.author, member, ctx.guild.me
    )
    if not can_moderate:
        await send_moderation_log(
            ctx.guild,
            ctx.author,
            "untimeout_member",
            member,
            reason.strip() or None,
            outcome="blocked",
            details=error_message,
        )
        await ctx.send(error_message)
        return

    timed_out_until = member.timed_out_until
    if timed_out_until is None or timed_out_until <= discord.utils.utcnow():
        await ctx.send("ℹ️ That member is not currently timed out.")
        return

    action_reason = reason.strip() or f"Timeout removed by {ctx.author} via bot"
    try:
        await member.timeout(None, reason=action_reason)
    except discord.Forbidden:
        logger.exception("Missing permission to remove timeout for member %s", member)
        await send_moderation_log(
            ctx.guild,
            ctx.author,
            "untimeout_member",
            member,
            action_reason,
            outcome="failed",
            details="Bot missing `Moderate Members` permission or role hierarchy block.",
        )
        await ctx.send(
            "❌ I can't remove timeout from that member. Check role hierarchy and `Moderate Members` permission."
        )
        return
    except discord.HTTPException:
        logger.exception("Failed to remove timeout for member %s", member)
        await send_moderation_log(
            ctx.guild,
            ctx.author,
            "untimeout_member",
            member,
            action_reason,
            outcome="failed",
            details="Discord API error while removing timeout.",
        )
        await ctx.send("❌ Failed to remove timeout. Try again.")
        return

    await send_moderation_log(
        ctx.guild,
        ctx.author,
        "untimeout_member",
        target=member,
        reason=action_reason,
        details="Timeout removed successfully.",
    )
    await ctx.send(f"✅ Removed timeout for **{member}**.")


@tree.command(
    name="add_role_member",
    description="Assign a role to a member",
    guild=discord.Object(id=GUILD_ID),
)
@app_commands.describe(
    member="Member to update", role="Role to add", reason="Reason for role assignment"
)
async def add_role_member_slash(
    interaction: discord.Interaction,
    member: discord.Member,
    role: discord.Role,
    reason: str | None = None,
):
    logger.info(
        "/add_role_member invoked by %s target=%s role=%s",
        interaction.user,
        member,
        role,
    )
    if not await ensure_interaction_command_access(interaction, "add_role_member"):
        return
    if interaction.guild is None:
        await interaction.response.send_message(
            "❌ This command can only be used in a server.", ephemeral=True
        )
        return

    bot_user_id = bot.user.id if bot.user else None
    bot_member = interaction.guild.me or (
        interaction.guild.get_member(bot_user_id) if bot_user_id else None
    )
    if bot_member is None:
        await interaction.response.send_message(
            "❌ Could not resolve bot member in this guild.", ephemeral=True
        )
        return
    if not bot_member.guild_permissions.manage_roles:
        await interaction.response.send_message(
            "❌ I need the `Manage Roles` permission to manage member roles.",
            ephemeral=True,
        )
        return

    can_moderate, member_error = validate_moderation_target(
        interaction.user, member, bot_member
    )
    if not can_moderate:
        await send_moderation_log(
            interaction.guild,
            interaction.user,
            "add_role_member",
            target=member,
            reason=reason,
            outcome="blocked",
            details=member_error,
        )
        await interaction.response.send_message(member_error, ephemeral=True)
        return

    can_manage_role, role_error = validate_manageable_role(
        interaction.user, role, bot_member
    )
    if not can_manage_role:
        await send_moderation_log(
            interaction.guild,
            interaction.user,
            "add_role_member",
            target=member,
            reason=reason,
            outcome="blocked",
            details=role_error,
        )
        await interaction.response.send_message(role_error, ephemeral=True)
        return

    if role in member.roles:
        await interaction.response.send_message(
            f"ℹ️ {member.mention} already has {role.mention}.",
            ephemeral=True,
        )
        return

    action_reason = (
        reason or ""
    ).strip() or f"Role assigned by {interaction.user} via bot"
    try:
        await member.add_roles(role, reason=action_reason)
    except discord.Forbidden:
        logger.exception("Missing permission to add role %s to member %s", role, member)
        await send_moderation_log(
            interaction.guild,
            interaction.user,
            "add_role_member",
            target=member,
            reason=action_reason,
            outcome="failed",
            details="Bot missing `Manage Roles` permission or role hierarchy block.",
        )
        await interaction.response.send_message(
            "❌ I can't assign that role. Check `Manage Roles` permission and role hierarchy.",
            ephemeral=True,
        )
        return
    except discord.HTTPException:
        logger.exception("Failed to add role %s to member %s", role, member)
        await send_moderation_log(
            interaction.guild,
            interaction.user,
            "add_role_member",
            target=member,
            reason=action_reason,
            outcome="failed",
            details="Discord API error while assigning role.",
        )
        await interaction.response.send_message(
            "❌ Failed to assign role. Try again.", ephemeral=True
        )
        return

    await send_moderation_log(
        interaction.guild,
        interaction.user,
        "add_role_member",
        target=member,
        reason=action_reason,
        details=f"Assigned role {role.mention} (`{role.id}`).",
    )
    await interaction.response.send_message(
        f"✅ Assigned {role.mention} to {member.mention}.",
        ephemeral=True,
    )


@bot.command(name="addrolemember")
async def add_role_member_prefix(
    ctx: commands.Context,
    member: discord.Member,
    role: discord.Role,
    *,
    reason: str = "",
):
    logger.info(
        "!addrolemember invoked by %s target=%s role=%s", ctx.author, member, role
    )
    if not await ensure_prefix_command_access(ctx, "add_role_member"):
        return
    if ctx.guild is None:
        await ctx.send("❌ This command can only be used in a server.")
        return

    bot_user_id = bot.user.id if bot.user else None
    bot_member = ctx.guild.me or (
        ctx.guild.get_member(bot_user_id) if bot_user_id else None
    )
    if bot_member is None:
        await ctx.send("❌ Could not resolve bot member in this guild.")
        return
    if not bot_member.guild_permissions.manage_roles:
        await ctx.send(
            "❌ I need the `Manage Roles` permission to manage member roles."
        )
        return

    can_moderate, member_error = validate_moderation_target(
        ctx.author, member, bot_member
    )
    if not can_moderate:
        await send_moderation_log(
            ctx.guild,
            ctx.author,
            "add_role_member",
            target=member,
            reason=reason.strip() or None,
            outcome="blocked",
            details=member_error,
        )
        await ctx.send(member_error)
        return

    can_manage_role, role_error = validate_manageable_role(ctx.author, role, bot_member)
    if not can_manage_role:
        await send_moderation_log(
            ctx.guild,
            ctx.author,
            "add_role_member",
            target=member,
            reason=reason.strip() or None,
            outcome="blocked",
            details=role_error,
        )
        await ctx.send(role_error)
        return

    if role in member.roles:
        await ctx.send(f"ℹ️ {member} already has {role}.")
        return

    action_reason = reason.strip() or f"Role assigned by {ctx.author} via bot"
    try:
        await member.add_roles(role, reason=action_reason)
    except discord.Forbidden:
        logger.exception("Missing permission to add role %s to member %s", role, member)
        await send_moderation_log(
            ctx.guild,
            ctx.author,
            "add_role_member",
            target=member,
            reason=action_reason,
            outcome="failed",
            details="Bot missing `Manage Roles` permission or role hierarchy block.",
        )
        await ctx.send(
            "❌ I can't assign that role. Check `Manage Roles` permission and role hierarchy."
        )
        return
    except discord.HTTPException:
        logger.exception("Failed to add role %s to member %s", role, member)
        await send_moderation_log(
            ctx.guild,
            ctx.author,
            "add_role_member",
            target=member,
            reason=action_reason,
            outcome="failed",
            details="Discord API error while assigning role.",
        )
        await ctx.send("❌ Failed to assign role. Try again.")
        return

    await send_moderation_log(
        ctx.guild,
        ctx.author,
        "add_role_member",
        target=member,
        reason=action_reason,
        details=f"Assigned role {role.mention} (`{role.id}`).",
    )
    await ctx.send(f"✅ Assigned {role.mention} to {member.mention}.")


@tree.command(
    name="remove_role_member",
    description="Remove a role from a member",
    guild=discord.Object(id=GUILD_ID),
)
@app_commands.describe(
    member="Member to update", role="Role to remove", reason="Reason for role removal"
)
async def remove_role_member_slash(
    interaction: discord.Interaction,
    member: discord.Member,
    role: discord.Role,
    reason: str | None = None,
):
    logger.info(
        "/remove_role_member invoked by %s target=%s role=%s",
        interaction.user,
        member,
        role,
    )
    if not await ensure_interaction_command_access(interaction, "remove_role_member"):
        return
    if interaction.guild is None:
        await interaction.response.send_message(
            "❌ This command can only be used in a server.", ephemeral=True
        )
        return

    bot_user_id = bot.user.id if bot.user else None
    bot_member = interaction.guild.me or (
        interaction.guild.get_member(bot_user_id) if bot_user_id else None
    )
    if bot_member is None:
        await interaction.response.send_message(
            "❌ Could not resolve bot member in this guild.", ephemeral=True
        )
        return
    if not bot_member.guild_permissions.manage_roles:
        await interaction.response.send_message(
            "❌ I need the `Manage Roles` permission to manage member roles.",
            ephemeral=True,
        )
        return

    can_moderate, member_error = validate_moderation_target(
        interaction.user, member, bot_member
    )
    if not can_moderate:
        await send_moderation_log(
            interaction.guild,
            interaction.user,
            "remove_role_member",
            target=member,
            reason=reason,
            outcome="blocked",
            details=member_error,
        )
        await interaction.response.send_message(member_error, ephemeral=True)
        return

    can_manage_role, role_error = validate_manageable_role(
        interaction.user, role, bot_member
    )
    if not can_manage_role:
        await send_moderation_log(
            interaction.guild,
            interaction.user,
            "remove_role_member",
            target=member,
            reason=reason,
            outcome="blocked",
            details=role_error,
        )
        await interaction.response.send_message(role_error, ephemeral=True)
        return

    if role not in member.roles:
        await interaction.response.send_message(
            f"ℹ️ {member.mention} does not currently have {role.mention}.",
            ephemeral=True,
        )
        return

    action_reason = (
        reason or ""
    ).strip() or f"Role removed by {interaction.user} via bot"
    try:
        await member.remove_roles(role, reason=action_reason)
    except discord.Forbidden:
        logger.exception(
            "Missing permission to remove role %s from member %s", role, member
        )
        await send_moderation_log(
            interaction.guild,
            interaction.user,
            "remove_role_member",
            target=member,
            reason=action_reason,
            outcome="failed",
            details="Bot missing `Manage Roles` permission or role hierarchy block.",
        )
        await interaction.response.send_message(
            "❌ I can't remove that role. Check `Manage Roles` permission and role hierarchy.",
            ephemeral=True,
        )
        return
    except discord.HTTPException:
        logger.exception("Failed to remove role %s from member %s", role, member)
        await send_moderation_log(
            interaction.guild,
            interaction.user,
            "remove_role_member",
            target=member,
            reason=action_reason,
            outcome="failed",
            details="Discord API error while removing role.",
        )
        await interaction.response.send_message(
            "❌ Failed to remove role. Try again.", ephemeral=True
        )
        return

    await send_moderation_log(
        interaction.guild,
        interaction.user,
        "remove_role_member",
        target=member,
        reason=action_reason,
        details=f"Removed role {role.mention} (`{role.id}`).",
    )
    await interaction.response.send_message(
        f"✅ Removed {role.mention} from {member.mention}.",
        ephemeral=True,
    )


@bot.command(name="removerolemember")
async def remove_role_member_prefix(
    ctx: commands.Context,
    member: discord.Member,
    role: discord.Role,
    *,
    reason: str = "",
):
    logger.info(
        "!removerolemember invoked by %s target=%s role=%s", ctx.author, member, role
    )
    if not await ensure_prefix_command_access(ctx, "remove_role_member"):
        return
    if ctx.guild is None:
        await ctx.send("❌ This command can only be used in a server.")
        return

    bot_user_id = bot.user.id if bot.user else None
    bot_member = ctx.guild.me or (
        ctx.guild.get_member(bot_user_id) if bot_user_id else None
    )
    if bot_member is None:
        await ctx.send("❌ Could not resolve bot member in this guild.")
        return
    if not bot_member.guild_permissions.manage_roles:
        await ctx.send(
            "❌ I need the `Manage Roles` permission to manage member roles."
        )
        return

    can_moderate, member_error = validate_moderation_target(
        ctx.author, member, bot_member
    )
    if not can_moderate:
        await send_moderation_log(
            ctx.guild,
            ctx.author,
            "remove_role_member",
            target=member,
            reason=reason.strip() or None,
            outcome="blocked",
            details=member_error,
        )
        await ctx.send(member_error)
        return

    can_manage_role, role_error = validate_manageable_role(ctx.author, role, bot_member)
    if not can_manage_role:
        await send_moderation_log(
            ctx.guild,
            ctx.author,
            "remove_role_member",
            target=member,
            reason=reason.strip() or None,
            outcome="blocked",
            details=role_error,
        )
        await ctx.send(role_error)
        return

    if role not in member.roles:
        await ctx.send(f"ℹ️ {member} does not currently have {role}.")
        return

    action_reason = reason.strip() or f"Role removed by {ctx.author} via bot"
    try:
        await member.remove_roles(role, reason=action_reason)
    except discord.Forbidden:
        logger.exception(
            "Missing permission to remove role %s from member %s", role, member
        )
        await send_moderation_log(
            ctx.guild,
            ctx.author,
            "remove_role_member",
            target=member,
            reason=action_reason,
            outcome="failed",
            details="Bot missing `Manage Roles` permission or role hierarchy block.",
        )
        await ctx.send(
            "❌ I can't remove that role. Check `Manage Roles` permission and role hierarchy."
        )
        return
    except discord.HTTPException:
        logger.exception("Failed to remove role %s from member %s", role, member)
        await send_moderation_log(
            ctx.guild,
            ctx.author,
            "remove_role_member",
            target=member,
            reason=action_reason,
            outcome="failed",
            details="Discord API error while removing role.",
        )
        await ctx.send("❌ Failed to remove role. Try again.")
        return

    await send_moderation_log(
        ctx.guild,
        ctx.author,
        "remove_role_member",
        target=member,
        reason=action_reason,
        details=f"Removed role {role.mention} (`{role.id}`).",
    )
    await ctx.send(f"✅ Removed {role.mention} from {member.mention}.")


@tree.command(
    name="unban_member",
    description="Unban a user by ID",
    guild=discord.Object(id=GUILD_ID),
)
@app_commands.describe(user_id="User ID to unban", reason="Reason for unban")
async def unban_member_slash(
    interaction: discord.Interaction, user_id: str, reason: str | None = None
):
    logger.info("/unban_member invoked by %s target=%s", interaction.user, user_id)
    if not await ensure_interaction_command_access(interaction, "unban_member"):
        return
    if interaction.guild is None:
        await interaction.response.send_message(
            "❌ This command can only be used in a server.", ephemeral=True
        )
        return

    target_user_id = parse_user_id_input(user_id)
    if target_user_id is None:
        await interaction.response.send_message("❌ Invalid user ID.", ephemeral=True)
        return

    action_reason = (reason or "").strip() or f"Unbanned by {interaction.user} via bot"
    try:
        await interaction.guild.unban(
            discord.Object(id=target_user_id), reason=action_reason
        )
    except discord.NotFound:
        await interaction.response.send_message(
            f"❌ User `{target_user_id}` is not currently banned.",
            ephemeral=True,
        )
        return
    except discord.Forbidden:
        logger.exception("Missing permission to unban user %s", target_user_id)
        await send_moderation_log(
            interaction.guild,
            interaction.user,
            "unban_member",
            reason=action_reason,
            outcome="failed",
            details="Bot missing `Ban Members` permission.",
        )
        await interaction.response.send_message(
            "❌ I can't unban users. Check `Ban Members` permission.",
            ephemeral=True,
        )
        return
    except discord.HTTPException:
        logger.exception("Failed to unban user %s", target_user_id)
        await send_moderation_log(
            interaction.guild,
            interaction.user,
            "unban_member",
            reason=action_reason,
            outcome="failed",
            details=f"Discord API error while unbanning user `{target_user_id}`.",
        )
        await interaction.response.send_message(
            "❌ Failed to unban that user. Try again.", ephemeral=True
        )
        return

    await send_moderation_log(
        interaction.guild,
        interaction.user,
        "unban_member",
        reason=action_reason,
        details=f"Unbanned user ID `{target_user_id}`.",
    )
    await interaction.response.send_message(
        f"✅ Unbanned user ID `{target_user_id}`.", ephemeral=True
    )


@bot.command(name="unbanmember")
async def unban_member_prefix(ctx: commands.Context, user_id: str, *, reason: str = ""):
    logger.info("!unbanmember invoked by %s target=%s", ctx.author, user_id)
    if not await ensure_prefix_command_access(ctx, "unban_member"):
        return
    if ctx.guild is None:
        await ctx.send("❌ This command can only be used in a server.")
        return

    target_user_id = parse_user_id_input(user_id)
    if target_user_id is None:
        await ctx.send("❌ Invalid user ID.")
        return

    action_reason = reason.strip() or f"Unbanned by {ctx.author} via bot"
    try:
        await ctx.guild.unban(discord.Object(id=target_user_id), reason=action_reason)
    except discord.NotFound:
        await ctx.send(f"❌ User `{target_user_id}` is not currently banned.")
        return
    except discord.Forbidden:
        logger.exception("Missing permission to unban user %s", target_user_id)
        await send_moderation_log(
            ctx.guild,
            ctx.author,
            "unban_member",
            reason=action_reason,
            outcome="failed",
            details="Bot missing `Ban Members` permission.",
        )
        await ctx.send("❌ I can't unban users. Check `Ban Members` permission.")
        return
    except discord.HTTPException:
        logger.exception("Failed to unban user %s", target_user_id)
        await send_moderation_log(
            ctx.guild,
            ctx.author,
            "unban_member",
            reason=action_reason,
            outcome="failed",
            details=f"Discord API error while unbanning user `{target_user_id}`.",
        )
        await ctx.send("❌ Failed to unban that user. Try again.")
        return

    await send_moderation_log(
        ctx.guild,
        ctx.author,
        "unban_member",
        reason=action_reason,
        details=f"Unbanned user ID `{target_user_id}`.",
    )
    await ctx.send(f"✅ Unbanned user ID `{target_user_id}`.")


@tree.command(
    name="search",
    description="Search GL.iNet forum and docs",
    guild=discord.Object(id=GUILD_ID),
)
@app_commands.describe(query="Enter search keywords")
async def search_slash(interaction: discord.Interaction, query: str):
    logger.info("/search invoked by %s with query %s", interaction.user, query)
    if not await ensure_interaction_command_access(interaction, "search"):
        return
    query = query.strip()
    if not query:
        await interaction.response.send_message(
            "❌ Please provide a search query.", ephemeral=True
        )
        return
    await interaction.response.defer(thinking=True)
    message = await asyncio.to_thread(build_search_message, query)
    await interaction.followup.send(message)


@bot.command(name="search")
async def search_prefix(ctx: commands.Context, *, query: str):
    logger.info("!search invoked by %s with query %s", ctx.author, query)
    if not await ensure_prefix_command_access(ctx, "search"):
        return
    query = query.strip()
    if not query:
        await ctx.send("❌ Please provide a search query.")
        return
    await ctx.send("🔍 Searching forum + docs...")
    message = await asyncio.to_thread(build_search_message, query)
    await ctx.send(message)


@tree.command(
    name="search_reddit",
    description="Search r/GlInet and return top 5 matching posts",
    guild=discord.Object(id=GUILD_ID),
)
@app_commands.describe(query="Enter search keywords")
async def search_reddit_slash(interaction: discord.Interaction, query: str):
    logger.info("/search_reddit invoked by %s with query %s", interaction.user, query)
    if not await ensure_interaction_command_access(interaction, "search_reddit"):
        return
    query = query.strip()
    if not query:
        await interaction.response.send_message(
            "❌ Please provide a search query.", ephemeral=True
        )
        return
    try:
        await interaction.response.defer(thinking=True)
        message = await asyncio.to_thread(build_reddit_search_message, query)
        await interaction.followup.send(message)
    except Exception:
        logger.exception(
            "search_reddit command failed for user=%s query=%s",
            interaction.user,
            query,
        )
        if interaction.response.is_done():
            await interaction.followup.send(
                "❌ Failed to fetch Reddit results. Please try again shortly.",
                ephemeral=True,
            )
        else:
            await interaction.response.send_message(
                "❌ Failed to fetch Reddit results. Please try again shortly.",
                ephemeral=True,
            )


@bot.command(name="searchreddit")
async def search_reddit_prefix(ctx: commands.Context, *, query: str):
    logger.info("!searchreddit invoked by %s with query %s", ctx.author, query)
    if not await ensure_prefix_command_access(ctx, "search_reddit"):
        return
    query = query.strip()
    if not query:
        await ctx.send("❌ Please provide a search query.")
        return
    try:
        await ctx.send("🔍 Searching Reddit...")
        message = await asyncio.to_thread(build_reddit_search_message, query)
        await ctx.send(message)
    except Exception:
        logger.exception(
            "searchreddit command failed for user=%s query=%s", ctx.author, query
        )
        await ctx.send("❌ Failed to fetch Reddit results. Please try again shortly.")


@tree.command(
    name="search_forum",
    description="Search the GL.iNet forum only",
    guild=discord.Object(id=GUILD_ID),
)
@app_commands.describe(query="Enter search keywords")
async def search_forum_slash(interaction: discord.Interaction, query: str):
    logger.info("/search_forum invoked by %s with query %s", interaction.user, query)
    if not await ensure_interaction_command_access(interaction, "search_forum"):
        return
    query = query.strip()
    if not query:
        await interaction.response.send_message(
            "❌ Please provide a search query.", ephemeral=True
        )
        return
    await interaction.response.defer(thinking=True)
    message = await asyncio.to_thread(build_forum_search_message, query)
    await interaction.followup.send(message)


@bot.command(name="searchforum")
async def search_forum_prefix(ctx: commands.Context, *, query: str):
    logger.info("!searchforum invoked by %s with query %s", ctx.author, query)
    if not await ensure_prefix_command_access(ctx, "search_forum"):
        return
    query = query.strip()
    if not query:
        await ctx.send("❌ Please provide a search query.")
        return
    await ctx.send("🔍 Searching forum...")
    message = await asyncio.to_thread(build_forum_search_message, query)
    await ctx.send(message)


@tree.command(
    name="search_kvm",
    description="Search KVM docs only",
    guild=discord.Object(id=GUILD_ID),
)
@app_commands.describe(query="Enter search keywords")
async def search_kvm_slash(interaction: discord.Interaction, query: str):
    logger.info("/search_kvm invoked by %s with query %s", interaction.user, query)
    if not await ensure_interaction_command_access(interaction, "search_kvm"):
        return
    query = query.strip()
    if not query:
        await interaction.response.send_message(
            "❌ Please provide a search query.", ephemeral=True
        )
        return
    await interaction.response.defer(thinking=True)
    message = await asyncio.to_thread(build_docs_site_search_message, query, "kvm")
    await interaction.followup.send(message)


@bot.command(name="searchkvm")
async def search_kvm_prefix(ctx: commands.Context, *, query: str):
    logger.info("!searchkvm invoked by %s with query %s", ctx.author, query)
    if not await ensure_prefix_command_access(ctx, "search_kvm"):
        return
    query = query.strip()
    if not query:
        await ctx.send("❌ Please provide a search query.")
        return
    await ctx.send("🔍 Searching KVM docs...")
    message = await asyncio.to_thread(build_docs_site_search_message, query, "kvm")
    await ctx.send(message)


@tree.command(
    name="search_iot",
    description="Search IoT docs only",
    guild=discord.Object(id=GUILD_ID),
)
@app_commands.describe(query="Enter search keywords")
async def search_iot_slash(interaction: discord.Interaction, query: str):
    logger.info("/search_iot invoked by %s with query %s", interaction.user, query)
    if not await ensure_interaction_command_access(interaction, "search_iot"):
        return
    query = query.strip()
    if not query:
        await interaction.response.send_message(
            "❌ Please provide a search query.", ephemeral=True
        )
        return
    await interaction.response.defer(thinking=True)
    message = await asyncio.to_thread(build_docs_site_search_message, query, "iot")
    await interaction.followup.send(message)


@bot.command(name="searchiot")
async def search_iot_prefix(ctx: commands.Context, *, query: str):
    logger.info("!searchiot invoked by %s with query %s", ctx.author, query)
    if not await ensure_prefix_command_access(ctx, "search_iot"):
        return
    query = query.strip()
    if not query:
        await ctx.send("❌ Please provide a search query.")
        return
    await ctx.send("🔍 Searching IoT docs...")
    message = await asyncio.to_thread(build_docs_site_search_message, query, "iot")
    await ctx.send(message)


@tree.command(
    name="search_router",
    description="Search Router v4 docs only",
    guild=discord.Object(id=GUILD_ID),
)
@app_commands.describe(query="Enter search keywords")
async def search_router_slash(interaction: discord.Interaction, query: str):
    logger.info("/search_router invoked by %s with query %s", interaction.user, query)
    if not await ensure_interaction_command_access(interaction, "search_router"):
        return
    query = query.strip()
    if not query:
        await interaction.response.send_message(
            "❌ Please provide a search query.", ephemeral=True
        )
        return
    await interaction.response.defer(thinking=True)
    message = await asyncio.to_thread(build_docs_site_search_message, query, "router")
    await interaction.followup.send(message)


@bot.command(name="searchrouter")
async def search_router_prefix(ctx: commands.Context, *, query: str):
    logger.info("!searchrouter invoked by %s with query %s", ctx.author, query)
    if not await ensure_prefix_command_access(ctx, "search_router"):
        return
    query = query.strip()
    if not query:
        await ctx.send("❌ Please provide a search query.")
        return
    await ctx.send("🔍 Searching Router v4 docs...")
    message = await asyncio.to_thread(build_docs_site_search_message, query, "router")
    await ctx.send(message)


start_web_admin_server()
bot.run(TOKEN, log_handler=None)

import json
import os
import re
import secrets
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from functools import wraps
from html import escape
from pathlib import Path
from urllib.parse import urlparse

from croniter import croniter
from flask import (
    Flask,
    flash,
    redirect,
    render_template_string,
    request,
    session,
    url_for,
)
from werkzeug.middleware.proxy_fix import ProxyFix
from werkzeug.security import check_password_hash, generate_password_hash

CHANNEL_ID_PATTERN = re.compile(r"^\d+$|^<#\d+>$")
PASSWORD_MAX_AGE_DAYS = 90
REMEMBER_LOGIN_DAYS = 5
AUTH_MODE_STANDARD = "standard"
AUTH_MODE_REMEMBER = "remember"
PASSWORD_HASH_METHOD = "pbkdf2:sha256:600000"
SESSION_TIMEOUT_MINUTE_OPTIONS = tuple(range(5, 31, 5))
POST_FORM_TAG_PATTERN = re.compile(
    r"(<form\b[^>]*\bmethod\s*=\s*[\"']?post[\"']?[^>]*>)",
    re.IGNORECASE,
)
INT_KEYS = {
    "GUILD_ID",
    "GENERAL_CHANNEL_ID",
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
    "WEB_PORT",
    "WEB_HOST_PORT",
    "WEB_DISCORD_CATALOG_TTL_SECONDS",
    "WEB_DISCORD_CATALOG_FETCH_TIMEOUT_SECONDS",
    "WEB_BULK_ASSIGN_TIMEOUT_SECONDS",
    "WEB_BULK_ASSIGN_MAX_UPLOAD_BYTES",
    "WEB_BULK_ASSIGN_REPORT_LIST_LIMIT",
    "WEB_BOT_PROFILE_TIMEOUT_SECONDS",
    "WEB_AVATAR_MAX_UPLOAD_BYTES",
    "WEB_SESSION_TIMEOUT_MINUTES",
}
SENSITIVE_KEYS = {
    "DISCORD_TOKEN",
    "WEB_ADMIN_DEFAULT_PASSWORD",
    "WEB_ADMIN_SESSION_SECRET",
}
ENV_FIELDS = [
    ("DISCORD_TOKEN", "Discord Token", "Bot token for Discord authentication."),
    ("GUILD_ID", "Guild ID", "Primary guild (server) ID."),
    (
        "GENERAL_CHANNEL_ID",
        "General Channel ID",
        "Default channel for invite generation.",
    ),
    ("LOG_LEVEL", "Log Level", "Bot log level (DEBUG, INFO, WARNING, ERROR)."),
    (
        "CONTAINER_LOG_LEVEL",
        "Container Log Level",
        "Container-wide error log level (DEBUG, INFO, WARNING, ERROR, CRITICAL).",
    ),
    ("DATA_DIR", "Data Directory", "Persistent data directory inside container."),
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
        "MOD_LOG_CHANNEL_ID",
        "Mod Log Channel ID",
        "Channel ID for moderation/server logs.",
    ),
    (
        "CSV_ROLE_ASSIGN_MAX_NAMES",
        "CSV Role Max Names",
        "Max unique names accepted per CSV bulk-assign.",
    ),
    (
        "firmware_notification_channel",
        "Firmware Notify Channel",
        "Channel ID or <#channel> mention for firmware alerts.",
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
        "WEB_HOST_PORT",
        "Web Host Port",
        "Host port mapped to WEB_PORT in Docker compose.",
    ),
    (
        "WEB_SESSION_TIMEOUT_MINUTES",
        "Web Auto Logout (Minutes)",
        "Session inactivity timeout in minutes (5, 10, 15, 20, 25, or 30).",
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
        if any(
            not (char.isascii() and (char.isalnum() or char == "-")) for char in label
        ):
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


def _normalize_session_timeout_minutes(raw_value, default_value: int = 5) -> int:
    try:
        parsed = int(str(raw_value).strip())
    except (TypeError, ValueError):
        return default_value
    if parsed not in SESSION_TIMEOUT_MINUTE_OPTIONS:
        return default_value
    return parsed


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


def _parse_iso_datetime(raw_value: str):
    text = str(raw_value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _password_change_required(user: dict) -> bool:
    baseline = (
        _parse_iso_datetime(user.get("password_changed_at"))
        or _parse_iso_datetime(user.get("updated_at"))
        or _parse_iso_datetime(user.get("created_at"))
    )
    if baseline is None:
        return True
    return datetime.now(timezone.utc) >= (baseline + timedelta(days=PASSWORD_MAX_AGE_DAYS))


def _password_age_days(user: dict) -> int:
    baseline = (
        _parse_iso_datetime(user.get("password_changed_at"))
        or _parse_iso_datetime(user.get("updated_at"))
        or _parse_iso_datetime(user.get("created_at"))
    )
    if baseline is None:
        return PASSWORD_MAX_AGE_DAYS
    delta = datetime.now(timezone.utc) - baseline
    return max(0, delta.days)


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _ensure_users_table_columns(conn):
    rows = conn.execute("PRAGMA table_info(web_users)").fetchall()
    columns = {str(row["name"]) for row in rows}
    alter_statements = []
    if "first_name" not in columns:
        alter_statements.append(
            "ALTER TABLE web_users ADD COLUMN first_name TEXT NOT NULL DEFAULT ''"
        )
    if "last_name" not in columns:
        alter_statements.append(
            "ALTER TABLE web_users ADD COLUMN last_name TEXT NOT NULL DEFAULT ''"
        )
    if "display_name" not in columns:
        alter_statements.append(
            "ALTER TABLE web_users ADD COLUMN display_name TEXT NOT NULL DEFAULT ''"
        )
    if "password_changed_at" not in columns:
        alter_statements.append(
            "ALTER TABLE web_users ADD COLUMN password_changed_at TEXT NOT NULL DEFAULT ''"
        )
    if "email_changed_at" not in columns:
        alter_statements.append(
            "ALTER TABLE web_users ADD COLUMN email_changed_at TEXT NOT NULL DEFAULT ''"
        )
    if "updated_at" not in columns:
        alter_statements.append(
            "ALTER TABLE web_users ADD COLUMN updated_at TEXT NOT NULL DEFAULT ''"
        )
    if "created_at" not in columns:
        alter_statements.append(
            "ALTER TABLE web_users ADD COLUMN created_at TEXT NOT NULL DEFAULT ''"
        )
    for statement in alter_statements:
        conn.execute(statement)

    now_iso = _now_iso()
    conn.execute(
        """
        UPDATE web_users
        SET created_at = COALESCE(NULLIF(TRIM(created_at), ''), ?)
        """,
        (now_iso,),
    )
    conn.execute(
        """
        UPDATE web_users
        SET updated_at = COALESCE(NULLIF(TRIM(updated_at), ''), created_at, ?)
        """,
        (now_iso,),
    )
    conn.execute(
        """
        UPDATE web_users
        SET password_changed_at = COALESCE(
            NULLIF(TRIM(password_changed_at), ''),
            NULLIF(TRIM(updated_at), ''),
            NULLIF(TRIM(created_at), ''),
            ?
        )
        """,
        (now_iso,),
    )
    conn.execute(
        """
        UPDATE web_users
        SET email_changed_at = COALESCE(
            NULLIF(TRIM(email_changed_at), ''),
            NULLIF(TRIM(updated_at), ''),
            NULLIF(TRIM(created_at), ''),
            ?
        )
        """,
        (now_iso,),
    )
    conn.execute(
        """
        UPDATE web_users
        SET first_name = COALESCE(first_name, ''),
            last_name = COALESCE(last_name, ''),
            display_name = COALESCE(display_name, '')
        """
    )
    conn.commit()


def _open_users_db(users_db_file: Path):
    conn = sqlite3.connect(str(users_db_file), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS web_users (
            email TEXT PRIMARY KEY,
            password_hash TEXT NOT NULL,
            is_admin INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    _ensure_users_table_columns(conn)
    try:
        os.chmod(users_db_file, 0o600)
    except (PermissionError, OSError):
        pass
    return conn


def _read_users(users_db_file: Path):
    conn = _open_users_db(users_db_file)
    try:
        rows = conn.execute(
            """
            SELECT
                email,
                password_hash,
                is_admin,
                first_name,
                last_name,
                display_name,
                password_changed_at,
                email_changed_at,
                created_at,
                updated_at
            FROM web_users
            ORDER BY created_at ASC, email ASC
            """
        ).fetchall()
    finally:
        conn.close()

    return [
        {
            "email": str(row["email"]).strip().lower(),
            "password_hash": str(row["password_hash"]),
            "is_admin": bool(row["is_admin"]),
            "first_name": _clean_profile_text(str(row["first_name"] or ""), max_length=80),
            "last_name": _clean_profile_text(str(row["last_name"] or ""), max_length=80),
            "display_name": _clean_profile_text(
                str(row["display_name"] or "") or _default_display_name(str(row["email"] or "")),
                max_length=80,
            ),
            "password_changed_at": str(
                row["password_changed_at"] or row["updated_at"] or row["created_at"] or _now_iso()
            ),
            "email_changed_at": str(
                row["email_changed_at"] or row["updated_at"] or row["created_at"] or _now_iso()
            ),
            "created_at": str(row["created_at"] or _now_iso()),
            "updated_at": str(row["updated_at"] or row["created_at"] or _now_iso()),
        }
        for row in rows
        if str(row["email"]).strip() and str(row["password_hash"]).strip()
    ]


def _save_users(users_db_file: Path, users):
    now_iso = _now_iso()
    conn = _open_users_db(users_db_file)
    try:
        with conn:
            conn.execute("DELETE FROM web_users")
            for entry in users:
                email = _normalize_email(entry.get("email", ""))
                password_hash = str(entry.get("password_hash", "")).strip()
                if not email or not password_hash:
                    continue
                is_admin = 1 if bool(entry.get("is_admin", False)) else 0
                first_name = _clean_profile_text(
                    str(entry.get("first_name", "")), max_length=80
                )
                last_name = _clean_profile_text(
                    str(entry.get("last_name", "")), max_length=80
                )
                display_name = _clean_profile_text(
                    str(entry.get("display_name", "")), max_length=80
                ) or _default_display_name(email)
                created_at = str(entry.get("created_at") or now_iso)
                password_changed_at = str(
                    entry.get("password_changed_at") or created_at or now_iso
                )
                email_changed_at = str(entry.get("email_changed_at") or created_at or now_iso)
                conn.execute(
                    """
                    INSERT INTO web_users (
                        email,
                        password_hash,
                        is_admin,
                        first_name,
                        last_name,
                        display_name,
                        password_changed_at,
                        email_changed_at,
                        created_at,
                        updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        email,
                        password_hash,
                        is_admin,
                        first_name,
                        last_name,
                        display_name,
                        password_changed_at,
                        email_changed_at,
                        created_at,
                        now_iso,
                    ),
                )
    finally:
        conn.close()


def _ensure_default_admin(
    users_db_file: Path, default_email: str, default_password: str, logger
):
    users = _read_users(users_db_file)
    if users:
        return

    email = _normalize_email(default_email) or "admin@example.com"
    if not _is_valid_email(email):
        email = "admin@example.com"

    password = default_password or ""
    if _password_policy_errors(password):
        message = (
            "WEB_ADMIN_DEFAULT_PASSWORD is missing or does not meet password policy. "
            "Set a strong password before first boot so the initial admin user can be created securely."
        )
        if logger:
            logger.error(message)
        raise ValueError(message)

    now_iso = _now_iso()
    display_name = _default_display_name(email)
    conn = _open_users_db(users_db_file)
    try:
        with conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO web_users (
                    email,
                    password_hash,
                    is_admin,
                    first_name,
                    last_name,
                    display_name,
                    password_changed_at,
                    email_changed_at,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    email,
                    _hash_password(password),
                    1,
                    "",
                    "",
                    display_name,
                    now_iso,
                    now_iso,
                    now_iso,
                    now_iso,
                ),
            )
    finally:
        conn.close()
    if logger:
        logger.info("Created default web admin user: %s", email)


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
            errors.append(
                "firmware_check_schedule must be a valid 5-field cron expression."
            )
        if (
            key == "firmware_notification_channel"
            and value
            and not CHANNEL_ID_PATTERN.fullmatch(value)
        ):
            errors.append(
                "firmware_notification_channel must be numeric ID or <#channel> format."
            )
        if key == "WEB_ADMIN_DEFAULT_USERNAME" and value and not _is_valid_email(value):
            errors.append("WEB_ADMIN_DEFAULT_USERNAME must be a valid email.")
        if key == "WEB_ADMIN_DEFAULT_PASSWORD" and value:
            errors.extend(_password_policy_errors(value))
        if key in {"LOG_LEVEL", "CONTAINER_LOG_LEVEL"}:
            if value.upper() not in valid_log_levels:
                errors.append(
                    f"{key} must be one of: DEBUG, INFO, WARNING, ERROR, CRITICAL."
                )
        if key == "WEB_RESTART_ENABLED" and value.lower() not in truthy_values:
            errors.append(
                "WEB_RESTART_ENABLED must be true/false (or 1/0, yes/no, on/off)."
            )
        if key in {
            "WEB_SESSION_COOKIE_SECURE",
            "WEB_TRUST_PROXY_HEADERS",
            "WEB_ENFORCE_CSRF",
            "WEB_ENFORCE_SAME_ORIGIN_POSTS",
            "WEB_HARDEN_FILE_PERMISSIONS",
        } and value.lower() not in truthy_values:
            errors.append(
                f"{key} must be true/false (or 1/0, yes/no, on/off)."
            )
        if key == "WEB_SESSION_TIMEOUT_MINUTES":
            parsed = _normalize_session_timeout_minutes(value, default_value=-1)
            if parsed == -1:
                errors.append(
                    "WEB_SESSION_TIMEOUT_MINUTES must be one of: 5, 10, 15, 20, 25, 30."
                )
        if (
            key == "WEB_GITHUB_WIKI_URL"
            and value
            and not value.startswith(("http://", "https://"))
        ):
            errors.append("WEB_GITHUB_WIKI_URL must start with http:// or https://.")
        if (
            key == "WEB_PUBLIC_BASE_URL"
            and value
            and not value.startswith(("http://", "https://"))
        ):
            errors.append("WEB_PUBLIC_BASE_URL must start with http:// or https://.")
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


def _render_select_input(
    name: str, selected_value: str, options: list[dict], placeholder: str = "Select..."
):
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
        rows.append(
            f"<option value='{escape(option_id, quote=True)}'{selected_attr}>"
            f"{escape(label)} ({escape(option_id)})</option>"
        )
    if selected and selected not in seen:
        rows.append(
            f"<option value='{escape(selected, quote=True)}' selected>"
            f"Current value (not found): {escape(selected)}</option>"
        )
    return f"<select name='{escape(name, quote=True)}'>" + "".join(rows) + "</select>"


def _render_fixed_select_input(
    name: str, selected_value: str, options: list[dict], placeholder: str = "Select..."
):
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
        rows.append(
            f"<option value='{escape(option_value, quote=True)}'{selected_attr}>"
            f"{escape(label)}</option>"
        )
    if selected and selected not in seen:
        rows.append(
            f"<option value='{escape(selected, quote=True)}' selected>"
            f"Current value: {escape(selected)}</option>"
        )
    return f"<select name='{escape(name, quote=True)}'>" + "".join(rows) + "</select>"


def _render_multi_select_input(
    name: str, selected_values, options: list[dict], size: int = 8
):
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
        rows.append(
            f"<option value='{escape(option_id, quote=True)}'{selected_attr}>"
            f"{escape(label)} ({escape(option_id)})</option>"
        )

    for missing_value in sorted(selected_set - seen):
        rows.append(
            f"<option value='{escape(missing_value, quote=True)}' selected>"
            f"Current value (not found): {escape(missing_value)}</option>"
        )

    return (
        f"<select name='{escape(name, quote=True)}' multiple size='{max(4, int(size))}'>"
        + "".join(rows)
        + "</select>"
    )


def _inject_csrf_token_inputs(body_html: str, csrf_token: str) -> str:
    token = str(csrf_token or "").strip()
    if not token:
        return body_html
    hidden_input = (
        f"<input type='hidden' name='csrf_token' value='{escape(token, quote=True)}' />"
    )
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
    github_wiki_url: str = "",
    restart_enabled: bool = False,
):
    return render_template_string(
        """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <meta name="csrf-token" content="{{ csrf_token }}" />
  <title>{{ title }}</title>
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
      justify-content: space-between;
      align-items: center;
      gap: 14px;
      position: sticky;
      top: 0;
      z-index: 10;
    }
    .header-right { display: flex; align-items: center; gap: 14px; flex-wrap: wrap; justify-content: flex-end; }
    .nav-controls { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
    .nav-controls a { text-decoration: none; }
    .current-user { color: var(--muted); font-size: 0.95rem; }
    .current-user-email { color: var(--muted); font-size: 0.85rem; }
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
    .dash-card h3 { margin-top: 0; margin-bottom: 8px; }
    .dash-card p { margin-top: 0; min-height: 50px; }
    .dash-actions { display: flex; gap: 10px; flex-wrap: wrap; }
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
    @media (max-width: 1080px) { .dash-grid { grid-template-columns: 1fr 1fr; } }
    @media (max-width: 900px) {
      .grid { grid-template-columns: 1fr; }
      .dash-grid { grid-template-columns: 1fr; }
      header { padding: 10px 12px; align-items: flex-start; }
      .wrap { margin: 14px auto; padding: 0 10px; }
      .card { padding: 14px; }
      .header-right { width: 100%; justify-content: flex-start; }
      .nav-controls { width: 100%; display: grid; grid-template-columns: 1fr; }
      .nav-select { width: 100%; max-width: 100%; min-width: 0; }
      .nav-controls .btn { width: 100%; }
      .inline-form { width: 100%; }
      .inline-form .btn { width: 100%; }
      .theme-switch { width: 100%; }
      .theme-btn { flex: 1; min-height: 42px; }
      .current-user-email { display: block; }
      .dash-actions .btn { width: 100%; }
      th, td { padding: 8px; }
      .table-scroll > table { min-width: 680px; }
    }
    @media (max-width: 600px) {
      .card { border-radius: 10px; }
      .table-scroll > table { min-width: 620px; }
    }
  </style>
</head>
<body data-theme="black">
  <header>
    <div><strong>Discord Bot Admin</strong></div>
    <div class="header-right">
      <div class="theme-switch" aria-label="Theme selector">
        <button type="button" class="theme-btn" data-theme-choice="light">Light</button>
        <button type="button" class="theme-btn" data-theme-choice="black">Black</button>
      </div>
      {% if current_email %}
        <nav class="nav-controls">
          <span class="current-user">{{ current_display_name or current_email }}</span>
          {% if current_display_name and current_display_name != current_email %}
            <span class="current-user-email">({{ current_email }})</span>
          {% endif %}
          <a class="btn secondary" href="{{ url_for('dashboard') }}">Dashboard</a>
          <label class="sr-only" for="nav-page-select">Open page</label>
          <select id="nav-page-select" class="nav-select">
            <option value="">Go to page...</option>
            <option value="{{ url_for('account') }}">My Account</option>
            {% if is_admin %}<option value="{{ url_for('bot_profile') }}">Bot Profile</option>{% endif %}
            {% if is_admin %}<option value="{{ url_for('command_permissions') }}">Command Permissions</option>{% endif %}
            {% if is_admin %}<option value="{{ url_for('settings') }}">Settings</option>{% endif %}
            <option value="{{ url_for('documentation') }}">Documentation</option>
            {% if github_wiki_url %}<option value="{{ github_wiki_url }}" data-external="1">GitHub Wiki</option>{% endif %}
            {% if is_admin %}<option value="{{ url_for('tag_responses') }}">Tag Responses</option>{% endif %}
            {% if is_admin %}<option value="{{ url_for('bulk_role_csv') }}">Bulk Role CSV</option>{% endif %}
            {% if is_admin %}<option value="{{ url_for('users') }}">Users</option>{% endif %}
            <option value="{{ url_for('logout') }}">Logout</option>
          </select>
          {% if is_admin and restart_enabled %}
            <form method="post" action="{{ url_for('restart_service') }}" class="inline-form" onsubmit="return confirm('WARNING: This will restart the container and temporarily disconnect the bot. Continue?');">
              <input type="hidden" name="confirm" value="yes" />
              <input type="hidden" name="csrf_token" value="{{ csrf_token }}" />
              <button class="btn danger" type="submit" title="Warning: restarts the running container process">Restart Container</button>
            </form>
          {% endif %}
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
    {{ body_html | safe }}
  </div>
  <script>
    (function () {
      const storageKey = "web_theme_choice";
      const fallbackTheme = "black";
      const allowed = { light: true, black: true };

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

      const navPageSelect = document.getElementById("nav-page-select");
      if (navPageSelect) {
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
      }

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
        body_html=body_html,
        current_email=current_email,
        current_display_name=current_display_name,
        csrf_token=csrf_token,
        is_admin=is_admin,
        github_wiki_url=github_wiki_url,
        restart_enabled=restart_enabled,
    )


def create_web_app(
    data_dir: str,
    env_file_path: str,
    tag_responses_file: str,
    default_admin_email: str,
    default_admin_password: str,
    on_env_settings_saved=None,
    on_get_tag_responses=None,
    on_save_tag_responses=None,
    on_tag_responses_saved=None,
    on_bulk_assign_role_csv=None,
    on_get_discord_catalog=None,
    on_get_command_permissions=None,
    on_save_command_permissions=None,
    on_get_bot_profile=None,
    on_update_bot_profile=None,
    on_update_bot_avatar=None,
    on_request_restart=None,
    logger=None,
):
    app = Flask(__name__)
    trust_proxy_headers = _is_truthy_env_value(
        os.getenv("WEB_TRUST_PROXY_HEADERS", "true")
    )
    if trust_proxy_headers:
        app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1)

    app.secret_key = os.getenv("WEB_ADMIN_SESSION_SECRET", "") or secrets.token_hex(32)
    max_bulk_upload = _get_int_env(
        "WEB_BULK_ASSIGN_MAX_UPLOAD_BYTES", 2 * 1024 * 1024, minimum=1024
    )
    max_avatar_upload = _get_int_env(
        "WEB_AVATAR_MAX_UPLOAD_BYTES", 2 * 1024 * 1024, minimum=1024
    )
    secure_session_cookie = _is_truthy_env_value(
        os.getenv("WEB_SESSION_COOKIE_SECURE", "true")
    )
    enforce_csrf = _is_truthy_env_value(os.getenv("WEB_ENFORCE_CSRF", "true"))
    enforce_same_origin_posts = _is_truthy_env_value(
        os.getenv("WEB_ENFORCE_SAME_ORIGIN_POSTS", "true")
    )
    harden_file_permissions = _is_truthy_env_value(
        os.getenv("WEB_HARDEN_FILE_PERMISSIONS", "true")
    )
    web_session_timeout_minutes = _normalize_session_timeout_minutes(
        os.getenv("WEB_SESSION_TIMEOUT_MINUTES", "5"),
        default_value=5,
    )
    session_timeout_state = {"minutes": web_session_timeout_minutes}
    app.config.update(
        SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SAMESITE="Strict",
        SESSION_COOKIE_SECURE=secure_session_cookie,
        SESSION_REFRESH_EACH_REQUEST=True,
        PERMANENT_SESSION_LIFETIME=timedelta(days=REMEMBER_LOGIN_DAYS),
        MAX_CONTENT_LENGTH=max(max_bulk_upload, max_avatar_upload) + (256 * 1024),
    )
    login_window_seconds = 15 * 60
    login_max_attempts = 6
    login_attempts = {}

    @app.after_request
    def apply_security_headers(response):
        request_host = _extract_hostname(str(request.host or ""))
        is_local_request = request_host in {"localhost", "127.0.0.1", "::1"}
        allow_coop = bool(request.is_secure or is_local_request)
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("Referrer-Policy", "no-referrer")
        response.headers.setdefault("X-Permitted-Cross-Domain-Policies", "none")
        if allow_coop:
            response.headers.setdefault("Cross-Origin-Opener-Policy", "same-origin")
        else:
            response.headers.pop("Cross-Origin-Opener-Policy", None)
        response.headers.setdefault(
            "Permissions-Policy", "geolocation=(), microphone=(), camera=()"
        )
        response.headers.setdefault("Cross-Origin-Resource-Policy", "same-origin")
        response.headers.setdefault("Cache-Control", "no-store")
        response.headers.setdefault("Pragma", "no-cache")
        response.headers.setdefault(
            "Content-Security-Policy",
            "default-src 'self'; img-src 'self' https: data:; style-src 'self' 'unsafe-inline'; "
            "script-src 'self' 'unsafe-inline'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'",
        )
        if request.is_secure:
            response.headers.setdefault(
                "Strict-Transport-Security",
                "max-age=31536000; includeSubDomains; preload",
            )

        # Allow local non-HTTPS testing to keep login working when secure cookies are enabled.
        if secure_session_cookie and (not request.is_secure) and is_local_request:
            session_cookie_name = str(app.config.get("SESSION_COOKIE_NAME", "session"))
            set_cookie_headers = response.headers.getlist("Set-Cookie")
            if set_cookie_headers:
                rewritten_headers = []
                for header_value in set_cookie_headers:
                    rewritten = header_value
                    if rewritten.startswith(f"{session_cookie_name}="):
                        rewritten = re.sub(
                            r";\s*Secure(?=;|$)",
                            "",
                            rewritten,
                            flags=re.IGNORECASE,
                        )
                    rewritten_headers.append(rewritten)
                if rewritten_headers != set_cookie_headers:
                    response.headers.pop("Set-Cookie", None)
                    for header_value in rewritten_headers:
                        response.headers.add("Set-Cookie", header_value)
        return response

    users_file = Path(data_dir) / "bot_data.db"
    users_file.parent.mkdir(parents=True, exist_ok=True)
    env_file = Path(env_file_path)
    if harden_file_permissions:
        try:
            os.chmod(users_file.parent, 0o700)
        except (PermissionError, OSError):
            pass
        if env_file.exists():
            try:
                os.chmod(env_file, 0o600)
            except (PermissionError, OSError):
                pass

    _ensure_default_admin(
        users_file, default_admin_email, default_admin_password, logger
    )
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
            parts = [
                part.strip() for part in x_forwarded_for.split(",") if part.strip()
            ]
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

    def _set_auth_session(email: str, remember_login: bool):
        now_dt = datetime.now(timezone.utc)
        now_iso = now_dt.isoformat()
        session["auth_email"] = _normalize_email(email)
        session["auth_mode"] = (
            AUTH_MODE_REMEMBER if remember_login else AUTH_MODE_STANDARD
        )
        session["auth_issued_at"] = now_iso
        session["auth_last_seen"] = now_iso
        if remember_login:
            session["auth_remember_until"] = (
                now_dt + timedelta(days=REMEMBER_LOGIN_DAYS)
            ).isoformat()
        else:
            session.pop("auth_remember_until", None)
        session.permanent = True

    def _session_timeout_minutes():
        return _normalize_session_timeout_minutes(
            session_timeout_state.get("minutes", 5),
            default_value=5,
        )

    def _is_active_auth_session():
        email = _normalize_email(session.get("auth_email", ""))
        if not email:
            return False

        now_dt = datetime.now(timezone.utc)
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
        else:
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
                    forwarded_token = (
                        str(forwarded_match.group(1) or "").strip().strip('"')
                    )
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
        origin_allowed = _match_allowed_host_from_url(origin)
        if origin_allowed is True:
            return True

        referer = str(request.headers.get("Referer", "")).strip()
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
        if request.method not in {"POST", "PUT", "PATCH", "DELETE"}:
            return None
        if request.endpoint == "healthz":
            return None

        if enforce_same_origin_posts and not _is_same_origin_request():
            flash("Blocked request due to origin policy.", "error")
            user = _current_user()
            if user:
                return redirect(url_for("dashboard"))
            return redirect(url_for("login"))

        if enforce_csrf:
            expected = str(session.get("csrf_token", "")).strip()
            submitted = str(
                request.form.get("csrf_token", "")
                or request.headers.get("X-CSRF-Token", "")
            ).strip()
            if not expected or not submitted or not secrets.compare_digest(
                expected, submitted
            ):
                flash("Session security token check failed. Please retry.", "error")
                user = _current_user()
                if user:
                    return redirect(url_for("dashboard"))
                return redirect(url_for("login"))
        return None

    def _prune_login_attempts(client_ip: str):
        now_ts = time.time()
        entries = login_attempts.get(client_ip, [])
        fresh_entries = [ts for ts in entries if (now_ts - ts) < login_window_seconds]
        if fresh_entries:
            login_attempts[client_ip] = fresh_entries
        else:
            login_attempts.pop(client_ip, None)
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
        if not resolved_display_name and normalized_email:
            for account in _read_users(users_file):
                if account.get("email") == normalized_email:
                    resolved_display_name = _clean_profile_text(
                        str(account.get("display_name", "")),
                        max_length=80,
                    )
                    break
        if not resolved_display_name and normalized_email:
            resolved_display_name = _default_display_name(normalized_email)
        return _render_layout(
            title,
            _inject_csrf_token_inputs(body_html, csrf_token),
            current_email,
            resolved_display_name,
            csrf_token,
            is_admin,
            github_wiki_url=_github_wiki_url(),
            restart_enabled=_restart_enabled(),
        )

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

    @app.route("/", methods=["GET"])
    def index():
        if _current_user():
            return redirect(url_for("dashboard"))
        return redirect(url_for("login"))

    @app.route("/login", methods=["GET", "POST"])
    def login():
        if request.method == "POST":
            client_ip = _client_ip()
            attempts = _prune_login_attempts(client_ip)
            if len(attempts) >= login_max_attempts:
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
                _set_auth_session(user["email"], remember_login=remember_login)
                if _password_change_required(user):
                    session["force_password_change_notice_shown"] = True
                    flash(
                        f"Password expired. You must change it every {PASSWORD_MAX_AGE_DAYS} days.",
                        "error",
                    )
                    return redirect(url_for("account"))
                return redirect(url_for("dashboard"))
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
                (
                    idx
                    for idx, entry in enumerate(users_data)
                    if entry.get("email") == user.get("email")
                ),
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
                    first_name = _clean_profile_text(
                        request.form.get("first_name", ""), max_length=80
                    )
                    last_name = _clean_profile_text(
                        request.form.get("last_name", ""), max_length=80
                    )
                    display_name = _clean_profile_text(
                        request.form.get("display_name", ""), max_length=80
                    )
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
                    if any(
                        row.get("email") == next_email
                        and row.get("email") != entry.get("email")
                        for row in users_data
                    ):
                        validation_errors.append(
                            "Another account already uses that email."
                        )
                    if not check_password_hash(entry["password_hash"], current_password):
                        validation_errors.append(
                            "Current password is required to update account details."
                        )

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
                if not check_password_hash(entry["password_hash"], current_password):
                    validation_errors.append("Current password is incorrect.")
                if new_password != confirm_password:
                    validation_errors.append("New password and confirmation must match.")
                validation_errors.extend(_password_policy_errors(new_password))
                if check_password_hash(entry["password_hash"], new_password):
                    validation_errors.append(
                        "New password must be different from the current password."
                    )

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
            else (
                f"<p class='muted'>Password age: {password_age_days} day(s). "
                f"Days remaining before forced reset: {days_remaining}.</p>"
            )
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
            <form method="post">
              <input type="hidden" name="action" value="password" />
              <label>Current Password</label>
              <input id="account_password_current" type="password" name="current_password" autocomplete="current-password" required />
              <label style="margin-top:10px;display:block;">New Password</label>
              <input id="account_password_new" type="password" name="new_password" autocomplete="new-password" required />
              <label style="margin-top:10px;display:block;">Confirm New Password</label>
              <input id="account_password_confirm" type="password" name="confirm_password" autocomplete="new-password" required />
              <label style="margin-top:8px;display:block;">
                <input type="checkbox"
                  onchange="document.getElementById('account_password_current').type=this.checked?'text':'password';document.getElementById('account_password_new').type=this.checked?'text':'password';document.getElementById('account_password_confirm').type=this.checked?'text':'password';" />
                Show passwords
              </label>
              <div style="margin-top:14px;">
                <button class="btn" type="submit">Update Password</button>
              </div>
            </form>
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
    def dashboard():
        user = _current_user()
        is_admin = bool(user.get("is_admin"))

        cards = []

        def add_dashboard_card(
            title: str,
            description: str,
            href: str,
            button_label: str,
            external: bool = False,
        ):
            link_target = (
                " target='_blank' rel='noopener noreferrer'" if external else ""
            )
            cards.append(
                f"""
                <div class="card dash-card">
                  <h3>{escape(title)}</h3>
                  <p class="muted">{escape(description)}</p>
                  <div class="dash-actions">
                    <a class="btn secondary" href="{escape(href, quote=True)}"{link_target}>{escape(button_label)}</a>
                  </div>
                </div>
                """
            )

        add_dashboard_card(
            "My Account",
            "Change your password, update your email, and manage profile display details.",
            url_for("account"),
            "Open My Account",
        )

        if is_admin:
            add_dashboard_card(
                "Bot Profile",
                "Rename the bot, update server nickname, and upload avatar.",
                url_for("bot_profile"),
                "Open Bot Profile",
            )
            add_dashboard_card(
                "Command Permissions",
                "Set access mode per command and pick restricted roles from Discord role lists.",
                url_for("command_permissions"),
                "Open Permissions",
            )
            add_dashboard_card(
                "Settings",
                "Edit runtime environment settings with channel and role dropdowns.",
                url_for("settings"),
                "Open Settings",
            )
            add_dashboard_card(
                "Tag Responses",
                "Manage dynamic tag-response mappings and refresh runtime commands.",
                url_for("tag_responses"),
                "Open Tag Responses",
            )
            add_dashboard_card(
                "Bulk Role CSV",
                "Upload a CSV of names and assign a role with a detailed result report.",
                url_for("bulk_role_csv"),
                "Open Bulk CSV",
            )
            add_dashboard_card(
                "Users",
                "Create web users, toggle admin rights, and reset passwords.",
                url_for("users"),
                "Open Users",
            )

        add_dashboard_card(
            "Documentation",
            "Browse embedded docs for commands, deployment, and operations.",
            url_for("documentation"),
            "Open Docs",
        )

        wiki_url = _github_wiki_url()
        if wiki_url:
            add_dashboard_card(
                "GitHub Wiki",
                "Open the external project wiki in a new tab.",
                wiki_url,
                "Open GitHub Wiki",
                external=True,
            )

        restart_card = ""
        if is_admin and _restart_enabled():
            restart_card = f"""
            <div class="card dash-card">
              <h3>Restart Container</h3>
              <p class="muted">Apply runtime-level changes that require a process restart.</p>
              <form method="post" action="{escape(url_for("restart_service"), quote=True)}"
                onsubmit="return confirm('WARNING: This will restart the container and temporarily disconnect the bot. Continue?');">
                <input type="hidden" name="confirm" value="yes" />
                <button class="btn danger" type="submit">Restart Container</button>
              </form>
            </div>
            """

        admin_note = (
            "<p class='muted'>Some Discord metadata changes may still require a restart after saving.</p>"
            if is_admin
            else "<p class='muted'>This account has limited access. Contact an admin for management actions.</p>"
        )

        body = f"""
        <div class="card">
          <h2>Dashboard</h2>
          <p>Quick actions for all available web interface functions.</p>
          {admin_note}
        </div>
        <div class="dash-grid">
          {"".join(cards)}
          {restart_card}
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
            body = (
                "<div class='card'><h2>Documentation</h2>"
                "<p class='muted'>No wiki pages were found in the runtime image.</p></div>"
            )
            return _render_page(
                "Documentation", body, user["email"], bool(user.get("is_admin"))
            )

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
        return _render_page(
            "Documentation", body, user["email"], bool(user.get("is_admin"))
        )

    @app.route("/admin/documentation/<page_slug>", methods=["GET"])
    @login_required
    def documentation_page(page_slug: str):
        user = _current_user()
        if not re.fullmatch(r"[A-Za-z0-9_-]+", page_slug or ""):
            return {"ok": False, "error": "Invalid documentation page."}, 404

        page_path = _get_wiki_page_map().get(page_slug.casefold())
        if page_path is None:
            return {"ok": False, "error": "Documentation page not found."}, 404
        if (
            not page_path.exists()
            or not page_path.is_file()
            or page_path.name.startswith("_")
        ):
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

    @app.route("/admin/bot-profile", methods=["GET", "POST"])
    @admin_required
    def bot_profile():
        user = _current_user()
        max_avatar_upload_bytes = _get_int_env(
            "WEB_AVATAR_MAX_UPLOAD_BYTES", 2 * 1024 * 1024, minimum=1024
        )
        profile = (
            on_get_bot_profile()
            if callable(on_get_bot_profile)
            else {"ok": False, "error": "Not configured"}
        )

        if request.method == "POST":
            action = str(request.form.get("action", "avatar")).strip().lower()
            if action == "identity":
                if not callable(on_update_bot_profile):
                    flash("Bot profile update callback is not configured.", "error")
                else:
                    username_input = str(request.form.get("bot_name", ""))
                    server_nickname_input = str(request.form.get("server_nickname", ""))
                    clear_server_nickname = str(
                        request.form.get("clear_server_nickname", "")
                    ).strip().lower() in {
                        "1",
                        "true",
                        "yes",
                        "on",
                    }
                    username_value = username_input.strip() or None
                    server_nickname_value = server_nickname_input.strip() or None
                    response = on_update_bot_profile(
                        username_value,
                        server_nickname_value,
                        clear_server_nickname,
                        user["email"],
                    )
                    if not isinstance(response, dict):
                        flash(
                            "Invalid response from bot profile update handler.", "error"
                        )
                    elif not response.get("ok"):
                        flash(
                            response.get("error", "Failed to update bot profile."),
                            "error",
                        )
                    else:
                        profile = response
                        flash(
                            str(
                                response.get("message")
                                or "Bot profile updated successfully."
                            ),
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
                        response = on_update_bot_avatar(
                            payload, uploaded_file.filename, user["email"]
                        )
                        if not isinstance(response, dict):
                            flash(
                                "Invalid response from avatar update handler.", "error"
                            )
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
            global_name = str(
                profile.get("global_name") or profile.get("display_name") or "Not set"
            )
            server_display_name = str(
                profile.get("server_display_name")
                or profile.get("display_name")
                or username
            )
            server_nickname = str(profile.get("server_nickname") or "Not set")
            guild_name = str(
                profile.get("guild_name") or "Configured guild unavailable"
            )
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
            profile_error = str(
                profile.get("error")
                if isinstance(profile, dict)
                else "Unable to load profile."
            )
            profile_html = f"<div class='card'><p class='muted'>Could not load bot profile: {escape(profile_error)}</p></div>"

        body = f"""
        <div class="grid">
          <div class="card">
            <h2>Bot Identity</h2>
            <p class="muted">Set bot username and server nickname for how the bot appears in Discord.</p>
            <p class="muted">Discord may rate-limit username changes.</p>
            <form method="post">
              <input type="hidden" name="action" value="identity" />
              <label>Bot username (global)</label>
              <input type="text" name="bot_name" placeholder="Leave blank to keep current username" />
              <label style="margin-top:10px;display:block;">Server nickname (this guild)</label>
              <input type="text" name="server_nickname" placeholder="Leave blank to keep current nickname" />
              <label style="margin-top:10px;display:block;">
                <input type="checkbox" name="clear_server_nickname" value="1" />
                Clear server nickname
              </label>
              <div style="margin-top:14px;">
                <button class="btn" type="submit">Update Identity</button>
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
        return _render_page(
            "Bot Profile", body, user["email"], bool(user.get("is_admin"))
        )

    @app.route("/admin/command-permissions", methods=["GET", "POST"])
    @admin_required
    def command_permissions():
        user = _current_user()
        permissions_payload = (
            on_get_command_permissions()
            if callable(on_get_command_permissions)
            else {"ok": False, "error": "Not configured"}
        )
        discord_catalog = (
            on_get_discord_catalog() if callable(on_get_discord_catalog) else None
        )
        role_options = []
        catalog_error = ""
        if isinstance(discord_catalog, dict):
            if discord_catalog.get("ok"):
                role_options = discord_catalog.get("roles", []) or []
            else:
                catalog_error = str(discord_catalog.get("error") or "")

        if request.method == "POST":
            if not callable(on_save_command_permissions):
                flash("Command permission save callback is not configured.", "error")
            else:
                command_updates = {}
                for command_key in request.form.getlist("command_key"):
                    selected_role_ids = request.form.getlist(f"role_ids__{command_key}")
                    manual_role_ids = request.form.get(
                        f"role_ids_text__{command_key}", ""
                    )
                    role_ids_payload = (
                        selected_role_ids if role_options else manual_role_ids
                    )
                    if (
                        role_options
                        and not selected_role_ids
                        and manual_role_ids.strip()
                    ):
                        role_ids_payload = manual_role_ids
                    command_updates[command_key] = {
                        "mode": request.form.get(f"mode__{command_key}", "default"),
                        "role_ids": role_ids_payload,
                    }
                response = on_save_command_permissions(
                    {"commands": command_updates}, user["email"]
                )
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

        if not isinstance(permissions_payload, dict) or not permissions_payload.get(
            "ok"
        ):
            error_text = str(
                permissions_payload.get("error")
                if isinstance(permissions_payload, dict)
                else "Unable to load command permissions."
            )
            body = (
                "<div class='card'><h2>Command Permissions</h2>"
                f"<p class='muted'>Could not load command permissions: {escape(error_text)}</p></div>"
            )
            return _render_page(
                "Command Permissions", body, user["email"], bool(user.get("is_admin"))
            )

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
                "<p class='muted'>Role dropdown loaded from Discord. "
                "Use Ctrl/Cmd-click to select multiple roles per command.</p>"
            )

        allowed_role_names = permissions_payload.get("allowed_role_names", []) or []
        moderator_role_ids = permissions_payload.get("moderator_role_ids", []) or []
        body = f"""
        <div class="card">
          <h2>Command Permissions</h2>
          <p class="muted">Set access mode per command. Default mode follows built-in behavior. Custom mode requires at least one role ID.</p>
          <p class="muted">Default named-role gate: {escape(", ".join(str(item) for item in allowed_role_names) or "None")}</p>
          <p class="muted">Current moderator role IDs: <span class="mono">{escape(",".join(str(item) for item in moderator_role_ids) or "None")}</span></p>
          {role_hint_html}
          <form method="post">
            <table>
              <thead>
                <tr><th>Command</th><th>Default Access</th><th>Mode</th><th>Custom Role Selection</th></tr>
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
        return _render_page(
            "Command Permissions", body, user["email"], bool(user.get("is_admin"))
        )

    @app.route("/admin/settings", methods=["GET", "POST"])
    @admin_required
    def settings():
        user = _current_user()
        file_values = _parse_env_file(env_file)
        discord_catalog = (
            on_get_discord_catalog() if callable(on_get_discord_catalog) else None
        )
        channel_options = []
        role_options = []
        catalog_error = ""
        if isinstance(discord_catalog, dict):
            if discord_catalog.get("ok"):
                channel_options = discord_catalog.get("channels", []) or []
                role_options = discord_catalog.get("roles", []) or []
            else:
                catalog_error = str(discord_catalog.get("error") or "")

        if request.method == "POST":
            updated_values = {}
            for key, _, _ in ENV_FIELDS:
                current = file_values.get(key, os.getenv(key, ""))
                if key in SENSITIVE_KEYS:
                    submitted = request.form.get(key, "")
                    updated_values[key] = submitted if submitted else current
                else:
                    updated_values[key] = request.form.get(key, "").strip()

            validation_errors = _validate_env_updates(updated_values)
            if validation_errors:
                for entry in validation_errors:
                    flash(entry, "error")
            else:
                final_values = dict(file_values)
                for key, value in updated_values.items():
                    if value == "":
                        final_values.pop(key, None)
                        os.environ.pop(key, None)
                    else:
                        final_values[key] = value
                        os.environ[key] = value
                _write_env_file(env_file, final_values)
                effective_timeout_minutes = _normalize_session_timeout_minutes(
                    final_values.get(
                        "WEB_SESSION_TIMEOUT_MINUTES",
                        os.getenv("WEB_SESSION_TIMEOUT_MINUTES", "5"),
                    ),
                    default_value=5,
                )
                session_timeout_state["minutes"] = effective_timeout_minutes
                if callable(on_env_settings_saved):
                    on_env_settings_saved(updated_values)
                flash("Settings saved to .env and applied where supported.", "success")
                file_values = _parse_env_file(env_file)

        rows = []
        for key, label, description in ENV_FIELDS:
            value = file_values.get(key, os.getenv(key, ""))
            safe_value = "" if key in SENSITIVE_KEYS else value
            placeholder = "•••••• (unchanged if blank)" if key in SENSITIVE_KEYS else ""
            input_type = "password" if key in SENSITIVE_KEYS else "text"
            static_select_options = []
            select_options = []
            select_placeholder = "Select..."
            if key == "WEB_SESSION_TIMEOUT_MINUTES":
                safe_value = str(
                    _normalize_session_timeout_minutes(safe_value or "5", default_value=5)
                )
                static_select_options = [
                    {"value": str(minutes), "label": f"{minutes} minutes"}
                    for minutes in SESSION_TIMEOUT_MINUTE_OPTIONS
                ]
                select_placeholder = "Select auto logout timeout..."
            elif key in {"LOG_LEVEL", "CONTAINER_LOG_LEVEL"}:
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
            if key == "firmware_notification_channel" or key.endswith("_CHANNEL_ID"):
                select_options = channel_options
            elif key.endswith("_ROLE_ID"):
                select_options = role_options

            if static_select_options:
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
            rows.append(
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
            guild_info = (
                discord_catalog.get("guild", {})
                if isinstance(discord_catalog, dict)
                else {}
            )
            guild_name = str(guild_info.get("name") or "unknown")
            guild_id = str(guild_info.get("id") or "unknown")
            catalog_note = (
                f"<p class='muted'>Loaded live Discord options from {escape(guild_name)} "
                f"({escape(guild_id)}). Channels: {len(channel_options)}; Roles: {len(role_options)}.</p>"
            )
        elif catalog_error:
            catalog_note = f"<p class='muted'>Could not load Discord options: {escape(catalog_error)}</p>"

        body = (
            "<div class='card'><h2>Environment Settings</h2>"
            "<p class='muted'>These map to runtime bot settings and persist in .env.</p>"
            f"{catalog_note}"
            "<form method='post'><table><thead><tr><th>Setting</th><th>Value</th><th>Description</th></tr></thead>"
            f"<tbody>{''.join(rows)}</tbody></table><div style='margin-top:14px;'><button class='btn' type='submit'>Save Settings</button></div></form></div>"
        )
        return _render_page("Settings", body, user["email"], bool(user.get("is_admin")))

    @app.route("/admin/tag-responses", methods=["GET", "POST"])
    @admin_required
    def tag_responses():
        user = _current_user()
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
                    response = on_save_tag_responses(parsed, user["email"])
                    if not isinstance(response, dict):
                        raise ValueError(
                            "Invalid response from tag response save handler"
                        )
                    if not response.get("ok"):
                        raise ValueError(
                            str(response.get("error") or "Failed to save tag responses")
                        )
                else:
                    path.write_text(json.dumps(parsed, indent=2) + "\n")
                if callable(on_tag_responses_saved):
                    on_tag_responses_saved()
                flash("Tag responses updated.", "success")
            except Exception as exc:
                flash(f"Invalid tag JSON: {exc}", "error")

        if callable(on_get_tag_responses):
            response = on_get_tag_responses()
            if isinstance(response, dict) and response.get("ok"):
                current_mapping = response.get("mapping", {}) or {}
                current = json.dumps(current_mapping, indent=2) + "\n"
            else:
                error_text = (
                    response.get("error")
                    if isinstance(response, dict)
                    else "Unknown error"
                )
                flash(
                    f"Could not load tag responses from storage: {error_text}", "error"
                )
                current = "{}\n"
        else:
            if not path.exists():
                path.write_text("{}\n")
            current = path.read_text()

        escaped_current = escape(current)
        body = f"""
        <div class="card">
          <h2>Tag Responses</h2>
          <p class="muted">Edit the tag-to-response JSON mapping used by slash and message tag commands.</p>
          <form method="post">
            <textarea name="tag_json">{escaped_current}</textarea>
            <div style="margin-top:14px;">
              <button class="btn" type="submit">Save Tag Responses</button>
            </div>
          </form>
        </div>
        """
        return _render_page(
            "Tag Responses", body, user["email"], bool(user.get("is_admin"))
        )

    @app.route("/admin/bulk-role-csv", methods=["GET", "POST"])
    @admin_required
    def bulk_role_csv():
        user = _current_user()
        operation_result = None
        max_upload_bytes = _get_int_env(
            "WEB_BULK_ASSIGN_MAX_UPLOAD_BYTES", 2 * 1024 * 1024, minimum=1024
        )
        report_list_limit = _get_int_env(
            "WEB_BULK_ASSIGN_REPORT_LIST_LIMIT", 50, minimum=1
        )
        discord_catalog = (
            on_get_discord_catalog() if callable(on_get_discord_catalog) else None
        )
        role_options = []
        catalog_error = ""
        if isinstance(discord_catalog, dict):
            if discord_catalog.get("ok"):
                role_options = discord_catalog.get("roles", []) or []
            else:
                catalog_error = str(discord_catalog.get("error") or "")

        if request.method == "POST":
            selected_role_input = request.form.get("role_id_select", "").strip()
            manual_role_input = request.form.get("role_id", "").strip()
            role_input = (
                selected_role_input
                if role_options
                else (manual_role_input or selected_role_input)
            )
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
                        role_input, payload, uploaded_file.filename, user["email"]
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
            summary_rows = "".join(
                f"<div class='mono'>{escape(line)}</div>" for line in summary_lines
            )
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
                items = "".join(
                    f"<li class='mono'>{escape(value)}</li>" for value in values[:limit]
                )
                overflow = len(values) - limit
                overflow_note = (
                    f"<p class='muted'>... and {overflow} more</p>"
                    if overflow > 0
                    else ""
                )
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
                + _render_select_input(
                    "role_id_select", "", role_options, "Choose role..."
                )
                + "<p class='muted'>Choose the target role using the current guild role list.</p>"
            )
        elif catalog_error:
            role_picker_html = f"<p class='muted'>Could not load role dropdown: {escape(catalog_error)}</p>"
        else:
            role_picker_html = "<p class='muted'>Role dropdown is unavailable. Use manual Role ID input.</p>"

        body = f"""
        <div class="card">
          <h2>Bulk Assign Role from CSV</h2>
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
        return _render_page(
            "Bulk Role CSV", body, user["email"], bool(user.get("is_admin"))
        )

    @app.route("/admin/users", methods=["GET", "POST"])
    @admin_required
    def users():
        user = _current_user()
        users_data = _read_users(users_file)

        if request.method == "POST":
            action = request.form.get("action", "").strip()
            if action == "create":
                email = _normalize_email(request.form.get("email", ""))
                password = request.form.get("password", "")
                first_name = _clean_profile_text(
                    request.form.get("first_name", ""), max_length=80
                )
                last_name = _clean_profile_text(
                    request.form.get("last_name", ""), max_length=80
                )
                display_name = _clean_profile_text(
                    request.form.get("display_name", ""), max_length=80
                )
                is_admin = bool(request.form.get("is_admin"))
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
                                "is_admin": is_admin,
                                "first_name": first_name,
                                "last_name": last_name,
                                "display_name": display_name,
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
                candidate = [
                    entry for entry in users_data if entry["email"] != target_email
                ]
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

            elif action == "toggle_admin":
                target_email = _normalize_email(request.form.get("email", ""))
                changed = False
                for entry in users_data:
                    if entry["email"] == target_email:
                        entry["is_admin"] = not entry.get("is_admin")
                        changed = True
                        break
                if changed:
                    if sum(1 for entry in users_data if entry.get("is_admin")) < 1:
                        flash("At least one admin account must remain.", "error")
                    else:
                        _save_users(users_file, users_data)
                        flash(f"Updated admin role for {target_email}.", "success")
                        users_data = _read_users(users_file)
                else:
                    flash("User not found.", "error")

        user_rows = []
        for entry in users_data:
            email = entry["email"]
            admin_label = "Yes" if entry.get("is_admin") else "No"
            display_name = str(entry.get("display_name") or _default_display_name(email))
            full_name = _clean_profile_text(
                f"{str(entry.get('first_name') or '')} {str(entry.get('last_name') or '')}",
                max_length=160,
            )
            user_rows.append(
                f"""
                <tr>
                  <td>{escape(display_name)}</td>
                  <td>{escape(full_name or "n/a")}</td>
                  <td class="mono">{escape(email)}</td>
                  <td>{escape(admin_label)}</td>
                  <td class="mono">{escape(str(entry.get("password_changed_at", "n/a")))}</td>
                  <td class="mono">{escape(str(entry.get("created_at", "n/a")))}</td>
                  <td>
                    <form method="post" style="display:inline;">
                      <input type="hidden" name="action" value="toggle_admin" />
                      <input type="hidden" name="email" value="{escape(email, quote=True)}" />
                      <button class="btn secondary" type="submit">Toggle Admin</button>
                    </form>
                    <form method="post" style="display:inline;margin-left:6px;">
                      <input type="hidden" name="action" value="delete" />
                      <input type="hidden" name="email" value="{escape(email, quote=True)}" />
                      <button class="btn secondary" type="submit">Delete</button>
                    </form>
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
              <label style="margin-top:8px;display:block;">
                <input type="checkbox"
                  onchange="document.getElementById('create_user_password').type=this.checked?'text':'password';" />
                Show password
              </label>
              <label style="margin-top:10px;display:block;"><input type="checkbox" name="is_admin" /> Admin user</label>
              <p class="muted">Password policy: 6-16 characters, at least 2 numbers, 1 uppercase letter, and 1 symbol.</p>
              <button class="btn" type="submit">Create User</button>
            </form>
          </div>
          <div class="card">
            <h2>Reset Password</h2>
            <form method="post">
              <input type="hidden" name="action" value="password" />
              <label>User Email</label>
              <input type="email" name="email" autocomplete="email" autocapitalize="none" spellcheck="false" required />
              <label style="margin-top:10px;display:block;">New Password</label>
              <input id="reset_user_password" type="password" name="password" autocomplete="new-password" required />
              <label style="margin-top:8px;display:block;">
                <input type="checkbox"
                  onchange="document.getElementById('reset_user_password').type=this.checked?'text':'password';" />
                Show password
              </label>
              <button class="btn" type="submit">Update Password</button>
            </form>
          </div>
        </div>
        <div class="card">
          <h2>Existing Users</h2>
          <table>
            <thead><tr><th>Display</th><th>Name</th><th>Email</th><th>Admin</th><th>Password Changed</th><th>Created</th><th>Actions</th></tr></thead>
            <tbody>{"".join(user_rows)}</tbody>
          </table>
        </div>
        """
        return _render_page("Users", body, user["email"], bool(user.get("is_admin")))

    return app


def start_web_admin_interface(
    host: str,
    port: int,
    data_dir: str,
    env_file_path: str,
    tag_responses_file: str,
    default_admin_email: str,
    default_admin_password: str,
    on_env_settings_saved=None,
    on_get_tag_responses=None,
    on_save_tag_responses=None,
    on_tag_responses_saved=None,
    on_bulk_assign_role_csv=None,
    on_get_discord_catalog=None,
    on_get_command_permissions=None,
    on_save_command_permissions=None,
    on_get_bot_profile=None,
    on_update_bot_profile=None,
    on_update_bot_avatar=None,
    on_request_restart=None,
    logger=None,
):
    app = create_web_app(
        data_dir=data_dir,
        env_file_path=env_file_path,
        tag_responses_file=tag_responses_file,
        default_admin_email=default_admin_email,
        default_admin_password=default_admin_password,
        on_env_settings_saved=on_env_settings_saved,
        on_get_tag_responses=on_get_tag_responses,
        on_save_tag_responses=on_save_tag_responses,
        on_tag_responses_saved=on_tag_responses_saved,
        on_bulk_assign_role_csv=on_bulk_assign_role_csv,
        on_get_discord_catalog=on_get_discord_catalog,
        on_get_command_permissions=on_get_command_permissions,
        on_save_command_permissions=on_save_command_permissions,
        on_get_bot_profile=on_get_bot_profile,
        on_update_bot_profile=on_update_bot_profile,
        on_update_bot_avatar=on_update_bot_avatar,
        on_request_restart=on_request_restart,
        logger=logger,
    )
    if logger:
        logger.info("Starting web admin interface on http://%s:%s", host, port)
    app.run(host=host, port=port, debug=False, use_reloader=False)

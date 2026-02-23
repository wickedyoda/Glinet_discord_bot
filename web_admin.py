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
from werkzeug.security import check_password_hash, generate_password_hash

CHANNEL_ID_PATTERN = re.compile(r"^\d+$|^<#\d+>$")
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


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


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
    return conn


def _read_users(users_db_file: Path):
    conn = _open_users_db(users_db_file)
    try:
        rows = conn.execute(
            "SELECT email, password_hash, is_admin, created_at FROM web_users ORDER BY created_at ASC, email ASC"
        ).fetchall()
    finally:
        conn.close()

    return [
        {
            "email": str(row["email"]).strip().lower(),
            "password_hash": str(row["password_hash"]),
            "is_admin": bool(row["is_admin"]),
            "created_at": str(row["created_at"] or _now_iso()),
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
                created_at = str(entry.get("created_at") or now_iso)
                conn.execute(
                    """
                    INSERT INTO web_users (email, password_hash, is_admin, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (email, password_hash, is_admin, created_at, now_iso),
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
    conn = _open_users_db(users_db_file)
    try:
        with conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO web_users (email, password_hash, is_admin, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (email, generate_password_hash(password), 1, now_iso, now_iso),
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


def _validate_env_updates(updated_values: dict):
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
        if key == "WEB_RESTART_ENABLED" and value.lower() not in {
            "1",
            "0",
            "true",
            "false",
            "yes",
            "no",
            "on",
            "off",
        }:
            errors.append(
                "WEB_RESTART_ENABLED must be true/false (or 1/0, yes/no, on/off)."
            )
        if (
            key == "WEB_GITHUB_WIKI_URL"
            and value
            and not value.startswith(("http://", "https://"))
        ):
            errors.append("WEB_GITHUB_WIKI_URL must start with http:// or https://.")
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


def _render_layout(
    title: str,
    body_html: str,
    current_email: str,
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
  <title>{{ title }}</title>
  <style>
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
    .nav-links { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
    .nav-links a { text-decoration: none; }
    .wrap { max-width: 1200px; margin: 22px auto; padding: 0 16px; }
    .card { background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 18px; margin-bottom: 16px; }
    .flash { padding: 10px 12px; border-radius: 8px; margin-bottom: 10px; border: 1px solid var(--border); }
    .flash.error { background: var(--flash-err-bg); color: var(--flash-err-fg); }
    .flash.success { background: var(--flash-ok-bg); color: var(--flash-ok-fg); }
    table { width: 100%; border-collapse: collapse; }
    th, td { border-bottom: 1px solid var(--border); padding: 10px; text-align: left; vertical-align: top; }
    input[type=text], input[type=password], textarea, select {
      width: 100%;
      box-sizing: border-box;
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 8px;
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
      display: inline-block;
    }
    .btn.secondary { background: var(--btn-secondary); }
    .btn.danger { background: var(--btn-danger); }
    .inline-form { display: inline; margin-left: 12px; }
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
    .dash-grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 14px; }
    .dash-card h3 { margin-top: 0; margin-bottom: 8px; }
    .dash-card p { margin-top: 0; min-height: 50px; }
    .dash-actions { display: flex; gap: 10px; flex-wrap: wrap; }
    @media (max-width: 1080px) { .dash-grid { grid-template-columns: 1fr 1fr; } }
    @media (max-width: 900px) {
      .grid { grid-template-columns: 1fr; }
      .dash-grid { grid-template-columns: 1fr; }
      .header-right { width: 100%; justify-content: flex-start; }
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
        <nav class="nav-links">
          <span>{{ current_email }}</span>
          <a href="{{ url_for('dashboard') }}">Dashboard</a>
          {% if is_admin %}<a href="{{ url_for('bot_profile') }}">Bot Profile</a>{% endif %}
          {% if is_admin %}<a href="{{ url_for('command_permissions') }}">Command Permissions</a>{% endif %}
          {% if is_admin %}<a href="{{ url_for('settings') }}">Settings</a>{% endif %}
          <a href="{{ url_for('documentation') }}">Documentation</a>
          {% if github_wiki_url %}<a href="{{ github_wiki_url }}" target="_blank" rel="noopener noreferrer">GitHub Wiki</a>{% endif %}
          {% if is_admin %}<a href="{{ url_for('tag_responses') }}">Tag Responses</a>{% endif %}
          {% if is_admin %}<a href="{{ url_for('bulk_role_csv') }}">Bulk Role CSV</a>{% endif %}
          {% if is_admin %}<a href="{{ url_for('users') }}">Users</a>{% endif %}
          {% if is_admin and restart_enabled %}
            <form method="post" action="{{ url_for('restart_service') }}" class="inline-form" onsubmit="return confirm('WARNING: This will restart the container and temporarily disconnect the bot. Continue?');">
              <input type="hidden" name="confirm" value="yes" />
              <button class="btn danger" type="submit" title="Warning: restarts the running container process">Restart Container</button>
            </form>
          {% endif %}
          <a href="{{ url_for('logout') }}">Logout</a>
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
    })();
  </script>
</body>
</html>
        """,
        title=title,
        body_html=body_html,
        current_email=current_email,
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
    app.secret_key = os.getenv("WEB_ADMIN_SESSION_SECRET", "") or secrets.token_hex(32)
    max_bulk_upload = _get_int_env(
        "WEB_BULK_ASSIGN_MAX_UPLOAD_BYTES", 2 * 1024 * 1024, minimum=1024
    )
    max_avatar_upload = _get_int_env(
        "WEB_AVATAR_MAX_UPLOAD_BYTES", 2 * 1024 * 1024, minimum=1024
    )
    app.config.update(
        SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SAMESITE="Strict",
        PERMANENT_SESSION_LIFETIME=timedelta(hours=8),
        MAX_CONTENT_LENGTH=max(max_bulk_upload, max_avatar_upload) + (256 * 1024),
    )
    login_window_seconds = 15 * 60
    login_max_attempts = 6
    login_attempts = {}

    @app.after_request
    def apply_security_headers(response):
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("Referrer-Policy", "no-referrer")
        response.headers.setdefault(
            "Permissions-Policy", "geolocation=(), microphone=(), camera=()"
        )
        response.headers.setdefault("Cross-Origin-Resource-Policy", "same-origin")
        response.headers.setdefault(
            "Content-Security-Policy",
            "default-src 'self'; img-src 'self' https: data:; style-src 'self' 'unsafe-inline'; "
            "script-src 'self' 'unsafe-inline'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'",
        )
        return response

    users_file = Path(data_dir) / "bot_data.db"
    users_file.parent.mkdir(parents=True, exist_ok=True)
    env_file = Path(env_file_path)

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
            "https://github.com/wickedyoda/Glinet_discord_bot/wiki",
        )
        return str(value or "").strip()

    def _restart_enabled():
        return _is_truthy_env_value(os.getenv("WEB_RESTART_ENABLED", "true"))

    def _client_ip():
        x_forwarded_for = str(request.headers.get("X-Forwarded-For", "")).strip()
        if x_forwarded_for:
            parts = [
                part.strip() for part in x_forwarded_for.split(",") if part.strip()
            ]
            if parts:
                return parts[0]
        return str(request.remote_addr or "unknown")

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

    def _render_page(title: str, body_html: str, current_email: str, is_admin: bool):
        return _render_layout(
            title,
            body_html,
            current_email,
            is_admin,
            github_wiki_url=_github_wiki_url(),
            restart_enabled=_restart_enabled(),
        )

    def login_required(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if _current_user() is None:
                return redirect(url_for("login"))
            return fn(*args, **kwargs)

        return wrapper

    def admin_required(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            user = _current_user()
            if user is None:
                return redirect(url_for("login"))
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
            user = next(
                (entry for entry in _read_users(users_file) if entry["email"] == email),
                None,
            )
            if user and check_password_hash(user["password_hash"], password):
                login_attempts.pop(client_ip, None)
                session["auth_email"] = user["email"]
                session.permanent = True
                return redirect(url_for("dashboard"))
            attempts.append(time.time())
            login_attempts[client_ip] = attempts[-login_max_attempts:]
            flash("Invalid email or password.", "error")

        return _render_page(
            "Login",
            """
            <div class="card" style="max-width:520px;margin:30px auto;">
              <h2>Admin Login</h2>
              <p class="muted">Email/password login. Users are created by an admin only.</p>
              <form method="post">
                <label>Email</label>
                <input type="text" name="email" placeholder="admin@example.com" required />
                <label style="margin-top:10px;display:block;">Password</label>
                <input type="password" name="password" required />
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
            select_options = []
            if key == "firmware_notification_channel" or key.endswith("_CHANNEL_ID"):
                select_options = channel_options
            elif key.endswith("_ROLE_ID"):
                select_options = role_options

            if select_options:
                input_html = _render_select_input(
                    name=key,
                    selected_value=safe_value,
                    options=select_options,
                    placeholder="Select from Discord...",
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
                is_admin = bool(request.form.get("is_admin"))
                if not _is_valid_email(email):
                    flash("Enter a valid email.", "error")
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
                                "password_hash": generate_password_hash(password),
                                "is_admin": is_admin,
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
                            entry["password_hash"] = generate_password_hash(
                                new_password
                            )
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
            user_rows.append(
                f"""
                <tr>
                  <td class="mono">{escape(email)}</td>
                  <td>{escape(admin_label)}</td>
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
              <label>Email</label>
              <input type="text" name="email" required />
              <label style="margin-top:10px;display:block;">Password</label>
              <input id="create_user_password" type="password" name="password" required />
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
              <input type="text" name="email" required />
              <label style="margin-top:10px;display:block;">New Password</label>
              <input id="reset_user_password" type="password" name="password" required />
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
            <thead><tr><th>Email</th><th>Admin</th><th>Created</th><th>Actions</th></tr></thead>
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

import json
import os
import re
import secrets
from datetime import datetime, timezone
from functools import wraps
from html import escape
from pathlib import Path

from croniter import croniter
from flask import Flask, flash, redirect, render_template_string, request, session, url_for
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
}
SENSITIVE_KEYS = {"DISCORD_TOKEN", "WEB_ADMIN_DEFAULT_PASSWORD", "WEB_ADMIN_SESSION_SECRET"}
ENV_FIELDS = [
    ("DISCORD_TOKEN", "Discord Token", "Bot token for Discord authentication."),
    ("GUILD_ID", "Guild ID", "Primary guild (server) ID."),
    ("GENERAL_CHANNEL_ID", "General Channel ID", "Default channel for invite generation."),
    ("LOG_LEVEL", "Log Level", "Bot log level (DEBUG, INFO, WARNING, ERROR)."),
    ("DATA_DIR", "Data Directory", "Persistent data directory inside container."),
    ("FORUM_BASE_URL", "Forum Base URL", "GL.iNet forum root URL."),
    ("FORUM_MAX_RESULTS", "Forum Max Results", "Max forum links returned per search."),
    ("DOCS_MAX_RESULTS_PER_SITE", "Docs Max/Site", "Max docs results for each docs source."),
    ("DOCS_INDEX_TTL_SECONDS", "Docs Index TTL", "Docs index cache TTL in seconds."),
    ("SEARCH_RESPONSE_MAX_CHARS", "Search Response Limit", "Max chars in search response message."),
    ("KICK_PRUNE_HOURS", "Kick Prune Hours", "Hours of message history to prune on kick."),
    ("MODERATOR_ROLE_ID", "Moderator Role ID", "Role ID allowed to run moderation commands."),
    ("ADMIN_ROLE_ID", "Admin Role ID", "Additional role ID allowed to moderate."),
    ("MOD_LOG_CHANNEL_ID", "Mod Log Channel ID", "Channel ID for moderation/server logs."),
    ("CSV_ROLE_ASSIGN_MAX_NAMES", "CSV Role Max Names", "Max unique names accepted per CSV bulk-assign."),
    ("firmware_notification_channel", "Firmware Notify Channel", "Channel ID or <#channel> mention for firmware alerts."),
    ("FIRMWARE_FEED_URL", "Firmware Feed URL", "Source URL used for firmware mirror checks."),
    ("firmware_check_schedule", "Firmware Cron Schedule", "5-field cron schedule in UTC."),
    ("FIRMWARE_REQUEST_TIMEOUT_SECONDS", "Firmware Request Timeout", "HTTP timeout for firmware fetch requests."),
    ("FIRMWARE_RELEASE_NOTES_MAX_CHARS", "Firmware Notes Max Chars", "Max release-notes excerpt size."),
    ("WEB_ENABLED", "Web UI Enabled", "Set to true/false to enable or disable the web admin UI."),
    ("WEB_BIND_HOST", "Web Bind Host", "Host/IP bind for web admin service."),
    ("WEB_PORT", "Web Container Port", "Internal HTTP port in the container (default 8080)."),
    ("WEB_HOST_PORT", "Web Host Port", "Host port mapped to WEB_PORT in Docker compose."),
    ("WEB_ADMIN_DEFAULT_USERNAME", "Default Admin Email", "Default admin email used for first boot user creation."),
    ("WEB_ADMIN_DEFAULT_PASSWORD", "Default Admin Password", "Default admin password for first boot user creation."),
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
        if any(not (char.isascii() and (char.isalnum() or char == "-")) for char in label):
            return False
    if len(labels[-1]) < 2:
        return False
    return True


def _password_policy_errors(password: str):
    candidate = password or ""
    digits = sum(1 for char in candidate if char.isdigit())
    uppercase = sum(1 for char in candidate if char.isupper())
    symbols = sum(1 for char in candidate if not char.isalnum())
    errors = []
    if digits < 6:
        errors.append("Password must contain at least 6 digits.")
    if uppercase < 2:
        errors.append("Password must contain at least 2 uppercase letters.")
    if symbols < 1:
        errors.append("Password must contain at least 1 symbol.")
    return errors


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _read_users(users_file: Path):
    if not users_file.exists():
        return []
    try:
        payload = json.loads(users_file.read_text())
    except Exception:
        return []
    users = payload.get("users", [])
    if not isinstance(users, list):
        return []
    normalized = []
    for user in users:
        if not isinstance(user, dict):
            continue
        email = _normalize_email(user.get("email", ""))
        password_hash = user.get("password_hash", "")
        if not email or not password_hash:
            continue
        normalized.append(
            {
                "email": email,
                "password_hash": password_hash,
                "is_admin": bool(user.get("is_admin", False)),
                "created_at": str(user.get("created_at", _now_iso())),
            }
        )
    return normalized


def _save_users(users_file: Path, users):
    payload = {"updated_at": _now_iso(), "users": users}
    users_file.write_text(json.dumps(payload, indent=2))


def _ensure_default_admin(users_file: Path, default_email: str, default_password: str, logger):
    users = _read_users(users_file)
    if users:
        return

    email = _normalize_email(default_email) or "admin@example.com"
    if not _is_valid_email(email):
        email = "admin@example.com"

    password = default_password or ""
    if _password_policy_errors(password):
        password = "AA!!123456"
        if logger:
            logger.warning(
                "Invalid WEB_ADMIN_DEFAULT_PASSWORD or missing admin defaults. "
                "Using fallback default password for first login; change it immediately."
            )

    users = [
        {
            "email": email,
            "password_hash": generate_password_hash(password),
            "is_admin": True,
            "created_at": _now_iso(),
        }
    ]
    _save_users(users_file, users)
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
            errors.append("firmware_check_schedule must be a valid 5-field cron expression.")
        if key == "firmware_notification_channel" and value and not CHANNEL_ID_PATTERN.fullmatch(value):
            errors.append("firmware_notification_channel must be numeric ID or <#channel> format.")
        if key == "WEB_ADMIN_DEFAULT_USERNAME" and value and not _is_valid_email(value):
            errors.append("WEB_ADMIN_DEFAULT_USERNAME must be a valid email.")
        if key == "WEB_ADMIN_DEFAULT_PASSWORD" and value:
            errors.extend(_password_policy_errors(value))
    return errors


def _render_layout(title: str, body_html: str, current_email: str, is_admin: bool):
    return render_template_string(
        """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{{ title }}</title>
  <style>
    body { font-family: "Segoe UI", Arial, sans-serif; margin: 0; background:#f6f7fb; color:#1f2937; }
    header { background:#111827; color:#fff; padding:12px 18px; display:flex; justify-content:space-between; align-items:center; }
    header a { color:#93c5fd; text-decoration:none; margin-left:12px; }
    .wrap { max-width:1100px; margin:22px auto; padding:0 16px; }
    .card { background:#fff; border:1px solid #e5e7eb; border-radius:10px; padding:18px; margin-bottom:16px; }
    .flash { padding:10px 12px; border-radius:8px; margin-bottom:10px; }
    .flash.error { background:#fee2e2; color:#991b1b; }
    .flash.success { background:#dcfce7; color:#166534; }
    table { width:100%; border-collapse:collapse; }
    th, td { border-bottom:1px solid #e5e7eb; padding:10px; text-align:left; vertical-align:top; }
    input[type=text], input[type=password], textarea, select {
      width:100%; box-sizing:border-box; border:1px solid #d1d5db; border-radius:8px; padding:8px;
    }
    textarea { min-height:220px; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
    .btn { background:#2563eb; border:0; color:#fff; padding:9px 14px; border-radius:8px; cursor:pointer; }
    .btn.secondary { background:#4b5563; }
    .grid { display:grid; grid-template-columns:1fr 1fr; gap:16px; }
    .muted { color:#6b7280; font-size:0.9rem; }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
    @media (max-width: 900px) { .grid { grid-template-columns:1fr; } }
  </style>
</head>
<body>
  <header>
    <div><strong>Discord Bot Admin</strong></div>
    <div>
      {% if current_email %}
        <span>{{ current_email }}</span>
        <a href="{{ url_for('dashboard') }}">Dashboard</a>
        <a href="{{ url_for('settings') }}">Settings</a>
        <a href="{{ url_for('tag_responses') }}">Tag Responses</a>
        {% if is_admin %}<a href="{{ url_for('users') }}">Users</a>{% endif %}
        <a href="{{ url_for('logout') }}">Logout</a>
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
</body>
</html>
        """,
        title=title,
        body_html=body_html,
        current_email=current_email,
        is_admin=is_admin,
    )


def create_web_app(
    data_dir: str,
    env_file_path: str,
    tag_responses_file: str,
    default_admin_email: str,
    default_admin_password: str,
    on_env_settings_saved=None,
    on_tag_responses_saved=None,
    logger=None,
):
    app = Flask(__name__)
    app.secret_key = os.getenv("WEB_ADMIN_SESSION_SECRET", "") or secrets.token_hex(32)
    app.config.update(
        SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SAMESITE="Lax",
    )

    users_file = Path(data_dir) / "web_users.json"
    users_file.parent.mkdir(parents=True, exist_ok=True)
    env_file = Path(env_file_path)

    _ensure_default_admin(users_file, default_admin_email, default_admin_password, logger)

    def _current_user():
        email = _normalize_email(session.get("auth_email", ""))
        if not email:
            return None
        for user in _read_users(users_file):
            if user["email"] == email:
                return user
        return None

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
            email = _normalize_email(request.form.get("email", ""))
            password = request.form.get("password", "")
            user = next((entry for entry in _read_users(users_file) if entry["email"] == email), None)
            if user and check_password_hash(user["password_hash"], password):
                session["auth_email"] = user["email"]
                return redirect(url_for("dashboard"))
            flash("Invalid email or password.", "error")

        return _render_layout(
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
        return _render_layout(
            "Dashboard",
            """
            <div class="card">
              <h2>Dashboard</h2>
              <p>Use <a href="/admin/settings">Settings</a> to edit environment-driven bot settings.</p>
              <p>Use <a href="/admin/tag-responses">Tag Responses</a> to manage dynamic tag commands.</p>
              <p>Use <a href="/admin/users">Users</a> to create/manage login users (admin only).</p>
              <p class="muted">Some Discord command metadata (for example guild-specific slash registration) may still require a process restart after changes.</p>
            </div>
            """,
            user["email"],
            bool(user.get("is_admin")),
        )

    @app.route("/admin/settings", methods=["GET", "POST"])
    @admin_required
    def settings():
        user = _current_user()
        file_values = _parse_env_file(env_file)

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
            rows.append(
                f"""
                <tr>
                  <td><strong>{escape(label)}</strong><div class="muted mono">{escape(key)}</div></td>
                  <td><input type="{escape(input_type)}" name="{escape(key)}" value="{escape(safe_value, quote=True)}" placeholder="{escape(placeholder, quote=True)}" /></td>
                  <td class="muted">{escape(description)}</td>
                </tr>
                """
            )
        body = (
            "<div class='card'><h2>Environment Settings</h2>"
            "<p class='muted'>These map to runtime bot settings and persist in .env.</p>"
            "<form method='post'><table><thead><tr><th>Setting</th><th>Value</th><th>Description</th></tr></thead>"
            f"<tbody>{''.join(rows)}</tbody></table><div style='margin-top:14px;'><button class='btn' type='submit'>Save Settings</button></div></form></div>"
        )
        return _render_layout("Settings", body, user["email"], bool(user.get("is_admin")))

    @app.route("/admin/tag-responses", methods=["GET", "POST"])
    @admin_required
    def tag_responses():
        user = _current_user()
        path = Path(tag_responses_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            path.write_text("{}\n")

        if request.method == "POST":
            raw = request.form.get("tag_json", "")
            try:
                parsed = json.loads(raw)
                if not isinstance(parsed, dict):
                    raise ValueError("JSON must be an object")
                for key, value in parsed.items():
                    if not isinstance(key, str) or not isinstance(value, str):
                        raise ValueError("All tag keys/values must be strings")
                path.write_text(json.dumps(parsed, indent=2) + "\n")
                if callable(on_tag_responses_saved):
                    on_tag_responses_saved()
                flash("Tag responses updated.", "success")
            except Exception as exc:
                flash(f"Invalid tag JSON: {exc}", "error")

        current = path.read_text()
        escaped_current = escape(current)
        body = f"""
        <div class="card">
          <h2>Tag Responses</h2>
          <p class="muted">Edit the same mapping stored in <span class="mono">{escape(str(path))}</span>.</p>
          <form method="post">
            <textarea name="tag_json">{escaped_current}</textarea>
            <div style="margin-top:14px;">
              <button class="btn" type="submit">Save Tag Responses</button>
            </div>
          </form>
        </div>
        """
        return _render_layout("Tag Responses", body, user["email"], bool(user.get("is_admin")))

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
                password_errors = _password_policy_errors(new_password)
                if password_errors:
                    for message in password_errors:
                        flash(message, "error")
                else:
                    changed = False
                    for entry in users_data:
                        if entry["email"] == target_email:
                            entry["password_hash"] = generate_password_hash(new_password)
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
                  <td class="mono">{escape(str(entry.get('created_at', 'n/a')))}</td>
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
              <input type="password" name="password" required />
              <label style="margin-top:10px;display:block;"><input type="checkbox" name="is_admin" /> Admin user</label>
              <p class="muted">Password policy: at least 6 digits, 2 uppercase letters, and 1 symbol.</p>
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
              <input type="password" name="password" required />
              <button class="btn" type="submit">Update Password</button>
            </form>
          </div>
        </div>
        <div class="card">
          <h2>Existing Users</h2>
          <table>
            <thead><tr><th>Email</th><th>Admin</th><th>Created</th><th>Actions</th></tr></thead>
            <tbody>{''.join(user_rows)}</tbody>
          </table>
        </div>
        """
        return _render_layout("Users", body, user["email"], bool(user.get("is_admin")))

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
    on_tag_responses_saved=None,
    logger=None,
):
    app = create_web_app(
        data_dir=data_dir,
        env_file_path=env_file_path,
        tag_responses_file=tag_responses_file,
        default_admin_email=default_admin_email,
        default_admin_password=default_admin_password,
        on_env_settings_saved=on_env_settings_saved,
        on_tag_responses_saved=on_tag_responses_saved,
        logger=logger,
    )
    if logger:
        logger.info("Starting web admin interface on http://%s:%s", host, port)
    app.run(host=host, port=port, debug=False, use_reloader=False)

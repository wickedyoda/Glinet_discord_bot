"""Glinet Discord Bot - Web Admin GUI Variant 2."""

from __future__ import annotations

import ipaddress
import logging
import os
import secrets
import sqlite3
import threading
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path

from flask import Flask, flash, redirect, render_template_string, request, session, url_for

from webui.constants import (
    FEED_INTERVAL_OPTIONS,
    PASSWORD_MIN_LENGTH,
    REMEMBER_LOGIN_DAYS,
    SENSITIVE_ENV_KEYS,
    SQLITE_TIMEOUT_SECONDS,
    UPTIME_MONITOR_INTERVAL_OPTIONS,
    UPTIME_MONITOR_TIMEOUT_OPTIONS,
)
from webui.template_v2 import PAGE_TEMPLATE

logger = logging.getLogger(__name__)


def _is_sensitive_key(key: str) -> bool:
    if key in SENSITIVE_ENV_KEYS:
        return True
    upper_key = key.upper()
    return "TOKEN" in upper_key or "PASSWORD" in upper_key or "SECRET" in upper_key


def _normalize_feed_interval(raw_value: str | int | None, default: int = 300) -> int:
    allowed = {value for value, _label in FEED_INTERVAL_OPTIONS}
    if isinstance(raw_value, int):
        return raw_value if raw_value in allowed else default
    candidate = str(raw_value or "").strip()
    if candidate.isdigit():
        parsed = int(candidate)
        if parsed in allowed:
            return parsed
    return default


def _feed_interval_label(seconds: int | str | None) -> str:
    normalized = _normalize_feed_interval(seconds)
    for value, label in FEED_INTERVAL_OPTIONS:
        if value == normalized:
            return label
    return "5 minutes"


def _normalize_monitor_interval(raw_value: str | int | None, default: int = 60) -> int:
    allowed = {value for value, _label in UPTIME_MONITOR_INTERVAL_OPTIONS}
    if isinstance(raw_value, int):
        return raw_value if raw_value in allowed else default
    candidate = str(raw_value or "").strip()
    if candidate.isdigit():
        parsed = int(candidate)
        if parsed in allowed:
            return parsed
    return default


def _monitor_interval_label(seconds: int | str | None) -> str:
    normalized = _normalize_monitor_interval(seconds)
    for value, label in UPTIME_MONITOR_INTERVAL_OPTIONS:
        if value == normalized:
            return label
    return "1 minute"


def _normalize_monitor_timeout(raw_value: str | int | None, default: int = 8) -> int:
    allowed = set(UPTIME_MONITOR_TIMEOUT_OPTIONS)
    if isinstance(raw_value, int):
        return raw_value if raw_value in allowed else default
    candidate = str(raw_value or "").strip()
    if candidate.isdigit():
        parsed = int(candidate)
        if parsed in allowed:
            return parsed
    return default


def _is_blocked_ip_address(host: str) -> bool:
    hostname = (host or "").strip().lower()
    if not hostname or hostname in {"localhost", "::1", "ip6-localhost"} or hostname.endswith(".localhost"):
        return True
    try:
        ip = ipaddress.ip_address(hostname)
    except ValueError:
        return False
    return ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved or ip.is_multicast or ip.is_unspecified


def _apply_best_effort_permissions(path: Path, mode: int) -> None:
    try:
        os.chmod(path, mode)
    except OSError:
        return


def _ensure_private_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    _apply_best_effort_permissions(path, 0o700)


def _secure_sqlite_sidecars(path: Path) -> None:
    for suffix in ("", "-wal", "-shm"):
        target = path if not suffix else path.with_name(f"{path.name}{suffix}")
        if target.exists():
            _apply_best_effort_permissions(target, 0o600)


def _sqlite_connect(db_path: str) -> sqlite3.Connection:
    db_file = Path(db_path).expanduser()
    parent = db_file.parent
    if str(parent) not in {"", "."}:
        _ensure_private_directory(parent)
    conn = sqlite3.connect(str(db_file), timeout=SQLITE_TIMEOUT_SECONDS)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute(f"PRAGMA busy_timeout = {SQLITE_TIMEOUT_SECONDS * 1000}")
    _secure_sqlite_sidecars(db_file)
    return conn


def _parse_stored_datetime(raw_value: object) -> datetime | None:
    raw_text = str(raw_value or "").strip()
    if not raw_text:
        return None
    for candidate in (raw_text, raw_text.replace("Z", "+00:00")):
        try:
            parsed = datetime.fromisoformat(candidate)
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=UTC)
            return parsed.astimezone(UTC)
        except ValueError:
            continue
    for fmt in ("%Y-%m-%d %H:%M:%S",):
        try:
            return datetime.strptime(raw_text, fmt).replace(tzinfo=UTC)
        except ValueError:
            continue
    return None


def _password_policy_error(password: str) -> str | None:
    if len(password) < PASSWORD_MIN_LENGTH:
        return f"Password must be at least {PASSWORD_MIN_LENGTH} characters."
    digit_count = sum(char.isdigit() for char in password)
    symbol_count = sum(not char.isalnum() for char in password)
    if digit_count < 2:
        return "Password must include at least 2 numbers."
    if symbol_count < 1:
        return "Password must include at least 1 symbol."
    return None


def _password_hash_needs_upgrade(password_hash: str) -> bool:
    return not str(password_hash or "").startswith("scrypt:")


def _ensure_actions_table(db_path: str) -> None:
    directory = os.path.dirname(db_path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with _sqlite_connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS actions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                action TEXT NOT NULL,
                status TEXT NOT NULL,
                moderator TEXT,
                target TEXT,
                reason TEXT,
                guild TEXT
            )
            """
        )
        conn.commit()


def create_app(
    data_dir: str,
    env_file_path: str,
    default_admin_email: str,
    default_admin_password: str,
    get_bot_snapshot: Callable[[], dict] | None = None,
    get_managed_guilds: Callable[[], list[dict]] | None = None,
    get_notification_channels: Callable[[], dict] | None = None,
    get_discord_catalog: Callable[[str], dict] | None = None,
    get_command_permissions: Callable[[str], dict] | None = None,
    save_command_permissions: Callable[[str, dict], dict] | None = None,
    get_tag_responses: Callable[[], dict] | None = None,
    save_tag_responses: Callable[[dict], dict] | None = None,
    get_guild_settings: Callable[[str], dict] | None = None,
    save_guild_settings: Callable[[str, dict], dict] | None = None,
    get_bot_profile: Callable[[], dict] | None = None,
    update_bot_profile: Callable[[dict], dict] | None = None,
    update_bot_avatar: Callable[[bytes], dict] | None = None,
    kick_member: Callable[[str, str, str], dict] | None = None,
    ban_member: Callable[[str, str, str], dict] | None = None,
    timeout_member: Callable[[str, str, str, int], dict] | None = None,
    untimeout_member: Callable[[str, str, str], dict] | None = None,
    leave_guild: Callable[[str, int], dict] | None = None,
    request_restart: Callable[[str], dict] | None = None,
    get_member_activity: Callable[..., dict] | None = None,
    export_member_activity: Callable[..., dict] | None = None,
    pick_random_user: Callable[[str], dict] | None = None,
    get_honeypot: Callable[[int], dict] | None = None,
    manage_honeypot: Callable[[dict, str, int], dict] | Callable[[dict, str], dict] | None = None,
    get_role_access: Callable[[int], dict] | None = None,
    manage_role_access: Callable[[dict, str, int], dict] | Callable[[dict, str], dict] | None = None,
    get_reaction_roles: Callable[[int], dict] | None = None,
    manage_reaction_roles: Callable[[dict, str, int], dict] | Callable[[dict, str], dict] | None = None,
    get_reddit_feeds: Callable[[int], dict] | None = None,
    manage_reddit_feeds: Callable[[dict, str, int], dict] | Callable[[dict, str], dict] | None = None,
    get_youtube_subscriptions: Callable[[int], dict] | None = None,
    manage_youtube_subscriptions: Callable[[dict, str, int], dict] | Callable[[dict, str], dict] | None = None,
    get_translate_channels: Callable[[int], dict] | None = None,
    manage_translate_channels: Callable[[dict, str, int], dict] | Callable[[dict, str], dict] | None = None,
    get_irc_bridges: Callable[[int], dict] | None = None,
    manage_irc_bridges: Callable[[dict, str, int], dict] | Callable[[dict, str], dict] | None = None,
    logger: logging.Logger | None = None,
) -> Flask:
    app = Flask(__name__)
    app.secret_key = os.getenv("WEB_ADMIN_SESSION_SECRET", "") or secrets.token_hex(32)
    secure_session_cookie = True
    session_cookie_samesite = "Lax"
    app.config.update(
        SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SAMESITE=session_cookie_samesite,
        SESSION_COOKIE_SECURE=secure_session_cookie,
        SESSION_REFRESH_EACH_REQUEST=True,
        PERMANENT_SESSION_LIFETIME=timedelta(days=REMEMBER_LOGIN_DAYS),
    )

    @app.after_request
    def apply_security_headers(response):
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Cache-Control"] = "no-store"
        return response

    def _render(page: str, body: str, email: str, is_admin: bool, **context):
        status = {}
        if callable(get_bot_snapshot):
            try:
                status = get_bot_snapshot() or {}
            except Exception:
                logger.exception("Failed to load bot snapshot for web GUI")
        return render_template_string(
            PAGE_TEMPLATE,
            page=page,
            title=context.get("title", page.replace("_", " ").title()),
            legacy_body=body,
            csrf_token=context.get("csrf_token", ""),
            session=session,
            status_summary=status,
            is_admin=is_admin,
            user_email=email,
        )

    def _current_user():
        user = session.get("user")
        if not user:
            return None
        return user

    def _require_login():
        user = _current_user()
        if not user:
            return redirect(url_for("login"))
        return None

    @app.route("/login", methods=["GET", "POST"])
    def login():
        if request.method == "POST":
            email = str(request.form.get("email", "") or "").strip().lower()
            password = str(request.form.get("password", "") or "")
            if not email or not password:
                flash("Email and password are required.", "danger")
                return redirect(url_for("login"))
            user = {
                "email": email,
                "is_admin": True,
                "display_name": email.split("@")[0],
            }
            session["user"] = user
            session.permanent = True
            flash("Signed in.", "success")
            return redirect(url_for("home"))
        body = ""
        return _render("login", body, "", False)

    @app.route("/logout", methods=["GET"])
    def logout():
        session.clear()
        flash("Signed out.", "success")
        return redirect(url_for("login"))

    @app.route("/")
    def home_redirect():
        return redirect(url_for("home"))

    @app.route("/admin/home")
    def home():
        user = _current_user()
        if not user:
            return redirect(url_for("login"))
        body = ""
        return _render("home", body, user.get("email", ""), bool(user.get("is_admin")))

    @app.route("/admin")
    def dashboard():
        user = _current_user()
        if not user:
            return redirect(url_for("login"))
        body = "<div class='card'><h2>Dashboard</h2><p class='muted'>Use Home for the command center.</p></div>"
        return _render("dashboard", body, user.get("email", ""), bool(user.get("is_admin")))

    @app.route("/admin/guilds", methods=["GET", "POST"])
    def guilds_page():
        user = _current_user()
        if not user:
            return redirect(url_for("login"))
        body = "<div class='card'><h2>Servers</h2><p class='muted'>Guild management is available in the Glinet bot web admin.</p></div>"
        return _render("guilds", body, user.get("email", ""), bool(user.get("is_admin")))

    @app.route("/admin/guild-settings", methods=["GET", "POST"])
    def guild_settings():
        user = _current_user()
        if not user:
            return redirect(url_for("login"))
        body = "<div class='card'><h2>Guild Settings</h2><p class='muted'>Guild settings are managed in the Glinet bot web admin.</p></div>"
        return _render("guild_settings", body, user.get("email", ""), bool(user.get("is_admin")))

    @app.route("/admin/command-permissions", methods=["GET", "POST"])
    def command_permissions():
        user = _current_user()
        if not user:
            return redirect(url_for("login"))
        body = "<div class='card'><h2>Command Permissions</h2><p class='muted'>Command permissions are managed in the Glinet bot web admin.</p></div>"
        return _render("command_permissions", body, user.get("email", ""), bool(user.get("is_admin")))

    @app.route("/admin/bot-profile", methods=["GET", "POST"])
    def bot_profile():
        user = _current_user()
        if not user:
            return redirect(url_for("login"))
        body = "<div class='card'><h2>Bot Profile</h2><p class='muted'>Bot profile management is available in the Glinet bot web admin.</p></div>"
        return _render("bot_profile", body, user.get("email", ""), bool(user.get("is_admin")))

    @app.route("/admin/account", methods=["GET", "POST"])
    def account():
        user = _current_user()
        if not user:
            return redirect(url_for("login"))
        body = "<div class='card'><h2>My Account</h2><p class='muted'>Account management is available in the Glinet bot web admin.</p></div>"
        return _render("account", body, user.get("email", ""), bool(user.get("is_admin")))

    @app.route("/admin/members", methods=["GET", "POST"])
    def members_page():
        user = _current_user()
        if not user:
            return redirect(url_for("login"))
        body = "<div class='card'><h2>Members</h2><p class='muted'>Member management is available in the Glinet bot web admin.</p></div>"
        return _render("members", body, user.get("email", ""), bool(user.get("is_admin")))

    @app.route("/admin/member-activity")
    def member_activity_page():
        user = _current_user()
        if not user:
            return redirect(url_for("login"))
        body = "<div class='card'><h2>Member Activity</h2><p class='muted'>Member activity is available in the Glinet bot web admin.</p></div>"
        return _render("member_activity", body, user.get("email", ""), bool(user.get("is_admin")))

    @app.route("/admin/moderation", methods=["GET", "POST"])
    def moderation_page():
        user = _current_user()
        if not user:
            return redirect(url_for("login"))
        body = "<div class='card'><h2>Moderation</h2><p class='muted'>Moderation settings are available in the Glinet bot web admin.</p></div>"
        return _render("moderation", body, user.get("email", ""), bool(user.get("is_admin")))

    @app.route("/admin/honeypot", methods=["GET", "POST"])
    def honeypot_page():
        user = _current_user()
        if not user:
            return redirect(url_for("login"))
        body = "<div class='card'><h2>Honeypot</h2><p class='muted'>Honeypot settings are available in the Glinet bot web admin.</p></div>"
        return _render("honeypot", body, user.get("email", ""), bool(user.get("is_admin")))

    @app.route("/admin/reddit", methods=["GET", "POST"])
    def reddit_feeds():
        user = _current_user()
        if not user:
            return redirect(url_for("login"))
        body = "<div class='card'><h2>Reddit Feeds</h2><p class='muted'>Reddit feeds are managed in the Glinet bot web admin.</p></div>"
        return _render("reddit_feeds", body, user.get("email", ""), bool(user.get("is_admin")))

    @app.route("/admin/youtube", methods=["GET", "POST"])
    def youtube_subscriptions():
        user = _current_user()
        if not user:
            return redirect(url_for("login"))
        body = "<div class='card'><h2>YouTube</h2><p class='muted'>YouTube subscriptions are managed in the Glinet bot web admin.</p></div>"
        return _render("youtube_subscriptions", body, user.get("email", ""), bool(user.get("is_admin")))

    @app.route("/admin/translate_channels", methods=["GET", "POST"])
    def translate_channels_page():
        user = _current_user()
        if not user:
            return redirect(url_for("login"))
        body = "<div class='card'><h2>Auto-Translate Channels</h2><p class='muted'>Translation mappings are managed in the Glinet bot web admin.</p></div>"
        return _render("translate_channels", body, user.get("email", ""), bool(user.get("is_admin")))

    @app.route("/admin/irc-bridge", methods=["GET", "POST"])
    def irc_bridge_page():
        user = _current_user()
        if not user:
            return redirect(url_for("login"))
        guild_id = int(request.args.get("guild_id") or 0)
        if guild_id <= 0:
            first_guild = None
            if callable(get_managed_guilds):
                try:
                    guilds = get_managed_guilds() or []
                except Exception:
                    guilds = []
                first_guild = next((g for g in guilds if g.get("id")), None)
            guild_id = int(first_guild.get("id") or 0) if first_guild else 0
        result = get_irc_bridges(guild_id) if guild_id > 0 and callable(get_irc_bridges) else {"ok": False, "error": "Select a server."}
        message_html = ""
        error_html = ""
        if request.method == "POST" and callable(manage_irc_bridges):
            action = str(request.form.get("action") or "").strip()
            payload = {
                "action": action,
                "id": request.form.get("id"),
                "server_id": request.form.get("server_id"),
                "irc_channel": request.form.get("irc_channel"),
                "discord_channel_id": request.form.get("discord_channel_id"),
                "enabled": request.form.get("enabled", "1"),
            }
            if action in {"create_entry", "update_entry"}:
                payload.update({
                    "server_name": request.form.get("server_name"),
                    "host": request.form.get("host"),
                    "port": request.form.get("port"),
                    "use_tls": request.form.get("use_tls", "0"),
                    "nickname": request.form.get("nickname"),
                    "password": request.form.get("password"),
                })
            post = manage_irc_bridges(payload, user.get("email", ""), guild_id)
            message = post.get("message")
            error = None if post.get("ok") else post.get("error")
            result = post if post.get("ok") else result
            if message:
                message_html = f"<div class='cc-flash cc-flash-success'>{message}</div>"
            if error:
                error_html = f"<div class='cc-flash cc-flash-danger'>{error}</div>"
        entries = result.get("entries", []) if isinstance(result, dict) else []
        servers = result.get("servers", []) if isinstance(result, dict) else []
        rows = []
        for entry in entries:
            server = next((s for s in servers if int(s.get("id") or 0) == int(entry.get("server_id") or 0)), {})
            rows.append(
                "<tr><td>"
                + str(server.get("name") or "")
                + "</td><td>"
                + str(entry.get("irc_channel") or "")
                + "</td><td>"
                + str(entry.get("discord_channel_id") or "")
                + "</td><td>"
                + ("Yes" if entry.get("enabled") else "No")
                + "</td><td>"
                + "<form method='post' class='d-inline'><input type='hidden' name='action' value='toggle_enabled'><input type='hidden' name='id' value='"
                + str(entry.get("id") or "")
                + "'><button class='btn btn-sm btn-secondary cc-btn-secondary' type='submit'>Toggle</button></form>"
                + "<form method='post' class='d-inline' onsubmit=\"return confirm('Delete mapping?')\"><input type='hidden' name='action' value='delete_entry'><input type='hidden' name='id' value='"
                + str(entry.get("id") or "")
                + "'><button class='btn btn-sm btn-danger cc-btn-danger' type='submit'>Delete</button></form>"
                + "</td></tr>"
            )
        body = (
            "<div class='card'>"
            "<h2>IRC Bridge</h2>"
            "<p class='muted'>Bridge IRC channels to Discord for guild <strong>#" + str(guild_id) + "</strong>.</p>"
            "<form method='post' class='row g-2 mb-3'>"
            "<input type='hidden' name='action' value='create_entry'>"
            "<div class='col-sm-3'><input class='form-control cc-form-control' name='server_name' placeholder='Server name' required></div>"
            "<div class='col-sm-2'><input class='form-control cc-form-control' name='host' placeholder='Host' required></div>"
            "<div class='col-sm-1'><input class='form-control cc-form-control' name='port' placeholder='Port' value='6667' required></div>"
            "<div class='col-sm-1'><input class='form-control cc-form-control' name='use_tls' placeholder='TLS 0/1' value='0' required></div>"
            "<div class='col-sm-2'><input class='form-control cc-form-control' name='nickname' placeholder='Nickname' required></div>"
            "<div class='col-sm-1'><input class='form-control cc-form-control' name='password' placeholder='Nickserv'></div>"
            "<div class='col-sm-2'><input class='form-control cc-form-control' name='irc_channel' placeholder='#irc-channel' required></div>"
            "<div class='col-sm-2'><input class='form-control cc-form-control' name='discord_channel_id' placeholder='Discord channel ID' required></div>"
            "<div class='col-sm-1 d-grid'><button class='btn btn-primary cc-btn' type='submit'>Add</button></div>"
            "</form>"
            + message_html
            + error_html
            + "<table class='table'>"
            "<thead><tr><th>Server</th><th>IRC</th><th>Discord</th><th>Enabled</th><th>Actions</th></tr></thead>"
            "<tbody>"
            + "".join(rows)
            + "</tbody>"
            "</table>"
            "</div>"
        )
        return _render("irc_bridge", body, user.get("email", ""), bool(user.get("is_admin")))

    @app.route("/admin/actions")
    def actions():
        user = _current_user()
        if not user:
            return redirect(url_for("login"))
        body = "<div class='card'><h2>Action Log</h2><p class='muted'>Action log is available in the Glinet bot web admin.</p></div>"
        return _render("actions", body, user.get("email", ""), bool(user.get("is_admin")))

    @app.route("/admin/logs", methods=["GET", "POST"])
    def logs():
        user = _current_user()
        if not user:
            return redirect(url_for("login"))
        body = "<div class='card'><h2>Logs</h2><p class='muted'>Logs viewer is available in the Glinet bot web admin.</p></div>"
        return _render("logs", body, user.get("email", ""), bool(user.get("is_admin")))

    @app.route("/status")
    def status_page():
        body = "<div class='card'><h2>Status</h2><p class='muted'>Public status is available in the Glinet bot web admin.</p></div>"
        return _render("status_public", body, "", False)

    return app


def start_gui2_web_admin_interface(
    host: str = "127.0.0.1",
    port: int = 8081,
    https_port: int = 8082,
    https_enabled: bool = False,
    data_dir: str = "data",
    env_file_path: str = ".env",
    tag_responses_file: str = "",
    default_admin_email: str = "",
    default_admin_password: str = "",
    get_bot_snapshot: Callable[[], dict] | None = None,
    get_managed_guilds: Callable[[], list[dict]] | None = None,
    get_notification_channels: Callable[[], dict] | None = None,
    get_discord_catalog: Callable[[str], dict] | None = None,
    get_command_permissions: Callable[[str], dict] | None = None,
    save_command_permissions: Callable[[str, dict], dict] | None = None,
    get_tag_responses: Callable[[], dict] | None = None,
    save_tag_responses: Callable[[dict], dict] | None = None,
    get_guild_settings: Callable[[str], dict] | None = None,
    save_guild_settings: Callable[[str, dict], dict] | None = None,
    get_bot_profile: Callable[[], dict] | None = None,
    update_bot_profile: Callable[[dict], dict] | None = None,
    update_bot_avatar: Callable[[bytes], dict] | None = None,
    kick_member: Callable[[str, str, str], dict] | None = None,
    ban_member: Callable[[str, str, str], dict] | None = None,
    timeout_member: Callable[[str, str, str, int], dict] | None = None,
    untimeout_member: Callable[[str, str, str], dict] | None = None,
    leave_guild: Callable[[str, int], dict] | None = None,
    request_restart: Callable[[str], dict] | None = None,
    get_member_activity: Callable[..., dict] | None = None,
    export_member_activity: Callable[..., dict] | None = None,
    pick_random_user: Callable[[str], dict] | None = None,
    get_honeypot: Callable[[int], dict] | None = None,
    manage_honeypot: Callable[[dict, str, int], dict] | Callable[[dict, str], dict] | None = None,
    get_role_access: Callable[[int], dict] | None = None,
    manage_role_access: Callable[[dict, str, int], dict] | Callable[[dict, str], dict] | None = None,
    get_reaction_roles: Callable[[int], dict] | None = None,
    manage_reaction_roles: Callable[[dict, str, int], dict] | Callable[[dict, str], dict] | None = None,
    get_reddit_feeds: Callable[[int], dict] | None = None,
    manage_reddit_feeds: Callable[[dict, str, int], dict] | Callable[[dict, str], dict] | None = None,
    get_youtube_subscriptions: Callable[[int], dict] | None = None,
    manage_youtube_subscriptions: Callable[[dict, str, int], dict] | Callable[[dict, str], dict] | None = None,
    get_translate_channels: Callable[[int], dict] | None = None,
    manage_translate_channels: Callable[[dict, str, int], dict] | Callable[[dict, str], dict] | None = None,
    get_irc_bridges: Callable[[int], dict] | None = None,
    manage_irc_bridges: Callable[[dict, str, int], dict] | Callable[[dict, str], dict] | None = None,
    logger: logging.Logger | None = None,
) -> threading.Thread:
    app = create_app(
        data_dir=data_dir,
        env_file_path=env_file_path,
        default_admin_email=default_admin_email,
        default_admin_password=default_admin_password,
        get_bot_snapshot=get_bot_snapshot,
        get_managed_guilds=get_managed_guilds,
        get_notification_channels=get_notification_channels,
        get_discord_catalog=get_discord_catalog,
        get_command_permissions=get_command_permissions,
        save_command_permissions=save_command_permissions,
        get_tag_responses=get_tag_responses,
        save_tag_responses=save_tag_responses,
        get_guild_settings=get_guild_settings,
        save_guild_settings=save_guild_settings,
        get_bot_profile=get_bot_profile,
        update_bot_profile=update_bot_profile,
        update_bot_avatar=update_bot_avatar,
        kick_member=kick_member,
        ban_member=ban_member,
        timeout_member=timeout_member,
        untimeout_member=untimeout_member,
        leave_guild=leave_guild,
        request_restart=request_restart,
        get_member_activity=get_member_activity,
        export_member_activity=export_member_activity,
        pick_random_user=pick_random_user,
        get_honeypot=get_honeypot,
        manage_honeypot=manage_honeypot,
        get_role_access=get_role_access,
        manage_role_access=manage_role_access,
        get_reaction_roles=get_reaction_roles,
        manage_reaction_roles=manage_reaction_roles,
        get_reddit_feeds=get_reddit_feeds,
        manage_reddit_feeds=manage_reddit_feeds,
        get_youtube_subscriptions=get_youtube_subscriptions,
        manage_youtube_subscriptions=manage_youtube_subscriptions,
        get_translate_channels=get_translate_channels,
        manage_translate_channels=manage_translate_channels,
        get_irc_bridges=get_irc_bridges,
        manage_irc_bridges=manage_irc_bridges,
        logger=logger,
    )

    def run() -> None:
        try:
            app.run(host=host, port=port, debug=False, use_reloader=False)
        except Exception:
            if logger:
                logger.exception("Web admin listener failed to start on %s:%s", host, port)

    thread = threading.Thread(target=run, daemon=True, name="web-admin-gui2")
    thread.start()
    return thread

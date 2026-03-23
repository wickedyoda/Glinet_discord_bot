import re
from pathlib import Path

from bs4 import BeautifulSoup

import web_admin
from web_admin import create_web_app


def _make_app(tmp_path: Path):
    env_file = tmp_path / "env.env"
    env_file.write_text(
        "\n".join(
            [
                "WEB_ENFORCE_CSRF=false",
                "WEB_ENFORCE_SAME_ORIGIN_POSTS=false",
                "WEB_TRUST_PROXY_HEADERS=true",
            ]
        )
    )

    def guilds():
        return {
            "ok": True,
            "guilds": [
                {
                    "id": "1234567890",
                    "name": "Test Guild",
                    "member_count": 42,
                    "icon_url": "",
                    "is_primary": True,
                }
            ],
            "primary_guild_id": "1234567890",
        }

    def catalog(guild_id):
        return {
            "ok": True,
            "guild": {"id": str(guild_id), "name": "Test Guild"},
            "channels": [
                {
                    "id": "9999",
                    "name": "alerts",
                    "type": "text",
                    "label": "#alerts [text]",
                }
            ],
            "roles": [
                {"id": "111", "name": "Member", "label": "@Member"},
                {"id": "222", "name": "Employee", "label": "@Employee"},
            ],
        }

    guild_settings_state = {
        "bot_log_channel_id": "",
        "mod_log_channel_id": "",
        "firmware_notify_channel_id": "",
        "access_role_id": "",
    }

    def get_guild_settings(guild_id):
        settings = dict(guild_settings_state)
        return {
            "ok": True,
            "guild_id": str(guild_id),
            "settings": settings,
            "effective": settings,
        }

    def save_guild_settings(payload, actor_email, guild_id):
        guild_settings_state.update(
            {
                "bot_log_channel_id": str(payload.get("bot_log_channel_id") or "").strip(),
                "mod_log_channel_id": str(payload.get("mod_log_channel_id") or "").strip(),
                "firmware_notify_channel_id": str(payload.get("firmware_notify_channel_id") or "").strip(),
                "access_role_id": str(payload.get("access_role_id") or "").strip(),
            }
        )
        settings = dict(guild_settings_state)
        return {
            "ok": True,
            "guild_id": str(guild_id),
            "message": f"Guild settings updated by {actor_email}.",
            "settings": settings,
            "effective": settings,
        }

    bot_profile_updates = []
    bot_profile_state = {
        "id": "1478110480806576259",
        "name": "GL.iNet UnOfficial Discord Bot",
        "display_name": "GL.iNet UnOfficial Discord Bot",
        "global_name": "GL.iNet UnOfficial Discord Bot",
        "guild_id": "1234567890",
        "guild_name": "Test Guild",
        "server_display_name": "GL.iNet UnOfficial Discord Bot",
        "server_nickname": "",
        "avatar_url": "",
    }

    def get_bot_profile(guild_id):
        return {"ok": True, **bot_profile_state, "guild_id": str(guild_id), "guild_name": "Test Guild"}

    def update_bot_profile(guild_id, username, server_nickname, clear_server_nickname, actor_email):
        bot_profile_updates.append(
            {
                "guild_id": str(guild_id),
                "username": username,
                "server_nickname": server_nickname,
                "clear_server_nickname": clear_server_nickname,
                "actor_email": actor_email,
            }
        )
        if username is not None:
            bot_profile_state["name"] = username
            bot_profile_state["display_name"] = username
            bot_profile_state["global_name"] = username
            bot_profile_state["server_display_name"] = bot_profile_state.get("server_nickname") or username
            return {
                "ok": True,
                "message": "Updated username.",
                **bot_profile_state,
            }
        if clear_server_nickname:
            bot_profile_state["server_nickname"] = ""
        elif server_nickname is not None:
            bot_profile_state["server_nickname"] = server_nickname
        bot_profile_state["server_display_name"] = bot_profile_state.get("server_nickname") or bot_profile_state["name"]
        return {
            "ok": True,
            "message": "Updated server nickname.",
            **bot_profile_state,
        }

    app = create_web_app(
        data_dir=str(tmp_path),
        env_file_path=str(env_file),
        tag_responses_file=str(tmp_path / "tags.json"),
        default_admin_email="admin@example.com",
        default_admin_password="Ab!12xy",
        on_get_guilds=guilds,
        on_get_discord_catalog=catalog,
        on_get_guild_settings=get_guild_settings,
        on_save_guild_settings=save_guild_settings,
        on_get_bot_profile=get_bot_profile,
        on_update_bot_profile=update_bot_profile,
        on_get_actions=lambda guild_id: {
            "ok": True,
            "actions": [
                {
                    "created_at": "2026-03-15T00:00:00+00:00",
                    "action": "test_action",
                    "status": "success",
                    "moderator": "admin@example.com",
                    "target": "target",
                    "reason": "reason",
                }
            ],
        },
        on_get_member_activity=lambda guild_id, role_id=None: {
            "ok": True,
            "top_limit": 20,
            "selected_role_id": int(role_id or 0),
            "excluded_role_ids": [],
            "excluded_role_names": ["Employee", "Admin", "Gl.iNet Moderator"],
            "windows": [
                {
                    "key": "last_90_days",
                    "label": "Last 90 Days",
                    "members": [
                        {
                            "rank": 1,
                            "display_name": "Tester",
                            "username": "tester",
                            "message_count": 123,
                            "active_days": 14,
                            "last_message_at": "2026-03-15T00:00:00+00:00",
                        }
                    ],
                }
            ],
        },
        on_export_member_activity=lambda guild_id, role_id=None: {
            "ok": True,
            "filename": "member_activity_test.zip",
            "content_type": "application/zip",
            "data": b"PK\x05\x06" + (b"\x00" * 18),
        },
        on_get_reddit_feeds=lambda guild_id: {"ok": True, "feeds": []},
        on_get_command_permissions=lambda guild_id: {
            "ok": True,
            "commands": [
                {
                    "key": "ping",
                    "label": "/ping",
                    "description": "Check that the bot is online.",
                    "default_policy": "public",
                    "default_policy_label": "Public",
                    "mode": "disabled",
                    "role_ids": [],
                },
                {
                    "key": "ban_member",
                    "label": "/ban_member",
                    "description": "Ban a member from the server.",
                    "default_policy": "moderator_role_ids",
                    "default_policy_label": "Mod Only",
                    "mode": "default",
                    "role_ids": [],
                },
                {
                    "key": "help",
                    "label": "/help",
                    "description": "Show command help.",
                    "default_policy": "public",
                    "default_policy_label": "Public",
                    "mode": "public",
                    "role_ids": [],
                }
            ],
            "allowed_role_names": ["Employee"],
            "moderator_role_ids": [123],
        },
        on_save_command_permissions=lambda payload, actor_email, guild_id: {
            "ok": True,
            "message": "Command permissions updated.",
            "commands": [
                {
                    "key": "ping",
                    "label": "/ping",
                    "description": "Check that the bot is online.",
                    "default_policy": "public",
                    "default_policy_label": "Public",
                    "mode": payload.get("commands", {}).get("ping", {}).get("mode", "default"),
                    "role_ids": [],
                },
                {
                    "key": "ban_member",
                    "label": "/ban_member",
                    "description": "Ban a member from the server.",
                    "default_policy": "moderator_role_ids",
                    "default_policy_label": "Mod Only",
                    "mode": payload.get("commands", {}).get("ban_member", {}).get("mode", "default"),
                    "role_ids": [],
                },
                {
                    "key": "help",
                    "label": "/help",
                    "description": "Show command help.",
                    "default_policy": "public",
                    "default_policy_label": "Public",
                    "mode": payload.get("commands", {}).get("help", {}).get("mode", "public"),
                    "role_ids": [],
                },
            ],
            "allowed_role_names": ["Employee"],
            "moderator_role_ids": [123],
        },
        on_get_youtube_subscriptions=lambda guild_id: {
            "ok": True,
            "subscriptions": [],
        },
        on_get_linkedin_subscriptions=lambda guild_id: {
            "ok": True,
            "subscriptions": [],
        },
        on_get_beta_program_subscriptions=lambda guild_id: {
            "ok": True,
            "source_url": "https://www.gl-inet.com/beta-testing/#register",
            "subscriptions": [],
        },
        on_leave_guild=lambda guild_id, actor_email: {
            "ok": True,
            "message": f"Bot left guild {guild_id} by {actor_email}.",
        },
    )
    app.config["TESTING"] = True
    app.config["BOT_PROFILE_UPDATES"] = bot_profile_updates
    return app


def _login(client):
    return _login_as(client, "admin@example.com", "Ab!12xy")


def _login_as(client, email: str, password: str):
    login_page = client.get("/login", base_url="https://docker.example:8443")
    assert login_page.status_code == 200
    html = login_page.get_data(as_text=True)
    match = re.search(r'<meta name="csrf-token" content="([^"]+)"', html)
    assert match is not None
    csrf_token = match.group(1)
    response = client.post(
        "/login",
        data={"email": email, "password": password},
        base_url="https://docker.example:8443",
        headers={"X-CSRF-Token": csrf_token},
        follow_redirects=True,
    )
    assert response.status_code == 200
    return response


def _extract_csrf_token(response):
    html = response.get_data(as_text=True)
    match = re.search(r'<meta name="csrf-token" content="([^"]+)"', html)
    assert match is not None
    return match.group(1)


def _select_guild(client):
    admin_page = client.get("/admin", base_url="https://docker.example:8443")
    assert admin_page.status_code == 200
    csrf_token = _extract_csrf_token(admin_page)
    response = client.post(
        "/admin/select-guild",
        data={"guild_id": "1234567890"},
        base_url="https://docker.example:8443",
        headers={"X-CSRF-Token": csrf_token},
        follow_redirects=True,
    )
    assert response.status_code == 200
    return response


def _page_csrf_token(client, path: str):
    response = client.get(path, base_url="https://docker.example:8443")
    assert response.status_code == 200
    return _extract_csrf_token(response)


def _form_payload(client, path: str):
    response = client.get(path, base_url="https://docker.example:8443")
    assert response.status_code == 200
    soup = BeautifulSoup(response.get_data(as_text=True), "html.parser")
    payload = {}
    for input_tag in soup.select("form input[name]"):
        name = input_tag.get("name", "")
        input_type = (input_tag.get("type") or "text").lower()
        if input_type in {"submit", "button", "file"}:
            continue
        payload[name] = input_tag.get("value", "")
    for select_tag in soup.select("form select[name]"):
        selected_option = select_tag.find("option", selected=True)
        if selected_option is None:
            selected_option = select_tag.find("option")
        payload[select_tag.get("name", "")] = selected_option.get("value", "") if selected_option else ""
    return payload


def test_healthz_route(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()

    response = client.get("/healthz")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["ok"] is True


def test_security_headers_depend_on_https(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()

    insecure_response = client.get("/healthz", base_url="http://docker.example:8080")
    secure_response = client.get("/healthz", base_url="https://docker.example:8443")

    assert insecure_response.status_code == 200
    assert secure_response.status_code == 200
    assert "Cross-Origin-Opener-Policy" not in insecure_response.headers
    assert secure_response.headers.get("Cross-Origin-Opener-Policy") == "same-origin"
    assert "Strict-Transport-Security" in secure_response.headers


def test_login_and_selected_guild_pages(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)

    with client.session_transaction() as session:
        session["selected_guild_id"] = "1234567890"

    for path in [
        "/admin",
        "/admin/dashboard",
        "/admin/guild-settings",
        "/admin/settings",
        "/admin/users",
        "/admin/actions",
        "/admin/member-activity",
        "/admin/youtube",
        "/admin/linkedin",
        "/admin/beta-programs",
        "/admin/documentation",
        "/admin/wiki",
        "/status",
        "/status/everything",
    ]:
        response = client.get(path, base_url="https://docker.example:8443", follow_redirects=True)
        assert response.status_code == 200, path

    header_response = client.get("/admin/dashboard", base_url="https://docker.example:8443", follow_redirects=True)
    assert b">Logout<" in header_response.data


def test_staus_redirects_to_status(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()

    response = client.get("/staus", base_url="https://docker.example:8443", follow_redirects=False)

    assert response.status_code == 302
    assert response.headers["Location"].endswith("/status")


def test_actions_page_renders_history(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    response = client.get("/admin/actions", base_url="https://docker.example:8443")

    assert response.status_code == 200
    assert b"test_action" in response.data


def test_admin_can_remove_bot_from_guild(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)

    response = client.post(
        "/admin/leave-guild",
        data={
            "guild_id": "1234567890",
            "confirm": "yes",
            "csrf_token": _page_csrf_token(client, "/admin"),
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"Bot left guild 1234567890 by admin@example.com." in response.data


def test_youtube_page_renders_form(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    response = client.get("/admin/youtube", base_url="https://docker.example:8443")

    assert response.status_code == 200
    assert b"YouTube Subscriptions" in response.data
    assert b"Save Subscription" in response.data


def test_linkedin_page_renders_form(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    response = client.get("/admin/linkedin", base_url="https://docker.example:8443")

    assert response.status_code == 200
    assert b"LinkedIn Profiles" in response.data
    assert b"Save Subscription" in response.data


def test_beta_program_page_renders_form(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    response = client.get("/admin/beta-programs", base_url="https://docker.example:8443")

    assert response.status_code == 200
    assert b"GL.iNet Beta Programs" in response.data
    assert b"Save Monitor" in response.data


def test_bot_profile_nickname_update_does_not_attempt_global_username_change(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    response = client.post(
        "/admin/bot-profile",
        data={
            "csrf_token": _page_csrf_token(client, "/admin/bot-profile"),
            "action": "nickname",
            "server_nickname": "Guild Helper",
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"Updated server nickname." in response.data
    assert b"Failed to update username" not in response.data
    assert app.config["BOT_PROFILE_UPDATES"][-1]["username"] is None
    assert app.config["BOT_PROFILE_UPDATES"][-1]["server_nickname"] == "Guild Helper"


def test_bot_profile_global_username_requires_separate_action(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    response = client.post(
        "/admin/bot-profile",
        data={
            "csrf_token": _page_csrf_token(client, "/admin/bot-profile"),
            "action": "username",
            "bot_name": "Renamed Bot",
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"Updated username." in response.data
    assert app.config["BOT_PROFILE_UPDATES"][-1]["username"] == "Renamed Bot"
    assert app.config["BOT_PROFILE_UPDATES"][-1]["server_nickname"] is None


def test_member_activity_page_renders_tables(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    response = client.get("/admin/member-activity", base_url="https://docker.example:8443")

    assert response.status_code == 200
    assert b"Member Activity" in response.data
    assert b"Last 90 Days" in response.data
    assert b"Tester" in response.data
    assert b"Download Activity Export" in response.data
    assert b"Top 20 by role" in response.data
    assert b"All eligible members" in response.data
    assert b"@Member" in response.data
    assert b"@Employee" not in response.data


def test_member_activity_export_downloads_zip(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    response = client.get("/admin/member-activity/export", base_url="https://docker.example:8443")

    assert response.status_code == 200
    assert response.headers.get("Content-Type", "").startswith("application/zip")
    assert "attachment;" in response.headers.get("Content-Disposition", "")
    assert response.data.startswith(b"PK")


def test_command_permissions_page_supports_disabled_mode(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    response = client.get("/admin/command-permissions", base_url="https://docker.example:8443")

    assert response.status_code == 200
    assert b"Command Permissions" in response.data
    assert b"Disabled" in response.data
    assert b'value="disabled" selected' in response.data


def test_dashboard_shows_command_status_for_selected_guild(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    response = client.get("/admin/dashboard", base_url="https://docker.example:8443")

    assert response.status_code == 200
    assert b"Command Status" in response.data
    assert b"/ping" in response.data
    assert b"/ban_member" in response.data
    assert b"/help" in response.data
    assert b"Disabled" in response.data
    assert b"Mod Only" in response.data
    assert b"Enabled" in response.data


def test_admin_can_save_guild_settings(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    payload = _form_payload(client, "/admin/guild-settings")
    payload.update(
        {
            "bot_log_channel_id": "9999",
            "mod_log_channel_id": "9999",
            "firmware_notify_channel_id": "9999",
            "access_role_id": "111",
        }
    )

    response = client.post(
        "/admin/guild-settings",
        data=payload,
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"Guild settings updated by admin@example.com." in response.data
    assert b"Current value (not found): 9999" not in response.data


def test_admin_can_save_global_settings(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    payload = _form_payload(client, "/admin/settings")
    payload["WEB_SESSION_TIMEOUT_MINUTES"] = "90"
    payload["WEB_RESTART_ENABLED"] = "true"
    payload["WEB_SESSION_COOKIE_SECURE"] = "true"
    payload["WEB_TRUST_PROXY_HEADERS"] = "true"
    payload["WEB_ENFORCE_CSRF"] = "false"
    payload["WEB_ENFORCE_SAME_ORIGIN_POSTS"] = "false"
    payload["WEB_HARDEN_FILE_PERMISSIONS"] = "true"
    payload["WEB_HTTPS_ENABLED"] = "true"

    response = client.post(
        "/admin/settings",
        data=payload,
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"Settings saved to" in response.data
    assert b"90 minutes" in response.data


def test_settings_post_handles_read_only_env_file(tmp_path: Path, monkeypatch):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    payload = _form_payload(client, "/admin/settings")
    payload["WEB_SESSION_TIMEOUT_MINUTES"] = "120"
    payload["WEB_RESTART_ENABLED"] = "true"
    payload["WEB_SESSION_COOKIE_SECURE"] = "true"
    payload["WEB_TRUST_PROXY_HEADERS"] = "true"
    payload["WEB_ENFORCE_CSRF"] = "false"
    payload["WEB_ENFORCE_SAME_ORIGIN_POSTS"] = "false"
    payload["WEB_HARDEN_FILE_PERMISSIONS"] = "true"
    payload["WEB_HTTPS_ENABLED"] = "true"

    original_write_env_file = web_admin._write_env_file

    def _raise_read_only_for_primary(env_file, values):
        if Path(env_file) == tmp_path / "env.env":
            raise OSError(30, "Read-only file system")
        return original_write_env_file(env_file, values)

    monkeypatch.setattr(web_admin, "_write_env_file", _raise_read_only_for_primary)

    response = client.post(
        "/admin/settings",
        data=payload,
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"Settings saved to fallback env file" in response.data
    assert b"web-settings.env" in response.data
    fallback_env = tmp_path / "web-settings.env"
    assert fallback_env.exists()
    assert "WEB_SESSION_TIMEOUT_MINUTES=120" in fallback_env.read_text()


def test_settings_fallback_env_file_excludes_sensitive_keys(tmp_path: Path, monkeypatch):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    payload = _form_payload(client, "/admin/settings")
    payload["DISCORD_TOKEN"] = "new-token-value"
    payload["WEB_ADMIN_DEFAULT_PASSWORD"] = "Stronger!234"
    payload["WEB_ADMIN_SESSION_SECRET"] = "new-session-secret"
    payload["WEB_SESSION_TIMEOUT_MINUTES"] = "120"
    payload["WEB_ENFORCE_CSRF"] = "false"
    payload["WEB_ENFORCE_SAME_ORIGIN_POSTS"] = "false"

    original_write_env_file = web_admin._write_env_file

    def _raise_read_only_for_primary(env_file, values):
        if Path(env_file) == tmp_path / "env.env":
            raise OSError(30, "Read-only file system")
        return original_write_env_file(env_file, values)

    monkeypatch.setattr(web_admin, "_write_env_file", _raise_read_only_for_primary)

    response = client.post(
        "/admin/settings",
        data=payload,
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"Sensitive settings were not written to the fallback env file" in response.data
    fallback_env = tmp_path / "web-settings.env"
    fallback_text = fallback_env.read_text()
    assert "DISCORD_TOKEN=" not in fallback_text
    assert "WEB_ADMIN_DEFAULT_PASSWORD=" not in fallback_text
    assert "WEB_ADMIN_SESSION_SECRET=" not in fallback_text


def test_load_effective_env_values_ignores_sensitive_fallback_overrides(tmp_path: Path):
    primary_env = tmp_path / "env.env"
    fallback_env = tmp_path / "web-settings.env"
    primary_env.write_text(
        "DISCORD_TOKEN=primary-token\n"
        "WEB_ADMIN_DEFAULT_PASSWORD=primary-password\n"
        "WEB_ADMIN_SESSION_SECRET=primary-secret\n"
        "LOG_LEVEL=INFO\n"
    )
    fallback_env.write_text(
        "DISCORD_TOKEN=fallback-token\n"
        "WEB_ADMIN_DEFAULT_PASSWORD=fallback-password\n"
        "WEB_ADMIN_SESSION_SECRET=fallback-secret\n"
        "LOG_LEVEL=DEBUG\n"
    )

    effective = web_admin._load_effective_env_values(primary_env, fallback_env)

    assert effective["DISCORD_TOKEN"] == "primary-token"
    assert effective["WEB_ADMIN_DEFAULT_PASSWORD"] == "primary-password"
    assert effective["WEB_ADMIN_SESSION_SECRET"] == "primary-secret"
    assert effective["LOG_LEVEL"] == "DEBUG"


def test_reddit_schedule_handles_read_only_env_file(tmp_path: Path, monkeypatch):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    payload = {
        "csrf_token": _page_csrf_token(client, "/admin/reddit-feeds"),
        "action": "schedule",
        "reddit_feed_schedule": "*/10 * * * *",
    }

    original_write_env_file = web_admin._write_env_file

    def _raise_read_only_for_primary(env_file, values):
        if Path(env_file) == tmp_path / "env.env":
            raise OSError(30, "Read-only file system")
        return original_write_env_file(env_file, values)

    monkeypatch.setattr(web_admin, "_write_env_file", _raise_read_only_for_primary)

    response = client.post(
        "/admin/reddit-feeds",
        data=payload,
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"saved to fallback env file" in response.data
    assert b"web-settings.env" in response.data
    fallback_env = tmp_path / "web-settings.env"
    assert fallback_env.exists()
    assert "REDDIT_FEED_CHECK_SCHEDULE=\"*/10 * * * *\"" in fallback_env.read_text()


def test_admin_can_edit_user_and_reset_password(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)

    create_response = client.post(
        "/admin/users",
        data={
            "action": "create",
            "csrf_token": _page_csrf_token(client, "/admin/users"),
            "first_name": "Target",
            "last_name": "User",
            "display_name": "Target User",
            "email": "target@example.com",
            "password": "Ab!12xy",
            "confirm_password": "Ab!12xy",
            "role": "read_only",
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )
    assert create_response.status_code == 200
    assert b"target@example.com" in create_response.data

    edit_response = client.post(
        "/admin/users",
        data={
            "action": "edit_user",
            "csrf_token": _page_csrf_token(client, "/admin/users"),
            "email": "target@example.com",
            "updated_email": "target-renamed@example.com",
            "first_name": "Target",
            "last_name": "Updated",
            "display_name": "Renamed User",
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )
    assert edit_response.status_code == 200
    assert b"target-renamed@example.com" in edit_response.data
    assert b"Renamed User" in edit_response.data

    password_response = client.post(
        "/admin/users",
        data={
            "action": "password",
            "csrf_token": _page_csrf_token(client, "/admin/users"),
            "email": "target-renamed@example.com",
            "password": "Ab!12xy",
            "confirm_password": "Ab!12xy",
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )
    assert password_response.status_code == 200
    assert b"target-renamed@example.com" in password_response.data


def test_glinet_read_only_role_is_pinned_to_primary_guild(tmp_path: Path):
    app = _make_app(tmp_path)
    admin_client = app.test_client()
    _login(admin_client)

    create_response = admin_client.post(
        "/admin/users",
        data={
            "action": "create",
            "csrf_token": _page_csrf_token(admin_client, "/admin/users"),
            "first_name": "Glinet",
            "last_name": "Viewer",
            "display_name": "Glinet Viewer",
            "email": "glinet@example.com",
            "password": "Ab!12xy",
            "confirm_password": "Ab!12xy",
            "role": "glinet_read_only",
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )
    assert create_response.status_code == 200
    assert b"glinet@example.com" in create_response.data
    assert b"Glinet-Read-Only" in create_response.data

    client = app.test_client()
    _login_as(client, "glinet@example.com", "Ab!12xy")

    guild_route_response = client.get(
        "/admin",
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )
    assert guild_route_response.status_code == 200
    assert b"Dashboard" in guild_route_response.data
    assert b"Command Status" in guild_route_response.data
    assert b"Test Guild" in guild_route_response.data

    member_activity_response = client.get("/admin/member-activity", base_url="https://docker.example:8443")
    assert member_activity_response.status_code == 200
    assert b"Member Activity" in member_activity_response.data
    assert b">Logout<" in member_activity_response.data

    dashboard_response = client.get(
        "/admin/dashboard",
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )
    assert dashboard_response.status_code == 200
    assert b"Dashboard" in dashboard_response.data
    assert b"Glinet-Read-Only account" in dashboard_response.data

    settings_response = client.get(
        "/admin/settings",
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )
    assert settings_response.status_code == 200
    assert b"GL.iNet-scoped access is limited to the primary GL.iNet Community Discord server." in settings_response.data
    assert b"Dashboard" in settings_response.data


def test_glinet_rw_role_can_edit_only_primary_guild_scoped_settings(tmp_path: Path):
    app = _make_app(tmp_path)
    admin_client = app.test_client()
    _login(admin_client)

    create_response = admin_client.post(
        "/admin/users",
        data={
            "action": "create",
            "csrf_token": _page_csrf_token(admin_client, "/admin/users"),
            "first_name": "Glinet",
            "last_name": "Editor",
            "display_name": "Glinet Editor",
            "email": "glinet-rw@example.com",
            "password": "Ab!12xy",
            "confirm_password": "Ab!12xy",
            "role": "glinet_rw",
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )
    assert create_response.status_code == 200
    assert b"glinet-rw@example.com" in create_response.data
    assert b"Glinet-RW" in create_response.data

    client = app.test_client()
    _login_as(client, "glinet-rw@example.com", "Ab!12xy")

    guild_settings_response = client.post(
        "/admin/guild-settings",
        data={
            "bot_log_channel_id": "9999",
            "mod_log_channel_id": "",
            "firmware_notify_channel_id": "",
            "access_role_id": "111",
            "csrf_token": _page_csrf_token(client, "/admin/guild-settings"),
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )
    assert guild_settings_response.status_code == 200
    assert b"Guild settings updated by glinet-rw@example.com." in guild_settings_response.data

    global_settings_response = client.post(
        "/admin/settings",
        data={
            "WEB_PORT": "8080",
            "WEB_HTTPS_PORT": "8081",
            "WEB_SESSION_TIMEOUT_MINUTES": "30",
            "csrf_token": _page_csrf_token(client, "/admin/dashboard"),
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )
    assert global_settings_response.status_code == 200
    assert b"GL.iNet-scoped access is limited to the primary GL.iNet Community Discord server." in global_settings_response.data
    assert b"Dashboard" in global_settings_response.data

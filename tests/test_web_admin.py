import re
from pathlib import Path

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

    app = create_web_app(
        data_dir=str(tmp_path),
        env_file_path=str(env_file),
        tag_responses_file=str(tmp_path / "tags.json"),
        default_admin_email="admin@example.com",
        default_admin_password="Ab!12xy",
        on_get_guilds=guilds,
        on_get_discord_catalog=catalog,
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
                }
            ],
            "allowed_role_names": ["Employee"],
            "moderator_role_ids": [123],
        },
        on_get_youtube_subscriptions=lambda guild_id: {
            "ok": True,
            "subscriptions": [],
        },
    )
    app.config["TESTING"] = True
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
        "/admin/users",
        "/admin/actions",
        "/admin/member-activity",
        "/admin/youtube",
        "/admin/documentation",
        "/admin/wiki",
        "/status/everything",
    ]:
        response = client.get(path, base_url="https://docker.example:8443", follow_redirects=True)
        assert response.status_code == 200, path


def test_actions_page_renders_history(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    response = client.get("/admin/actions", base_url="https://docker.example:8443")

    assert response.status_code == 200
    assert b"test_action" in response.data


def test_youtube_page_renders_form(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    response = client.get("/admin/youtube", base_url="https://docker.example:8443")

    assert response.status_code == 200
    assert b"YouTube Subscriptions" in response.data
    assert b"Save Subscription" in response.data


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


def test_glinet_role_is_limited_to_member_activity(tmp_path: Path):
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
            "role": "glinet",
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )
    assert create_response.status_code == 200
    assert b"glinet@example.com" in create_response.data
    assert b"Glinet" in create_response.data

    client = app.test_client()
    _login_as(client, "glinet@example.com", "Ab!12xy")
    _select_guild(client)

    member_activity_response = client.get("/admin/member-activity", base_url="https://docker.example:8443")
    assert member_activity_response.status_code == 200
    assert b"Member Activity" in member_activity_response.data

    dashboard_response = client.get(
        "/admin/dashboard",
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )
    assert dashboard_response.status_code == 200
    assert b"Member Activity" in dashboard_response.data
    assert b"Glinet access is limited to member activity only." in dashboard_response.data

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
            "roles": [],
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
        on_get_reddit_feeds=lambda guild_id: {"ok": True, "feeds": []},
        on_get_youtube_subscriptions=lambda guild_id: {
            "ok": True,
            "subscriptions": [],
        },
    )
    app.config["TESTING"] = True
    return app


def _login(client):
    login_page = client.get("/login", base_url="https://docker.example:8443")
    assert login_page.status_code == 200
    html = login_page.get_data(as_text=True)
    match = re.search(r'<meta name="csrf-token" content="([^"]+)"', html)
    assert match is not None
    csrf_token = match.group(1)
    response = client.post(
        "/login",
        data={"email": "admin@example.com", "password": "Ab!12xy"},
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
        "/admin/actions",
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

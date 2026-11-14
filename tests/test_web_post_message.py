"""Post a message to a channel as the bot: the page, the gate, and the sender."""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import discord
from bs4 import BeautifulSoup

from tests.test_web_admin import _login, _login_as, _make_app, _select_guild
from tests.test_web_permission_groups import _create_account

BASE_URL = "https://docker.example:8443"


def _logged_in_client(tmp_path: Path, post_message=None):
    app = _make_app(tmp_path, post_message=post_message)
    client = app.test_client()
    _login(client)
    _select_guild(client)
    return app, client


def _csrf(client, path: str) -> str:
    page = client.get(path, base_url=BASE_URL)
    assert page.status_code == 200
    soup = BeautifulSoup(page.data, "html.parser")
    token = soup.find("meta", attrs={"name": "csrf-token"})
    assert token is not None
    return str(token.get("content") or "")


# --- the page -------------------------------------------------------------


def test_page_renders_the_composer(tmp_path: Path):
    _app, client = _logged_in_client(tmp_path)

    response = client.get("/admin/post-message", base_url=BASE_URL)

    assert response.status_code == 200
    soup = BeautifulSoup(response.data, "html.parser")
    assert soup.find("textarea", attrs={"name": "content"}) is not None
    channel_select = soup.find("select", attrs={"name": "channel_id"})
    assert channel_select is not None
    # only channels that can receive a message
    labels = [o.get_text(strip=True) for o in channel_select.find_all("option")]
    assert any("alerts" in label for label in labels)
    assert soup.find("input", attrs={"name": "confirm", "type": "checkbox"}) is not None
    assert "Post message" in soup.get_text(" ", strip=True)


def test_page_is_admin_only(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)
    _create_account(client, "poster-viewer@example.com", "read_only")

    viewer = app.test_client()
    _login_as(viewer, "poster-viewer@example.com", "Ab!12xy")
    response = viewer.get("/admin/post-message", base_url=BASE_URL)

    assert response.status_code == 302
    assert response.headers["Location"].endswith("/admin/dashboard")


def test_page_requires_login(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()

    response = client.get("/admin/post-message", base_url=BASE_URL)

    assert response.status_code == 302
    assert "/login" in response.headers["Location"]


# --- posting --------------------------------------------------------------


def test_posting_calls_the_sender_with_channel_and_content(tmp_path: Path):
    calls = []

    def fake_post(payload, actor_email, guild_id):
        calls.append((payload, actor_email, guild_id))
        return {"ok": True, "channel_name": "alerts", "message_id": 42}

    _app, client = _logged_in_client(tmp_path, post_message=fake_post)
    token = _csrf(client, "/admin/post-message")

    response = client.post(
        "/admin/post-message",
        data={
            "channel_id": "9999",
            "content": "  Maintenance at 22:00 UTC.  ",
            "confirm": "yes",
            "csrf_token": token,
        },
        base_url=BASE_URL,
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert len(calls) == 1
    payload, actor_email, guild_id = calls[0]
    assert payload == {"channel_id": "9999", "content": "Maintenance at 22:00 UTC."}
    assert actor_email == "admin@example.com"
    assert guild_id == "1234567890"
    assert "Posted to #alerts as the bot." in response.get_data(as_text=True)


def test_unconfirmed_post_never_reaches_discord(tmp_path: Path):
    calls = []
    _app, client = _logged_in_client(
        tmp_path, post_message=lambda payload, actor, guild: calls.append(payload)
    )
    token = _csrf(client, "/admin/post-message")

    response = client.post(
        "/admin/post-message",
        data={"channel_id": "9999", "content": "hello", "csrf_token": token},
        base_url=BASE_URL,
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert calls == []
    assert "Tick the confirmation before posting." in response.get_data(as_text=True)


def test_empty_message_is_refused_before_sending(tmp_path: Path):
    calls = []
    _app, client = _logged_in_client(
        tmp_path, post_message=lambda payload, actor, guild: calls.append(payload)
    )
    token = _csrf(client, "/admin/post-message")

    response = client.post(
        "/admin/post-message",
        data={"channel_id": "9999", "content": "   ", "confirm": "yes", "csrf_token": token},
        base_url=BASE_URL,
        follow_redirects=True,
    )

    assert calls == []
    assert "The message cannot be empty." in response.get_data(as_text=True)


def test_missing_channel_is_refused_before_sending(tmp_path: Path):
    calls = []
    _app, client = _logged_in_client(
        tmp_path, post_message=lambda payload, actor, guild: calls.append(payload)
    )
    token = _csrf(client, "/admin/post-message")

    response = client.post(
        "/admin/post-message",
        data={"channel_id": "", "content": "hello", "confirm": "yes", "csrf_token": token},
        base_url=BASE_URL,
        follow_redirects=True,
    )

    assert calls == []
    assert "Choose a channel to post to." in response.get_data(as_text=True)


def test_sender_error_is_shown_to_the_admin(tmp_path: Path):
    _app, client = _logged_in_client(
        tmp_path,
        post_message=lambda payload, actor, guild: {
            "ok": False,
            "error": "I do not have permission to post in #alerts.",
        },
    )
    token = _csrf(client, "/admin/post-message")

    response = client.post(
        "/admin/post-message",
        data={"channel_id": "9999", "content": "hello", "confirm": "yes", "csrf_token": token},
        base_url=BASE_URL,
        follow_redirects=True,
    )

    assert "I do not have permission to post in #alerts." in response.get_data(as_text=True)


# --- the sender in bot.py -------------------------------------------------


class _FakeChannel:
    def __init__(self, *, name="alerts", channel_id=9999, can_send=True):
        self.name = name
        self.id = channel_id
        self.sent = []
        self._can_send = can_send

    def permissions_for(self, _member):
        return SimpleNamespace(send_messages=self._can_send)

    async def send(self, content):
        self.sent.append(content)
        return SimpleNamespace(id=4242, jump_url="https://discord.com/channels/1/2/4242")


def _fake_guild(channel):
    async def fetch_channel(_channel_id):
        raise discord.NotFound(SimpleNamespace(status=404, reason="Not Found"), "Unknown Channel")

    return SimpleNamespace(
        id=1234567890,
        me=object(),
        get_channel=lambda channel_id: channel,
        fetch_channel=fetch_channel,
    )


def _run(monkeypatch, guild, payload, actor_email="admin@example.com"):
    import bot as bot_module

    monkeypatch.setattr(bot_module, "bot", SimpleNamespace(get_guild=lambda _gid: guild))
    monkeypatch.setattr(
        bot_module, "build_web_actor_audit_label", lambda email: f"web:{email}"
    )
    return asyncio.run(
        bot_module.run_web_post_channel_message_async(payload, actor_email, 1234567890)
    )


def test_sender_posts_and_reports_the_message(monkeypatch):
    channel = _FakeChannel()
    result = _run(
        monkeypatch,
        _fake_guild(channel),
        {"channel_id": "9999", "content": "  hello members  "},
    )

    assert result["ok"] is True
    assert channel.sent == ["hello members"]
    assert result["message_id"] == 4242
    assert result["channel_name"] == "alerts"
    assert result["jump_url"].endswith("/4242")


def test_sender_refuses_an_empty_message(monkeypatch):
    channel = _FakeChannel()
    result = _run(monkeypatch, _fake_guild(channel), {"channel_id": "9999", "content": "   "})

    assert result["ok"] is False
    assert "cannot be empty" in result["error"]
    assert channel.sent == []


def test_sender_refuses_a_message_over_the_limit(monkeypatch):
    channel = _FakeChannel()
    result = _run(
        monkeypatch,
        _fake_guild(channel),
        {"channel_id": "9999", "content": "x" * 2001},
    )

    assert result["ok"] is False
    assert "2000 characters" in result["error"]
    assert channel.sent == []


def test_sender_refuses_a_channel_it_cannot_write_to(monkeypatch):
    channel = _FakeChannel(can_send=False)
    result = _run(monkeypatch, _fake_guild(channel), {"channel_id": "9999", "content": "hi"})

    assert result["ok"] is False
    assert "permission to post" in result["error"]
    assert channel.sent == []


def test_sender_reports_a_deleted_channel(monkeypatch):
    guild = _fake_guild(_FakeChannel())
    guild.get_channel = lambda _channel_id: None
    result = _run(monkeypatch, guild, {"channel_id": "9999", "content": "hi"})

    assert result["ok"] is False
    assert "no longer exists" in result["error"]


def test_sender_never_logs_the_message_body(monkeypatch, caplog):
    channel = _FakeChannel()
    with caplog.at_level("INFO"):
        result = _run(
            monkeypatch,
            _fake_guild(channel),
            {"channel_id": "9999", "content": "secret-ish announcement text"},
        )

    assert result["ok"] is True
    joined = "\n".join(record.getMessage() for record in caplog.records)
    assert "Web admin post:" in joined
    assert "web:admin@example.com" in joined
    assert "secret-ish announcement text" not in joined

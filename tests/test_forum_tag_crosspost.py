"""Tests for forum tag cross-post subscriptions.

Covers tag normalisation, the Discord message format, the Discourse tag API call,
the SQLite subscription helpers, the poller's baseline/new-topic behaviour, and
the web admin callbacks.
"""

from __future__ import annotations

import sqlite3
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import test_web_admin

import bot
from app.discourse_api import DiscourseApiError, fetch_discourse_tag_topics
from app.forum_tag_web_callbacks import ForumTagWebCallbacks

_login_as = test_web_admin._login_as
_select_guild = test_web_admin._select_guild
_page_csrf_token = test_web_admin._page_csrf_token

# ---------------------------------------------------------------------------
# Tag normalisation
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("KB", "kb"),
        ("  kb  ", "kb"),
        ("#KB", "kb"),
        ("#kb", "kb"),
        ("Multi Word", "multi-word"),
        ("  ", ""),
        ("", ""),
        (None, ""),
    ],
)
def test_normalize_forum_tag_name(raw, expected):
    assert bot.normalize_forum_tag_name(raw) == expected


# ---------------------------------------------------------------------------
# Message formatting
# ---------------------------------------------------------------------------

def test_message_contains_body_and_link():
    message = bot.format_forum_tag_post_message(
        "kb",
        {
            "title": "How to install NetBird",
            "url": "https://forum.gl-inet.com/t/slug/72161",
            "author": "admin",
            "body": "First, download the package.",
        },
    )
    assert "New forum post tagged `#kb`" in message
    assert "How to install NetBird" in message
    assert "First, download the package." in message
    assert "https://forum.gl-inet.com/t/slug/72161" in message


def test_message_is_truncated_to_discord_limit():
    message = bot.format_forum_tag_post_message(
        "kb",
        {"title": "T" * 300, "url": "https://forum.gl-inet.com/t/x/1", "author": "a", "body": "B" * 6000},
    )
    assert len(message) <= 2000


def test_message_without_body_still_carries_the_link():
    message = bot.format_forum_tag_post_message(
        "kb", {"title": "Title", "url": "https://forum.gl-inet.com/t/x/2", "author": "a", "body": ""}
    )
    assert "https://forum.gl-inet.com/t/x/2" in message


# ---------------------------------------------------------------------------
# Discourse tag API
# ---------------------------------------------------------------------------

def test_fetch_discourse_tag_topics_uses_the_tag_endpoint():
    payload = {
        "topics": [
            {
                "id": 11,
                "title": "A topic",
                "slug": "a-topic",
                "excerpt": "ex",
                "category_id": 3,
                "posts_count": 1,
                "last_posted_at": "2026-01-01T00:00:00Z",
            }
        ]
    }
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = payload

    with patch("app.discourse_api.requests.get", return_value=response) as get:
        topics = fetch_discourse_tag_topics(
            base_url="https://forum.gl-inet.com",
            tag="KB",
            max_results=5,
            source_name="Discourse tag feed",
            timeout_seconds=15,
            user_agent="test",
            api_key="key",
            api_username="system",
        )

    called_url = get.call_args.args[0]
    assert called_url == "https://forum.gl-inet.com/tag/KB/l/latest.json"
    assert get.call_args.kwargs["headers"]["Api-Key"] == "key"
    assert topics[0]["id"] == 11
    assert topics[0]["url"].endswith("/t/a-topic/11")


def test_fetch_discourse_tag_topics_rejects_a_blank_tag():
    with pytest.raises(DiscourseApiError):
        fetch_discourse_tag_topics(
            base_url="https://forum.gl-inet.com",
            tag="   ",
            max_results=5,
            source_name="Discourse tag feed",
            timeout_seconds=15,
            user_agent="test",
        )


# ---------------------------------------------------------------------------
# SQLite helpers
# ---------------------------------------------------------------------------

@pytest.fixture
def forum_db(tmp_path, monkeypatch):
    """Point the bot's shared connection at an isolated database."""
    db_file = tmp_path / "forum_tags.db"
    conn = sqlite3.connect(str(db_file))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")

    monkeypatch.setattr(bot, "db_connection", conn)
    monkeypatch.setattr(bot, "db_lock", _NoOpLock())
    bot.ensure_db_schema()
    yield conn
    conn.close()


class _NoOpLock:
    def acquire(self):
        return None

    def release(self):
        return None

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *exc_info):
        self.release()
        return False


def test_subscription_crud_round_trip(forum_db):
    bot.create_forum_tag_subscription(123, "KB", 999, "admin@example.com")

    rows = bot.list_forum_tag_subscriptions(guild_id=123)
    assert len(rows) == 1
    assert rows[0]["tag"] == "kb"
    assert rows[0]["channel_id"] == 999
    assert rows[0]["enabled"] is True

    subscription_id = rows[0]["id"]
    assert bot.get_forum_tag_subscription(subscription_id, guild_id=123)["tag"] == "kb"
    # scoped lookup must not leak across guilds
    assert bot.get_forum_tag_subscription(subscription_id, guild_id=456) is None

    assert bot.set_forum_tag_subscription_enabled(subscription_id, False, "admin@example.com") is True
    assert bot.list_forum_tag_subscriptions(guild_id=123, enabled_only=True) == []

    assert bot.update_forum_tag_subscription(subscription_id, 123, "Featured", 555, "admin@example.com") is True
    updated = bot.get_forum_tag_subscription(subscription_id, guild_id=123)
    assert updated["tag"] == "featured"
    assert updated["channel_id"] == 555

    assert bot.delete_forum_tag_subscription(subscription_id) is True
    assert bot.list_forum_tag_subscriptions(guild_id=123) == []


def test_create_rejects_blank_tag(forum_db):
    with pytest.raises(ValueError):
        bot.create_forum_tag_subscription(123, "   ", 999, "admin@example.com")


def test_create_rejects_invalid_channel(forum_db):
    with pytest.raises(ValueError):
        bot.create_forum_tag_subscription(123, "kb", 0, "admin@example.com")


def test_duplicate_tag_is_rejected(forum_db):
    bot.create_forum_tag_subscription(123, "KB", 999, "admin@example.com")
    with pytest.raises(sqlite3.IntegrityError):
        bot.create_forum_tag_subscription(123, "kb", 555, "admin@example.com")


def test_seen_topic_ids_merge_and_dedupe(forum_db):
    bot.create_forum_tag_subscription(123, "kb", 999, "admin@example.com")
    subscription_id = bot.list_forum_tag_subscriptions(guild_id=123)[0]["id"]

    bot.merge_forum_tag_seen_topic_ids(subscription_id, [1, 2, 2, "3"])
    assert bot.load_forum_tag_seen_topic_ids(subscription_id) == {1, 2, 3}

    # merging again is idempotent
    bot.merge_forum_tag_seen_topic_ids(subscription_id, [1, 2, 3])
    assert bot.load_forum_tag_seen_topic_ids(subscription_id) == {1, 2, 3}


def test_deleting_a_subscription_clears_seen_posts(forum_db):
    bot.create_forum_tag_subscription(123, "kb", 999, "admin@example.com")
    subscription_id = bot.list_forum_tag_subscriptions(guild_id=123)[0]["id"]
    bot.merge_forum_tag_seen_topic_ids(subscription_id, [1, 2])

    bot.delete_forum_tag_subscription(subscription_id)

    remaining = forum_db.execute("SELECT COUNT(*) FROM forum_tag_seen_posts").fetchone()[0]
    assert remaining == 0


# ---------------------------------------------------------------------------
# Poller
# ---------------------------------------------------------------------------

def _subscription(**overrides):
    base = {
        "id": 1,
        "guild_id": 123,
        "tag": "kb",
        "channel_id": 999,
        "enabled": True,
        "created_at": "",
        "updated_at": "",
        "created_by_email": "",
        "updated_by_email": "",
        "last_checked_at": "",
        "last_posted_at": "",
        "last_error": "",
    }
    base.update(overrides)
    return base


def test_first_poll_records_a_baseline_and_posts_nothing(forum_db):
    bot.create_forum_tag_subscription(123, "kb", 999, "admin@example.com")
    subscription = bot.list_forum_tag_subscriptions(guild_id=123)[0]
    channel = MagicMock()
    channel.send = AsyncMock()

    with (
        patch.object(bot, "resolve_discourse_forum_settings", return_value={
            "base_url": "https://forum.gl-inet.com",
            "api_key": "key",
            "api_username": "system",
            "timeout_seconds": 15,
        }),
        patch.object(bot, "FORUM_API_KEY", "test_key"),
        patch.object(bot, "fetch_discourse_tag_topics", return_value=[
            {"id": 11, "title": "Existing", "url": "https://forum.gl-inet.com/t/x/11", "author": "a", "excerpt": ""},
        ]),
        # If the baseline guard were removed this mock would let the post succeed,
        # so assert_not_awaited() below proves the guard -- not a broken posting path.
        patch.object(bot, "fetch_discourse_topic", return_value={
            "id": 11,
            "title": "Existing",
            "url": "https://forum.gl-inet.com/t/x/11",
            "posts": [{"id": 1, "username": "a", "text": "Body.", "cooked": "<p>Body.</p>"}],
        }),
        patch.object(bot, "resolve_forum_tag_channel", new=AsyncMock(return_value=(channel, ""))),
        patch.object(bot, "is_managed_guild_id", return_value=True),
    ):
        import asyncio
        asyncio.run(bot.process_forum_tag_subscription(subscription))

    channel.send.assert_not_awaited()
    assert bot.load_forum_tag_seen_topic_ids(subscription["id"]) == {11}


def test_new_topic_is_cross_posted_with_body_and_link(forum_db):
    bot.create_forum_tag_subscription(123, "kb", 999, "admin@example.com")
    subscription = bot.list_forum_tag_subscriptions(guild_id=123)[0]
    bot.merge_forum_tag_seen_topic_ids(subscription["id"], [11])

    channel = MagicMock()
    channel.send = AsyncMock()

    with (
        patch.object(bot, "resolve_discourse_forum_settings", return_value={
            "base_url": "https://forum.gl-inet.com",
            "api_key": "key",
            "api_username": "system",
            "timeout_seconds": 15,
        }),
        patch.object(bot, "FORUM_API_KEY", "test_key"),
        patch.object(bot, "fetch_discourse_tag_topics", return_value=[
            {"id": 11, "title": "Existing", "url": "https://forum.gl-inet.com/t/x/11", "author": "a", "excerpt": ""},
            {"id": 12, "title": "Brand new", "url": "https://forum.gl-inet.com/t/y/12", "author": "b", "excerpt": ""},
        ]),
        patch.object(bot, "fetch_discourse_topic", return_value={
            "id": 12,
            "title": "Brand new",
            "url": "https://forum.gl-inet.com/t/y/12",
            "posts": [{"id": 1, "username": "b", "text": "The actual post body.", "cooked": "<p>The actual post body.</p>"}],
        }),
        patch.object(bot, "resolve_forum_tag_channel", new=AsyncMock(return_value=(channel, ""))),
        patch.object(bot, "is_managed_guild_id", return_value=True),
    ):
        import asyncio
        asyncio.run(bot.process_forum_tag_subscription(subscription))

    channel.send.assert_awaited_once()
    sent = channel.send.call_args.args[0]
    assert "Brand new" in sent
    assert "The actual post body." in sent
    assert "https://forum.gl-inet.com/t/y/12" in sent
    # the already-seen topic was not posted again
    assert "Existing" not in sent
    assert bot.load_forum_tag_seen_topic_ids(subscription["id"]) == {11, 12}


def test_rate_limit_is_recorded_as_an_error(forum_db):
    from app.discourse_api import DiscourseRateLimitError

    bot.create_forum_tag_subscription(123, "kb", 999, "admin@example.com")
    subscription = bot.list_forum_tag_subscriptions(guild_id=123)[0]

    with (
        patch.object(bot, "resolve_discourse_forum_settings", return_value={
            "base_url": "https://forum.gl-inet.com",
            "api_key": "key",
            "api_username": "system",
            "timeout_seconds": 15,
        }),
        patch.object(bot, "FORUM_API_KEY", "test_key"),
        patch.object(bot, "fetch_discourse_tag_topics", side_effect=DiscourseRateLimitError("slow down")),
        patch.object(bot, "is_managed_guild_id", return_value=True),
    ):
        import asyncio
        asyncio.run(bot.process_forum_tag_subscription(subscription))

    updated = bot.get_forum_tag_subscription(subscription["id"], guild_id=123)
    assert "rate limit" in updated["last_error"].lower()


def test_missing_api_key_is_reported(forum_db):
    bot.create_forum_tag_subscription(123, "kb", 999, "admin@example.com")
    subscription = bot.list_forum_tag_subscriptions(guild_id=123)[0]

    with patch.object(bot, "FORUM_API_KEY", ""):
        import asyncio
        asyncio.run(bot.process_forum_tag_subscription(subscription))

    updated = bot.get_forum_tag_subscription(subscription["id"], guild_id=123)
    assert "api key" in updated["last_error"].lower()


# ---------------------------------------------------------------------------
# Web callbacks
# ---------------------------------------------------------------------------

def _callbacks():
    return ForumTagWebCallbacks(
        normalize_target_guild_id=lambda g: int(g or 0),
        normalize_forum_tag_name=bot.normalize_forum_tag_name,
        list_forum_tag_subscriptions=lambda guild_id=None, enabled_only=False: [],
        get_forum_tag_subscription=lambda subscription_id, guild_id=None: None,
        create_forum_tag_subscription=MagicMock(),
        update_forum_tag_subscription=MagicMock(return_value=True),
        set_forum_tag_subscription_enabled=MagicMock(return_value=True),
        delete_forum_tag_subscription=MagicMock(return_value=True),
        record_action_safe=MagicMock(),
        build_web_actor_audit_label=lambda email, display_name="": str(email),
        truncate_log_text=lambda value: str(value)[:100],
        logger=MagicMock(),
    )


def test_callback_rejects_a_blank_tag_before_calling_the_store():
    callbacks = _callbacks()
    payload = {"action": "add", "tag": "   ", "channel_id": "999"}
    result = callbacks.run_web_manage_forum_tags(payload, "admin@example.com", 123)

    assert result["ok"] is False
    assert "valid forum tag" in result["error"]
    callbacks.create_forum_tag_subscription.assert_not_called()


def test_callback_rejects_an_invalid_channel():
    callbacks = _callbacks()
    payload = {"action": "add", "tag": "KB", "channel_id": "0"}
    result = callbacks.run_web_manage_forum_tags(payload, "admin@example.com", 123)

    assert result["ok"] is False
    assert "valid Discord channel" in result["error"]


def test_callback_add_normalises_the_tag_and_audits():
    callbacks = _callbacks()
    payload = {"action": "add", "tag": "#KB", "channel_id": "999"}
    result = callbacks.run_web_manage_forum_tags(payload, "admin@example.com", 123)

    assert result["ok"] is True
    callbacks.create_forum_tag_subscription.assert_called_once_with(
        123, "kb", 999, "admin@example.com"
    )
    callbacks.record_action_safe.assert_called_once()
    assert callbacks.record_action_safe.call_args.kwargs["action"] == "forum_tag_add"


def test_callback_rejects_an_unknown_action():
    callbacks = _callbacks()
    result = callbacks.run_web_manage_forum_tags({"action": "frobnicate"}, "admin@example.com", 123)
    assert result["ok"] is False

# ---------------------------------------------------------------------------
# Web admin route
# ---------------------------------------------------------------------------


def test_web_admin_forum_tags_page_renders_and_adds_a_subscription(tmp_path):
    from web_admin import create_web_app

    created = []

    app = create_web_app(
        data_dir=str(tmp_path),
        env_file_path=str(tmp_path / "e.env"),
        tag_responses_file=str(tmp_path / "t.json"),
        default_admin_email="admin@example.com",
        default_admin_password="Ab!12xy",
        on_get_guilds=lambda: {
            "ok": True,
            "guilds": [
                {"id": "1234567890", "name": "Test Guild", "member_count": 1, "icon_url": "", "is_primary": True}
            ],
        },
        on_get_discord_catalog=lambda guild_id: {
            "ok": True,
            "channels": [{"id": "999", "name": "general", "position": 0}],
            "roles": [],
        },
        on_get_forum_tags=lambda guild_id: {"ok": True, "subscriptions": []},
        on_manage_forum_tags=lambda payload, actor_email, guild_id, display_name="": (
            created.append(payload) or {"ok": True, "message": "Added.", "subscriptions": []}
        ),
    )

    client = app.test_client()
    _login_as(client, "admin@example.com", "Ab!12xy")
    _select_guild(client)

    token = _page_csrf_token(client, "/admin/forum-tags")
    response = client.get("/admin/forum-tags", base_url="https://docker.example:8443")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "Forum Tag Cross-Posts" in html
    assert "Add Tag Cross-Post" in html

    response = client.post(
        "/admin/forum-tags",
        data={"action": "add", "tag": "#KB", "channel_id": "999", "csrf_token": token},
        base_url="https://docker.example:8443",
        headers={"X-CSRF-Token": token},
        follow_redirects=True,
    )
    assert response.status_code == 200
    assert created == [{"action": "add", "tag": "#KB", "channel_id": "999", "subscription_id": "", "enabled": ""}]


def test_web_admin_forum_tags_page_reports_a_catalog_failure(tmp_path):
    from web_admin import create_web_app

    app = create_web_app(
        data_dir=str(tmp_path),
        env_file_path=str(tmp_path / "e.env"),
        tag_responses_file=str(tmp_path / "t.json"),
        default_admin_email="admin@example.com",
        default_admin_password="Ab!12xy",
        on_get_guilds=lambda: {
            "ok": True,
            "guilds": [
                {"id": "1234567890", "name": "Test Guild", "member_count": 1, "icon_url": "", "is_primary": True}
            ],
        },
        on_get_discord_catalog=lambda guild_id: {"ok": False, "error": "Discord is rate limiting this bot."},
        on_get_forum_tags=lambda guild_id: {"ok": False, "error": "Storage is unavailable."},
    )

    client = app.test_client()
    _login_as(client, "admin@example.com", "Ab!12xy")
    _select_guild(client)

    response = client.get("/admin/forum-tags", base_url="https://docker.example:8443")
    assert response.status_code == 200
    assert "Storage is unavailable." in response.get_data(as_text=True)

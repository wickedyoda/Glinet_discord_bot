"""Tests for forum tag cross-post subscriptions.

Covers tag normalisation, the Discord message format, the Discourse tag API call,
the SQLite subscription helpers, the poller's baseline/new-topic behaviour, and
the web admin callbacks.
"""

from __future__ import annotations

import sqlite3
from unittest.mock import AsyncMock, MagicMock, patch

import discord
import pytest
import test_web_admin

import bot
from app.discourse_api import (
    DiscourseApiError,
    extract_discourse_topics,
    fetch_discourse_category_topics,
    fetch_discourse_tag_topics,
    fetch_discourse_tags,
)
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


def test_extractor_reads_topics_nested_under_topic_list():
    """The tag endpoint nests topics under topic_list, unlike the search endpoint."""
    payload = {
        "topic_list": {
            "topics": [
                {
                    "id": 72161,
                    "title": "Nested topic",
                    "fancy_title": "Nested topic",
                    "slug": "nested-topic",
                    "category_id": 3,
                    "posts_count": 2,
                    "last_posted_at": "2026-01-01T00:00:00Z",
                }
            ]
        }
    }
    topics = extract_discourse_topics(payload, base_url="https://forum.gl-inet.com", max_results=5)
    assert len(topics) == 1
    assert topics[0]["id"] == 72161
    assert topics[0]["url"] == "https://forum.gl-inet.com/t/nested-topic/72161"


def test_extractor_still_reads_a_top_level_topics_list():
    payload = {
        "topics": [
            {
                "id": 5,
                "title": "Flat topic",
                "slug": "flat-topic",
                "category_id": 3,
                "posts_count": 1,
                "last_posted_at": "2026-01-01T00:00:00Z",
            }
        ]
    }
    topics = extract_discourse_topics(payload, base_url="https://forum.gl-inet.com", max_results=5)
    assert [t["id"] for t in topics] == [5]


def test_message_strips_discourse_image_placeholders():
    message = bot.format_forum_tag_post_message(
        "kb",
        {
            "title": "Title",
            "url": "https://forum.gl-inet.com/t/x/1",
            "author": "a",
            "body": "Before. image 400\u00d7300 31.7 KB image 700\u00d7700 54.4 KB After.",
        },
    )
    assert "image 400" not in message
    assert "31.7 KB" not in message
    assert "Before. After." in message


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
    bot.mark_forum_tag_baseline_initialized(subscription["id"])

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
        fetch_forum_tag_options=lambda guild_id: {"ok": True, "tags": [], "error": ""},
        fetch_forum_category_options=lambda guild_id: {"ok": True, "categories": [], "error": ""},
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
        123, "kb", 999, "admin@example.com", feed_type="tag"
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
    assert created == [{
        "action": "add",
        "tag": "#KB",
        "feed_type": "tag",
        "channel_id": "999",
        "subscription_id": "",
        "enabled": "",
    }]


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

# ---------------------------------------------------------------------------
# Regression tests for the PR review findings
# ---------------------------------------------------------------------------


def _run(coro):
    import asyncio

    return asyncio.run(coro)


def _make_subscription(forum_db, guild_id=123, channel_id=999, tag="kb"):
    bot.create_forum_tag_subscription(guild_id, tag, channel_id, "admin@example.com")
    return bot.list_forum_tag_subscriptions(guild_id=guild_id)[0]


def _config(**overrides):
    base = {
        "base_url": "https://forum.gl-inet.com",
        "api_key": "key",
        "api_username": "system",
        "timeout_seconds": 15,
    }
    base.update(overrides)
    return base


def test_cross_guild_channel_is_rejected():
    """A channel id saved for one guild must not be able to post into another."""
    other_guild = MagicMock()
    other_guild.id = 999

    channel = MagicMock(spec=discord.TextChannel)
    channel.guild = other_guild

    with patch.object(bot, "bot") as fake_bot:
        fake_bot.get_channel.return_value = channel
        resolved, error = _run(bot.resolve_forum_tag_channel(999, guild_id=123))

    assert resolved is None
    assert error == "channel_wrong_guild"


def test_matching_guild_channel_is_accepted():
    guild = MagicMock()
    guild.id = 123

    channel = MagicMock(spec=discord.TextChannel)
    channel.guild = guild

    with patch.object(bot, "bot") as fake_bot:
        fake_bot.get_channel.return_value = channel
        resolved, error = _run(bot.resolve_forum_tag_channel(999, guild_id=123))

    assert resolved is channel
    assert error == ""


def test_send_restricts_allowed_mentions(forum_db):
    """Untrusted forum text must never be able to ping @everyone or a role."""
    subscription = _make_subscription(forum_db)
    bot.mark_forum_tag_baseline_initialized(subscription["id"])

    channel = MagicMock()
    channel.send = AsyncMock()

    with (
        patch.object(bot, "resolve_discourse_forum_settings", return_value=_config()),
        patch.object(bot, "fetch_discourse_tag_topics", return_value=[
            {"id": 12, "title": "T", "url": "https://forum.gl-inet.com/t/x/12", "author": "a", "excerpt": ""},
        ]),
        patch.object(bot, "fetch_discourse_topic", return_value={
            "id": 12, "title": "T", "url": "https://forum.gl-inet.com/t/x/12",
            "posts": [{"id": 1, "username": "a", "text": "@everyone look", "cooked": "<p>@everyone look</p>"}],
        }),
        patch.object(bot, "resolve_forum_tag_channel", new=AsyncMock(return_value=(channel, ""))),
        patch.object(bot, "is_managed_guild_id", return_value=True),
    ):
        _run(bot.process_forum_tag_subscription(subscription))

    channel.send.assert_awaited_once()
    mentions = channel.send.call_args.kwargs["allowed_mentions"]
    assert mentions.everyone is False
    assert mentions.roles is False
    assert mentions.users is False


def test_long_body_keeps_the_source_link():
    message = bot.format_forum_tag_post_message(
        "kb",
        {
            "title": "A very long topic",
            "url": "https://forum.gl-inet.com/t/slug/12345",
            "author": "someone",
            "body": "Word " * 1200,
        },
    )
    assert len(message) <= 1900
    assert "https://forum.gl-inet.com/t/slug/12345" in message
    assert "Posted by `someone`" in message


def test_guild_level_api_key_is_sufficient(forum_db):
    """A guild with its own discourse_api_key must work without the global key."""
    subscription = _make_subscription(forum_db)
    bot.mark_forum_tag_baseline_initialized(subscription["id"])

    channel = MagicMock()
    channel.send = AsyncMock()

    with (
        patch.object(bot, "FORUM_API_KEY", ""),
        patch.object(bot, "resolve_discourse_forum_settings", return_value=_config(api_key="guild-key")),
        patch.object(bot, "fetch_discourse_tag_topics", return_value=[
            {"id": 12, "title": "T", "url": "https://forum.gl-inet.com/t/x/12", "author": "a", "excerpt": ""},
        ]),
        patch.object(bot, "fetch_discourse_topic", return_value={
            "id": 12, "title": "T", "url": "https://forum.gl-inet.com/t/x/12",
            "posts": [{"id": 1, "username": "a", "text": "Body.", "cooked": "<p>Body.</p>"}],
        }),
        patch.object(bot, "resolve_forum_tag_channel", new=AsyncMock(return_value=(channel, ""))),
        patch.object(bot, "is_managed_guild_id", return_value=True),
    ):
        _run(bot.process_forum_tag_subscription(subscription))

    channel.send.assert_awaited_once()
    updated = bot.get_forum_tag_subscription(subscription["id"], guild_id=123)
    assert updated["last_error"] == ""


def test_empty_first_feed_still_baseline_initializes(forum_db):
    """A tag with no topics yet must not swallow its first real topic."""
    subscription = _make_subscription(forum_db)
    channel = MagicMock()
    channel.send = AsyncMock()

    # First poll: the tag has no topics at all.
    with (
        patch.object(bot, "resolve_discourse_forum_settings", return_value=_config()),
        patch.object(bot, "fetch_discourse_tag_topics", return_value=[]),
        patch.object(bot, "is_managed_guild_id", return_value=True),
    ):
        _run(bot.process_forum_tag_subscription(subscription))

    channel.send.assert_not_awaited()
    assert bot.is_forum_tag_baseline_initialized(subscription["id"]) is True

    # Second poll: the first topic appears and must be posted.
    with (
        patch.object(bot, "resolve_discourse_forum_settings", return_value=_config()),
        patch.object(bot, "fetch_discourse_tag_topics", return_value=[
            {"id": 21, "title": "First!", "url": "https://forum.gl-inet.com/t/x/21", "author": "a", "excerpt": ""},
        ]),
        patch.object(bot, "fetch_discourse_topic", return_value={
            "id": 21, "title": "First!", "url": "https://forum.gl-inet.com/t/x/21",
            "posts": [{"id": 1, "username": "a", "text": "Body.", "cooked": "<p>Body.</p>"}],
        }),
        patch.object(bot, "resolve_forum_tag_channel", new=AsyncMock(return_value=(channel, ""))),
        patch.object(bot, "is_managed_guild_id", return_value=True),
    ):
        _run(bot.process_forum_tag_subscription(subscription))

    channel.send.assert_awaited_once()


def test_topics_beyond_the_per_run_cap_are_not_marked_seen(forum_db):
    subscription = _make_subscription(forum_db)
    bot.mark_forum_tag_baseline_initialized(subscription["id"])

    channel = MagicMock()
    channel.send = AsyncMock()

    topics = [
        {"id": i, "title": f"T{i}", "url": f"https://forum.gl-inet.com/t/x/{i}", "author": "a", "excerpt": ""}
        for i in range(100, 100 + bot.FORUM_TAG_MAX_POSTS_PER_RUN + 2)
    ]

    with (
        patch.object(bot, "resolve_discourse_forum_settings", return_value=_config()),
        patch.object(bot, "fetch_discourse_tag_topics", return_value=topics),
        patch.object(bot, "fetch_discourse_topic", side_effect=lambda **kw: {
            "id": kw["topic_id"], "title": "T", "url": "https://forum.gl-inet.com/t/x/1",
            "posts": [{"id": 1, "username": "a", "text": "Body.", "cooked": "<p>Body.</p>"}],
        }),
        patch.object(bot, "resolve_forum_tag_channel", new=AsyncMock(return_value=(channel, ""))),
        patch.object(bot, "is_managed_guild_id", return_value=True),
    ):
        _run(bot.process_forum_tag_subscription(subscription))

    assert channel.send.await_count == bot.FORUM_TAG_MAX_POSTS_PER_RUN

    seen = bot.load_forum_tag_seen_topic_ids(subscription["id"])
    assert len(seen) == bot.FORUM_TAG_MAX_POSTS_PER_RUN
    # Derived from the cap, not hardcoded: the topic just past it stays pending.
    first_pending_id = 100 + bot.FORUM_TAG_MAX_POSTS_PER_RUN
    assert first_pending_id not in seen
    assert 100 in seen


def test_failed_topic_fetch_leaves_the_topic_retryable(forum_db):
    subscription = _make_subscription(forum_db)
    bot.mark_forum_tag_baseline_initialized(subscription["id"])

    channel = MagicMock()
    channel.send = AsyncMock()

    with (
        patch.object(bot, "resolve_discourse_forum_settings", return_value=_config()),
        patch.object(bot, "fetch_discourse_tag_topics", return_value=[
            {"id": 31, "title": "T", "url": "https://forum.gl-inet.com/t/x/31", "author": "a", "excerpt": ""},
        ]),
        patch.object(bot, "fetch_discourse_topic", side_effect=DiscourseApiError("boom")),
        patch.object(bot, "resolve_forum_tag_channel", new=AsyncMock(return_value=(channel, ""))),
        patch.object(bot, "is_managed_guild_id", return_value=True),
    ):
        _run(bot.process_forum_tag_subscription(subscription))

    channel.send.assert_not_awaited()
    assert bot.load_forum_tag_seen_topic_ids(subscription["id"]) == set()
    updated = bot.get_forum_tag_subscription(subscription["id"], guild_id=123)
    assert "boom" in updated["last_error"]


def test_toggle_form_submits_the_next_enabled_state():
    from app.web_forum_tags import render_forum_tags_body

    enabled_row = render_forum_tags_body(
        guild_name="G",
        subscriptions=[{"id": 1, "tag": "kb", "channel_id": 999, "enabled": True,
                        "last_checked_at": "", "last_posted_at": "", "last_error": ""}],
        channel_options=[{"id": "999", "name": "general", "type": "text"}],
        render_select_input=lambda *a, **k: "",
    )
    assert 'name="enabled" value="0"' in enabled_row

    disabled_row = render_forum_tags_body(
        guild_name="G",
        subscriptions=[{"id": 1, "tag": "kb", "channel_id": 999, "enabled": False,
                        "last_checked_at": "", "last_posted_at": "", "last_error": ""}],
        channel_options=[{"id": "999", "name": "general", "type": "text"}],
        render_select_input=lambda *a, **k: "",
    )
    assert 'name="enabled" value="1"' in disabled_row


def test_forum_tags_page_offers_only_text_channels(tmp_path):
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
        on_get_discord_catalog=lambda guild_id: {
            "ok": True,
            "channels": [
                {"id": "111", "name": "general", "type": "text", "position": 0},
                {"id": "222", "name": "voice-lounge", "type": "voice", "position": 1},
                {"id": "333", "name": "random", "type": "text", "position": 2},
            ],
            "roles": [],
        },
        on_get_forum_tags=lambda guild_id: {"ok": True, "subscriptions": []},
    )

    client = app.test_client()
    _login_as(client, "admin@example.com", "Ab!12xy")
    _select_guild(client)
    html = client.get("/admin/forum-tags", base_url="https://docker.example:8443").get_data(as_text=True)

    assert "general" in html
    assert "random" in html
    assert "voice-lounge" not in html

# ---------------------------------------------------------------------------
# Live tag-list integration
# ---------------------------------------------------------------------------


def test_fetch_discourse_tags_parses_and_sorts():
    payload = {
        "tags": [
            {"id": 1, "name": "Featured", "slug": "featured", "count": 59},
            {"id": 2, "name": "KB", "slug": "kb", "count": 97},
            {"id": 3, "name": "KB", "slug": "kb", "count": 1},  # duplicate slug
            "not-a-dict",
        ]
    }
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = payload

    with patch("app.discourse_api.requests.get", return_value=response) as get:
        tags = fetch_discourse_tags(
            base_url="https://forum.gl-inet.com",
            timeout_seconds=15,
            source_name="Discourse tag list",
            user_agent="test",
        )

    assert get.call_args.args[0] == "https://forum.gl-inet.com/tags.json"
    # most-used first, duplicate slug dropped
    assert [t["slug"] for t in tags] == ["kb", "featured"]
    assert tags[0]["count"] == 97


def test_fetch_forum_tag_options_returns_tags():
    with patch.object(bot, "resolve_discourse_forum_settings", return_value=_config()), \
         patch.object(bot, "fetch_discourse_tags", return_value=[
             {"slug": "kb", "name": "KB", "count": 97},
             {"slug": "featured", "name": "Featured", "count": 59},
         ]):
        result = bot.fetch_forum_tag_options(123)

    assert result["ok"] is True
    assert [t["slug"] for t in result["tags"]] == ["kb", "featured"]


def test_fetch_forum_tag_options_survives_a_failure():
    with patch.object(bot, "resolve_discourse_forum_settings", return_value=_config()), \
         patch.object(bot, "fetch_discourse_tags", side_effect=DiscourseApiError("boom")):
        result = bot.fetch_forum_tag_options(123)

    assert result["ok"] is False
    assert result["tags"] == []
    assert "boom" in result["error"]


def test_page_offers_a_dropdown_of_real_tags():
    from app.web_forum_tags import render_forum_tags_body

    html = render_forum_tags_body(
        guild_name="G",
        subscriptions=[],
        channel_options=[{"id": "999", "name": "general", "type": "text"}],
        render_select_input=lambda name, value, options, placeholder="": (
            f'<select name="{name}">'
            + "".join(
                f'<option value="{o.get("id")}">{o.get("label") or o.get("name")}</option>'
                for o in options
            )
            + "</select>"
        ),
        available_tags=[
            {"slug": "kb", "name": "KB", "count": 97},
            {"slug": "featured", "name": "Featured", "count": 59},
        ],
    )

    assert '<select name="feed"' in html
    assert '<optgroup label="Tags">' in html
    assert '<option value="tag:kb">KB (97 topics)</option>' in html
    assert '<option value="tag:featured">Featured (59 topics)</option>' in html
    # the free-text fallback must not also be present
    assert 'name="tag" placeholder' not in html


def test_page_falls_back_to_text_input_when_tags_unavailable():
    from app.web_forum_tags import render_forum_tags_body

    html = render_forum_tags_body(
        guild_name="G",
        subscriptions=[],
        channel_options=[{"id": "999", "name": "general", "type": "text"}],
        render_select_input=lambda name, value, options, placeholder="": f'<select name="{name}"></select>',
        available_tags=[],
        tag_options_error="Could not load forum tags: Forum is rate limiting requests.",
    )

    assert '<select name="tag">' not in html
    assert 'name="tag" placeholder' in html
    assert "rate limiting" in html


def test_route_loads_the_live_tag_list(tmp_path):
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
        on_get_discord_catalog=lambda guild_id: {
            "ok": True,
            "channels": [{"id": "999", "name": "general", "type": "text", "position": 0}],
            "roles": [],
        },
        on_get_forum_tags=lambda guild_id: {"ok": True, "subscriptions": []},
        on_get_forum_tag_options=lambda guild_id: {
            "ok": True,
            "tags": [{"slug": "kb", "name": "KB", "count": 97}],
            "error": "",
        },
    )

    client = app.test_client()
    _login_as(client, "admin@example.com", "Ab!12xy")
    _select_guild(client)
    html = client.get("/admin/forum-tags", base_url="https://docker.example:8443").get_data(as_text=True)

    assert '<select name="feed"' in html
    assert '<optgroup label="Tags">' in html
    assert "KB (97 topics)" in html

# ---------------------------------------------------------------------------
# Category feeds
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("tag", "tag"), ("TAG", "tag"), ("category", "category"), ("Category", "category"),
     ("", "tag"), (None, "tag"), ("nonsense", "tag")],
)
def test_normalize_forum_feed_type(raw, expected):
    assert bot.normalize_forum_feed_type(raw) == expected


def test_fetch_discourse_category_topics_reads_the_category_feed():
    payload = {
        "topic_list": {
            "topics": [
                {
                    "id": 70690,
                    "title": "Join the Mango 2 Creator Program",
                    "slug": "join-the-mango-2-creator-program",
                    "category_id": 22,
                    "posts_count": 1,
                    "last_posted_at": "2026-10-28T00:00:00Z",
                }
            ]
        }
    }
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = payload

    with patch("app.discourse_api.requests.get", return_value=response) as get:
        topics = fetch_discourse_category_topics(
            base_url="https://forum.gl-inet.com",
            category_slug="announcement",
            max_results=5,
            source_name="Discourse category feed",
            timeout_seconds=15,
            user_agent="test",
        )

    assert get.call_args.args[0] == "https://forum.gl-inet.com/c/announcement/l/latest.json"
    assert topics[0]["id"] == 70690
    assert topics[0]["url"].endswith("/t/join-the-mango-2-creator-program/70690")


def test_fetch_discourse_category_topics_rejects_a_blank_category():
    with pytest.raises(DiscourseApiError):
        fetch_discourse_category_topics(
            base_url="https://forum.gl-inet.com",
            category_slug="   ",
            max_results=5,
            source_name="Discourse category feed",
            timeout_seconds=15,
            user_agent="test",
        )


def test_fetch_forum_category_options_returns_categories():
    with patch.object(bot, "resolve_discourse_forum_settings", return_value=_config()), \
         patch.object(bot, "fetch_discourse_categories", return_value=[
             {"id": 22, "name": "Announcement", "slug": "announcement", "topic_count": 93},
         ]):
        result = bot.fetch_forum_category_options(123)

    assert result["ok"] is True
    assert result["categories"][0]["slug"] == "announcement"


def test_category_subscription_is_cross_posted(forum_db):
    """The Announcement category must drive the same cross-post path as a tag."""
    bot.create_forum_tag_subscription(
        123, "announcement", 999, "admin@example.com", feed_type="category"
    )
    subscription = bot.list_forum_tag_subscriptions(guild_id=123)[0]
    assert subscription["feed_type"] == "category"
    bot.mark_forum_tag_baseline_initialized(subscription["id"])

    channel = MagicMock()
    channel.send = AsyncMock()

    with (
        patch.object(bot, "resolve_discourse_forum_settings", return_value=_config()),
        patch.object(bot, "fetch_discourse_category_topics", return_value=[
            {"id": 70690, "title": "Join the Mango 2 Creator Program",
             "url": "https://forum.gl-inet.com/t/x/70690", "author": "a", "excerpt": ""},
        ]) as category_fetch,
        patch.object(bot, "fetch_discourse_tag_topics") as tag_fetch,
        patch.object(bot, "fetch_discourse_topic", return_value={
            "id": 70690, "title": "Join the Mango 2 Creator Program",
            "url": "https://forum.gl-inet.com/t/x/70690",
            "posts": [{"id": 1, "username": "a", "text": "Body text.", "cooked": "<p>Body text.</p>"}],
        }),
        patch.object(bot, "resolve_forum_tag_channel", new=AsyncMock(return_value=(channel, ""))),
        patch.object(bot, "is_managed_guild_id", return_value=True),
    ):
        _run(bot.process_forum_tag_subscription(subscription))

    category_fetch.assert_called_once()
    tag_fetch.assert_not_called()
    channel.send.assert_awaited_once()
    sent = channel.send.call_args.args[0]
    assert "New forum post in `#announcement`" in sent
    assert "Body text." in sent
    assert "https://forum.gl-inet.com/t/x/70690" in sent


def test_category_picker_lists_categories():
    from app.web_forum_tags import render_forum_tags_body

    html = render_forum_tags_body(
        guild_name="G",
        subscriptions=[],
        channel_options=[{"id": "999", "name": "general", "type": "text"}],
        render_select_input=lambda *a, **k: "",
        available_tags=[{"slug": "kb", "name": "KB", "count": 97}],
        available_categories=[{"slug": "announcement", "name": "Announcement", "topic_count": 93}],
    )

    assert '<optgroup label="Tags">' in html
    assert '<optgroup label="Categories">' in html
    assert '<option value="tag:kb">KB (97 topics)</option>' in html
    assert '<option value="category:announcement">Announcement (93 topics)</option>' in html


def test_schedule_defaults_to_every_twelve_hours():
    assert bot.DEFAULT_FORUM_TAG_CHECK_SCHEDULE == "0 */12 * * *"

# ---------------------------------------------------------------------------
# Review follow-ups: ordering, drain limits, and the feed_type unique key
# ---------------------------------------------------------------------------


def test_category_feed_requests_creation_order():
    """Activity order would re-announce a years-old topic after a reply bump."""
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {"topic_list": {"topics": []}}

    with patch("app.discourse_api.requests.get", return_value=response) as get:
        fetch_discourse_category_topics(
            base_url="https://forum.gl-inet.com",
            category_slug="announcement",
            max_results=5,
            source_name="Discourse category feed",
            timeout_seconds=15,
            user_agent="test",
        )

    assert get.call_args.kwargs["params"] == {"order": "created", "ascending": "false"}


def test_delivery_cap_is_never_below_the_fetch_window():
    """A pending topic must not age out of the next fetch window."""
    assert bot.FORUM_TAG_MAX_POSTS_PER_RUN >= bot.FORUM_TAG_FETCH_LIMIT


def _old_schema_db(path):
    """Build a database with the pre-fix UNIQUE(guild_id, tag) constraint."""
    import sqlite3

    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE forum_tag_subscriptions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER NOT NULL DEFAULT 0,
            tag TEXT NOT NULL,
            feed_type TEXT NOT NULL DEFAULT 'tag',
            channel_id INTEGER NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            created_by_email TEXT NOT NULL DEFAULT '',
            updated_by_email TEXT NOT NULL DEFAULT '',
            last_checked_at TEXT NOT NULL DEFAULT '',
            last_posted_at TEXT NOT NULL DEFAULT '',
            last_error TEXT NOT NULL DEFAULT '',
            baseline_initialized INTEGER NOT NULL DEFAULT 0,
            UNIQUE(guild_id, tag)
        );
        CREATE TABLE forum_tag_seen_posts (
            subscription_id INTEGER NOT NULL,
            topic_id INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY(subscription_id, topic_id),
            FOREIGN KEY(subscription_id) REFERENCES forum_tag_subscriptions(id) ON DELETE CASCADE
        );
        INSERT INTO forum_tag_subscriptions
            (id, guild_id, tag, feed_type, channel_id, enabled, created_at, updated_at,
             created_by_email, updated_by_email, last_checked_at, last_posted_at,
             last_error, baseline_initialized)
        VALUES (7, 123, 'kb', 'tag', 999, 1, '2026-01-01', '2026-01-01', '', '', '', '', '', 1);
        INSERT INTO forum_tag_seen_posts VALUES (7, 111, '2026-01-01'), (7, 222, '2026-01-02');
        """
    )
    conn.commit()
    conn.close()


def test_migration_admits_a_tag_and_a_category_with_the_same_slug(forum_db):
    """UNIQUE(guild_id, tag) made the second insert look like a duplicate."""
    bot.create_forum_tag_subscription(123, "announcement", 999, "a@example.com", feed_type="tag")
    bot.create_forum_tag_subscription(123, "announcement", 999, "a@example.com", feed_type="category")

    subs = bot.list_forum_tag_subscriptions(guild_id=123)
    assert sorted(s["feed_type"] for s in subs) == ["category", "tag"]

    # a true duplicate in the same guild still collides
    import sqlite3

    with pytest.raises(sqlite3.IntegrityError):
        bot.create_forum_tag_subscription(123, "announcement", 999, "a@example.com", feed_type="category")


def test_migration_rebuilds_old_schema_and_preserves_rows(tmp_path, monkeypatch):
    """Existing subscription ids and seen-topic rows must survive the rebuild."""

    db_path = tmp_path / "old.db"
    _old_schema_db(db_path)

    monkeypatch.setattr(bot, "DB_FILE", str(db_path))
    bot.db_connection = None
    try:
        bot.ensure_db_schema()
        conn = bot.get_db_connection()

        # the unique index now spans feed_type
        unique_cols = []
        for idx in conn.execute("PRAGMA index_list(forum_tag_subscriptions)").fetchall():
            if idx["unique"]:
                cols = {c["name"] for c in conn.execute(f"PRAGMA index_info({idx['name']})").fetchall()}
                if {"guild_id", "tag"} <= cols:
                    unique_cols = cols
                    break
        assert "feed_type" in unique_cols, unique_cols

        # the pre-existing subscription kept its id and baseline flag
        row = conn.execute("SELECT * FROM forum_tag_subscriptions WHERE id = 7").fetchone()
        assert row is not None
        assert row["tag"] == "kb" and row["feed_type"] == "tag"
        assert row["baseline_initialized"] == 1

        # the cascade did not eat the seen-topic rows
        seen = {
            r["topic_id"]
            for r in conn.execute(
                "SELECT topic_id FROM forum_tag_seen_posts WHERE subscription_id = 7"
            ).fetchall()
        }
        assert seen == {111, 222}
    finally:
        bot.db_connection = None

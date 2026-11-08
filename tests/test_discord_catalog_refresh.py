"""Tests for the web GUI Discord catalog refresh action.

The channel/role catalog is cached for ``WEB_DISCORD_CATALOG_TTL_SECONDS`` so the
web GUI does not hit Discord on every page load. That cache is also why a channel
created in Discord moments before an admin opens the reaction-role composer does
not appear in the dropdown until the TTL expires.

``run_web_refresh_discord_catalog`` backs the web GUI refresh button and must
bypass that TTL rather than return the cached snapshot.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import threading
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import bot


@pytest.fixture
def running_loop():
    """A real event loop in a background thread, as the web admin thread has."""
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    try:
        yield loop
    finally:
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=5)
        loop.close()


def _catalog(*, channels=None, roles=None, fetched_at: float = 1_000.0):
    return {
        "ok": True,
        "guild": {"id": "123", "name": "Test Guild"},
        "channels": channels
        if channels is not None
        else [{"id": "1", "name": "general", "type": "text", "position": 0, "label": "#general [text]"}],
        "roles": roles
        if roles is not None
        else [{"id": "11", "name": "Gamer", "position": 1, "label": "@Gamer"}],
        "fetched_at": fetched_at,
    }


def _seed_cache(catalog: dict, *, fetched_at: float = 500.0) -> None:
    bot.discord_catalog_cache.clear()
    bot.discord_catalog_cache[123] = {"fetched_at": fetched_at, "data": catalog}


def test_refresh_bypasses_a_still_fresh_cache(running_loop):
    """A cached snapshot inside the TTL must not be returned by the refresh action."""
    stale = _catalog(channels=[{"id": "1", "name": "old-channel", "type": "text", "position": 0, "label": "#old"}])
    _seed_cache(stale, fetched_at=9_999_999_000.0)  # far future => always "fresh"

    fresh = _catalog(
        channels=[{"id": "2", "name": "brand-new", "type": "text", "position": 1, "label": "#brand-new"}]
    )

    with (
        patch("bot.bot") as bot_mock,
        patch("bot.fetch_discord_catalog_async", new=AsyncMock(return_value=fresh)) as fetch_mock,
    ):
        bot_mock.loop = running_loop

        result = bot.run_web_refresh_discord_catalog(123)

    assert result["ok"] is True
    assert [channel["name"] for channel in result["channels"]] == ["brand-new"]
    fetch_mock.assert_awaited_once()


def test_refresh_replaces_the_cached_entry_with_fresh_data(running_loop):
    stale = _catalog()
    _seed_cache(stale, fetched_at=9_999_999_000.0)
    fresh = _catalog(roles=[{"id": "99", "name": "NewRole", "position": 5, "label": "@NewRole"}])

    with (
        patch("bot.bot") as bot_mock,
        patch("bot.fetch_discord_catalog_async", new=AsyncMock(return_value=fresh)),
    ):
        bot_mock.loop = running_loop
        bot.run_web_refresh_discord_catalog(123)

    cached = bot.discord_catalog_cache[123]["data"]
    assert [role["name"] for role in cached["roles"]] == ["NewRole"]


def test_refresh_does_not_leave_a_stale_entry_when_fetch_fails(running_loop):
    """A failed refresh must not resurrect the old snapshot as if it were current."""
    stale = _catalog()
    _seed_cache(stale, fetched_at=9_999_999_000.0)

    with (
        patch("bot.bot") as bot_mock,
        patch("bot.fetch_discord_catalog_async", new=AsyncMock(return_value={"ok": False, "error": "nope"})),
    ):
        bot_mock.loop = running_loop
        result = bot.run_web_refresh_discord_catalog(123)

    assert result["ok"] is False
    assert result["error"] == "nope"
    assert 123 not in bot.discord_catalog_cache


def test_refresh_without_running_loop_returns_error():
    with patch("bot.bot") as bot_mock:
        bot_mock.loop = None
        result = bot.run_web_refresh_discord_catalog(123)

    assert result["ok"] is False
    assert "loop" in result["error"].lower()


def _discard(coro):
    """Close a coroutine that a patched run_coroutine_threadsafe never awaits."""
    coro.close()


def test_refresh_timeout_returns_error_and_cancels_future():
    pending = MagicMock()
    pending.result.side_effect = concurrent.futures.TimeoutError()

    def fake_submit(coro, _loop):
        _discard(coro)
        return pending

    with (
        patch("bot.bot") as bot_mock,
        patch("bot.asyncio.run_coroutine_threadsafe", side_effect=fake_submit) as submit_mock,
    ):
        bot_mock.loop = MagicMock()
        bot_mock.loop.is_running.return_value = True

        result = bot.run_web_refresh_discord_catalog(123)

    assert result["ok"] is False
    assert "Timed out" in result["error"]
    pending.cancel.assert_called_once()
    submit_mock.assert_called_once()


def test_refresh_scheduling_failure_returns_error():
    """A loop that dies between the is_running() check and submit must not escape."""

    def failing_submit(coro, _loop):
        _discard(coro)
        raise RuntimeError("boom")

    with (
        patch("bot.bot") as bot_mock,
        patch("bot.asyncio.run_coroutine_threadsafe", side_effect=failing_submit),
    ):
        bot_mock.loop = MagicMock()
        bot_mock.loop.is_running.return_value = True

        result = bot.run_web_refresh_discord_catalog(123)

    assert result["ok"] is False
    assert "Unexpected error" in result["error"]


def test_getter_still_serves_the_cache_inside_the_ttl(running_loop):
    """Regression guard: the refresh action must not disable the normal cache."""
    cached = _catalog()
    _seed_cache(cached, fetched_at=9_999_999_000.0)

    with (
        patch("bot.bot") as bot_mock,
        patch("bot.fetch_discord_catalog_async", new=AsyncMock()) as fetch_mock,
    ):
        bot_mock.loop = running_loop
        result = bot.run_web_get_discord_catalog(123)

    assert result is cached
    fetch_mock.assert_not_awaited()


def test_getter_refetches_once_the_ttl_has_expired(running_loop):
    cached = _catalog()
    _seed_cache(cached, fetched_at=0.0)  # long expired
    fresh = _catalog(channels=[{"id": "7", "name": "recent", "type": "text", "position": 0, "label": "#recent"}])

    with (
        patch("bot.bot") as bot_mock,
        patch("bot.fetch_discord_catalog_async", new=AsyncMock(return_value=fresh)) as fetch_mock,
    ):
        bot_mock.loop = running_loop
        result = bot.run_web_get_discord_catalog(123)

    assert [channel["name"] for channel in result["channels"]] == ["recent"]
    fetch_mock.assert_awaited_once()

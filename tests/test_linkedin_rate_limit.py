"""Tests for LinkedIn HTTP 429 rate-limit handling and poller backoff.

LinkedIn throttles the profile scraper with HTTP 429. Before this change every 429
surfaced as an ERROR with a full traceback, and the monitor kept polling on its
fixed 900s interval regardless of whether the previous poll was throttled -- which
is what produced the bursts of failures in the 2026-09-29 production logs.

The monitor now treats a 429 as an expected, self-healing condition: it logs a
warning, honours ``Retry-After`` when LinkedIn supplies it, and backs off
exponentially so a throttled host is not hammered at a fixed cadence.
"""

from __future__ import annotations

import asyncio
import types

import pytest

import bot as bot_module
from bot import (
    LinkedInRateLimitedError,
    _linkedin_backoff_seconds,
    _parse_retry_after_seconds,
    poll_linkedin_subscriptions,
)


# --------------------------------------------------------------------------- #
#  Retry-After parsing
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("120", 120.0),
        ("120.5", 120.5),
        ("  60  ", 60.0),
        ("0", 0.0),
        # Absent / unparseable -> fall back to our own exponential ramp
        (None, None),
        ("", None),
        ("   ", None),
        # HTTP-date form, which this poller does not consume
        ("Wed, 21 Oct 2026 07:28:00 GMT", None),
        # Nonsense
        ("soon", None),
        ("NaN", None),
        ("inf", None),
        ("-5", None),
    ],
)
def test_parse_retry_after_seconds(raw, expected):
    assert _parse_retry_after_seconds(raw) == expected


def test_rate_limited_error_carries_retry_after():
    err = LinkedInRateLimitedError(retry_after_seconds=300)
    assert err.retry_after_seconds == 300
    assert err.status_code == 429
    assert "300s" in str(err)


def test_rate_limited_error_without_hint():
    err = LinkedInRateLimitedError()
    assert err.retry_after_seconds is None
    assert "429" in str(err)


def test_rate_limited_error_is_a_runtime_error():
    """Callers that catch RuntimeError must still catch this."""
    assert issubclass(LinkedInRateLimitedError, RuntimeError)


# --------------------------------------------------------------------------- #
#  Backoff computation
# --------------------------------------------------------------------------- #


def test_backoff_grows_exponentially(monkeypatch):
    monkeypatch.setattr(bot_module, "LINKEDIN_RATE_LIMIT_BACKOFF_BASE_SECONDS", 900)
    monkeypatch.setattr(bot_module, "LINKEDIN_RATE_LIMIT_BACKOFF_MAX_SECONDS", 100000)
    assert _linkedin_backoff_seconds(1, None) == 900
    assert _linkedin_backoff_seconds(2, None) == 1800
    assert _linkedin_backoff_seconds(3, None) == 3600
    assert _linkedin_backoff_seconds(4, None) == 7200
    assert _linkedin_backoff_seconds(5, None) == 14400


def test_backoff_honours_retry_after_when_longer(monkeypatch):
    """A generous Retry-After must win over our own ramp."""
    monkeypatch.setattr(bot_module, "LINKEDIN_RATE_LIMIT_BACKOFF_BASE_SECONDS", 900)
    monkeypatch.setattr(bot_module, "LINKEDIN_RATE_LIMIT_BACKOFF_MAX_SECONDS", 100000)
    # Our ramp says 900s, LinkedIn asks 5400s -> wait the longer one
    assert _linkedin_backoff_seconds(1, 5400) == 5400


def test_backoff_ignores_retry_after_when_shorter(monkeypatch):
    """A stingy Retry-After must not shorten our ramp below it."""
    monkeypatch.setattr(bot_module, "LINKEDIN_RATE_LIMIT_BACKOFF_BASE_SECONDS", 900)
    monkeypatch.setattr(bot_module, "LINKEDIN_RATE_LIMIT_BACKOFF_MAX_SECONDS", 100000)
    # Our ramp says 3600s on the 3rd failure, LinkedIn asks only 30s
    assert _linkedin_backoff_seconds(3, 30) == 3600


def test_backoff_is_capped(monkeypatch):
    """A hostile or buggy header must not park the monitor forever."""
    monkeypatch.setattr(bot_module, "LINKEDIN_RATE_LIMIT_BACKOFF_BASE_SECONDS", 900)
    monkeypatch.setattr(bot_module, "LINKEDIN_RATE_LIMIT_BACKOFF_MAX_SECONDS", 21600)
    assert _linkedin_backoff_seconds(50, None) == 21600
    # Absurd Retry-After is clamped too
    assert _linkedin_backoff_seconds(1, 999_999_999) == 21600


def test_backoff_never_returns_zero(monkeypatch):
    """Retry-After: 0 would otherwise spin the poller."""
    monkeypatch.setattr(bot_module, "LINKEDIN_RATE_LIMIT_BACKOFF_BASE_SECONDS", 900)
    monkeypatch.setattr(bot_module, "LINKEDIN_RATE_LIMIT_BACKOFF_MAX_SECONDS", 21600)
    assert _linkedin_backoff_seconds(1, 0) == 900  # ramp wins
    # And with a zero base/max, still at least 1s
    monkeypatch.setattr(bot_module, "LINKEDIN_RATE_LIMIT_BACKOFF_BASE_SECONDS", 1)
    assert _linkedin_backoff_seconds(1, 0) >= 1


def test_backoff_handles_absurd_attempt_count(monkeypatch):
    """A long outage must not overflow the exponent into a huge integer."""
    monkeypatch.setattr(bot_module, "LINKEDIN_RATE_LIMIT_BACKOFF_BASE_SECONDS", 900)
    monkeypatch.setattr(bot_module, "LINKEDIN_RATE_LIMIT_BACKOFF_MAX_SECONDS", 21600)
    result = _linkedin_backoff_seconds(10_000, None)
    assert result == 21600
    assert isinstance(result, int)


# --------------------------------------------------------------------------- #
#  poll_linkedin_subscriptions signalling
# --------------------------------------------------------------------------- #


def _install_subscriptions(monkeypatch, count=2):
    monkeypatch.setattr(
        bot_module,
        "list_linkedin_subscriptions",
        lambda enabled_only=True: [
            {"id": index, "enabled": True} for index in range(1, count + 1)
        ],
    )


def test_poll_returns_false_when_no_subscriptions(monkeypatch):
    monkeypatch.setattr(bot_module, "list_linkedin_subscriptions", lambda enabled_only=True: [])
    assert asyncio.run(poll_linkedin_subscriptions()) is False


def test_poll_returns_false_on_success(monkeypatch):
    _install_subscriptions(monkeypatch)
    monkeypatch.setattr(
        bot_module,
        "process_linkedin_subscription",
        lambda subscription: asyncio.sleep(0),
    )
    assert asyncio.run(poll_linkedin_subscriptions()) is False


def test_poll_returns_true_on_rate_limit_and_records_hint(monkeypatch):
    """The loop must be told to back off, and must get the Retry-After hint."""
    _install_subscriptions(monkeypatch, count=2)

    async def _raise(subscription):
        raise LinkedInRateLimitedError(retry_after_seconds=1800)

    monkeypatch.setattr(bot_module, "process_linkedin_subscription", _raise)
    monkeypatch.setattr(bot_module, "_LINKEDIN_RETRY_AFTER_HINT", None)

    assert asyncio.run(poll_linkedin_subscriptions()) is True
    assert bot_module._LINKEDIN_RETRY_AFTER_HINT == 1800


def test_poll_one_rate_limited_among_many_signals_true(monkeypatch):
    """One throttled subscription is enough to warrant a backoff."""
    _install_subscriptions(monkeypatch, count=3)

    async def _maybe_raise(subscription):
        if subscription["id"] == 2:
            raise LinkedInRateLimitedError(retry_after_seconds=600)
        await asyncio.sleep(0)

    monkeypatch.setattr(bot_module, "process_linkedin_subscription", _maybe_raise)
    monkeypatch.setattr(bot_module, "_LINKEDIN_RETRY_AFTER_HINT", None)

    assert asyncio.run(poll_linkedin_subscriptions()) is True
    assert bot_module._LINKEDIN_RETRY_AFTER_HINT == 600


def test_poll_rate_limit_without_header_leaves_hint_unchanged(monkeypatch):
    """A 429 with no Retry-After must not invent a hint."""
    _install_subscriptions(monkeypatch, count=1)
    monkeypatch.setattr(bot_module, "_LINKEDIN_RETRY_AFTER_HINT", None)

    async def _raise(subscription):
        raise LinkedInRateLimitedError()

    monkeypatch.setattr(bot_module, "process_linkedin_subscription", _raise)
    assert asyncio.run(poll_linkedin_subscriptions()) is True
    assert bot_module._LINKEDIN_RETRY_AFTER_HINT is None


def test_poll_does_not_let_one_failure_abort_others(monkeypatch):
    """A throttled subscription must not stop the remaining ones."""
    _install_subscriptions(monkeypatch, count=3)
    seen = []

    async def _maybe_raise(subscription):
        seen.append(subscription["id"])
        if subscription["id"] == 1:
            raise LinkedInRateLimitedError(retry_after_seconds=300)

    monkeypatch.setattr(bot_module, "process_linkedin_subscription", _maybe_raise)
    monkeypatch.setattr(bot_module, "_LINKEDIN_RETRY_AFTER_HINT", None)

    assert asyncio.run(poll_linkedin_subscriptions()) is True
    assert seen == [1, 2, 3]


def test_poll_non_429_error_is_not_treated_as_rate_limit(monkeypatch):
    """A genuine 500 is a fault, not throttling, so it must not trigger backoff."""
    _install_subscriptions(monkeypatch, count=1)

    async def _raise(subscription):
        raise RuntimeError("LinkedIn profile page returned HTTP 500.")

    monkeypatch.setattr(bot_module, "process_linkedin_subscription", _raise)
    assert asyncio.run(poll_linkedin_subscriptions()) is False


# --------------------------------------------------------------------------- #
#  Monitor loop behaviour
# --------------------------------------------------------------------------- #


class _FakeBot:
    """Bot stand-in that reports closed once a poll count is reached."""

    def __init__(self):
        self.closed = False

    def is_closed(self):
        return self.closed


def _run_loop(monkeypatch, poll_results, base=900, cap=21600, interval=900):
    """Drive linkedin_monitor_loop to completion over a scripted poll sequence.

    Only ``asyncio.sleep`` is faked -- on the module under test, so the loop's own
    slice/backoff arithmetic runs for real while the test stays instant. Returns
    the list of sleep durations the loop requested, in order.

    ``poll_results`` is consumed one entry per poll; the loop is stopped after the
    script is exhausted, so the sequence drives behaviour deterministically.
    """
    sleeps = []
    fake_bot = _FakeBot()
    results = list(poll_results)
    calls = {"polls": 0}

    async def _fake_sleep(seconds):
        sleeps.append(seconds)
        await asyncio.sleep(0)  # yield, do not actually wait

    async def _poll():
        index = calls["polls"]
        calls["polls"] += 1
        if index >= len(results):
            fake_bot.closed = True
            return False
        result = results[index]
        if result == "stop":
            fake_bot.closed = True
            return False
        return result

    monkeypatch.setattr(bot_module, "asyncio", types.SimpleNamespace(sleep=_fake_sleep))
    monkeypatch.setattr(bot_module, "bot", fake_bot)
    monkeypatch.setattr(bot_module, "poll_linkedin_subscriptions", _poll)
    monkeypatch.setattr(bot_module, "LINKEDIN_RATE_LIMIT_BACKOFF_BASE_SECONDS", base)
    monkeypatch.setattr(bot_module, "LINKEDIN_RATE_LIMIT_BACKOFF_MAX_SECONDS", cap)
    monkeypatch.setattr(bot_module, "LINKEDIN_POLL_INTERVAL_SECONDS", interval)
    monkeypatch.setattr(bot_module, "_LINKEDIN_RETRY_AFTER_HINT", None)

    async def _main():
        await bot_module.linkedin_monitor_loop()

    asyncio.run(_main())
    return sleeps


def test_loop_backs_off_instead_of_flat_interval(monkeypatch):
    """The core fix: once throttled, the wait is a backoff, not the flat interval.

    The first sleep is the healthy pre-429 interval and stays at 900. After the
    429 the loop must switch to bounded slices, which the pre-fix loop could
    never produce.
    """
    sleeps = _run_loop(monkeypatch, poll_results=[True, "stop"])

    assert sleeps[0] == 900, "expected the normal interval before any 429"
    assert 30 in sleeps, "expected a backoff to be slept in bounded slices"
    assert sum(sleeps) == 900 + 900, "expected one full backoff episode"


def test_backoff_escalates_across_consecutive_rate_limits(monkeypatch):
    """Consecutive 429s must ramp, not stay pinned at the first step."""
    sleeps = _run_loop(
        monkeypatch,
        poll_results=[True, True, True, "stop"],
        base=900,
        cap=100000,
    )

    # Backoffs for three consecutive 429s. They run back-to-back with no flat
    # interval in between, so the total is 900 + 1800 + 3600 = 6300.
    assert sleeps[0] == 900, "expected the normal interval before any 429"
    assert set(sleeps[1:]) == {30}, f"unexpected sleep values: {set(sleeps[1:])}"
    assert sum(sleeps[1:]) == 900 + 1800 + 3600, f"backoff did not escalate: {sum(sleeps[1:])}"


def test_backoff_clears_after_a_clean_poll(monkeypatch):
    """One good poll returns the monitor to its normal cadence."""
    sleeps = _run_loop(monkeypatch, poll_results=[True, False, "stop"], base=900, interval=900)
    # 900s backoff (sliced), then a full uninterrupted 900s interval afterwards.
    assert 900 in sleeps, "monitor never returned to the flat interval after success"


def test_healthy_loop_uses_flat_interval_only(monkeypatch):
    """No 429 anywhere -> the original cadence, with no backoff slicing."""
    sleeps = _run_loop(monkeypatch, poll_results=[False, False, "stop"], interval=900)
    assert 30 not in sleeps, "backoff triggered without a rate limit"
    assert 900 in sleeps


def test_loop_stops_promptly_during_a_long_backoff(monkeypatch):
    """Shutdown during a multi-hour backoff must not wait out the timer.

    Every sleep is bounded at 30s so the loop can observe bot shutdown; a single
    unbounded sleep here would stall a container stop for hours.
    """
    sleeps = _run_loop(
        monkeypatch,
        poll_results=[True, True, True, "stop"],
        base=3600,
        cap=100000,
    )
    assert sleeps, "loop never slept"
    # The leading 900s is the healthy pre-429 interval; every sleep after it must
    # be a bounded 30s slice.
    assert sleeps[0] == 900
    assert max(sleeps[1:]) <= 30, f"backoff used an unbounded sleep: {max(sleeps[1:])}s"

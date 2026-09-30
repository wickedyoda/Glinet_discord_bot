"""Behavioural tests: /create-ticket-from-message end to end with fakes.

The source-level tests in test_ticket_from_message_fetch.py prove the code is
shaped correctly. These drive the command with fake Discord objects to prove it
behaves correctly -- in particular that it works from ANY channel, which is the
distinction from the four channel-scoped commands.

Runs the real callback with stubbed interaction/guild objects so no network or
Discord connection is involved.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import discord
import pytest

import bot as bot_module


class FakeResponse:
    def __init__(self):
        self.messages = []

    async def send_message(self, content, **kwargs):
        self.messages.append(str(content))

    @property
    def text(self):
        return "\n".join(self.messages)


class FakeChannel:
    def __init__(self, name="support"):
        self.name = name
        self.id = 999


class FakeMessage:
    def __init__(self, content, channel=None):
        self.content = content
        self.channel = channel or FakeChannel()


def _http_error(exc_type, status, reason):
    """Build a real discord exception with the shape the library expects."""
    return exc_type(SimpleNamespace(status=status, reason=reason), f"{reason} ({status})")


class FakeGuild:
    """Resolves messages from any channel, or raises like Discord would."""

    def __init__(self, message=None, error=None):
        self._message = message
        self._error = error
        self.requested_ids = []

    async def get_message(self, message_id):
        self.requested_ids.append(message_id)
        if self._error is not None:
            raise self._error
        if self._message is None:
            raise _http_error(discord.NotFound, 404, "Not Found")
        return self._message


def _make_interaction(guild, channel_id=777):
    user = SimpleNamespace(id=42, __str__=lambda self: "tester")
    return SimpleNamespace(
        guild=guild,
        guild_id=1234,
        channel_id=channel_id,
        user=user,
        response=FakeResponse(),
    )


@pytest.fixture(autouse=True)
def _patch_guards(monkeypatch):
    """Neutralise the checks unrelated to message resolution."""
    monkeypatch.setattr(bot_module, "can_use_freshdesk_admin", lambda i: True)
    monkeypatch.setattr(
        bot_module,
        "resolve_freshdesk_config",
        lambda: {"enabled": True, "base_url": "https://acme.freshdesk.com",
                 "api_key": "key", "timeout": 15},
    )
    monkeypatch.setattr(
        bot_module, "_resolve_freshdesk_ticket_target_channel_id", lambda i: 555
    )

    captured = {}

    class RecordingView:
        def __init__(self, config, target_channel_id):
            captured["target_channel_id"] = target_channel_id
            captured["view"] = self
            self.message_content = None

    monkeypatch.setattr(bot_module, "SupportTicketCategoryView", RecordingView)
    return captured


def _callback():
    command = bot_module.create_ticket_from_message
    return getattr(command, "callback", command)


def _run(guild, message_id="123456789"):
    interaction = _make_interaction(guild)
    asyncio.run(_callback()(interaction, message_id))
    return interaction


# --------------------------------------------------------------------------- #
#  Happy path -- and specifically, from a channel that is NOT the intake channel
# --------------------------------------------------------------------------- #


def test_fetches_and_prepopulates_from_any_channel(_patch_guards):
    """The message is fetched guild-wide and carried into the view."""
    message = FakeMessage("Router keeps dropping the 5GHz client", FakeChannel("general"))
    guild = FakeGuild(message=message)

    interaction = _run(guild)

    assert guild.requested_ids == [123456789], "the message ID should be resolved"
    view = _patch_guards["view"]
    assert view.message_content == "Router keeps dropping the 5GHz client"
    assert "general" in interaction.response.text, (
        "the prompt should say which channel the message came from"
    )
    # The thread destination is still the configured intake channel.
    assert _patch_guards["target_channel_id"] == 555


def test_works_even_when_invoked_in_a_different_channel(_patch_guards):
    """Channel 777 is not the intake channel (555), and that must be fine."""
    guild = FakeGuild(message=FakeMessage("escalate me"))
    interaction = _make_interaction(guild, channel_id=777)

    asyncio.run(_callback()(interaction, "999888777"))

    assert guild.requested_ids == [999888777]
    assert _patch_guards["view"].message_content == "escalate me"
    assert "Wrong channel" not in interaction.response.text
    assert _patch_guards["target_channel_id"] == 555


# --------------------------------------------------------------------------- #
#  Guard paths
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("bad", ["", "abc", "not-a-snowflake", "12.5", None])
def test_rejects_a_non_numeric_message_id(_patch_guards, bad):
    guild = FakeGuild(message=FakeMessage("unused"))

    interaction = _run(guild, message_id=bad)

    assert guild.requested_ids == [], "should not attempt a fetch for a bad ID"
    assert "not valid" in interaction.response.text
    assert not hasattr(_patch_guards, "view") or _patch_guards.get("view") is None


@pytest.mark.parametrize("bad", ["0", "-1"])
def test_rejects_a_non_positive_message_id(_patch_guards, bad):
    guild = FakeGuild(message=FakeMessage("unused"))
    interaction = _run(guild, message_id=bad)
    assert guild.requested_ids == []
    assert "not valid" in interaction.response.text


def test_missing_message_reports_clearly(_patch_guards):
    guild = FakeGuild(message=None)
    interaction = _run(guild)
    assert "could not be found" in interaction.response.text


def test_forbidden_channel_reports_clearly(_patch_guards):
    guild = FakeGuild(error=_http_error(discord.Forbidden, 403, "Forbidden"))
    interaction = _run(guild)
    assert "cannot read the channel" in interaction.response.text


def test_http_failure_is_reported_and_logged(_patch_guards):
    guild = FakeGuild(error=_http_error(discord.HTTPException, 500, "Server Error"))
    interaction = _run(guild)
    assert "could not be read just now" in interaction.response.text


def test_empty_message_is_rejected_rather_than_opening_a_blank_ticket(_patch_guards):
    """An attachment-only message has no body to escalate."""
    guild = FakeGuild(message=FakeMessage("   "))
    interaction = _run(guild)
    assert "no text content" in interaction.response.text


def test_overlong_message_is_truncated_to_the_modal_limit(_patch_guards):
    long_body = "x" * 9000
    guild = FakeGuild(message=FakeMessage(long_body))
    _run(guild)
    assert len(_patch_guards["view"].message_content) == 4000


def test_message_content_is_stripped(_patch_guards):
    guild = FakeGuild(message=FakeMessage("  padded body  "))
    _run(guild)
    assert _patch_guards["view"].message_content == "padded body"

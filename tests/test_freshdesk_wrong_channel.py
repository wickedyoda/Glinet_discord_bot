"""Tests for the Freshdesk wrong-channel guidance.

Regression coverage for the bug where invoking a Freshdesk command outside the
configured intake channel produced the "Freshdesk integration is not configured
or disabled" message, which is both wrong and unactionable.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import discord

from bot import _freshdesk_wrong_channel_reply

INTAKE_CHANNEL_ID = 111111111
OTHER_CHANNEL_ID = 222222222


def _build_interaction(channel_id: int, channel_name: str = "random-chat"):
    interaction = MagicMock(spec=discord.Interaction)
    interaction.guild = MagicMock()
    interaction.guild_id = 999
    interaction.channel_id = channel_id
    interaction.channel = MagicMock()
    interaction.channel.name = channel_name
    return interaction


def _mention_lookup(expected_channel):
    """bot.get_channel -> expected_channel; guild.get_channel -> same object."""

    def fake_get_channel(_cid):
        return expected_channel

    return fake_get_channel


def test_wrong_channel_returns_guidance_pointing_at_intake_channel():
    expected = MagicMock()
    expected.mention = "<#111111111>"
    interaction = _build_interaction(OTHER_CHANNEL_ID)

    with (
        patch("bot._resolve_freshdesk_ticket_target_channel_id", return_value=INTAKE_CHANNEL_ID),
        patch("bot.bot") as bot_mock,
    ):
        bot_mock.get_channel.side_effect = _mention_lookup(expected)
        reply = _freshdesk_wrong_channel_reply(interaction)

    assert reply is not None
    assert "Wrong channel" in reply
    assert "<#111111111>" in reply
    assert "nothing was submitted" in reply


def test_correct_channel_returns_none():
    interaction = _build_interaction(INTAKE_CHANNEL_ID)
    with patch("bot._resolve_freshdesk_ticket_target_channel_id", return_value=INTAKE_CHANNEL_ID):
        assert _freshdesk_wrong_channel_reply(interaction) is None


def test_no_configured_channel_returns_none_so_existing_handling_applies():
    """When no intake channel is configured there is no 'right' channel to name.

    Returning None lets the command fall through to its existing
    not-configured message instead of inventing a destination.
    """
    interaction = _build_interaction(OTHER_CHANNEL_ID)
    with patch("bot._resolve_freshdesk_ticket_target_channel_id", return_value=None):
        assert _freshdesk_wrong_channel_reply(interaction) is None


def test_dm_invocation_returns_none():
    """DMs have no channel_id to compare; do not emit a bogus wrong-channel error."""
    interaction = _build_interaction(OTHER_CHANNEL_ID)
    interaction.channel_id = None
    with patch("bot._resolve_freshdesk_ticket_target_channel_id", return_value=INTAKE_CHANNEL_ID):
        assert _freshdesk_wrong_channel_reply(interaction) is None


def test_reply_includes_current_channel_name_for_context():
    expected = MagicMock()
    expected.mention = "<#111111111>"
    interaction = _build_interaction(OTHER_CHANNEL_ID, channel_name="general")

    with (
        patch("bot._resolve_freshdesk_ticket_target_channel_id", return_value=INTAKE_CHANNEL_ID),
        patch("bot.bot") as bot_mock,
    ):
        bot_mock.get_channel.side_effect = _mention_lookup(expected)
        reply = _freshdesk_wrong_channel_reply(interaction)

    assert "**#general**" in reply


def test_reply_falls_back_to_snowflake_when_channel_object_missing():
    """Bot cache miss should still name the channel rather than crash or omit it."""
    interaction = _build_interaction(OTHER_CHANNEL_ID)
    with (
        patch("bot._resolve_freshdesk_ticket_target_channel_id", return_value=INTAKE_CHANNEL_ID),
        patch("bot.bot") as bot_mock,
    ):
        bot_mock.get_channel.return_value = None
        interaction.guild.get_channel.return_value = None
        reply = _freshdesk_wrong_channel_reply(interaction)

    assert f"<#{INTAKE_CHANNEL_ID}>" in reply

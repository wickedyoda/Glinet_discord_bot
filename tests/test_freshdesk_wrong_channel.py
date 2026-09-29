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


def _config(**overrides):
    base = {
        "enabled": True,
        "base_url": "https://example.freshdesk.com",
        "api_key": "key",
        "timeout": 15,
        "wrong_channel_message": "",
    }
    base.update(overrides)
    return base


def _reply(interaction, config, expected_channel=None):
    """Call the helper with the intake channel resolvable and the bot cache mocked."""
    with (
        patch("bot._resolve_freshdesk_ticket_target_channel_id", return_value=INTAKE_CHANNEL_ID),
        patch("bot.bot") as bot_mock,
    ):
        if expected_channel is None:
            bot_mock.get_channel.return_value = None
            interaction.guild.get_channel.return_value = None
        else:
            bot_mock.get_channel.side_effect = lambda _cid: expected_channel
        return _freshdesk_wrong_channel_reply(interaction, config)


def test_wrong_channel_returns_guidance_pointing_at_intake_channel():
    expected = MagicMock()
    expected.mention = "<#111111111>"
    reply = _reply(_build_interaction(OTHER_CHANNEL_ID), _config(), expected_channel=expected)

    assert reply is not None
    assert "Wrong channel" in reply
    assert "<#111111111>" in reply
    assert "nothing was submitted" in reply


def test_default_message_uses_a_real_newline_not_a_literal_backslash_n():
    """A literal backslash-n would render as text in Discord instead of a line break."""
    expected = MagicMock()
    expected.mention = "<#111111111>"
    reply = _reply(_build_interaction(OTHER_CHANNEL_ID), _config(), expected_channel=expected)

    assert "\\n" not in reply
    assert "\n" in reply


def test_correct_channel_returns_none():
    interaction = _build_interaction(INTAKE_CHANNEL_ID)
    with patch("bot._resolve_freshdesk_ticket_target_channel_id", return_value=INTAKE_CHANNEL_ID):
        assert _freshdesk_wrong_channel_reply(interaction, _config()) is None


def test_no_configured_channel_returns_none_so_existing_handling_applies():
    """When no intake channel is configured there is no 'right' channel to point at.

    Returning None lets the command fall through to its existing
    not-configured message instead of inventing a destination.
    """
    interaction = _build_interaction(OTHER_CHANNEL_ID)
    with patch("bot._resolve_freshdesk_ticket_target_channel_id", return_value=None):
        assert _freshdesk_wrong_channel_reply(interaction, _config()) is None


def test_dm_invocation_returns_none():
    """DMs have no channel_id to compare; do not emit a bogus wrong-channel error."""
    interaction = _build_interaction(OTHER_CHANNEL_ID)
    interaction.channel_id = None
    with patch("bot._resolve_freshdesk_ticket_target_channel_id", return_value=INTAKE_CHANNEL_ID):
        assert _freshdesk_wrong_channel_reply(interaction, _config()) is None


def test_reply_includes_current_channel_name_for_context():
    expected = MagicMock()
    expected.mention = "<#111111111>"
    reply = _reply(
        _build_interaction(OTHER_CHANNEL_ID, channel_name="general"),
        _config(),
        expected_channel=expected,
    )

    assert "**#general**" in reply


def test_reply_falls_back_to_snowflake_when_channel_object_missing():
    """Bot cache miss should still name the channel rather than crash or omit it."""
    reply = _reply(_build_interaction(OTHER_CHANNEL_ID), _config())

    assert f"<#{INTAKE_CHANNEL_ID}>" in reply


def test_channel_lookup_failure_does_not_break_the_command():
    """A raising get_channel must still yield a usable message."""
    interaction = _build_interaction(OTHER_CHANNEL_ID)
    with (
        patch("bot._resolve_freshdesk_ticket_target_channel_id", return_value=INTAKE_CHANNEL_ID),
        patch("bot.bot") as bot_mock,
    ):
        bot_mock.get_channel.side_effect = RuntimeError("cache offline")
        interaction.guild.get_channel.side_effect = RuntimeError("cache offline")
        reply = _freshdesk_wrong_channel_reply(interaction, _config())

    assert f"<#{INTAKE_CHANNEL_ID}>" in reply


# --------------------------------------------------------------------------- #
#  Custom message (FRESHDESK_WRONG_CHANNEL_MESSAGE)
# --------------------------------------------------------------------------- #
def test_custom_message_replaces_default_wording():
    expected = MagicMock()
    expected.mention = "<#111111111>"
    config = _config(wrong_channel_message="Support tickets belong in the help desk channel.")
    reply = _reply(_build_interaction(OTHER_CHANNEL_ID), config, expected_channel=expected)

    assert reply == "Support tickets belong in the help desk channel."


def test_custom_message_placeholders_are_substituted():
    expected = MagicMock()
    expected.mention = "<#111111111>"
    config = _config(
        wrong_channel_message="You used {wrong_channel}. Please use {right_channel} instead."
    )
    reply = _reply(
        _build_interaction(OTHER_CHANNEL_ID, channel_name="general"),
        config,
        expected_channel=expected,
    )

    assert "#general" in reply
    assert "<#111111111>" in reply
    assert "{wrong_channel}" not in reply
    assert "{right_channel}" not in reply


def test_custom_message_with_literal_braces_does_not_raise():
    """str.format would raise KeyError/IndexError on admin text containing braces.

    Example: an admin pastes a JSON snippet or writes "{not_a_placeholder}".
    """
    expected = MagicMock()
    expected.mention = "<#111111111>"
    config = _config(wrong_channel_message='Example JSON: {"a": 1} — use {right_channel} {0}')
    reply = _reply(_build_interaction(OTHER_CHANNEL_ID), config, expected_channel=expected)

    assert reply is not None
    assert '{"a": 1}' in reply
    assert "<#111111111>" in reply


def test_custom_message_is_truncated_to_discord_limit():
    expected = MagicMock()
    expected.mention = "<#111111111>"
    config = _config(wrong_channel_message="X" * 5000)
    reply = _reply(_build_interaction(OTHER_CHANNEL_ID), config, expected_channel=expected)

    assert len(reply) <= 2000


def test_blank_custom_message_falls_back_to_default():
    expected = MagicMock()
    expected.mention = "<#111111111>"
    config = _config(wrong_channel_message="   ")
    reply = _reply(_build_interaction(OTHER_CHANNEL_ID), config, expected_channel=expected)

    assert "Wrong channel" in reply

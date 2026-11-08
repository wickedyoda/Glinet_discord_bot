"""Tests for emoji shortcode resolution and pre-post validation.

Discord's own composer accepts shortcode names (``:thumbsup:``), so users type them
into the web form. discord.py's ``add_reaction()`` rejects them with
``400 In emoji_id: Value "" is not snowflake`` because it parses ``:thumbsup:`` as a
custom emoji with an empty id. The bot therefore has to resolve them to the real
Unicode emoji — and must refuse to post a message whose reactions could not be added.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import discord
import pytest

from app.reaction_roles import (
    normalize_reaction_role_emoji,
    resolve_reaction_emoji_shortcode,
)


def test_common_discord_shortcode_resolves_to_unicode():
    result = normalize_reaction_role_emoji(":thumbsup:")
    assert result["emoji_key"] == "unicode:\U0001f44d"
    assert result["emoji_text"] == "\U0001f44d"


def test_discord_specific_alias_missing_from_the_emoji_package():
    """:tools: is a Discord name the `emoji` package does not carry."""
    result = normalize_reaction_role_emoji(":tools:")
    assert result["emoji_key"].startswith("unicode:")
    assert result["emoji_key"] != "unicode::tools:"
    assert len(result["emoji_text"]) > 0


def test_plain_unicode_emoji_is_unchanged():
    result = normalize_reaction_role_emoji("\U0001f3ae")
    assert result["emoji_key"] == "unicode:\U0001f3ae"
    assert result["emoji_text"] == "\U0001f3ae"


def test_custom_emoji_is_still_treated_as_custom():
    result = normalize_reaction_role_emoji("<:gamer:123456789>")
    assert result["emoji_key"] == "custom:123456789"
    assert result["emoji_text"] == "<:gamer:123456789>"


def test_unknown_shortcode_is_left_alone_so_the_post_is_refused():
    """An unresolvable shortcode must survive normalisation, not be silently mangled."""
    result = normalize_reaction_role_emoji(":definitely_not_an_emoji:")
    assert result["emoji_key"] == "unicode::definitely_not_an_emoji:"


def test_shortcode_helper_returns_none_for_non_shortcodes():
    assert resolve_reaction_emoji_shortcode("\U0001f3ae") is None
    assert resolve_reaction_emoji_shortcode("<:gamer:123>") is None
    assert resolve_reaction_emoji_shortcode("") is None


def test_shortcode_helper_resolves_known_alias():
    assert resolve_reaction_emoji_shortcode(":thumbsup:") == "\U0001f44d"


def _make_guild(*, with_role=True):
    guild = MagicMock(spec=discord.Guild)
    channel = MagicMock(spec=discord.TextChannel)
    bot_member = MagicMock(spec=discord.Member)
    bot_member.guild_permissions.manage_roles = True

    top_role = MagicMock(spec=discord.Role)
    top_role.name = "BotRole"
    top_role.__gt__ = lambda self, other: True
    top_role.__ge__ = lambda self, other: True
    top_role.__le__ = lambda self, other: False
    top_role.__lt__ = lambda self, other: False
    bot_member.top_role = top_role
    guild.me = bot_member

    role = MagicMock(spec=discord.Role)
    role.id = 111
    role.name = "Gamer"
    role.managed = False
    role.__le__ = lambda self, other: True
    guild.get_role.side_effect = lambda rid: role if rid == 111 else None
    guild.get_channel.side_effect = lambda cid: channel if cid == 999 else None

    perms = MagicMock()
    perms.send_messages = True
    perms.add_reactions = True
    channel.permissions_for.return_value = perms
    return guild, channel


def test_unresolvable_emoji_prevents_the_message_from_being_posted():
    """The reported bug: a message went out with no clickable reactions."""
    import bot

    guild, channel = _make_guild()
    sent_message = AsyncMock(spec=discord.Message)
    sent_message.id = 55555555
    channel.send = AsyncMock(return_value=sent_message)

    save_mock = MagicMock()
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(bot.bot, "get_guild", lambda gid: guild)
        mp.setattr(bot, "save_reaction_role_mapping", save_mock)
        mp.setattr(bot, "_refresh_reaction_role_cache_for_guild", MagicMock())

        result = asyncio.run(
            bot.post_reaction_role_message_for_guild_async(
                guild_id=123,
                channel_id=999,
                message_content="React below:",
                pairs=[{"emoji": ":definitely_not_an_emoji:", "role_id": 111}],
            )
        )

    assert result["ok"] is False
    assert "not a recognised emoji" in result["error"]
    # Nothing was posted and nothing was persisted.
    channel.send.assert_not_awaited()
    save_mock.assert_not_called()


def test_shortcoded_emoji_is_posted_with_a_real_reaction():
    """End to end: the user's ':thumbsup:' becomes an actual \U0001f44d reaction."""
    import bot

    guild, channel = _make_guild()
    sent_message = AsyncMock(spec=discord.Message)
    sent_message.id = 55555555
    sent_message.jump_url = "https://discord.com/channels/123/999/55555555"
    channel.send = AsyncMock(return_value=sent_message)

    save_mock = MagicMock()
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(bot.bot, "get_guild", lambda gid: guild)
        mp.setattr(bot, "save_reaction_role_mapping", save_mock)
        mp.setattr(bot, "_refresh_reaction_role_cache_for_guild", MagicMock())

        result = asyncio.run(
            bot.post_reaction_role_message_for_guild_async(
                guild_id=123,
                channel_id=999,
                message_content="React below:",
                pairs=[{"emoji": ":thumbsup:", "role_id": 111}],
            )
        )

    assert result["ok"] is True
    assert result["failed_emojis"] == []
    sent_message.add_reaction.assert_awaited_once_with("\U0001f44d")
    # Stored under the resolved emoji so on_raw_reaction_add can match it.
    assert save_mock.call_args.kwargs["emoji"] == ":thumbsup:"


def test_mapping_stored_under_the_resolved_emoji_key():
    """The stored key must match what on_raw_reaction_add derives from the real emoji."""
    import bot

    guild, channel = _make_guild()
    sent_message = AsyncMock(spec=discord.Message)
    sent_message.id = 55555555
    channel.send = AsyncMock(return_value=sent_message)

    save_mock = MagicMock()
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(bot.bot, "get_guild", lambda gid: guild)
        mp.setattr(bot, "save_reaction_role_mapping", save_mock)
        mp.setattr(bot, "_refresh_reaction_role_cache_for_guild", MagicMock())

        asyncio.run(
            bot.post_reaction_role_message_for_guild_async(
                guild_id=123,
                channel_id=999,
                message_content="React below:",
                pairs=[{"emoji": ":thumbsup:", "role_id": 111}],
            )
        )

    saved_emoji = save_mock.call_args.kwargs["emoji"]
    normalized = normalize_reaction_role_emoji(saved_emoji)
    assert normalized["emoji_key"] == "unicode:\U0001f44d"
    # on_raw_reaction_add builds its key from payload.emoji.name for Unicode emoji.
    assert normalized["emoji_key"] == f"unicode:{normalized['emoji_text']}"

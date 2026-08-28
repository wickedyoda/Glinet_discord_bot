from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import discord

os.environ.setdefault("DISCORD_TOKEN", "mock-token-for-tests")
os.environ.setdefault("GUILD_ID", "123456789")
os.environ.setdefault("DATA_DIR", "/tmp/bot-test-data")

from bot import (
    _resolve_role_change_actor,
    _resolve_role_create_actor,
    _resolve_channel_create_actor,
    send_server_event_log,
)


class AsyncIterator:
    def __init__(self, items):
        self.items = list(items)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self.items:
            raise StopAsyncIteration
        return self.items.pop(0)


def test_resolve_role_change_actor_success():
    async def _run():
        guild = MagicMock(spec=discord.Guild)
        guild.id = 123456

        mock_mod = MagicMock(spec=discord.Member)
        mock_mod.mention = "<@99999>"
        mock_mod.id = 99999

        mock_target = MagicMock(spec=discord.Member)
        mock_target.id = 234452991745196042

        entry = MagicMock(spec=discord.AuditLogEntry)
        entry.target = mock_target
        entry.user = mock_mod
        entry.created_at = datetime.now(UTC)

        guild.audit_logs = MagicMock(return_value=AsyncIterator([entry]))

        actor_label = await _resolve_role_change_actor(guild, 234452991745196042)
        assert actor_label == "<@99999> (`99999`)"

    asyncio.run(_run())


def test_resolve_role_change_actor_not_in_log():
    async def _run():
        guild = MagicMock(spec=discord.Guild)
        guild.id = 123456

        mock_target = MagicMock(spec=discord.Member)
        mock_target.id = 111111111

        entry = MagicMock(spec=discord.AuditLogEntry)
        entry.target = mock_target
        entry.user = MagicMock(id=99999, mention="<@99999>")
        entry.created_at = datetime.now(UTC)

        guild.audit_logs = MagicMock(return_value=AsyncIterator([entry]))

        # Target id is different
        actor_label = await _resolve_role_change_actor(guild, 234452991745196042)
        assert actor_label == "Unknown (not in audit log)"

    asyncio.run(_run())


def test_resolve_role_create_actor():
    async def _run():
        guild = MagicMock(spec=discord.Guild)
        guild.id = 123456

        mock_creator = MagicMock(spec=discord.Member)
        mock_creator.mention = "<@88888>"
        mock_creator.id = 88888

        mock_role = MagicMock(spec=discord.Role)
        mock_role.id = 55555

        entry = MagicMock(spec=discord.AuditLogEntry)
        entry.target = mock_role
        entry.user = mock_creator
        entry.created_at = datetime.now(UTC)

        guild.audit_logs = MagicMock(return_value=AsyncIterator([entry]))

        actor_label = await _resolve_role_create_actor(guild, 55555)
        assert actor_label == "<@88888> (`88888`)"

    asyncio.run(_run())


def test_send_server_event_log_custom_actor():
    async def _run():
        guild = MagicMock(spec=discord.Guild)
        guild.id = 123456

        mock_channel = AsyncMock(spec=discord.TextChannel)
        mock_channel.guild = guild

        with patch("bot.resolve_mod_log_channel", AsyncMock(return_value=mock_channel)), \
             patch("bot.record_action_safe"):
            sent = await send_server_event_log(
                guild,
                "member_role_added",
                "**Member:** <@123> (`123`)\n**Role Added:** <@&456> (`456`)\n",
                actor="<@999> (`999`)",
            )
            assert sent is True
            mock_channel.send.assert_called_once()
            sent_message = mock_channel.send.call_args[0][0]
            assert "📌 **Server Event:** `member_role_added`" in sent_message
            assert "**Actor:** <@999> (`999`)" in sent_message
            assert sent_message.count("**Actor:**") == 1

    asyncio.run(_run())

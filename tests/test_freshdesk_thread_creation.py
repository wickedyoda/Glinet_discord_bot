from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import discord

from bot import _freshdesk_create_on_submit


def _build_modal():
    modal = MagicMock()
    modal.name.value = "Alice"
    modal.email.value = "alice@example.com"
    modal.subject.value = "Need help"
    modal.message_body.value = "Please assist."
    return modal


def _build_interaction(target_channel):
    interaction = MagicMock(spec=discord.Interaction)
    interaction.user = MagicMock(spec=discord.Member)
    interaction.user.__str__.return_value = "Alice#1234"
    interaction.response.defer = AsyncMock()
    interaction.followup.send = AsyncMock()
    interaction.guild = MagicMock()
    interaction.guild.get_channel.return_value = target_channel
    return interaction


def test_freshdesk_create_adds_requester_to_private_thread_before_posting():
    async def _run():
        target_channel = MagicMock()
        thread = AsyncMock()
        thread.mention = "#support-ticket-321"
        steps: list[str] = []

        async def add_user(user):
            steps.append("add_user")

        async def send(*args, **kwargs):
            steps.append("send")

        thread.add_user.side_effect = add_user
        thread.send.side_effect = send
        target_channel.create_thread = AsyncMock(return_value=thread)

        interaction = _build_interaction(target_channel)
        modal = _build_modal()
        ticket = {"id": 321, "url": "https://example.freshdesk.com/a/tickets/321", "status": 2, "priority": 1}

        with patch("bot.asyncio.to_thread", AsyncMock(return_value=ticket)):
            await _freshdesk_create_on_submit(
                interaction,
                modal,
                target_channel_id=123,
                config={"base_url": "https://example.freshdesk.com", "api_key": "key", "timeout": 5},
            )

        interaction.response.defer.assert_awaited_once_with(ephemeral=True)
        target_channel.create_thread.assert_awaited_once()
        thread.add_user.assert_awaited_once_with(interaction.user)
        thread.send.assert_awaited_once()
        assert steps == ["add_user", "send"]

    asyncio.run(_run())


def test_freshdesk_create_reports_add_user_failure_without_posting_body():
    async def _run():
        target_channel = MagicMock()
        thread = AsyncMock()
        thread.mention = "#support-ticket-321"
        thread.add_user.side_effect = discord.Forbidden(MagicMock(), "missing permission")
        target_channel.create_thread = AsyncMock(return_value=thread)

        interaction = _build_interaction(target_channel)
        modal = _build_modal()
        ticket = {"id": 321, "url": "https://example.freshdesk.com/a/tickets/321", "status": 2, "priority": 1}

        with patch("bot.asyncio.to_thread", AsyncMock(return_value=ticket)):
            await _freshdesk_create_on_submit(
                interaction,
                modal,
                target_channel_id=123,
                config={"base_url": "https://example.freshdesk.com", "api_key": "key", "timeout": 5},
            )

        thread.send.assert_not_awaited()
        interaction.followup.send.assert_awaited_once()
        message = interaction.followup.send.await_args.args[0]
        assert "could not be added to the Discord thread" in message
        assert "missing permission" in message
        assert interaction.followup.send.await_args.kwargs["ephemeral"] is True

    asyncio.run(_run())

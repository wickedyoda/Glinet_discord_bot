from __future__ import annotations

import asyncio
import io
import json
import os
import urllib.error
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import discord

os.environ.setdefault("DISCORD_TOKEN", "mock-token-for-tests")
os.environ.setdefault("GUILD_ID", "123456789")
os.environ.setdefault("DATA_DIR", "/tmp/bot-test-data")

from app.translate import (
    TranslationResult,
    get_language_name,
    translate_text,
)
from bot import translate_to_english_ctx


def test_get_language_name():
    assert get_language_name("fr") == "French"
    assert get_language_name("zh-cn") == "Chinese (Simplified)"
    assert get_language_name("es") == "Spanish"
    assert get_language_name("de") == "German"
    assert get_language_name("unknown_code") == "UNKNOWN_CODE"
    assert get_language_name("") == "Unknown"


def test_translate_text_empty():
    with pytest.raises(ValueError, match="Cannot translate empty text"):
        translate_text("")
    with pytest.raises(ValueError, match="Cannot translate empty text"):
        translate_text("   ")


def test_translate_text_mocked_success():
    fake_response = [
        [["Hello world! ", "Bonjour le monde! ", None, None]],
        None,
        "fr",
    ]
    raw_bytes = json.dumps(fake_response).encode("utf-8")

    mock_resp = MagicMock()
    mock_resp.read.return_value = raw_bytes
    mock_resp.__enter__.return_value = mock_resp
    mock_resp.__exit__.return_value = None

    with patch("urllib.request.urlopen", return_value=mock_resp):
        res = translate_text("Bonjour le monde!", target_lang="en")
        assert isinstance(res, TranslationResult)
        assert res.text == "Hello world!"
        assert res.source_language == "fr"
        assert res.source_language_name == "French"
        assert res.target_language == "en"
        assert res.target_language_name == "English"


def test_translate_text_network_error():
    with patch("urllib.request.urlopen", side_effect=urllib.error.URLError("Network down")):
        with pytest.raises(RuntimeError, match="Unable to reach translation service"):
            translate_text("Hello")


def test_translate_context_menu_empty_message():
    async def _run():
        interaction = MagicMock(spec=discord.Interaction)
        interaction.user = MagicMock(id=1234, spec=discord.Member)
        interaction.response.send_message = AsyncMock()

        message = MagicMock(spec=discord.Message)
        message.content = ""

        with patch("bot.ensure_interaction_command_access", AsyncMock(return_value=True)):
            await translate_to_english_ctx.callback(interaction, message)
            interaction.response.send_message.assert_called_once()
            assert "contains no text" in interaction.response.send_message.call_args[0][0]

    asyncio.run(_run())


def test_translate_context_menu_success():
    async def _run():
        interaction = MagicMock(spec=discord.Interaction)
        interaction.user = MagicMock(id=1234, spec=discord.Member)
        interaction.response.defer = AsyncMock()
        interaction.followup.send = AsyncMock()

        message = MagicMock(spec=discord.Message)
        message.id = 99887766
        message.content = "Bonjour"
        message.jump_url = "https://discord.com/channels/1/2/3"
        message.author = MagicMock(spec=discord.Member)
        message.author.display_name = "Pierre"
        message.author.mention = "<@5555>"
        message.author.display_avatar.url = "https://cdn.discordapp.com/avatar.png"

        fake_result = TranslationResult(
            text="Hello",
            source_language="fr",
            source_language_name="French",
            target_language="en",
            target_language_name="English",
        )

        with patch("bot.ensure_interaction_command_access", AsyncMock(return_value=True)), \
             patch("bot.translate_text", return_value=fake_result):
            await translate_to_english_ctx.callback(interaction, message)
            interaction.response.defer.assert_called_once_with(ephemeral=False)
            interaction.followup.send.assert_called_once()
            call_kwargs = interaction.followup.send.call_args[1]
            assert "embed" in call_kwargs
            embed = call_kwargs["embed"]
            assert embed.description == "Hello"
            assert "French (fr)" in embed.footer.text
            assert "https://discord.com/channels/1/2/3" in call_kwargs["content"]

    asyncio.run(_run())

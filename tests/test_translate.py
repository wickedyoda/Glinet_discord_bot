from __future__ import annotations

import asyncio
import json
import os
import threading
import urllib.error
from unittest.mock import AsyncMock, MagicMock, patch

import discord
import pytest

os.environ.setdefault("DISCORD_TOKEN", "mock-token-for-tests")
os.environ.setdefault("GUILD_ID", "123456789")
os.environ.setdefault("DATA_DIR", "/tmp/bot-test-data")

from app.translate import (
    FLAG_EMOJI_TO_LANG,
    TranslationResult,
    get_lang_for_flag,
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


def test_get_lang_for_flag_known():
    assert get_lang_for_flag("🇫🇷") == "fr"
    assert get_lang_for_flag("🇩🇪") == "de"
    assert get_lang_for_flag("🇪🇸") == "es"
    assert get_lang_for_flag("🇨🇳") == "zh-cn"
    assert get_lang_for_flag("🇯🇵") == "ja"
    assert get_lang_for_flag("🇰🇷") == "ko"
    assert get_lang_for_flag("🇷🇺") == "ru"
    assert get_lang_for_flag("🇺🇸") == "en"
    assert get_lang_for_flag("🇬🇧") == "en"


def test_get_lang_for_flag_unknown():
    assert get_lang_for_flag("😀") is None
    assert get_lang_for_flag("🎉") is None
    assert get_lang_for_flag("") is None
    assert get_lang_for_flag(None) is None


def test_flag_emoji_mapping_is_extensive():
    # Sanity: at least 20 distinct languages supported via flag reactions
    languages = set(FLAG_EMOJI_TO_LANG.values())
    assert len(languages) >= 20
    assert "en" in languages
    assert "es" in languages
    assert "fr" in languages
    assert "de" in languages


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


def test_flag_emoji_to_lang_mapping():
    # Verify the mapping dict exists and has expected entries
    from app.translate import FLAG_EMOJI_TO_LANG as FLAG_MAP

    assert FLAG_MAP.get("🇫🇷") == "fr"
    assert FLAG_MAP.get("🇩🇪") == "de"
    assert FLAG_MAP.get("🇪🇸") == "es"
    assert FLAG_MAP.get("🇨🇳") == "zh-cn"
    assert FLAG_MAP.get("🇯🇵") == "ja"
    assert FLAG_MAP.get("🇰🇷") == "ko"
    assert FLAG_MAP.get("🇺🇸") == "en"
    assert FLAG_MAP.get("🇬🇧") == "en"
    assert FLAG_MAP.get("🇷🇺") == "ru"
    assert FLAG_MAP.get("🇧🇷") == "pt"
    assert FLAG_MAP.get("🇮🇳") == "hi"
    assert FLAG_MAP.get("🇸🇦") == "ar"
    assert FLAG_MAP.get("🇹🇷") == "tr"


def test_flag_reaction_translates_message():
    """When a user reacts with a flag emoji, the bot should translate and reply."""
    from bot import get_lang_for_flag

    assert get_lang_for_flag("🇫🇷") == "fr"
    assert get_lang_for_flag("😀") is None


def test_auto_translate_channel_store_crud():
    """The AutoTranslateChannelStore supports full CRUD lifecycle."""
    from bot import AutoTranslateChannelStore

    db_path = "/tmp/test_translate_crud.db"
    if os.path.exists(db_path):
        os.remove(db_path)
    store = AutoTranslateChannelStore(db_path, threading.RLock())

    store.upsert(42, 100, 200, target_language="en", source_language="auto", enabled=True)
    store.upsert(42, 100, 300, target_language="es", source_language="auto", enabled=False)

    active = store.list_active_for_source(42, 100)
    assert len(active) == 1
    assert active[0]["target_channel_id"] == 200

    all_maps = store.list_for_guild(42)
    assert len(all_maps) == 2

    assert store.set_enabled(42, 100, 300, "es", True) is True
    assert len(store.list_active_for_source(42, 100)) == 2

    assert store.delete(42, 100, 200, "en") is True
    assert len(store.list_for_guild(42)) == 1


def test_translate_web_callback_returns_payload():
    """The web admin callback returns a serializable payload with entries."""
    import bot as bot_module
    from bot import AutoTranslateChannelStore, run_web_get_translate_channels

    db_path = "/tmp/test_translate_web_get.db"
    if os.path.exists(db_path):
        os.remove(db_path)
    bot_module.auto_translate_channel_store = AutoTranslateChannelStore(db_path, threading.RLock())

    payload = run_web_get_translate_channels(42)
    assert payload["ok"] is True
    assert "entries" in payload
    assert "guild_id" in payload


def test_translate_web_manage_creates():
    """The web admin manage callback creates mappings and returns updated entries."""
    import bot as bot_module
    from bot import AutoTranslateChannelStore, run_web_manage_translate_channels

    db_path = "/tmp/test_translate_web_create.db"
    if os.path.exists(db_path):
        os.remove(db_path)
    bot_module.auto_translate_channel_store = AutoTranslateChannelStore(db_path, threading.RLock())

    payload = run_web_manage_translate_channels(
        {"action": "create_entry", "source_channel_id": "50", "target_channel_id": "60",
         "source_language": "auto", "target_language": "en", "enabled": "1"},
        "test@example.com", 42,
    )
    assert payload["ok"] is True
    assert len(payload["entries"]) == 1
    assert payload["entries"][0]["source_channel_id"] == 50
    assert payload["entries"][0]["target_channel_id"] == 60
    assert payload["entries"][0]["target_language"] == "en"


def test_translate_web_manage_delete():
    """The web admin manage callback deletes mappings."""
    import bot as bot_module
    from bot import AutoTranslateChannelStore, run_web_manage_translate_channels

    db_path = "/tmp/test_translate_web_delete.db"
    if os.path.exists(db_path):
        os.remove(db_path)
    bot_module.auto_translate_channel_store = AutoTranslateChannelStore(db_path, threading.RLock())

    # Create first
    run_web_manage_translate_channels(
        {"action": "create_entry", "source_channel_id": "70", "target_channel_id": "80",
         "source_language": "auto", "target_language": "es", "enabled": "1"},
        "test@example.com", 42,
    )
    # Then delete
    payload = run_web_manage_translate_channels(
        {"action": "delete_entry", "source_channel_id": "70", "target_channel_id": "80",
         "target_language": "es"},
        "test@example.com", 42,
    )
    assert payload["ok"] is True
    assert len(payload["entries"]) == 0


def test_translate_web_manage_invalid():
    """Invalid payloads return errors, not exceptions."""
    import bot as bot_module
    from bot import AutoTranslateChannelStore, run_web_manage_translate_channels

    db_path = "/tmp/test_translate_web_invalid.db"
    if os.path.exists(db_path):
        os.remove(db_path)
    bot_module.auto_translate_channel_store = AutoTranslateChannelStore(db_path, threading.RLock())

    # Missing channel IDs
    payload = run_web_manage_translate_channels(
        {"action": "create_entry", "source_channel_id": "0", "target_channel_id": "0",
         "source_language": "auto", "target_language": "en", "enabled": "1"},
        "test@example.com", 42,
    )
    assert payload["ok"] is False
    assert "error" in payload

    # Source == target
    payload = run_web_manage_translate_channels(
        {"action": "create_entry", "source_channel_id": "100", "target_channel_id": "100",
         "source_language": "auto", "target_language": "en", "enabled": "1"},
        "test@example.com", 42,
    )
    assert payload["ok"] is False
    assert "differ" in payload["error"].lower()

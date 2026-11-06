from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import discord
import pytest

from app.reaction_role_web_callbacks import ReactionRolesWebCallbacks
from app.reaction_roles import (
    normalize_reaction_role_emoji,
    normalize_reaction_role_message_id,
)
from app.web_reaction_roles import (
    process_reaction_roles_submission,
    render_reaction_roles_body,
)


def _make_callbacks(post_reaction_role_message_for_guild=None):
    return ReactionRolesWebCallbacks(
        normalize_target_guild_id=lambda g: int(g or 0),
        normalize_reaction_role_emoji=normalize_reaction_role_emoji,
        normalize_reaction_role_message_id=normalize_reaction_role_message_id,
        list_reaction_role_mappings=lambda g: [],
        save_reaction_role_mapping=MagicMock(),
        set_reaction_role_mapping_status=MagicMock(return_value=True),
        delete_reaction_role_mapping=MagicMock(return_value=True),
        build_web_actor_audit_label=lambda a: str(a),
        record_action_safe=MagicMock(),
        truncate_log_text=lambda t: str(t)[:100],
        logger=MagicMock(),
        validate_reaction_role_message_for_guild=MagicMock(return_value={"ok": True}),
        post_reaction_role_message_for_guild=post_reaction_role_message_for_guild,
    )


def test_process_submission_post_message_valid():
    form = {
        "action": "post_message",
        "post_channel_id": "123456",
        "post_message_content": "React for roles:\n🎮 = Gamer\n📢 = News",
        "post_emoji_1": "🎮",
        "post_role_id_1": "111",
        "post_emoji_2": "📢",
        "post_role_id_2": "222",
    }
    captured_payload = None

    def on_manage(payload, actor_email, guild_id):
        nonlocal captured_payload
        captured_payload = payload
        return {"ok": True, "message": "Success"}

    resp, msgs = process_reaction_roles_submission(
        form=form,
        on_manage_reaction_roles=on_manage,
        actor_email="admin@example.com",
        selected_guild_id="123",
    )
    assert resp is not None
    assert msgs == [("Success", "success")]
    assert captured_payload["action"] == "post_message"
    assert captured_payload["channel_id"] == "123456"
    assert captured_payload["message_content"] == "React for roles:\n🎮 = Gamer\n📢 = News"
    assert len(captured_payload["pairs"]) == 2
    assert captured_payload["pairs"][0] == {"emoji": "🎮", "role_id": "111"}
    assert captured_payload["pairs"][1] == {"emoji": "📢", "role_id": "222"}


def test_process_submission_post_message_missing_channel():
    form = {
        "action": "post_message",
        "post_channel_id": "",
        "post_message_content": "React for roles",
        "post_emoji_1": "🎮",
        "post_role_id_1": "111",
    }
    resp, msgs = process_reaction_roles_submission(
        form=form,
        on_manage_reaction_roles=MagicMock(),
        actor_email="admin@example.com",
        selected_guild_id="123",
    )
    assert resp is None
    assert msgs == [("Please select a target channel to post the message in.", "error")]


def test_process_submission_post_message_empty_content():
    form = {
        "action": "post_message",
        "post_channel_id": "123456",
        "post_message_content": "   ",
        "post_emoji_1": "🎮",
        "post_role_id_1": "111",
    }
    resp, msgs = process_reaction_roles_submission(
        form=form,
        on_manage_reaction_roles=MagicMock(),
        actor_email="admin@example.com",
        selected_guild_id="123",
    )
    assert resp is None
    assert msgs == [("Message content cannot be empty.", "error")]


def test_process_submission_post_message_no_pairs():
    form = {
        "action": "post_message",
        "post_channel_id": "123456",
        "post_message_content": "React for roles",
    }
    resp, msgs = process_reaction_roles_submission(
        form=form,
        on_manage_reaction_roles=MagicMock(),
        actor_email="admin@example.com",
        selected_guild_id="123",
    )
    assert resp is None
    assert msgs == [("Please configure at least one emoji and role connection.", "error")]


def test_process_submission_post_message_incomplete_pairs_rejected():
    form_missing_role = {
        "action": "post_message",
        "post_channel_id": "123456",
        "post_message_content": "React for roles",
        "post_emoji_1": "🎮",
        "post_role_id_1": "",
    }
    resp, msgs = process_reaction_roles_submission(
        form=form_missing_role,
        on_manage_reaction_roles=MagicMock(),
        actor_email="admin@example.com",
        selected_guild_id="123",
    )
    assert resp is None
    assert any("Emoji '🎮' was provided without selecting a role" in m[0] for m in msgs)

    form_missing_emoji = {
        "action": "post_message",
        "post_channel_id": "123456",
        "post_message_content": "React for roles",
        "post_emoji_1": "",
        "post_role_id_1": "111",
    }
    resp, msgs = process_reaction_roles_submission(
        form=form_missing_emoji,
        on_manage_reaction_roles=MagicMock(),
        actor_email="admin@example.com",
        selected_guild_id="123",
    )
    assert resp is None
    assert any("A role was selected without providing an emoji" in m[0] for m in msgs)


def test_callbacks_post_message_rejects_duplicate_emojis():
    callbacks = _make_callbacks()
    payload = {
        "action": "post_message",
        "channel_id": "123456",
        "message_content": "React below:",
        "pairs": [
            {"emoji": "🎮", "role_id": 111},
            {"emoji": "🎮", "role_id": 222},
        ],
    }
    res = callbacks.run_web_manage_reaction_roles(payload, "admin@example.com", 123)
    assert res["ok"] is False
    assert "Duplicate emoji specified" in res["error"]


def test_callbacks_post_message_rejects_invalid_emoji():
    callbacks = _make_callbacks()
    payload = {
        "action": "post_message",
        "channel_id": "123456",
        "message_content": "React below:",
        "pairs": [
            {"emoji": "", "role_id": 111},
        ],
    }
    res = callbacks.run_web_manage_reaction_roles(payload, "admin@example.com", 123)
    assert res["ok"] is False
    assert "Invalid emoji" in res["error"]


def test_callbacks_post_message_calls_poster_and_records_audit():
    post_mock = MagicMock(return_value={"ok": True, "message_id": 99998888, "saved_count": 2})
    callbacks = _make_callbacks(post_reaction_role_message_for_guild=post_mock)
    payload = {
        "action": "post_message",
        "channel_id": "123456",
        "message_content": "React below for roles:",
        "pairs": [
            {"emoji": "🎮", "role_id": 111},
            {"emoji": "📢", "role_id": 222},
        ],
    }
    res = callbacks.run_web_manage_reaction_roles(payload, "admin@example.com", 123)
    assert res["ok"] is True
    assert "99998888" in res["message"]
    post_mock.assert_called_once_with(
        123,
        channel_id=123456,
        message_content="React below for roles:",
        pairs=[
            {"emoji": "🎮", "emoji_key": "unicode:🎮", "emoji_text": "🎮", "role_id": 111},
            {"emoji": "📢", "emoji_key": "unicode:📢", "emoji_text": "📢", "role_id": 222},
        ],
    )
    callbacks.record_action_safe.assert_called_once()
    assert callbacks.record_action_safe.call_args.kwargs["action"] == "reaction_role_post_create"


def test_callbacks_post_message_surfaces_failed_emojis_warning():
    post_mock = MagicMock(return_value={"ok": True, "message_id": 99998888, "saved_count": 1, "failed_emojis": ["<:missing:123>"]})
    callbacks = _make_callbacks(post_reaction_role_message_for_guild=post_mock)
    payload = {
        "action": "post_message",
        "channel_id": "123456",
        "message_content": "React below:",
        "pairs": [
            {"emoji": "<:missing:123>", "role_id": 111},
        ],
    }
    res = callbacks.run_web_manage_reaction_roles(payload, "admin@example.com", 123)
    assert res["ok"] is True
    assert "Could not add reactions for: <:missing:123>" in res["message"]


def test_render_reaction_roles_body_includes_post_builder():
    channel_options = [{"id": "123", "name": "welcome"}]
    role_options = [{"id": "111", "name": "Gamer"}]
    html = render_reaction_roles_body(
        guild_name="Test Guild",
        mappings=[],
        channel_options=channel_options,
        role_options=role_options,
        catalog_error="",
        render_select_input=lambda name, val, opts, placeholder="": f"<select name='{name}'></select>",
        render_fixed_select_input=lambda name, val, opts, placeholder="": f"<select name='{name}'></select>",
    )
    assert "Post New Reaction Role Message" in html
    assert "post_message_content" in html
    assert "post_emoji_1" in html
    assert "post_role_id_1" in html
    assert "Post Message &amp; Setup Reaction Roles" in html or "Post Message & Setup Reaction Roles" in html


def test_post_reaction_role_message_for_guild_async_permissions_and_flow():
    import bot

    guild = MagicMock(spec=discord.Guild)
    channel = MagicMock(spec=discord.TextChannel)
    bot_member = MagicMock(spec=discord.Member)
    bot_member.guild_permissions.manage_roles = True
    role_top = MagicMock(spec=discord.Role, name="BotRole")
    role_top.name = "BotRole"
    role_top.__gt__ = lambda self, other: True
    role_top.__ge__ = lambda self, other: True
    role_top.__le__ = lambda self, other: False
    role_top.__lt__ = lambda self, other: False
    bot_member.top_role = role_top
    guild.me = bot_member

    gamer_role = MagicMock(spec=discord.Role, name="GamerRole")
    gamer_role.id = 111
    gamer_role.name = "Gamer"
    gamer_role.managed = False
    gamer_role.__le__ = lambda self, other: True
    guild.get_role.side_effect = lambda rid: gamer_role if rid == 111 else None
    guild.get_channel.side_effect = lambda cid: channel if cid == 999 else None

    perms = MagicMock()
    perms.send_messages = True
    perms.add_reactions = True
    channel.permissions_for.return_value = perms

    sent_message = AsyncMock(spec=discord.Message)
    sent_message.id = 55555555
    sent_message.jump_url = "https://discord.com/channels/123/999/55555555"
    channel.send = AsyncMock(return_value=sent_message)

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(bot.bot, "get_guild", lambda gid: guild)
        mp.setattr(bot, "save_reaction_role_mapping", MagicMock())
        mp.setattr(bot, "_refresh_reaction_role_cache_for_guild", MagicMock())

        res = asyncio.run(
            bot.post_reaction_role_message_for_guild_async(
                guild_id=123,
                channel_id=999,
                message_content="Click below for roles:",
                pairs=[{"emoji": "🎮", "role_id": 111}],
            )
        )

        assert res["ok"] is True
        assert res["message_id"] == 55555555
        channel.send.assert_awaited_once_with("Click below for roles:")
        sent_message.add_reaction.assert_awaited_once_with("🎮")
        bot.save_reaction_role_mapping.assert_called_once_with(
            channel_id=channel.id,
            message_id=55555555,
            emoji="🎮",
            role_id=111,
            guild_id=123,
            status="active",
        )


def test_post_reaction_role_message_for_guild_async_requires_manage_roles():
    import bot

    guild = MagicMock(spec=discord.Guild)
    channel = MagicMock(spec=discord.TextChannel)
    bot_member = MagicMock(spec=discord.Member)
    bot_member.guild_permissions.manage_roles = False
    guild.me = bot_member
    guild.get_channel.return_value = channel

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(bot.bot, "get_guild", lambda gid: guild)

        res = asyncio.run(
            bot.post_reaction_role_message_for_guild_async(
                guild_id=123,
                channel_id=999,
                message_content="Click below for roles:",
                pairs=[{"emoji": "🎮", "role_id": 111}],
            )
        )
        assert res["ok"] is False
        assert "Manage Roles" in res["error"]

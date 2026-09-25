from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING

import discord
from discord import app_commands

if TYPE_CHECKING:
    from discord import (
        Guild,
        Member,
        Message,
        RawReactionActionEvent,
        TextChannel,
        User,
        abc as discord_abc,
    )
    from discord.ext import commands

logger = logging.getLogger("invite_bot")

__all__ = [
    "on_ready",
    "on_guild_join",
    "on_guild_remove",
    "on_error",
    "on_command_error",
    "on_tree_error",
    "on_member_join",
    "on_member_remove",
    "on_message_delete",
    "on_bulk_message_delete",
    "on_user_update",
    "on_member_update",
    "on_raw_reaction_add",
    "on_raw_reaction_remove",
    "on_invite_create",
    "on_guild_channel_create",
    "on_guild_channel_delete",
    "on_guild_role_create",
    "on_message",
    "register_all_handlers",
    "_resolve_role_change_actor",
    "_resolve_role_create_actor",
    "_resolve_channel_create_actor",
    "_post_translation_reply",
]


# ---------------------------------------------------------------------------
# Internal helpers used by multiple handlers
# ---------------------------------------------------------------------------

async def _resolve_role_change_actor(
    guild: Guild,
    target_member_id: int,
) -> str:
    try:
        for attempt in range(2):
            now_utc = bot_time_utc()
            async for entry in guild.audit_logs(
                limit=15,
                action=discord.AuditLogAction.member_role_update,
            ):
                target_id = getattr(entry.target, "id", None)
                if target_id is not None and int(target_id) == int(target_member_id):
                    if entry.created_at:
                        entry_created = entry.created_at
                        if entry_created.tzinfo is None:
                            entry_created = entry_created.replace(tzinfo=UTC())
                        if (now_utc - entry_created).total_seconds() > 60:
                            continue
                    user = entry.user
                    if user:
                        return f"{user.name} (`{user.id}`)"
                    user_id = getattr(entry, "user_id", None) or getattr(
                        entry, "_user_id", None
                    )
                    if user_id:
                        return f"`{user_id}`"
            if attempt == 0:
                await asyncio.sleep(0.4)
    except discord.Forbidden:
        return "Unknown (bot lacks audit log permission)"
    except Exception:
        import bot

        bot.logger.exception(
            "Failed resolving role change actor for member %s in guild %s",
            target_member_id,
            guild.id,
        )
        return "Unknown"
    return "Unknown (not in audit log)"


async def _resolve_role_create_actor(
    guild: Guild,
    role_id: int,
) -> str:
    try:
        for attempt in range(2):
            now_utc = bot_time_utc()
            async for entry in guild.audit_logs(
                limit=15,
                action=discord.AuditLogAction.role_create,
            ):
                target_id = getattr(entry.target, "id", None)
                if target_id is not None and int(target_id) == int(role_id):
                    if entry.created_at:
                        entry_created = entry.created_at
                        if entry_created.tzinfo is None:
                            entry_created = entry_created.replace(tzinfo=UTC())
                        if (now_utc - entry_created).total_seconds() > 60:
                            continue
                    user = entry.user
                    if user:
                        return f"{user.name} (`{user.id}`)"
                    user_id = getattr(entry, "user_id", None) or getattr(
                        entry, "_user_id", None
                    )
                    if user_id:
                        return f"`{user_id}`"
            if attempt == 0:
                await asyncio.sleep(0.4)
    except discord.Forbidden:
        return "Unknown (bot lacks audit log permission)"
    except Exception:
        import bot

        bot.logger.exception(
            "Failed resolving role create actor for role %s in guild %s",
            role_id,
            guild.id,
        )
        return "Unknown"
    return "Unknown (not in audit log)"


async def _resolve_channel_create_actor(
    guild: Guild,
    channel_id: int,
) -> str:
    try:
        for attempt in range(2):
            now_utc = bot_time_utc()
            async for entry in guild.audit_logs(
                limit=15,
                action=discord.AuditLogAction.channel_create,
            ):
                target_id = getattr(entry.target, "id", None)
                if target_id is not None and int(target_id) == int(channel_id):
                    if entry.created_at:
                        entry_created = entry.created_at
                        if entry_created.tzinfo is None:
                            entry_created = entry_created.replace(tzinfo=UTC())
                        if (now_utc - entry_created).total_seconds() > 60:
                            continue
                    user = entry.user
                    if user:
                        return f"{user.name} (`{user.id}`)"
                    user_id = getattr(entry, "user_id", None) or getattr(
                        entry, "_user_id", None
                    )
                    if user_id:
                        return f"`{user_id}`"
            if attempt == 0:
                await asyncio.sleep(0.4)
    except discord.Forbidden:
        return "Unknown (bot lacks audit log permission)"
    except Exception:
        import bot

        bot.logger.exception(
            "Failed resolving channel create actor for channel %s in guild %s",
            channel_id,
            guild.id,
        )
        return "Unknown"
    return "Unknown (not in audit log)"


async def _post_translation_reply(
    target_channel: discord_abc.MessageableChannel,
    original_message: Message,
    result,
    target_lang: str,
) -> bool:
    embed = discord.Embed(
        description=result.text[:4096],
        color=discord.Color.blue(),
    )
    author_name = original_message.author.display_name
    author_icon = (
        original_message.author.display_avatar.url
        if original_message.author.display_avatar
        else None
    )
    if author_icon:
        embed.set_author(
            name=f"{author_name}'s message",
            icon_url=author_icon,
            url=original_message.jump_url,
        )
    else:
        embed.set_author(
            name=f"{author_name}'s message",
            url=original_message.jump_url,
        )
    embed.set_footer(
        text=(
            f"Translated from {result.source_language_name} "
            f"({result.source_language}) to {result.target_language_name}"
        )
    )
    try:
        await target_channel.send(
            content=(
                f"🌐 **Translation of {original_message.author.mention}'s "
                f"[message]({original_message.jump_url}) to {target_lang.upper()}:**"
            ),
            embed=embed,
            reference=original_message,
            mention_author=False,
        )
        return True
    except discord.HTTPException:
        import bot

        bot.logger.exception(
            "Failed posting translation reply in channel %s",
            getattr(target_channel, "id", "?"),
        )
        return False


# ---------------------------------------------------------------------------
# Event handlers
# ---------------------------------------------------------------------------

async def on_ready() -> None:
    import bot

    bot.install_asyncio_exception_logging(asyncio.get_running_loop())
    if bot.auto_translate_channel_store is None:
        from app.translate_channels import AutoTranslateChannelStore

        bot.auto_translate_channel_store = AutoTranslateChannelStore(
            bot.DB_FILE, bot.db_lock
        )
    purged_archives = bot.purge_expired_guild_archives()
    if purged_archives:
        logger.info(
            "Purged expired archived guild data for %s guild(s): %s",
            len(purged_archives),
            ", ".join(str(guild_id) for guild_id in purged_archives),
        )
    logger.info("Logged in as %s", bot.user.name)
    await bot._flush_web_admin_pending_critical_alerts()
    if callable(bot.__dict__.get("register_tag_commands_for_guild")):
        total_synced = await bot.sync_commands_for_all_guilds()
        logger.info(
            "Synced %d command(s) across %d guild(s)",
            total_synced,
            len(bot.get_managed_guilds()),
        )
    else:
        logger.warning(
            "Tag slash commands not registered: register_tag_commands_for_guild missing"
        )

    if bot.firmware_monitor_task is None or bot.firmware_monitor_task.done():
        bot.firmware_monitor_task = asyncio.create_task(
            bot.firmware_monitor_loop(), name="firmware_monitor"
        )
    if bot.reddit_feed_monitor_task is None or bot.reddit_feed_monitor_task.done():
        bot.reddit_feed_monitor_task = asyncio.create_task(
            bot.reddit_feed_monitor_loop(), name="reddit_feed_monitor"
        )
    if bot.youtube_monitor_task is None or bot.youtube_monitor_task.done():
        bot.youtube_monitor_task = asyncio.create_task(
            bot.youtube_monitor_loop(), name="youtube_monitor"
        )
    if bot.linkedin_monitor_task is None or bot.linkedin_monitor_task.done():
        bot.linkedin_monitor_task = asyncio.create_task(
            bot.linkedin_monitor_loop(), name="linkedin_monitor"
        )
    if bot.beta_program_monitor_task is None or bot.beta_program_monitor_task.done():
        bot.beta_program_monitor_task = asyncio.create_task(
            bot.beta_program_monitor_loop(), name="beta_program_monitor"
        )
    if bot.forum_announcement_task is None or bot.forum_announcement_task.done():
        bot.forum_announcement_task = asyncio.create_task(
            bot.forum_announcement_monitor_loop(), name="forum_announcement_monitor"
        )
    if (
        bot.FORUM_API_KEY
        and bot.FORUM_MONITOR_CATEGORIES
        and (
            bot.forum_monitor_task is None or bot.forum_monitor_task.done()
        )
    ):
        bot.forum_monitor_task = asyncio.create_task(
            bot._forum_monitor_loop(), name="forum_monitor"
        )
        logger.info("Forum monitor task scheduled")
    if bot.service_monitor_task is None or bot.service_monitor_task.done():
        bot.service_monitor_task = asyncio.create_task(
            bot.service_monitor_loop(), name="service_monitor"
        )
    if bot.uptime_status_monitor_task is None or bot.uptime_status_monitor_task.done():
        bot.uptime_status_monitor_task = asyncio.create_task(
            bot.uptime_status_monitor_loop(), name="uptime_status_monitor"
        )
    if (
        bot.MEMBER_ACTIVITY_BACKFILL_ENABLED
        and (
            bot.member_activity_backfill_task is None
            or bot.member_activity_backfill_task.done()
        )
    ):
        bot.member_activity_backfill_task = asyncio.create_task(
            bot.member_activity_backfill_job(), name="member_activity_backfill"
        )
    try:
        asyncio.create_task(
            bot.get_irc_bridge_service(bot).start(), name="irc_bridge"
        )
        logger.info("IRC bridge scheduled for startup")
    except Exception:
        logger.exception("Failed to start IRC bridge")


async def on_guild_join(guild: Guild) -> None:
    import bot

    try:
        restored = bot.restore_archived_guild_data(guild.id)
        if restored.get("restored"):
            logger.info(
                "Restored archived guild data for %s (%s) archived_at=%s purge_after_at=%s",
                guild.name,
                guild.id,
                restored.get("archived_at", ""),
                restored.get("purge_after_at", ""),
            )
    except Exception:
        logger.exception(
            "Failed restoring archived guild data for %s (%s)",
            guild.name,
            guild.id,
        )
    if not bot.is_managed_guild_id(guild.id):
        logger.info(
            "Joined unmanaged guild %s (%s); skipping command sync due to MANAGED_GUILD_IDS filter",
            guild.name,
            guild.id,
        )
        return
    bot.invite_roles_by_guild.setdefault(guild.id, {})
    bot.reaction_roles_by_guild[guild.id] = (
        bot.load_reaction_roles_impl(
            bot.get_db_connection, bot.db_lock, guild.id
        ).get(guild.id, {})
    )
    await bot.sync_commands_for_guild(guild)
    await bot.refresh_invite_cache_for_guild(guild)
    logger.info("Joined guild %s (%s) and synced commands", guild.name, guild.id)


async def on_guild_remove(guild: Guild) -> None:
    import bot

    try:
        archive_info = bot.archive_guild_data(guild.id)
        logger.info(
            "Archived guild data for %s (%s) until %s",
            guild.name,
            guild.id,
            archive_info.get("purge_after_at", ""),
        )
    except Exception:
        logger.exception(
            "Failed archiving guild data for %s (%s)",
            guild.name,
            guild.id,
        )
    bot.clear_guild_runtime_state(guild.id)
    bot.reaction_roles_by_guild.pop(guild.id, None)


async def on_error(event_method: str, *args, **kwargs) -> None:
    import bot

    bot.logger.exception("Unhandled exception in event '%s'", event_method)


async def on_command_error(
    ctx: commands.Context,
    error: commands.CommandError,
) -> None:
    import bot

    if isinstance(error, commands.CommandOnCooldown):
        try:
            await ctx.send(
                f"⏳ You're using this command too quickly. Try again in {error.retry_after:.1f}s."
            )
        except discord.HTTPException:
            bot.logger.warning(
                "Could not send cooldown notice for prefix command %s to %s",
                ctx.command,
                f"{ctx.author} (id: {ctx.author.id})",
            )
        return
    if isinstance(error, commands.CommandNotFound):
        return
    bot.logger.error(
        "Unhandled error in prefix command %s invoked by %s",
        ctx.command.qualified_name if ctx.command else "unknown",
        f"{ctx.author} (id: {ctx.author.id})",
        exc_info=(type(error), error, error.__traceback__),
    )


async def on_tree_error(
    interaction: discord.Interaction,
    error: app_commands.AppCommandError,
) -> None:
    import bot

    command_name = (
        interaction.command.name if interaction.command else "unknown"
    )
    bot.logger.error(
        "Unhandled app command error in /%s invoked by %s",
        command_name,
        f"{interaction.user} (id: {interaction.user.id})",
        exc_info=(type(error), error, error.__traceback__),
    )
    if isinstance(error, app_commands.CommandNotFound):
        await bot.send_safe_interaction_message(
            interaction,
            "❌ This command is still syncing. Please wait 30-60 seconds and try again.",
            ephemeral=True,
        )
        return
    if isinstance(error, app_commands.CommandOnCooldown):
        await bot.send_safe_interaction_message(
            interaction,
            f"⏳ You're using this command too quickly. Try again in {error.retry_after:.1f}s.",
            ephemeral=True,
        )
        return
    await bot.send_safe_interaction_message(
        interaction,
        "❌ An unexpected error occurred while processing that command.",
        ephemeral=True,
    )


async def on_member_join(member: Member) -> None:
    import bot

    guild = member.guild
    if not bot.is_managed_guild_id(guild.id):
        return
    try:
        if await bot.apply_honeypot_join_guard(member):
            return
    except Exception:
        logger.exception(
            "Failed applying honeypot join guard for %s in guild %s",
            member,
            guild.id,
        )
    guild_invite_roles = bot.invite_roles_by_guild.get(guild.id) or {}
    guild_invite_uses = bot.invite_uses_by_guild.setdefault(guild.id, {})
    used_invite = None
    try:
        invites = await guild.invites()
        for inv in invites:
            if inv.code in guild_invite_roles and inv.uses > guild_invite_uses.get(
                inv.code, 0
            ):
                guild_invite_uses[inv.code] = inv.uses
                used_invite = inv
                break
    except Exception:
        logger.exception("Failed to fetch invites on member join")

    if used_invite:
        role_id = guild_invite_roles.get(used_invite.code)
        role = guild.get_role(role_id)
        if role:
            try:
                await member.add_roles(role)
                logger.info(
                    "Assigned role %s to %s via invite %s",
                    role.id,
                    member,
                    used_invite.code,
                )
            except Exception:
                logger.exception("Failed to assign role on join for %s", member)

    join_details = (
        f"**Member:** {member.mention} (`{member.id}`)\n"
        f"**Created:** <t:{int(member.created_at.timestamp())}:f>\n"
    )
    if used_invite:
        join_details += f"**Invite:** `{used_invite.code}`\n"
    await bot.send_server_event_log(guild, "member_join", join_details)
    await bot.send_configured_welcome_messages(member)


async def on_member_remove(member: Member) -> None:
    import bot

    guild = member.guild
    if not bot.is_managed_guild_id(guild.id):
        return

    details = (
        f"**Member:** {member} (`{member.id}`)\n"
        f"**Nickname:** {bot.clip_text(member.nick or 'N/A')}\n"
    )
    await bot.send_server_event_log(guild, "member_leave", details)


async def on_message_delete(message: Message) -> None:
    import bot

    guild = message.guild
    if guild is None:
        return
    if not bot.is_managed_guild_id(guild.id):
        return

    if message.id in bot._bot_deleted_message_ids:
        bot._bot_deleted_message_ids.discard(message.id)
        return

    channel_name = (
        message.channel.mention
        if hasattr(message.channel, "mention")
        else f"`{message.channel.id}`"
    )
    details = (
        f"**Author:** {message.author} (`{message.author.id}`)\n"
        f"**Channel:** {channel_name}\n"
        f"**Message ID:** `{message.id}`\n"
        f"**Content:** {bot.clip_text(message.content)}\n"
        f"**Attachments:** `{len(message.attachments)}`\n"
    )
    await bot.send_server_event_log(guild, "message_delete", details)


async def on_bulk_message_delete(messages: list[Message]) -> None:
    import bot

    if not messages:
        return
    guild = messages[0].guild
    if guild is None:
        return
    if not bot.is_managed_guild_id(guild.id):
        return

    channel = messages[0].channel
    channel_name = (
        channel.mention
        if hasattr(channel, "mention")
        else f"`{channel.id}`"
    )
    details = (
        f"**Channel:** {channel_name}\n"
        f"**Messages Deleted:** `{len(messages)}`\n"
    )
    await bot.send_server_event_log(guild, "bulk_message_delete", details)


async def on_user_update(before: User, after: User) -> None:
    import bot

    guild = bot.get_preferred_managed_guild_for_user(after.id)
    if guild is None:
        return

    member = guild.get_member(after.id)
    if member is None:
        return

    if before.name != after.name or before.global_name != after.global_name:
        details = (
            f"**User:** {member.mention} (`{after.id}`)\n"
            f"**Username:** {bot.clip_text(before.name)} -> {bot.clip_text(after.name)}\n"
            f"**Global Name:** {bot.clip_text(before.global_name or 'N/A')} -> {bot.clip_text(after.global_name or 'N/A')}\n"
        )
        await bot.send_server_event_log(guild, "user_name_change", details)

    if before.display_avatar != after.display_avatar:
        details = (
            f"**User:** {member.mention} (`{after.id}`)\n"
            f"**New Avatar:** {after.display_avatar.url}\n"
        )
        await bot.send_server_event_log(guild, "user_avatar_change", details)


async def on_member_update(before: Member, after: Member) -> None:
    import bot

    guild = after.guild
    if not bot.is_managed_guild_id(guild.id):
        return

    if before.nick != after.nick:
        details = (
            f"**Member:** {after.mention} (`{after.id}`)\n"
            f"**Nickname:** {bot.clip_text(before.nick or 'N/A')} -> {bot.clip_text(after.nick or 'N/A')}\n"
        )
        await bot.send_server_event_log(guild, "member_nickname_change", details)

    before_role_map = {role.id: role for role in before.roles}
    after_role_map = {role.id: role for role in after.roles}
    added_role_ids = sorted(set(after_role_map) - set(before_role_map))
    removed_role_ids = sorted(set(before_role_map) - set(after_role_map))

    if not added_role_ids and not removed_role_ids:
        return

    actor_label = await _resolve_role_change_actor(guild, after.id)

    for role_id in added_role_ids:
        role = after_role_map[role_id]
        details = (
            f"**Member:** {after.mention} (`{after.id}`)\n"
            f"**Role Added:** {role.mention} (`{role.id}`)\n"
        )
        await bot.send_server_event_log(
            guild, "member_role_added", details, actor=actor_label
        )

    for role_id in removed_role_ids:
        role = before_role_map[role_id]
        details = (
            f"**Member:** {after.mention} (`{after.id}`)\n"
            f"**Role Removed:** {role.name} (`{role.id}`)\n"
        )
        await bot.send_server_event_log(
            guild, "member_role_removed", details, actor=actor_label
        )


async def on_raw_reaction_add(payload: RawReactionActionEvent) -> None:
    import bot

    if payload.user_id == (bot.user.id if bot.user else 0):
        return
    if payload.guild_id is None:
        return
    if not bot.is_managed_guild_id(payload.guild_id):
        return

    emoji_key = bot.reaction_role_emoji_key_from_payload(payload.emoji)
    if not emoji_key:
        return

    guild_id = bot.normalize_target_guild_id(payload.guild_id)
    message_map = bot.reaction_roles_by_guild.get(guild_id, {}).get(
        int(payload.message_id), {}
    )
    role_id = int(message_map.get(emoji_key) or 0)
    if role_id <= 0:
        mapping = bot.find_reaction_role_mapping_impl(
            bot.get_db_connection,
            bot.db_lock,
            guild_id,
            int(payload.message_id),
            payload.emoji,
        )
        if mapping is None:
            return
        role_id = int(mapping.get("role_id") or 0)
        if role_id <= 0:
            return

    guild = bot.bot.get_guild(guild_id)
    if guild is None:
        return
    role = guild.get_role(role_id)
    if role is None or role.managed or role == guild.default_role:
        return

    member = guild.get_member(payload.user_id)
    if member is None:
        try:
            member = await guild.fetch_member(payload.user_id)
        except Exception:
            logger.exception(
                "Failed to resolve member %s for reaction role add in guild %s",
                payload.user_id,
                guild_id,
            )
            return

    bot_member = guild.me or guild.get_member(bot.user.id if bot.user else 0)
    if bot_member is not None and bot_member.top_role <= role:
        logger.warning(
            "Cannot assign reaction role %s in guild %s because it is above the bot's top role",
            role_id,
            guild_id,
        )
        return

    try:
        await member.add_roles(
            role, reason=f"Reaction role assigned for message {payload.message_id}"
        )
    except Exception:
        logger.exception(
            "Failed to assign reaction role %s to member %s in guild %s",
            role_id,
            payload.user_id,
            guild_id,
        )

    try:
        emoji_str = str(payload.emoji) if payload.emoji else ""
        target_lang = bot.get_lang_for_flag(emoji_str)
        if target_lang:
            guild = bot.bot.get_guild(guild_id)
            channel = (
                guild.get_channel(int(payload.channel_id)) if guild else None
            )
            if channel is not None and hasattr(channel, "fetch_message"):
                try:
                    target_message = await channel.fetch_message(
                        int(payload.message_id)
                    )
                except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                    target_message = None
                if target_message is not None:
                    raw_text = str(getattr(target_message, "content", "") or "").strip()
                    if raw_text:
                        try:
                            result = await asyncio.to_thread(
                                bot.translate_text, raw_text, target_lang, "auto"
                            )
                        except Exception:
                            logger.exception(
                                "Flag-reaction translation failed for message %s -> %s",
                                target_message.id,
                                target_lang,
                            )
                        else:
                            try:
                                await _post_translation_reply(
                                    channel, target_message, result, target_lang
                                )
                            except Exception:
                                logger.exception(
                                    "Flag-reaction reply failed in channel %s",
                                    channel.id,
                                )
                            else:
                                logger.info(
                                    "Flag reaction translated message %s -> %s by user %s in guild %s",
                                    target_message.id,
                                    target_lang,
                                    payload.user_id,
                                    payload.guild_id,
                                )
    except Exception:
        logger.exception(
            "Unexpected error in flag-reaction translation for message %s",
            payload.message_id,
        )


async def on_raw_reaction_remove(payload: RawReactionActionEvent) -> None:
    import bot

    if payload.guild_id is None:
        return
    if not bot.is_managed_guild_id(payload.guild_id):
        return

    emoji_key = bot.reaction_role_emoji_key_from_payload(payload.emoji)
    if not emoji_key:
        return

    guild_id = bot.normalize_target_guild_id(payload.guild_id)
    message_map = bot.reaction_roles_by_guild.get(guild_id, {}).get(
        int(payload.message_id), {}
    )
    role_id = int(message_map.get(emoji_key) or 0)
    if role_id <= 0:
        mapping = bot.find_reaction_role_mapping_impl(
            bot.get_db_connection,
            bot.db_lock,
            guild_id,
            int(payload.message_id),
            payload.emoji,
        )
        if mapping is None:
            return
        role_id = int(mapping.get("role_id") or 0)
        if role_id <= 0:
            return

    guild = bot.bot.get_guild(guild_id)
    if guild is None:
        return
    role = guild.get_role(role_id)
    if role is None or role.managed or role == guild.default_role:
        return

    member = guild.get_member(payload.user_id)
    if member is None:
        try:
            member = await guild.fetch_member(payload.user_id)
        except Exception:
            logger.exception(
                "Failed to resolve member %s for reaction role removal in guild %s",
                payload.user_id,
                guild_id,
            )
            return

    try:
        await member.remove_roles(
            role, reason=f"Reaction role removed for message {payload.message_id}"
        )
    except Exception:
        logger.exception(
            "Failed to remove reaction role %s from member %s in guild %s",
            role_id,
            payload.user_id,
            guild_id,
        )


async def on_invite_create(invite: discord.Invite) -> None:
    import bot

    guild = invite.guild
    if guild is None:
        return
    if not bot.is_managed_guild_id(guild.id):
        return

    inviter_text = (
        f"{invite.inviter} (`{invite.inviter.id}`)"
        if invite.inviter
        else "Unknown"
    )
    channel_text = (
        invite.channel.mention if getattr(invite, "channel", None) else "N/A"
    )
    details = (
        f"**Invite Code:** `{invite.code}`\n"
        f"**Inviter:** {inviter_text}\n"
        f"**Channel:** {channel_text}\n"
        f"**Max Uses:** `{invite.max_uses}`\n"
        f"**Max Age:** `{invite.max_age}`\n"
    )
    await bot.send_server_event_log(guild, "invite_created", details)


async def on_guild_channel_create(channel: discord_abc.GuildChannel) -> None:
    import bot

    guild = channel.guild
    if not bot.is_managed_guild_id(guild.id):
        return

    if isinstance(channel, discord.CategoryChannel):
        event_name = "category_created"
    else:
        event_name = "channel_created"

    parent_name = channel.category.name if channel.category else "N/A"
    actor_label = await _resolve_channel_create_actor(guild, channel.id)
    details = (
        f"**Name:** {bot.clip_text(channel.name)}\n"
        f"**ID:** `{channel.id}`\n"
        f"**Type:** `{channel.type}`\n"
        f"**Category:** {bot.clip_text(parent_name)}\n"
    )
    await bot.send_server_event_log(guild, event_name, details, actor=actor_label)


async def on_guild_channel_delete(channel: discord_abc.GuildChannel) -> None:
    import bot

    guild = channel.guild
    if not bot.is_managed_guild_id(guild.id):
        return

    if isinstance(channel, discord.CategoryChannel):
        event_name = "category_deleted"
    else:
        event_name = "channel_deleted"

    parent_name = channel.category.name if channel.category else "N/A"
    details = (
        f"**Name:** {bot.clip_text(channel.name)}\n"
        f"**ID:** `{channel.id}`\n"
        f"**Type:** `{channel.type}`\n"
        f"**Category:** {bot.clip_text(parent_name)}\n"
    )
    await bot.send_server_event_log(guild, event_name, details)


async def on_guild_role_create(role: discord.Role) -> None:
    import bot

    guild = role.guild
    if not bot.is_managed_guild_id(guild.id):
        return

    actor_label = await _resolve_role_create_actor(guild, role.id)
    details = (
        f"**Role:** {role.mention} (`{role.id}`)\n"
        f"**Color:** `{role.color}`\n"
        f"**Position:** `{role.position}`\n"
    )
    await bot.send_server_event_log(guild, "role_created", details, actor=actor_label)


async def on_message(message: Message) -> None:
    import bot

    if message.author.bot:
        return
    if message.guild is not None and not bot.is_managed_guild_id(message.guild.id):
        return
    if message.guild is not None:
        honeypot_entry = bot.load_honeypot_entry(
            message.guild.id, getattr(message.channel, "id", 0)
        )
        if honeypot_entry is not None and int(honeypot_entry.get("enabled") or 0) > 0:
            try:
                if await bot.apply_honeypot_action(message, honeypot_entry):
                    return
            except Exception:
                logger.exception(
                    "Failed applying honeypot action for message %s in guild %s",
                    getattr(message, "id", "unknown"),
                    getattr(message.guild, "id", "unknown"),
                )
    if message.guild is not None and isinstance(
        message.author, discord.Member
    ) and not bot.has_moderator_access(message.author):
        try:
            if await bot.apply_bad_word_moderation(message):
                return
        except Exception:
            logger.exception(
                "Failed applying bad-word moderation to message %s in guild %s",
                getattr(message, "id", "unknown"),
                getattr(message.guild, "id", "unknown"),
            )
    if message.guild is not None:
        try:
            bot.record_member_message_activity(message)
        except Exception:
            logger.exception(
                "Failed to record member activity for message %s in guild %s",
                getattr(message, "id", "unknown"),
                getattr(message.guild, "id", "unknown"),
            )
    if message.guild is not None:
        hi_channel_id = bot.get_effective_guild_setting(
            message.guild.id, "hi_channel_id", 0
        )
        if hi_channel_id > 0 and getattr(message.channel, "id", 0) == hi_channel_id:
            hi_channel_text = str(
                bot.load_guild_settings(message.guild.id).get("hi_channel_text")
                or "Hi :)"
            ).strip() or "Hi :)"
            hi_member_name = discord.utils.remove_markdown(
                str(
                    getattr(message.author, "nick", "")
                    or getattr(message.author, "display_name", "")
                    or getattr(message.author, "global_name", "")
                    or getattr(message.author, "name", "")
                    or "Member"
                ).strip()
            ).strip() or "Member"
            hi_response_text = f"{hi_member_name} says {hi_channel_text}"
            try:
                await message.delete()
                bot._bot_deleted_message_ids.add(message.id)
            except discord.Forbidden:
                bot.logger.warning(
                    "Cannot delete message %s in hi channel %s for guild %s",
                    getattr(message, "id", "unknown"),
                    hi_channel_id,
                    getattr(message.guild, "id", "unknown"),
                )
            except discord.HTTPException:
                logger.exception(
                    "Failed deleting message %s in hi channel %s for guild %s",
                    getattr(message, "id", "unknown"),
                    hi_channel_id,
                    getattr(message.guild, "id", "unknown"),
                )
                return
            try:
                await message.channel.send(
                    hi_response_text,
                    allowed_mentions=discord.AllowedMentions.none(),
                )
            except discord.Forbidden:
                bot.logger.warning(
                    "Cannot send hi replacement in channel %s for guild %s",
                    hi_channel_id,
                    getattr(message.guild, "id", "unknown"),
                )
            except discord.HTTPException:
                logger.exception(
                    "Failed sending hi replacement in channel %s for guild %s",
                    hi_channel_id,
                    getattr(message.guild, "id", "unknown"),
                )
            return
    if message.content:
        tag = bot.normalize_tag(message.content.strip().split()[0])
        if tag == "!list":
            await bot.process_commands(message)
            return
        response = bot.get_tag_responses(
            message.guild.id if message.guild else bot.GUILD_ID
        ).get(tag)
        if response:
            if bot.can_use_command(
                message.author,
                "tag_commands",
                guild_id=message.guild.id if message.guild else None,
            ):
                await message.channel.send(response)
    if message.guild is not None and message.content and (
        bot.auto_translate_channel_store is not None
    ):
        try:
            mappings = bot.auto_translate_channel_store.list_active_for_source(
                int(message.guild.id),
                int(message.channel.id),
            )
        except Exception:
            logger.exception(
                "Failed loading auto-translate mappings for channel %s",
                message.channel.id,
            )
            mappings = []
        for mapping in mappings:
            target_channel_id = int(mapping.get("target_channel_id") or 0)
            target_language = (
                str(mapping.get("target_language") or "en").strip().lower() or "en"
            )
            source_language = (
                str(mapping.get("source_language") or "auto").strip().lower() or "auto"
            )
            if target_channel_id <= 0 or target_channel_id == int(
                message.channel.id
            ):
                continue
            target_channel = message.guild.get_channel(target_channel_id)
            if target_channel is None or not hasattr(target_channel, "send"):
                continue
            raw_text = str(message.content or "").strip()
            if not raw_text or len(raw_text) > 1900:
                continue
            try:
                result = await asyncio.to_thread(
                    bot.translate_text, raw_text, target_language, source_language
                )
            except Exception:
                logger.exception(
                    "Auto-translate channel bridge failed: guild=%s src=%s tgt=%s",
                    message.guild.id,
                    message.channel.id,
                    target_channel_id,
                )
                continue
            embed = discord.Embed(
                description=result.text[:4096],
                color=discord.Color.blue(),
            )
            author_name = message.author.display_name
            author_icon = (
                message.author.display_avatar.url
                if message.author.display_avatar
                else None
            )
            if author_icon:
                embed.set_author(
                    name=f"{author_name} in #{message.channel.name}",
                    icon_url=author_icon,
                    url=message.jump_url,
                )
            else:
                embed.set_author(
                    name=f"{author_name} in #{message.channel.name}",
                    url=message.jump_url,
                )
            embed.set_footer(
                text=(
                    f"Auto-translated from {result.source_language_name} "
                    f"({result.source_language}) to {result.target_language_name}"
                )
            )
            try:
                await target_channel.send(
                    content=(
                        f"🌐 **{message.author.mention} said in <#{message.channel.id}>:**"
                    ),
                    embed=embed,
                )
            except discord.Forbidden:
                logger.warning(
                    "Auto-translate bridge: cannot post in %s (missing permissions)",
                    target_channel_id,
                )
            except discord.HTTPException:
                logger.exception(
                    "Auto-translate bridge: HTTP error posting in %s",
                    target_channel_id,
                )
    await bot.process_commands(message)


# ---------------------------------------------------------------------------
# Registration helper
# ---------------------------------------------------------------------------

def register_all_handlers(bot_obj: commands.Bot, tree: app_commands.CommandTree) -> None:
    """Register all event handlers with the given bot/tree instances."""
    bot_obj.event(on_ready)
    bot_obj.event(on_guild_join)
    bot_obj.event(on_guild_remove)
    bot_obj.event(on_error)
    bot_obj.event(on_command_error)
    tree.error(on_tree_error)
    bot_obj.event(on_member_join)
    bot_obj.event(on_member_remove)
    bot_obj.event(on_message_delete)
    bot_obj.event(on_bulk_message_delete)
    bot_obj.event(on_user_update)
    bot_obj.event(on_member_update)
    bot_obj.event(on_raw_reaction_add)
    bot_obj.event(on_raw_reaction_remove)
    bot_obj.event(on_invite_create)
    bot_obj.event(on_guild_channel_create)
    bot_obj.event(on_guild_channel_delete)
    bot_obj.event(on_guild_role_create)
    bot_obj.event(on_message)


# ---------------------------------------------------------------------------
# Tiny helpers to avoid importing bot at module level
# ---------------------------------------------------------------------------

def bot_time_utc():
    from datetime import UTC, datetime

    return datetime.now(UTC())


def UTC():
    from datetime import UTC

    return UTC

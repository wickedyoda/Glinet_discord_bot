from __future__ import annotations

from datetime import UTC, datetime, timedelta

import discord

from app.moderation import BAD_WORD_ACTION_TIMEOUT, find_bad_word_match


def build_bad_word_warning_message(
    *,
    guild_name: str,
    matched_term: str,
    warning_count: int,
    warning_threshold: int,
    warning_window_hours: int,
    action_taken: str,
    timeout_minutes: int,
) -> str:
    if action_taken == BAD_WORD_ACTION_TIMEOUT:
        action_note = (
            f"You have been timed out for {timeout_minutes} minute(s) after reaching "
            f"{warning_threshold} warning(s) within {warning_window_hours} hour(s)."
        )
    else:
        action_note = (
            f"This is warning {warning_count} of {warning_threshold} within "
            f"{warning_window_hours} hour(s)."
        )
    return (
        f"Your message in **{guild_name}** was removed for a blocked word or phrase: `{matched_term}`.\n"
        f"{action_note}"
    )


async def send_bad_word_warning_dm(
    member: discord.Member,
    *,
    guild_name: str,
    matched_term: str,
    warning_count: int,
    warning_threshold: int,
    warning_window_hours: int,
    action_taken: str,
    timeout_minutes: int,
):
    try:
        await member.send(
            build_bad_word_warning_message(
                guild_name=guild_name,
                matched_term=matched_term,
                warning_count=warning_count,
                warning_threshold=warning_threshold,
                warning_window_hours=warning_window_hours,
                action_taken=action_taken,
                timeout_minutes=timeout_minutes,
            )
        )
        return True
    except (discord.Forbidden, discord.HTTPException):
        return False


async def apply_bad_word_moderation(
    *,
    message: discord.Message,
    bot_user_id: int,
    load_guild_settings,
    parse_int_setting,
    count_recent_warnings,
    record_warning,
    send_moderation_log,
    logger,
    clip_text,
):
    if message.guild is None or not isinstance(message.author, discord.Member):
        return False
    settings = load_guild_settings(message.guild.id)
    if int(settings.get("bad_words_enabled") or 0) <= 0:
        return False
    matched_term = find_bad_word_match(message.content, settings.get("bad_words_list_json"))
    if not matched_term:
        return False

    warning_window_hours = parse_int_setting(settings.get("bad_words_warning_window_hours"), 72, minimum=1)
    warning_threshold = parse_int_setting(settings.get("bad_words_warning_threshold"), 3, minimum=1)
    timeout_minutes = parse_int_setting(settings.get("bad_words_timeout_minutes"), 60, minimum=1)
    configured_action = str(settings.get("bad_words_action") or BAD_WORD_ACTION_TIMEOUT).strip().lower()
    if configured_action != BAD_WORD_ACTION_TIMEOUT:
        configured_action = "warn_only"

    message_deleted = False
    try:
        await message.delete()
        message_deleted = True
    except (discord.Forbidden, discord.HTTPException):
        logger.warning(
            "Failed to delete bad-word message %s in guild %s",
            getattr(message, "id", "unknown"),
            getattr(message.guild, "id", "unknown"),
        )

    warning_count = count_recent_warnings(
        message.guild.id,
        message.author.id,
        within_hours=warning_window_hours,
    ) + 1
    action_taken = "warning_only"
    escalated = False
    if configured_action == BAD_WORD_ACTION_TIMEOUT and warning_count >= warning_threshold:
        timeout_until = datetime.now(UTC) + timedelta(minutes=timeout_minutes)
        try:
            await message.author.timeout(
                timeout_until,
                reason=f"Blocked language after {warning_count} warnings in {warning_window_hours}h",
            )
            action_taken = BAD_WORD_ACTION_TIMEOUT
            escalated = True
        except (discord.Forbidden, discord.HTTPException):
            logger.exception(
                "Failed to apply bad-word timeout to member %s in guild %s",
                message.author.id,
                message.guild.id,
            )
            action_taken = "timeout_failed"

    warning_dm_sent = await send_bad_word_warning_dm(
        message.author,
        guild_name=message.guild.name,
        matched_term=matched_term,
        warning_count=warning_count,
        warning_threshold=warning_threshold,
        warning_window_hours=warning_window_hours,
        action_taken=action_taken if action_taken == BAD_WORD_ACTION_TIMEOUT else "warn_only",
        timeout_minutes=timeout_minutes,
    )
    record_warning(
        guild_id=message.guild.id,
        user_id=message.author.id,
        matched_term=matched_term,
        message_excerpt=clip_text(message.content, max_chars=250),
        action_taken=action_taken,
    )

    actor = message.guild.me or (message.guild.get_member(bot_user_id) if bot_user_id else None)
    if isinstance(actor, discord.Member):
        await send_moderation_log(
            message.guild,
            actor,
            "bad_word_timeout" if escalated else "bad_word_warning",
            target=message.author,
            reason=f"Blocked word or phrase matched: {matched_term}",
            outcome="success" if (message_deleted or warning_dm_sent or escalated) else "partial",
            details=(
                f"warnings={warning_count}/{warning_threshold} "
                f"window_hours={warning_window_hours} "
                f"message_deleted={message_deleted} "
                f"warning_dm_sent={warning_dm_sent} "
                f"action={action_taken}"
            ),
        )
    return True

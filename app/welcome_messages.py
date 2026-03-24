from __future__ import annotations

import base64
import binascii
import io

import discord

DEFAULT_WELCOME_CHANNEL_MESSAGE = "Welcome to {guild_name}, {member_mention}."
DEFAULT_WELCOME_DM_MESSAGE = "Welcome to {guild_name}, {member_name}. We're glad you're here."


def build_welcome_message(template: str | None, member: discord.Member, *, default_template: str, logger):
    guild = member.guild
    safe_template = str(template or "").strip() or default_template
    mapping = {
        "member_mention": member.mention,
        "member_name": member.name,
        "display_name": member.display_name,
        "guild_name": guild.name,
        "member_count": str(getattr(guild, "member_count", 0) or 0),
        "account_created_at": f"<t:{int(member.created_at.timestamp())}:f>",
    }
    try:
        return safe_template.format(**mapping)
    except Exception:
        logger.warning("Invalid welcome message template for guild %s; using default template.", guild.id)
        return default_template.format(**mapping)


async def send_configured_welcome_messages(member: discord.Member, *, load_guild_settings, logger):
    guild = member.guild
    settings = load_guild_settings(guild.id)
    image_payload = None
    image_filename = str(settings.get("welcome_image_filename") or "").strip() or "welcome-image.png"
    image_base64 = str(settings.get("welcome_image_base64") or "").strip()
    if image_base64:
        try:
            image_payload = base64.b64decode(image_base64)
        except (ValueError, binascii.Error):
            logger.warning("Configured welcome image for guild %s is not valid base64; ignoring image.", guild.id)
            image_payload = None

    welcome_channel_id = int(settings.get("welcome_channel_id") or 0)
    if welcome_channel_id > 0:
        welcome_channel = guild.get_channel(welcome_channel_id)
        if isinstance(welcome_channel, discord.TextChannel):
            try:
                channel_file = None
                if image_payload and int(settings.get("welcome_channel_image_enabled") or 0) > 0:
                    channel_file = discord.File(io.BytesIO(image_payload), filename=image_filename)
                await welcome_channel.send(
                    build_welcome_message(
                        settings.get("welcome_channel_message"),
                        member,
                        default_template=DEFAULT_WELCOME_CHANNEL_MESSAGE,
                        logger=logger,
                    ),
                    file=channel_file,
                )
            except Exception:
                logger.exception(
                    "Failed sending welcome channel message for member %s in guild %s",
                    member.id,
                    guild.id,
                )
        else:
            logger.warning(
                "Configured welcome channel %s is not available as a text channel for guild %s",
                welcome_channel_id,
                guild.id,
            )

    if int(settings.get("welcome_dm_enabled") or 0) > 0:
        try:
            dm_file = None
            if image_payload and int(settings.get("welcome_dm_image_enabled") or 0) > 0:
                dm_file = discord.File(io.BytesIO(image_payload), filename=image_filename)
            await member.send(
                build_welcome_message(
                    settings.get("welcome_dm_message"),
                    member,
                    default_template=DEFAULT_WELCOME_DM_MESSAGE,
                    logger=logger,
                ),
                file=dm_file,
            )
        except discord.Forbidden:
            logger.info("Could not send welcome DM to member %s in guild %s: DMs disabled or blocked", member.id, guild.id)
        except Exception:
            logger.exception("Failed sending welcome DM to member %s in guild %s", member.id, guild.id)

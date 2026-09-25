from __future__ import annotations

import json
import re
import threading
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import discord

if TYPE_CHECKING:
    from discord import Guild
    from discord.ext import commands

from app.command_permissions import (
    ADMIN_ONLY_COMMAND_KEYS,
    COMMAND_PERMISSION_MODE_CUSTOM_ROLES,
    COMMAND_PERMISSION_MODE_DEFAULT,
    COMMAND_PERMISSION_MODE_DISABLED,
    COMMAND_PERMISSION_MODE_PUBLIC,
    COMMAND_PERMISSION_METADATA,
    COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC,
    COMMAND_PERMISSION_DEFAULT_POLICY_ALLOWED_NAMES,
    COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS,
    COMMAND_PERMISSION_DEFAULT_POLICY_ADMINISTRATOR,
)


def has_allowed_role(member: discord.Member) -> bool:
    from bot import DEFAULT_ALLOWED_ROLE_NAMES, logger

    has_role = any(role.name in DEFAULT_ALLOWED_ROLE_NAMES for role in member.roles)
    logger.debug("User %s allowed: %s", member, has_role)
    return has_role


def has_moderator_access(member: discord.Member) -> bool:
    from bot import MODERATOR_ROLE_IDS

    return any(role.id in MODERATOR_ROLE_IDS for role in member.roles)


def has_admin_access(member: discord.Member) -> bool:
    return bool(
        getattr(member.guild_permissions, "administrator", False)
        or member.guild.owner_id == member.id
    )


def is_random_choice_eligible(member: discord.Member) -> bool:
    if member.bot:
        return False
    if has_moderator_access(member):
        return False
    if has_allowed_role(member):
        return False
    return True


def normalize_permission_mode(value: str | None) -> str:
    raw = (value or "").strip().lower()
    if raw in {
        COMMAND_PERMISSION_MODE_DEFAULT,
        COMMAND_PERMISSION_MODE_PUBLIC,
        COMMAND_PERMISSION_MODE_DISABLED,
        COMMAND_PERMISSION_MODE_CUSTOM_ROLES,
    }:
        return raw
    return COMMAND_PERMISSION_MODE_DEFAULT


def normalize_role_ids(values) -> list[int]:
    normalized: list[int] = []
    seen: set[int] = set()
    if isinstance(values, str):
        values = re.split(r"[\s,]+", values.strip()) if values.strip() else []
    if not isinstance(values, list):
        values = []
    for value in values:
        cleaned = str(value or "").strip()
        if cleaned.startswith("<@&") and cleaned.endswith(">"):
            cleaned = cleaned[3:-1]
        try:
            role_id = int(cleaned)
        except (TypeError, ValueError):
            continue
        if role_id <= 0 or role_id in seen:
            continue
        seen.add(role_id)
        normalized.append(role_id)
    return normalized


def normalize_command_permission_rule(raw_rule) -> dict:
    if not isinstance(raw_rule, dict):
        return {"mode": COMMAND_PERMISSION_MODE_DEFAULT, "role_ids": []}
    mode = normalize_permission_mode(raw_rule.get("mode"))
    role_ids = normalize_role_ids(raw_rule.get("role_ids", []))
    if mode != COMMAND_PERMISSION_MODE_CUSTOM_ROLES:
        role_ids = []
    return {"mode": mode, "role_ids": role_ids}


def load_command_permission_rules(guild_id: int | None = None) -> dict:
    from bot import (
        COMMAND_PERMISSION_DEFAULTS,
        command_permissions_cache,
        command_permissions_lock,
        db_kv_get,
        logger,
        with_db,
    )

    safe_guild_id = guild_id
    with command_permissions_lock:
        cache_entry = command_permissions_cache.get(safe_guild_id, {})
        version = db_kv_get(f"command_permissions_updated_at:{safe_guild_id}") or "bootstrap"
        if cache_entry.get("mtime") == version:
            return cache_entry.get("rules", {})

        with with_db() as conn:
            rows = conn.execute(
                """
                SELECT command_key, mode, role_ids_json
                FROM command_permissions
                WHERE guild_id = ?
                """,
                (safe_guild_id,),
            ).fetchall()
        normalized_rules: dict = {}
        for row in rows:
            command_key = str(row["command_key"])
            if command_key not in COMMAND_PERMISSION_DEFAULTS:
                continue
            try:
                role_ids_payload = json.loads(row["role_ids_json"] or "[]")
            except Exception:
                role_ids_payload = []
            normalized_rules[command_key] = normalize_command_permission_rule(
                {"mode": row["mode"], "role_ids": role_ids_payload}
            )

        command_permissions_cache[safe_guild_id] = {
            "mtime": version,
            "rules": normalized_rules,
        }
        return normalized_rules


def save_command_permission_rules(
    rules: dict,
    actor_email: str = "",
    guild_id: int | None = None,
) -> dict:
    from bot import (
        COMMAND_PERMISSION_DEFAULTS,
        command_permissions_cache,
        command_permissions_lock,
        db_kv_set,
        logger,
        normalize_target_guild_id,
        with_db,
    )

    safe_guild_id = normalize_target_guild_id(guild_id)
    safe_rules: dict = {}
    for command_key, raw_rule in (rules or {}).items():
        if command_key not in COMMAND_PERMISSION_DEFAULTS:
            continue
        normalized_rule = normalize_command_permission_rule(raw_rule)
        if normalized_rule["mode"] == COMMAND_PERMISSION_MODE_DEFAULT:
            continue
        safe_rules[command_key] = normalized_rule

    updated_at = datetime.now(UTC).isoformat()
    with command_permissions_lock:
        with with_db(commit=True) as conn:
            conn.execute(
                "DELETE FROM command_permissions WHERE guild_id = ?",
                (safe_guild_id,),
            )
            for command_key, normalized_rule in safe_rules.items():
                conn.execute(
                    """
                    INSERT INTO command_permissions (
                        guild_id, command_key, mode, role_ids_json, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        safe_guild_id,
                        command_key,
                        normalized_rule["mode"],
                        json.dumps(normalized_rule["role_ids"]),
                        updated_at,
                    ),
                )
        db_kv_set(f"command_permissions_updated_at:{safe_guild_id}", updated_at)
        db_kv_set(
            f"command_permissions_updated_by:{safe_guild_id}",
            actor_email or "unknown",
        )
        command_permissions_cache[safe_guild_id] = {
            "mtime": updated_at,
            "rules": safe_rules,
        }
    return safe_rules


def resolve_command_permission_state(
    command_key: str,
    guild_id: int | None = None,
):
    from bot import COMMAND_PERMISSION_DEFAULTS

    default_policy = COMMAND_PERMISSION_DEFAULTS.get(
        command_key, COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC
    )
    rule = load_command_permission_rules(guild_id=guild_id).get(
        command_key,
        {"mode": COMMAND_PERMISSION_MODE_DEFAULT, "role_ids": []},
    )
    mode = rule.get("mode", COMMAND_PERMISSION_MODE_DEFAULT)
    role_ids = normalize_role_ids(rule.get("role_ids", []))
    return default_policy, mode, role_ids


def member_has_any_role_id(
    member: discord.Member | discord.User,
    role_ids: list[int],
) -> bool:
    if not isinstance(member, discord.Member):
        return False
    if not role_ids:
        return False
    member_role_ids = {role.id for role in member.roles}
    return any(role_id in member_role_ids for role_id in role_ids)


def can_use_command(
    member: discord.Member | discord.User,
    command_key: str,
    guild_id: int | None = None,
) -> bool:
    default_policy, mode, role_ids = resolve_command_permission_state(
        command_key, guild_id=guild_id
    )

    if mode == COMMAND_PERMISSION_MODE_DISABLED:
        return False
    if mode == COMMAND_PERMISSION_MODE_PUBLIC:
        return True
    if mode == COMMAND_PERMISSION_MODE_CUSTOM_ROLES:
        return member_has_any_role_id(member, role_ids)

    if default_policy == COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC:
        return True
    if default_policy == COMMAND_PERMISSION_DEFAULT_POLICY_ALLOWED_NAMES:
        return isinstance(member, discord.Member) and has_allowed_role(member)
    if default_policy == COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS:
        return isinstance(member, discord.Member) and has_moderator_access(member)
    if default_policy == COMMAND_PERMISSION_DEFAULT_POLICY_ADMINISTRATOR:
        return isinstance(member, discord.Member) and has_admin_access(member)
    return False


def build_command_permission_denied_message(
    command_key: str,
    guild: Guild | None = None,
    guild_id: int | None = None,
) -> str:
    default_policy, mode, role_ids = resolve_command_permission_state(
        command_key,
        guild_id=guild_id or (guild.id if guild else None),
    )
    if mode == COMMAND_PERMISSION_MODE_DISABLED:
        return "⛔ This command is disabled in this server."
    if mode == COMMAND_PERMISSION_MODE_CUSTOM_ROLES:
        if guild is None or not role_ids:
            return "❌ You do not have one of the roles allowed to use this command."
        mentions = []
        for role_id in role_ids:
            role = guild.get_role(role_id)
            mentions.append(role.mention if role else f"`{role_id}`")
        return f"❌ You need one of these roles: {', '.join(mentions)}."

    if default_policy == COMMAND_PERMISSION_DEFAULT_POLICY_ALLOWED_NAMES:
        from bot import DEFAULT_ALLOWED_ROLE_NAMES

        names = ", ".join(sorted(DEFAULT_ALLOWED_ROLE_NAMES))
        return f"❌ You need one of these roles: `{names}`."
    if default_policy == COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS:
        return "❌ Only moderators can use this command."
    if default_policy == COMMAND_PERMISSION_DEFAULT_POLICY_ADMINISTRATOR:
        return "❌ Only server administrators can use this command."
    return "❌ You do not have permission to use this command."


async def ensure_interaction_command_access(
    interaction: discord.Interaction,
    command_key: str,
) -> bool:
    from bot import can_use_command as _can_use_command

    guild_id = interaction.guild.id if interaction.guild else None
    if _can_use_command(interaction.user, command_key, guild_id=guild_id):
        return True
    await interaction.response.send_message(
        build_command_permission_denied_message(
            command_key, interaction.guild, guild_id=guild_id
        ),
        ephemeral=True,
    )
    return False


async def ensure_prefix_command_access(
    ctx: commands.Context,
    command_key: str,
) -> bool:
    from bot import can_use_command as _can_use_command

    guild_id = ctx.guild.id if ctx.guild else None
    if _can_use_command(ctx.author, command_key, guild_id=guild_id):
        return True
    await ctx.send(
        build_command_permission_denied_message(
            command_key, ctx.guild, guild_id=guild_id
        )
    )
    return False


async def send_safe_interaction_message(
    interaction: discord.Interaction,
    message_text: str,
    ephemeral: bool = True,
) -> bool:
    from bot import logger

    try:
        if interaction.response.is_done():
            await interaction.followup.send(message_text, ephemeral=ephemeral)
        else:
            await interaction.response.send_message(message_text, ephemeral=ephemeral)
        return True
    except discord.NotFound as exc:
        logger.warning(
            "Interaction expired before sending response message for user=%s command=%s code=%s",
            f"{interaction.user} (id: {interaction.user.id})",
            interaction.command.name if interaction.command else "unknown",
            getattr(exc, "code", "unknown"),
        )
        return False
    except discord.HTTPException:
        logger.exception(
            "Failed sending interaction response message for user=%s command=%s",
            f"{interaction.user} (id: {interaction.user.id})",
            interaction.command.name if interaction.command else "unknown",
        )
        return False


async def send_safe_interaction_modal(
    interaction: discord.Interaction,
    modal: discord.ui.Modal,
    *,
    stale_interaction_dm_text: str | None = None,
) -> bool:
    from bot import logger

    try:
        if interaction.response.is_done():
            logger.warning(
                "Cannot open modal because interaction response is already complete for user=%s command=%s",
                f"{interaction.user} (id: {interaction.user.id})",
                interaction.command.name if interaction.command else "unknown",
            )
            return False
        await interaction.response.send_modal(modal)
        return True
    except discord.NotFound as exc:
        logger.warning(
            "Interaction expired before opening modal for user=%s command=%s code=%s",
            f"{interaction.user} (id: {interaction.user.id})",
            interaction.command.name if interaction.command else "unknown",
            getattr(exc, "code", "unknown"),
        )
        if stale_interaction_dm_text:
            try:
                await interaction.user.send(stale_interaction_dm_text)
            except discord.HTTPException:
                logger.warning(
                    "Failed sending stale interaction DM for user=%s command=%s",
                    f"{interaction.user} (id: {interaction.user.id})",
                    interaction.command.name if interaction.command else "unknown",
                )
        return False
    except discord.HTTPException:
        logger.exception(
            "Failed opening interaction modal for user=%s command=%s",
            f"{interaction.user} (id: {interaction.user.id})",
            interaction.command.name if interaction.command else "unknown",
        )
        return False


async def reply_with_default_visibility(
    interaction: discord.Interaction,
    message_text: str,
) -> bool:
    from bot import COMMAND_RESPONSES_EPHEMERAL

    return await send_safe_interaction_message(
        interaction, message_text, ephemeral=COMMAND_RESPONSES_EPHEMERAL
    )


async def get_text_channel(client: commands.Bot, channel_id: int):
    channel = client.get_channel(channel_id)
    if isinstance(channel, discord.TextChannel):
        return channel
    try:
        fetched = await client.fetch_channel(channel_id)
    except (discord.NotFound, discord.Forbidden, discord.HTTPException):
        return None
    if isinstance(fetched, discord.TextChannel):
        return fetched
    return None


async def log_interaction(
    interaction: discord.Interaction,
    action: str,
    target: discord.abc.User | None = None,
    reason: str | None = None,
    success: bool = True,
) -> None:
    from bot import GUILD_ID, logger, record_action_safe, truncate_log_text

    guild_id = interaction.guild.id if interaction.guild else GUILD_ID
    status = "success" if success else "failed"
    moderator = (
        f"{interaction.user} ({interaction.user.id})" if interaction.user else "Unknown"
    )
    target_text = f"{target} ({target.id})" if target else ""
    record_action_safe(
        action=action,
        status=status,
        moderator=moderator,
        target=target_text,
        reason=truncate_log_text(str(reason or "")),
        guild_id=guild_id,
    )


def build_command_permissions_web_payload(guild_id: int):
    from bot import (
        COMMAND_PERMISSION_DEFAULTS,
        COMMAND_PERMISSION_POLICY_LABELS,
        DEFAULT_ALLOWED_ROLE_NAMES,
        MODERATOR_ROLE_IDS,
        normalize_command_permission_rule,
        normalize_target_guild_id,
    )

    safe_guild_id = normalize_target_guild_id(guild_id)
    rules = load_command_permission_rules(guild_id=safe_guild_id)
    commands_payload = []
    for command_key, metadata in COMMAND_PERMISSION_METADATA.items():
        default_policy = COMMAND_PERMISSION_DEFAULTS.get(
            command_key, COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC
        )
        rule = normalize_command_permission_rule(rules.get(command_key, {}))
        commands_payload.append(
            {
                "key": command_key,
                "label": metadata.get("label", command_key),
                "description": metadata.get("description", ""),
                "default_policy": default_policy,
                "default_policy_label": COMMAND_PERMISSION_POLICY_LABELS.get(
                    default_policy, default_policy
                ),
                "mode": rule["mode"],
                "role_ids": rule["role_ids"],
            }
        )
    return {
        "ok": True,
        "guild_id": safe_guild_id,
        "commands": commands_payload,
        "allowed_role_names": sorted(DEFAULT_ALLOWED_ROLE_NAMES),
        "moderator_role_ids": sorted(MODERATOR_ROLE_IDS),
    }


def run_web_get_command_permissions(guild_id: int):
    from bot import logger

    try:
        return build_command_permissions_web_payload(guild_id)
    except Exception:
        logger.exception("Failed to build command permissions payload for web admin")
        return {"ok": False, "error": "Unexpected error while loading command permissions."}


def run_web_update_command_permissions(payload: dict, actor_email: str, guild_id: int):
    from bot import (
        COMMAND_PERMISSION_METADATA,
        COMMAND_PERMISSION_MODE_CUSTOM_ROLES,
        logger,
        normalize_permission_mode,
        normalize_role_ids,
        normalize_target_guild_id,
        save_command_permission_rules,
    )

    if not isinstance(payload, dict):
        return {"ok": False, "error": "Invalid payload type for command permissions update."}
    commands_payload = payload.get("commands", {})
    if not isinstance(commands_payload, dict):
        return {"ok": False, "error": "Payload is missing a commands object."}

    updated_rules: dict = {}
    validation_errors: list[str] = []
    for command_key in COMMAND_PERMISSION_METADATA.keys():
        raw_rule = commands_payload.get(command_key, {})
        if not isinstance(raw_rule, dict):
            raw_rule = {}

        mode = normalize_permission_mode(raw_rule.get("mode"))
        role_ids = normalize_role_ids(raw_rule.get("role_ids", []))
        if mode == COMMAND_PERMISSION_MODE_CUSTOM_ROLES and not role_ids:
            validation_errors.append(
                f"{command_key}: mode `custom_roles` requires at least one role ID."
            )
        updated_rules[command_key] = {"mode": mode, "role_ids": role_ids}

    if validation_errors:
        return {"ok": False, "error": " ".join(validation_errors)}

    try:
        save_command_permission_rules(
            updated_rules,
            actor_email=actor_email,
            guild_id=normalize_target_guild_id(guild_id),
        )
    except Exception:
        logger.exception("Failed to save command permissions from web admin")
        return {"ok": False, "error": "Failed to save command permissions."}

    response = build_command_permissions_web_payload(guild_id)
    response["message"] = "Command permissions updated."
    logger.info("Command permissions updated via web admin")
    return response


def normalize_honeypot_record(raw_entry) -> dict:
    from bot import (
        clamp_honeypot_delete_message_days,
        clamp_honeypot_timeout_hours,
        normalize_honeypot_action,
    )

    entry = dict(raw_entry or {})
    return {
        "id": int(entry.get("id") or 0),
        "guild_id": int(entry.get("guild_id") or 0),
        "channel_id": int(entry.get("channel_id") or 0),
        "action": normalize_honeypot_action(entry.get("action")),
        "delete_message_days": clamp_honeypot_delete_message_days(
            entry.get("delete_message_days")
        ),
        "timeout_hours": clamp_honeypot_timeout_hours(entry.get("timeout_hours")),
        "role_id": int(entry.get("role_id") or 0),
        "enabled": 1 if int(entry.get("enabled") or 0) > 0 else 0,
        "created_at": str(entry.get("created_at") or ""),
        "updated_at": str(entry.get("updated_at") or ""),
        "created_by_email": str(entry.get("created_by_email") or ""),
        "updated_by_email": str(entry.get("updated_by_email") or ""),
    }


def load_honeypot_entries(guild_id: int | None) -> list:
    from bot import normalize_target_guild_id, with_db

    safe_guild_id = normalize_target_guild_id(guild_id)
    with with_db() as conn:
        rows = conn.execute(
            """
            SELECT id, guild_id, channel_id, action, delete_message_days, timeout_hours,
                   role_id, enabled, created_at, updated_at, created_by_email, updated_by_email
            FROM honeypot_channels
            WHERE guild_id = ?
            ORDER BY channel_id ASC
            """,
            (safe_guild_id,),
        ).fetchall()
    return rows

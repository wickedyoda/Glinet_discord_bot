from __future__ import annotations

import json
import re
import threading
from datetime import UTC, datetime

import discord

if False:
    pass  # was: if TYPE_CHECKING: pass

# ---------------------------------------------------------------------------
# Constants — defined in dependency order so names are available when used
# ---------------------------------------------------------------------------

from app.http_client import get  # noqa: F401 - needed for transitive import graph

# Policy string constants (used by DEFAULTS, METADATA, POLICY_LABELS)
COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC = "public"
COMMAND_PERMISSION_DEFAULT_POLICY_ALLOWED_NAMES = "allowed_role_names"
COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS = "moderator_role_ids"
COMMAND_PERMISSION_DEFAULT_POLICY_ADMINISTRATOR = "administrator"

# Mode string constants
COMMAND_PERMISSION_MODE_DEFAULT = "default"
COMMAND_PERMISSION_MODE_PUBLIC = "public"
COMMAND_PERMISSION_MODE_DISABLED = "disabled"
COMMAND_PERMISSION_MODE_CUSTOM_ROLES = "custom_roles"

# Role name sets — overridden at runtime by bot.py during refresh
DEFAULT_ALLOWED_ROLE_NAMES = {"Employee", "Admin", "Gl.iNet Moderator"}

# Admin-only and moderator-only command key sets
ADMIN_ONLY_COMMAND_KEYS = {
    "honeypot_create",
    "honeypot_edit",
    "honeypot_list",
    "honeypot_info",
    "honeypot_delete",
    "honeypot_delete_all",
    "honeypot_log_set_channel",
    "honeypot_log_clear_channel",
    "honeypot_log_set_role",
    "honeypot_log_clear_role",
    "honeypot_log_show",
    "honeypot_join_guard_set",
    "honeypot_join_guard_disable",
    "honeypot_join_guard_show",
    "set_hello_channel",
    "set_hello_text",
}

MODERATOR_ONLY_COMMAND_KEYS = {
    "add_role_member",
    "ban_member",
    "bulk_assign_role_csv",
    "clear_member_nickname",
    "create_role",
    "delete_role",
    "edit_role",
    "kick_member",
    "random_choice",
    "restore_code",
    "remove_role_member",
    "set_member_nickname",
    "timeout_member",
    "unban_member",
    "untimeout_member",
    "voice_deafen_member",
    "voice_disconnect_member",
    "voice_move_member",
    "voice_mute_member",
}

# Default policy map — built from the policy constants above
COMMAND_PERMISSION_DEFAULTS = {
    "list": COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC,
    "help": COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC,
    "ping": COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC,
    "sayhi": COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC,
    "happy": COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC,
    "coin_flip": COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC,
    "eight_ball": COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC,
    "meme": COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC,
    "dad_joke": COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC,
    "shorten": COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC,
    "expand": COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC,
    "uptime": COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC,
    "stats": COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC,
    "tag_commands": COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC,
    "ticket": COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS,
    "ticket_stats": COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS,
    "submitrole": COMMAND_PERMISSION_DEFAULT_POLICY_ALLOWED_NAMES,
    "bulk_assign_role_csv": COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS,
    "enter_role": COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC,
    "getaccess": COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC,
    "country": COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC,
    "clear_country": COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC,
    "create_role": COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS,
    "delete_role": COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS,
    "edit_role": COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS,
    "modlog_test": COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS,
    "ban_member": COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS,
    "kick_member": COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS,
    "timeout_member": COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS,
    "untimeout_member": COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS,
    "unban_member": COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS,
    "add_role_member": COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS,
    "remove_role_member": COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS,
    "prune_messages": COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS,
    "logs": COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS,
    "random_choice": COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS,
    "search_reddit": COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC,
    "search_forum": COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC,
    "search_openwrt_forum": COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC,
    "search_kvm": COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC,
    "search_iot": COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC,
    "search_router": COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC,
    "search_astrowarp": COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC,
    "translate_to_english": COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC,
    "translate_channels_manage": COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS,
    "honeypot_create": COMMAND_PERMISSION_DEFAULT_POLICY_ADMINISTRATOR,
    "honeypot_edit": COMMAND_PERMISSION_DEFAULT_POLICY_ADMINISTRATOR,
    "honeypot_list": COMMAND_PERMISSION_DEFAULT_POLICY_ADMINISTRATOR,
    "honeypot_info": COMMAND_PERMISSION_DEFAULT_POLICY_ADMINISTRATOR,
    "honeypot_delete": COMMAND_PERMISSION_DEFAULT_POLICY_ADMINISTRATOR,
    "honeypot_delete_all": COMMAND_PERMISSION_DEFAULT_POLICY_ADMINISTRATOR,
    "honeypot_log_set_channel": COMMAND_PERMISSION_DEFAULT_POLICY_ADMINISTRATOR,
    "honeypot_log_clear_channel": COMMAND_PERMISSION_DEFAULT_POLICY_ADMINISTRATOR,
    "honeypot_log_set_role": COMMAND_PERMISSION_DEFAULT_POLICY_ADMINISTRATOR,
    "honeypot_log_clear_role": COMMAND_PERMISSION_DEFAULT_POLICY_ADMINISTRATOR,
    "honeypot_log_show": COMMAND_PERMISSION_DEFAULT_POLICY_ADMINISTRATOR,
    "honeypot_join_guard_set": COMMAND_PERMISSION_DEFAULT_POLICY_ADMINISTRATOR,
    "honeypot_join_guard_disable": COMMAND_PERMISSION_DEFAULT_POLICY_ADMINISTRATOR,
    "honeypot_join_guard_show": COMMAND_PERMISSION_DEFAULT_POLICY_ADMINISTRATOR,
    "set_hello_channel": COMMAND_PERMISSION_DEFAULT_POLICY_ADMINISTRATOR,
    "set_hello_text": COMMAND_PERMISSION_DEFAULT_POLICY_ADMINISTRATOR,
    "support_ticket_search": COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS,
    "support_ticket_view": COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS,
    "support_ticket_categories": COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS,
    "create_ticket": COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS,
}

for _command_key in MODERATOR_ONLY_COMMAND_KEYS:
    COMMAND_PERMISSION_DEFAULTS[_command_key] = COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS
for _command_key in ADMIN_ONLY_COMMAND_KEYS:
    COMMAND_PERMISSION_DEFAULTS[_command_key] = COMMAND_PERMISSION_DEFAULT_POLICY_ADMINISTRATOR

COMMAND_PERMISSION_MODE_DEFAULT = "default"
COMMAND_PERMISSION_MODE_PUBLIC = "public"
COMMAND_PERMISSION_MODE_DISABLED = "disabled"
COMMAND_PERMISSION_MODE_CUSTOM_ROLES = "custom_roles"

COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC = "public"
COMMAND_PERMISSION_DEFAULT_POLICY_ALLOWED_NAMES = "allowed_role_names"
COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS = "moderator_role_ids"
COMMAND_PERMISSION_DEFAULT_POLICY_ADMINISTRATOR = "administrator"

COMMAND_PERMISSION_POLICY_LABELS = {
    COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC: "Public (any member)",
    COMMAND_PERMISSION_DEFAULT_POLICY_ALLOWED_NAMES: "Named roles: Employee/Admin/Gl.iNet Moderator",
    COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS: "Moderator/Admin role IDs (env)",
    COMMAND_PERMISSION_DEFAULT_POLICY_ADMINISTRATOR: "Server administrators only",
}

COMMAND_PERMISSION_METADATA = {
    "list": {"label": "!list", "description": "Show available tag commands."},
    "help": {"label": "/help", "description": "Show command help and wiki links."},
    "ping": {"label": "/ping", "description": "Check that the bot is online."},
    "sayhi": {"label": "/sayhi", "description": "Post a short bot introduction and point users to /help."},
    "happy": {"label": "/happy", "description": "Post a random puppy image."},
    "coin_flip": {"label": "/coin_flip", "description": "Flip a coin."},
    "eight_ball": {"label": "/eight_ball", "description": "Ask the magic 8-ball a question."},
    "meme": {"label": "/meme", "description": "Post a random meme."},
    "dad_joke": {"label": "/dad_joke", "description": "Post a dad joke."},
    "shorten": {"label": "/shorten", "description": "Create a short URL through the configured shortener."},
    "expand": {"label": "/expand", "description": "Expand a short URL or short code."},
    "uptime": {"label": "/uptime", "description": "Show uptime monitor summary."},
    "stats": {"label": "/stats", "description": "Show your private member activity stats."},
    "tag_commands": {"label": "/tag", "description": "Send a configured slash tag response from persistent storage."},
    "ticket": {"label": "/ticket", "description": "Open a support ticket with a role-restricted admin panel."},
    "ticket_stats": {"label": "/ticket-stats", "description": "Show per-category ticket counts."},
    "submitrole": {"label": "/submitrole", "description": "Create role invite + code mapping."},
    "restore_code": {"label": "/restore_code", "description": "Restore a specific 6-digit role access code and optional invite."},
    "bulk_assign_role_csv": {"label": "/bulk_assign_role_csv", "description": "Bulk-assign a role from CSV."},
    "enter_role": {"label": "/enter_role", "description": "Enter a role via a valid invite code."},
    "getaccess": {"label": "/getaccess", "description": "Get your access codes and invites."},
    "country": {"label": "/country", "description": "Set your country."},
    "clear_country": {"label": "/clear-country", "description": "Clear your country setting."},
    "create_role": {"label": "/create-role", "description": "Create a new role."},
    "delete_role": {"label": "/delete-role", "description": "Delete a role."},
    "edit_role": {"label": "/edit-role", "description": "Edit a role."},
    "modlog_test": {"label": "/modlog-test", "description": "Test mod log permissions."},
    "ban_member": {"label": "/ban-member", "description": "Ban a member."},
    "kick_member": {"label": "/kick-member", "description": "Kick a member."},
    "timeout_member": {"label": "/timeout-member", "description": "Timeout a member."},
    "untimeout_member": {"label": "/untimeout-member", "description": "Remove a timeout from a member."},
    "unban_member": {"label": "/unban-member", "description": "Unban a member."},
    "add_role_member": {"label": "/add-role-member", "description": "Add a role to a member."},
    "remove_role_member": {"label": "/remove-role-member", "description": "Remove a role from a member."},
    "prune_messages": {"label": "/prune-messages", "description": "Prune recent messages."},
    "logs": {"label": "/logs", "description": "View moderation logs."},
    "random_choice": {"label": "/random-choice", "description": "Randomly pick a member."},
    "search_reddit": {"label": "/search-reddit", "description": "Search Reddit."},
    "search_forum": {"label": "/search-forum", "description": "Search GL.iNet forum."},
    "search_openwrt_forum": {"label": "/search-openwrt-forum", "description": "Search OpenWRT forum."},
    "search_kvm": {"label": "/search-kvm", "description": "Search KVM forum."},
    "search_iot": {"label": "/search-iot", "description": "Search IoT forum."},
    "search_router": {"label": "/search-router", "description": "Search router forum."},
    "search_astrowarp": {"label": "/search-astrowarp", "description": "Search Astrowarp forum."},
    "translate_to_english": {"label": "/translate-to-english", "description": "Translate a message to English."},
    "translate_channels_manage": {"label": "/translate_channels_*", "description": "Moderator: list, add, or remove auto-translate channel mappings."},
    "honeypot_create": {"label": "/honeypot-create", "description": "Create a honeypot channel."},
    "honeypot_edit": {"label": "/honeypot-edit", "description": "Edit a honeypot channel."},
    "honeypot_list": {"label": "/honeypot-list", "description": "List honeypot channels."},
    "honeypot_info": {"label": "/honeypot-info", "description": "Show honeypot channel info."},
    "honeypot_delete": {"label": "/honeypot-delete", "description": "Delete a honeypot channel."},
    "honeypot_delete_all": {"label": "/honeypot-delete-all", "description": "Delete all honeypot channels."},
    "honeypot_log_set_channel": {"label": "/honeypot-log-set-channel", "description": "Set honeypot log channel."},
    "honeypot_log_clear_channel": {"label": "/honeypot-log-clear-channel", "description": "Clear honeypot log channel."},
    "honeypot_log_set_role": {"label": "/honeypot-log-set-role", "description": "Set honeypot log role."},
    "honeypot_log_clear_role": {"label": "/honeypot-log-clear-role", "description": "Clear honeypot log role."},
    "honeypot_log_show": {"label": "/honeypot-log-show", "description": "Show honeypot log settings."},
    "honeypot_join_guard_set": {"label": "/honeypot-join-guard-set", "description": "Set honeypot join guard."},
    "honeypot_join_guard_disable": {"label": "/honeypot-join-guard-disable", "description": "Disable honeypot join guard."},
    "honeypot_join_guard_show": {"label": "/honeypot-join-guard-show", "description": "Show honeypot join guard settings."},
    "set_hello_channel": {"label": "/set-hello-channel", "description": "Set hello channel."},
    "set_hello_text": {"label": "/set-hello-text", "description": "Set hello text."},
    "support_ticket_search": {"label": "/support-ticket-search", "description": "Search Freshdesk tickets."},
    "support_ticket_view": {"label": "/support-ticket-view", "description": "View a Freshdesk ticket by ID."},
    "support_ticket_categories": {"label": "/support-ticket-categories", "description": "List Freshdesk solution/knowledge-base categories."},
    "create_ticket": {"label": "/create-ticket", "description": "Create a Freshdesk ticket from Discord."},
}

COMMAND_PERMISSIONS_FILE = "command_permissions.json"  # path relative to DATA_DIR; actual path built by caller

# Runtime state mirrors module-level globals in bot.py; injected by factory/reload.
command_permissions_lock = threading.Lock()
command_permissions_cache: dict = {}


# ---------------------------------------------------------------------------
# Permission check functions
# ---------------------------------------------------------------------------
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
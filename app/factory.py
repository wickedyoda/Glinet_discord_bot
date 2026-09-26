from __future__ import annotations

from typing import TYPE_CHECKING

import discord
from discord import app_commands
from discord.ext import commands

__all__ = [
    "create_bot",
    "bot",
    "tree",
    "intents",
    "sync_commands_for_all_guilds",
    "refresh_invite_cache_for_guild",
    "build_command_list",
    "find_tag_response_key",
    "autocomplete_tag_response_name",
    "register_tag_commands_for_guild",
    "sync_commands_for_guild",
    "reload_tag_commands_runtime",
    "schedule_tag_command_refresh",
    "generate_code",
    "build_docs_site_search_message",
    "upgrade_legacy_default_tag_responses",
    "load_tag_responses",
    "get_tag_responses",
]


# ---------------------------------------------------------------------------
# Bot instance — created here so other modules can import without
# triggering circular imports.  Lazy imports from bot.py are used for
# runtime configuration (intents, token, etc.).
# ---------------------------------------------------------------------------

intents: discord.Intents | None = None
bot: commands.Bot | None = None
tree: app_commands.CommandTree | None = None


def create_bot() -> None:
    """Initialise the global bot/tree/intents instances.

    Called once from bot.py at module load time, after all env-driven
    configuration constants have been resolved.
    """
    global intents, bot, tree
    from bot import ENABLE_MEMBERS_INTENT

    intents = discord.Intents.default()
    intents.members = ENABLE_MEMBERS_INTENT
    intents.message_content = True
    bot = commands.Bot(command_prefix="!", intents=intents, case_insensitive=True)
    tree = bot.tree


# ---------------------------------------------------------------------------
# Tag-command helpers — extracted from bot.py
# ---------------------------------------------------------------------------

def normalize_tag(tag: str) -> str:
    return tag.strip().lower()


def get_tag_responses(guild_id: int | None = None):
    from bot import tag_response_cache, db_kv_get, load_tag_responses as _load, normalize_target_guild_id

    safe_guild_id = normalize_target_guild_id(guild_id)
    current_version = db_kv_get(f"tag_responses_updated_at:{safe_guild_id}") or "bootstrap"
    cached = tag_response_cache.get(safe_guild_id) or {}
    if cached.get("mtime") != current_version:
        tag_response_cache[safe_guild_id] = {
            "mtime": current_version,
            "mapping": _load(safe_guild_id),
        }
    return dict(tag_response_cache.get(safe_guild_id, {}).get("mapping") or {})


def load_tag_responses(guild_id: int | None = None):
    from bot import load_tag_responses as _load_tag_responses

    return _load_tag_responses(guild_id)


def upgrade_legacy_default_tag_responses(guild_id: int | None = None) -> None:
    from bot import upgrade_legacy_default_tag_responses as _upgrade_legacy_default_tag_responses

    return _upgrade_legacy_default_tag_responses(guild_id)


def build_command_list(guild_id: int | None = None) -> str:
    tags = sorted(get_tag_responses(guild_id).keys())
    if not tags:
        return "No tag commands are available yet."
    return "Tag commands:\n" + "\n".join(tags)


def find_tag_response_key(raw_value: str, guild_id: int | None = None):
    from bot import normalize_tag as _normalize, get_tag_responses

    requested = _normalize(raw_value)
    if not requested:
        return None
    tags = get_tag_responses(guild_id)
    if requested in tags:
        return requested
    if not requested.startswith("!"):
        prefixed = f"!{requested}"
        if prefixed in tags:
            return prefixed
    return None


async def autocomplete_tag_response_name(interaction: discord.Interaction, current: str):
    from bot import get_tag_responses, normalize_tag as _normalize, GUILD_ID, logger

    guild_id = interaction.guild.id if interaction.guild else GUILD_ID
    requested = _normalize(current or "").lstrip("!")
    choices = []
    for tag in sorted(get_tag_responses(guild_id).keys()):
        candidate = str(tag or "").strip()
        if not candidate:
            continue
        match_text = candidate.lower().lstrip("!")
        if requested and requested not in match_text:
            continue
        from discord import app_commands

        choices.append(app_commands.Choice(name=candidate, value=candidate))
        if len(choices) >= 25:
            break
    return choices


def register_tag_commands_for_guild(guild_id: int | None) -> None:
    from bot import get_tag_responses, normalize_target_guild_id, tag_command_names_by_guild

    safe_guild_id = normalize_target_guild_id(guild_id)
    tag_command_names_by_guild[safe_guild_id] = set(get_tag_responses(safe_guild_id).keys())


async def sync_commands_for_guild(guild: discord.Guild):
    from bot import logger, register_tag_commands_for_guild

    command_tree = globals().get("tree")
    if command_tree is None:
        logger.warning("Command tree is not initialized; skipping guild sync for %s", guild.id)
        return []
    guild_obj = discord.Object(id=guild.id)
    command_tree.clear_commands(guild=guild_obj)
    command_tree.copy_global_to(guild=guild_obj)
    register_tag_commands_for_guild(guild.id)
    try:
        synced = await command_tree.sync(guild=guild_obj)
    except TimeoutError:
        logger.warning("Timed out syncing commands to guild %s", guild.id)
        return []
    except discord.HTTPException:
        logger.exception("Failed to sync commands to guild %s", guild.id)
        return []
    logger.info("Synced %d command(s) to guild %s", len(synced), guild.id)
    return synced


async def reload_tag_commands_runtime(guild_id: int | None = None) -> None:
    from bot import logger, normalize_target_guild_id, register_tag_commands_for_guild, tag_command_names_by_guild

    safe_guild_id = normalize_target_guild_id(guild_id)
    try:
        previous_count = len(tag_command_names_by_guild.get(safe_guild_id) or set())
        register_tag_commands_for_guild(safe_guild_id)
        logger.info(
            "Tag responses reloaded for guild %s: previous=%s current=%s",
            safe_guild_id,
            previous_count,
            len(tag_command_names_by_guild.get(safe_guild_id) or set()),
        )
    except Exception:
        logger.exception("Failed to reload tag responses for guild %s", safe_guild_id)


def schedule_tag_command_refresh(guild_id: int | None = None) -> bool:
    from bot import bot as _bot, logger

    loop = getattr(_bot, "loop", None)
    if loop is None or not loop.is_running():
        logger.warning("Cannot refresh tag slash commands yet: bot loop is not running")
        return False

    def _start_refresh() -> None:
        import asyncio

        from bot import reload_tag_commands_runtime, normalize_target_guild_id

        asyncio.create_task(
            reload_tag_commands_runtime(guild_id),
            name=f"tag_commands_refresh_{normalize_target_guild_id(guild_id)}",
        )

    loop.call_soon_threadsafe(_start_refresh)
    return True


def generate_code() -> str:
    import secrets
    import logging

    logger = logging.getLogger("invite_bot")
    while True:
        code = ""
        last_digit = None
        streak = 1
        for _ in range(6):
            digit = str(secrets.randbelow(10))
            if digit == last_digit:
                streak += 1
            else:
                streak = 1
            if streak > 2:
                break
            code += digit
            last_digit = digit
        if len(code) == 6:
            logger.debug("Generated code %s", code)
            return code


async def refresh_invite_cache_for_guild(guild: discord.Guild) -> None:
    from bot import invite_uses_by_guild, invite_roles_by_guild, logger

    guild_invite_uses = invite_uses_by_guild.setdefault(guild.id, {})
    invite_roles = invite_roles_by_guild.get(guild.id) or {}
    if not invite_roles:
        guild_invite_uses.clear()
        return
    try:
        invites = await guild.invites()
    except Exception:
        logger.exception("Failed to cache invites for guild %s", guild.id)
        return

    guild_invite_uses.clear()
    for invite in invites:
        if invite.code in invite_roles:
            guild_invite_uses[invite.code] = invite.uses


async def sync_commands_for_all_guilds() -> int:
    from bot import get_managed_guilds, get_tag_responses, logger, sync_commands_for_guild

    total_synced = 0
    for guild in get_managed_guilds():
        upgrade_legacy_default_tag_responses(guild.id)
        get_tag_responses(guild.id)
        synced = await sync_commands_for_guild(guild)
        total_synced += len(synced)
        await refresh_invite_cache_for_guild(guild)
    return total_synced


def build_docs_site_search_message(query: str, site_key: str) -> str:
    from bot import DOCS_SITE_MAP, search_docs_site_links, trim_search_message, suppress_discord_link_embed

    site_info = DOCS_SITE_MAP.get(site_key)
    if not site_info:
        return "❌ Invalid documentation site."

    site_name, base_url = site_info
    site_results = search_docs_site_links(query, base_url)
    lines = [f"🔎 {site_name} results for: `{query}`", "", f"**{site_name}**"]
    if site_results:
        for _, title, link in site_results:
            lines.append(f"- {title} - {suppress_discord_link_embed(link)}")
    else:
        lines.append("- No matching docs results found.")
    return trim_search_message("\n".join(lines))

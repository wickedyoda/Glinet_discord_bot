"""GL.iNet community forum monitor — polls Discourse API for new posts."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any

import discord
from discord import Colour, Embed

from app.http_client import get_session
from bot import is_managed_guild_id

logger = logging.getLogger(__name__)


def get_http_session():
    return get_session()


def get_cached_setting(key: str, default: str = "") -> str:
    """Read a setting from environment (simple fallback for config_cache)."""
    return os.getenv(key, default)

FORUM_BASE_URL = os.getenv("FORUM_BASE_URL", "https://forum.gl-inet.com")
FORUM_API_KEY = os.getenv("FORUM_API_KEY", "")
FORUM_USERNAME = os.getenv("FORUM_USERNAME", "")
FORUM_POLL_INTERVAL = int(os.getenv("FORUM_POLL_INTERVAL_SECONDS", "300"))
FORUM_MONITOR_CATEGORIES = json.loads(
    os.getenv("FORUM_MONITOR_CATEGORIES", "[]")
)
FORUM_STATE_FILE = os.path.join(
    os.getenv("DATA_DIR", "data"), "forum_state.json"
)

# Category ID → Discord channel ID mapping (populated at runtime)
FORUM_CATEGORY_CHANNELS: dict[str, int] = {}


@dataclass
class ForumPost:
    topic_id: int
    title: str
    author: str
    category_id: int
    category_name: str
    excerpt: str
    url: str
    created_at: str


@dataclass
class ForumMonitorState:
    seen_topic_ids: set[int] = field(default_factory=set)
    last_poll: float = 0.0


_state: ForumMonitorState | None = None


def _load_state() -> ForumMonitorState:
    """Load or initialize monitor state from disk."""
    global _state
    if _state is not None:
        return _state
    state = ForumMonitorState()
    if os.path.exists(FORUM_STATE_FILE):
        try:
            with open(FORUM_STATE_FILE, encoding="utf-8") as f:
                data = json.load(f)
            state.seen_topic_ids = set(data.get("seen_topic_ids", []))
            state.last_poll = data.get("last_poll", 0.0)
        except Exception:
            logger.exception("Failed to load forum monitor state")
    _state = state
    return state


def _save_state() -> None:
    """Persist monitor state to disk."""
    global _state
    if _state is None:
        return
    try:
        os.makedirs(os.path.dirname(FORUM_STATE_FILE), exist_ok=True)
        with open(FORUM_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "seen_topic_ids": list(_state.seen_topic_ids),
                    "last_poll": _state.last_poll,
                },
                f,
                indent=2,
            )
    except Exception:
        logger.exception("Failed to save forum monitor state")


def _forum_api_get(url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    """Make an authenticated GET request to the Discourse API."""
    session = get_http_session()
    headers: dict[str, str] = {}
    if FORUM_API_KEY:
        headers["Api-Key"] = FORUM_API_KEY
    if FORUM_USERNAME:
        headers["Api-Username"] = FORUM_USERNAME
    response = session.get(url, params=params, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()


def _resolve_category_channels() -> dict[str, int]:
    """Build mapping of category IDs to Discord channel IDs from settings."""
    mapping: dict[str, int] = {}
    for cat_id in FORUM_MONITOR_CATEGORIES:
        channel_id = get_cached_setting(f"FORUM_CAT_{cat_id}_CHANNEL_ID")
        if channel_id:
            try:
                mapping[str(cat_id)] = int(channel_id)
            except (ValueError, TypeError):
                logger.warning(
                    "Invalid channel ID for forum category %s: %s",
                    cat_id,
                    channel_id,
                )
    return mapping


async def fetch_latest_posts(category_id: int) -> list[ForumPost]:
    """Fetch latest topics from a specific forum category."""
    url = f"{FORUM_BASE_URL}/c/{category_id}/latest.json"
    try:
        data = _forum_api_get(url)
    except Exception:
        logger.exception("Failed to fetch forum posts for category %s", category_id)
        return []

    posts: list[ForumPost] = []
    for topic in data.get("topic_list", {}).get("topics", []):
        post = ForumPost(
            topic_id=topic["id"],
            title=topic["title"],
            author=topic.get("last_posted_by", topic.get("last_poster", "Unknown")),
            category_id=topic.get("category_id", category_id),
            category_name=topic.get("category_name", str(category_id)),
            excerpt=topic.get("excerpt", ""),
            url=f"{FORUM_BASE_URL}/t/{topic['id']}",
            created_at=topic.get("created_at", ""),
        )
        posts.append(post)
    return posts


async def search_forum(query: str, limit: int = 5) -> list[ForumPost]:
    """Search forum posts and return matching results."""
    url = f"{FORUM_BASE_URL}/search.json"
    try:
        data = _forum_api_get(url, params={"q": query, "limit": limit})
    except Exception:
        logger.exception("Failed to search forum for query: %s", query)
        return []

    posts: list[ForumPost] = []
    for topic in data.get("topics", [])[:limit]:
        post = ForumPost(
            topic_id=topic["id"],
            title=topic["title"],
            author=topic.get("last_posted_by", "Unknown"),
            category_id=topic.get("category_id", 0),
            category_name=topic.get("category_name", "Unknown"),
            excerpt=topic.get("excerpt", ""),
            url=f"{FORUM_BASE_URL}/t/{topic['id']}",
            created_at=topic.get("created_at", ""),
        )
        posts.append(post)
    return posts


async def post_forum_alert(post: ForumPost, guild: discord.Guild) -> bool:
    """Send a forum post notification to the appropriate Discord channel."""
    if not is_managed_guild_id(guild.id):
        return False

    # Find the configured channel for this category
    category_channel = FORUM_CATEGORY_CHANNELS.get(str(post.category_id))
    if not category_channel:
        logger.debug(
            "No channel configured for forum category %s", post.category_id
        )
        return False

    try:
        channel = guild.get_channel(category_channel) or await guild.fetch_channel(
            category_channel
        )
        if channel is None:
            return False

        embed = Embed(
            title=f"📌 {post.title}",
            url=post.url,
            description=post.excerpt[:200] if post.excerpt else "No excerpt",
            colour=Colour.blue(),
        )
        embed.set_author(name=post.author)
        embed.add_field(name="Category", value=post.category_name, inline=True)
        embed.add_field(name="Topic ID", value=str(post.topic_id), inline=True)
        embed.set_footer(text="GL.iNet Forum")
        from datetime import datetime as dt
        embed.timestamp = dt.now()

        await channel.send(embed=embed)
        return True
    except Exception:
        logger.exception(
            "Failed to send forum alert for topic %s to channel %s",
            post.topic_id,
            category_channel,
        )
        return False


async def check_new_posts() -> int:
    """Check all monitored categories for new posts. Returns count of new posts."""
    if not FORUM_API_KEY:
        logger.warning("FORUM_API_KEY not set — forum monitoring disabled")
        return 0

    state = _load_state()
    if not FORUM_MONITOR_CATEGORIES:
        logger.debug("No forum categories configured for monitoring")
        return 0

    # Refresh category→channel mapping
    FORUM_CATEGORY_CHANNELS.update(_resolve_category_channels())

    new_count = 0

    # We need a guild to send to — use the first managed guild from bot
    from bot import bot as _bot_instance

    guilds = [g for g in _bot_instance.guilds if is_managed_guild_id(g.id)]

    for category_id in FORUM_MONITOR_CATEGORIES:
        posts = await fetch_latest_posts(int(category_id))
        for post in posts:
            if post.topic_id in state.seen_topic_ids:
                continue
            state.seen_topic_ids.add(post.topic_id)
            for guild in guilds:
                await post_forum_alert(post, guild)
            new_count += 1

    state.last_poll = time.time()
    _save_state()
    return new_count


def setup_forum_scheduler(loop: asyncio.AbstractEventLoop) -> None:
    """Schedule periodic forum checks."""
    if not FORUM_API_KEY or not FORUM_MONITOR_CATEGORIES:
        logger.info("Forum monitoring not configured — skipping scheduler")
        return

    async def _poll():
        while True:
            try:
                count = await check_new_posts()
                if count > 0:
                    logger.info("Posted %d new forum alerts", count)
            except Exception:
                logger.exception("Forum monitor poll failed")
            await asyncio.sleep(FORUM_POLL_INTERVAL)

    loop.create_task(_poll())
    logger.info(
        "Forum monitor scheduler started — polling every %ds",
        FORUM_POLL_INTERVAL,
    )


# Discord slash command handler
async def forum_search(interaction, query: str, limit: int = 5):
    """Search the GL.iNet forum and return results."""
    posts = await search_forum(query, limit)
    if not posts:
        await interaction.response.send_message(
            "No results found.", ephemeral=True
        )
        return
    # Build embed response
    embeds = []
    for post in posts:
        embed = Embed(
            title=post.title,
            url=post.url,
            description=post.excerpt[:200] if post.excerpt else "No excerpt",
            colour=Colour.blue(),
        )
        embed.set_author(name=post.author)
        embed.add_field(
            name="Category", value=post.category_name, inline=True
        )
        embeds.append(embed)
    await interaction.response.send_message(embeds=embeds, ephemeral=False)


def get_forum_commands():
    """Return slash command definitions for forum integration."""
    return [forum_search]

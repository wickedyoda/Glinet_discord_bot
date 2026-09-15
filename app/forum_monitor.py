"""GL.iNet community forum monitor — polls Discourse API for new posts."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import discord
from discord import Colour, Embed

from app.http_client import get_session

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration (read once at import time — cached via config_cache where used)
# ---------------------------------------------------------------------------

FORUM_BASE_URL = os.getenv("FORUM_BASE_URL", "https://forum.gl-inet.com")
FORUM_API_KEY = os.getenv("FORUM_API_KEY", "")
FORUM_USERNAME = os.getenv("FORUM_API_USERNAME", "")
FORUM_POLL_INTERVAL = int(os.getenv("FORUM_POLL_INTERVAL_SECONDS", "300"))
FORUM_MONITOR_CATEGORIES = json.loads(
    os.getenv("FORUM_MONITOR_CATEGORIES", "[]")
)
FORUM_STATE_FILE = os.path.join(
    os.getenv("DATA_DIR", "data"), "forum_state.json"
)

# Category ID → Discord channel ID mapping (populated at runtime)
FORUM_CATEGORY_CHANNELS: dict[str, int] = {}

# Cap the size of seen_topic_ids to prevent unbounded memory growth
_MAX_SEEN_IDS = 10_000
# Only persist state to disk at most once per N seconds to reduce I/O
_SAVE_STATE_INTERVAL = 30

# Module-level auth headers — rebuilt only when the API key changes
_AUTH_HEADERS: dict[str, str] = {}


def _build_auth_headers() -> dict[str, str]:
    """Build HTTP headers with Discourse API credentials (cached)."""
    global _AUTH_HEADERS
    if not _AUTH_HEADERS and FORUM_API_KEY:
        _AUTH_HEADERS = {"Api-Key": FORUM_API_KEY}
        if FORUM_USERNAME:
            _AUTH_HEADERS["Api-Username"] = FORUM_USERNAME
    return _AUTH_HEADERS


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


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
    last_save: float = 0.0


_state: ForumMonitorState | None = None
_state_lock = asyncio.Lock()


# ---------------------------------------------------------------------------
# State persistence (async, non-blocking)
# ---------------------------------------------------------------------------


async def _load_state() -> ForumMonitorState:
    """Load or initialize monitor state from disk."""
    global _state
    if _state is not None:
        return _state

    state = ForumMonitorState()

    if os.path.exists(FORUM_STATE_FILE):
        try:
            data = await asyncio.to_thread(_read_json_sync, FORUM_STATE_FILE)
            state.seen_topic_ids = set(data.get("seen_topic_ids", []))
            state.last_poll = data.get("last_poll", 0.0)
            state.last_save = data.get("last_save", 0.0)
        except Exception:
            logger.exception("Failed to load forum monitor state")

    _state = state
    return state


def _read_json_sync(path: str) -> dict[str, Any]:
    """Synchronous JSON read — called via asyncio.to_thread."""
    with open(path, encoding="utf-8") as f:
        return json.load(f)


async def _save_state() -> None:
    """Persist monitor state to disk with rate limiting."""
    global _state
    async with _state_lock:
        if _state is None:
            return

        now = time.time()
        # Rate-limit saves to avoid excessive disk I/O
        if now - _state.last_save < _SAVE_STATE_INTERVAL:
            return

        try:
            # Cap the set to prevent unbounded memory/disk growth
            seen = list(_state.seen_topic_ids)
            if len(seen) > _MAX_SEEN_IDS:
                # Keep only the most recent _MAX_SEEN_IDS entries
                # (set ordering is insertion order in CPython 3.7+)
                _state.seen_topic_ids = set(seen[-_MAX_SEEN_IDS:])

            await asyncio.to_thread(_write_json_sync, FORUM_STATE_FILE, {
                "seen_topic_ids": list(_state.seen_topic_ids),
                "last_poll": _state.last_poll,
                "last_save": now,
            })
            _state.last_save = now
        except Exception:
            logger.exception("Failed to save forum monitor state")


def _write_json_sync(path: str, data: dict[str, Any]) -> None:
    """Synchronous JSON write — called via asyncio.to_thread."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------


def _forum_api_get(url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    """Make an authenticated GET request to the Discourse API (sync).

    Wrapped in threads via _async_forum_api_get to avoid blocking the
    event loop on network I/O.
    """
    session = get_session()
    headers = _build_auth_headers()
    response = session.get(url, params=params, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()


async def _async_forum_api_get(
    url: str, params: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Async wrapper around _forum_api_get using a thread."""
    return await asyncio.to_thread(_forum_api_get, url, params)


def _resolve_category_channels() -> dict[str, int]:
    """Build mapping of category IDs to Discord channel IDs from env."""
    mapping: dict[str, int] = {}
    for cat_id in FORUM_MONITOR_CATEGORIES:
        channel_id = os.getenv(f"FORUM_CAT_{cat_id}_CHANNEL_ID")
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


# ---------------------------------------------------------------------------
# Embed builder (shared between alert and search)
# ---------------------------------------------------------------------------


EXCERPT_TRUNCATE = 200


def _build_forum_embed(post: ForumPost) -> Embed:
    """Build a Discord embed from a ForumPost (shared by alerts and search)."""
    embed = Embed(
        title=f"📌 {post.title}",
        url=post.url,
        description=post.excerpt[:EXCERPT_TRUNCATE] if post.excerpt else "No excerpt",
        colour=Colour.blue(),
    )
    embed.set_author(name=post.author)
    embed.add_field(name="Category", value=post.category_name, inline=True)
    embed.add_field(name="Topic ID", value=str(post.topic_id), inline=True)
    embed.set_footer(text="GL.iNet Forum")
    embed.timestamp = datetime.now(timezone.utc)  # noqa: UP017
    return embed


# ---------------------------------------------------------------------------
# API operations
# ---------------------------------------------------------------------------


async def fetch_latest_posts(category_id: int) -> list[ForumPost]:
    """Fetch latest topics from a specific forum category."""
    url = f"{FORUM_BASE_URL}/c/{category_id}/latest.json"
    try:
        data = await _async_forum_api_get(url)
    except Exception:
        logger.exception("Failed to fetch forum posts for category %s", category_id)
        return []

    posts: list[ForumPost] = []
    for topic in data.get("topic_list", {}).get("topics", []):
        posts.append(_topic_to_post(topic, category_id))
    return posts


async def search_forum(query: str, limit: int = 5) -> list[ForumPost]:
    """Search forum posts and return matching results."""
    url = f"{FORUM_BASE_URL}/search.json"
    try:
        data = await _async_forum_api_get(url, params={"q": query, "limit": limit})
    except Exception:
        logger.exception("Failed to search forum for query: %s", query)
        return []

    return [_topic_to_post(topic, 0) for topic in data.get("topics", [])[:limit]]


def _topic_to_post(topic: dict[str, Any], fallback_category: int) -> ForumPost:
    """Convert a raw Discourse topic dict into a ForumPost dataclass."""
    return ForumPost(
        topic_id=topic["id"],
        title=topic["title"],
        author=topic.get("last_posted_by", topic.get("last_poster", "Unknown")),
        category_id=topic.get("category_id", fallback_category),
        category_name=topic.get("category_name", str(fallback_category)),
        excerpt=topic.get("excerpt", ""),
        url=f"{FORUM_BASE_URL}/t/{topic['id']}",
        created_at=topic.get("created_at", ""),
    )


# ---------------------------------------------------------------------------
# Alert posting
# ---------------------------------------------------------------------------


async def post_forum_alert(post: ForumPost, guild: discord.Guild) -> bool:
    """Send a forum post notification to the appropriate Discord channel."""
    category_channel = FORUM_CATEGORY_CHANNELS.get(str(post.category_id))
    if not category_channel:
        logger.debug(
            "No channel configured for forum category %s", post.category_id
        )
        return False

    try:
        # Try cache first, fall back to API fetch
        channel = guild.get_channel(category_channel)
        if channel is None:
            channel = await guild.fetch_channel(category_channel)
        if channel is None:
            return False

        embed = _build_forum_embed(post)
        await channel.send(embed=embed)
        return True
    except Exception:
        logger.exception(
            "Failed to send forum alert for topic %s to channel %s",
            post.topic_id,
            category_channel,
        )
        return False


# ---------------------------------------------------------------------------
# Main check loop
# ---------------------------------------------------------------------------


async def check_new_posts() -> int:
    """Check all monitored categories for new posts.

    Returns the count of new posts that triggered alerts.
    """
    if not FORUM_API_KEY:
        logger.warning("FORUM_API_KEY not set — forum monitoring disabled")
        return 0

    if not FORUM_MONITOR_CATEGORIES:
        logger.debug("No forum categories configured for monitoring")
        return 0

    state = await _load_state()
    FORUM_CATEGORY_CHANNELS.update(_resolve_category_channels())

    if not FORUM_CATEGORY_CHANNELS:
        logger.debug("No forum category channels mapped — skipping monitoring")
        return 0

    # Lazily import the bot to avoid circular imports
    from bot import bot as _bot_instance

    guilds = [g for g in _bot_instance.guilds]
    if not guilds:
        logger.warning("No guilds available — cannot post forum alerts")
        return 0

    new_count = 0
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
    await _save_state()
    return new_count


def setup_forum_scheduler(loop: asyncio.AbstractEventLoop) -> None:
    """Schedule periodic forum checks on the given event loop."""
    if not FORUM_API_KEY or not FORUM_MONITOR_CATEGORIES:
        logger.info("Forum monitoring not configured — skipping scheduler")
        return

    async def _poll() -> None:
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
async def forum_search(interaction: discord.Interaction, query: str, limit: int = 5):
    """Search the GL.iNet forum and return results."""
    posts = await search_forum(query, limit)
    if not posts:
        await interaction.response.send_message("No results found.", ephemeral=True)
        return

    embeds = [_build_forum_embed(post) for post in posts]
    await interaction.response.send_message(embeds=embeds, ephemeral=False)

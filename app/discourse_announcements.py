from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import List, Optional

import requests
from bs4 import BeautifulSoup

from app.discourse_api import (
    DiscourseApiError,
    DiscourseRateLimitError,
    build_discourse_headers,
    clean_discourse_text,
)

logger = logging.getLogger(__name__)

DEFAULT_FORUM_ANNOUNCEMENT_BASE_URL = "https://forum.gl-inet.com"
DEFAULT_FORUM_ANNOUNCEMENT_CATEGORY_PATH = "/c/glrouter/5"
DEFAULT_FORUM_ANNOUNCEMENT_REQUEST_TIMEOUT = 20
FORUM_ANNOUNCEMENT_REQUEST_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/137.0.0.0 Safari/537.36"
)


class ForumAnnouncementError(RuntimeError):
    pass


class ForumAnnouncementRateLimitError(ForumAnnouncementError):
    pass


def _normalize_http_url(value: str, fallback: str) -> str:
    candidate = (value or fallback or "").strip()
    if not candidate:
        raise ValueError("URL is required.")
    if "://" not in candidate:
        candidate = f"https://{candidate}"
    return candidate.rstrip("/")


def _coerce_int(value, default: int, minimum: int = 0) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed >= minimum else default


def fetch_announcement_category_html(
    base_url: str,
    category_path: str = DEFAULT_FORUM_ANNOUNCEMENT_CATEGORY_PATH,
    *,
    request_timeout: int = DEFAULT_FORUM_ANNOUNCEMENT_REQUEST_TIMEOUT,
    api_key: str = "",
    api_username: str = "",
) -> tuple[str, str]:
    resolved_base = _normalize_http_url(base_url, DEFAULT_FORUM_ANNOUNCEMENT_BASE_URL)
    resolved_category = (category_path or DEFAULT_FORUM_ANNOUNCEMENT_CATEGORY_PATH).strip()
    if not resolved_category.startswith("/"):
        resolved_category = f"/{resolved_category}"
    target_url = f"{resolved_base}{resolved_category}"
    response = requests.get(
        target_url,
        timeout=max(1, int(request_timeout or DEFAULT_FORUM_ANNOUNCEMENT_REQUEST_TIMEOUT)),
        headers={
            "User-Agent": FORUM_ANNOUNCEMENT_REQUEST_USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            **build_discourse_headers(
                user_agent=FORUM_ANNOUNCEMENT_REQUEST_USER_AGENT,
                api_key=api_key,
                api_username=api_username,
            ),
        },
    )
    if response.status_code == 429:
        raise ForumAnnouncementRateLimitError("Forum announcement category page is rate-limited right now.")
    if response.status_code != 200:
        raise ForumAnnouncementError(
            f"Forum announcement category page returned HTTP {response.status_code}."
        )
    return response.text, target_url


def parse_announcement_topics(page_html: str, source_url: str, max_results: int = 20) -> List[dict]:
    soup = BeautifulSoup(page_html, "html.parser")
    results: List[dict] = []
    seen_topic_ids = set()
    for row in soup.select("tr.topic-list-item"):
        title_tag = row.select_one(".main-link a.title, a.title")
        if title_tag is None:
            continue
        href = (title_tag.get("href") or "").strip()
        slug, _, topic_id_str = href.rpartition("/")
        try:
            topic_id = int((topic_id_str or "").split("-", 1)[0])
        except (TypeError, ValueError):
            continue
        if topic_id in seen_topic_ids:
            continue
        seen_topic_ids.add(topic_id)
        title = clean_discourse_text(title_tag.get_text(" ", strip=True)).strip() or f"Topic {topic_id}"
        excerpt_tag = row.select_one(".topic-excerpt, .excerpt, .snippet")
        excerpt = clean_discourse_text(excerpt_tag.get_text(" ", strip=True)).strip() if excerpt_tag else ""
        created_at = ""
        date_tag = row.select_one("td.created .post-date, td.created time, time")
        if date_tag is not None:
            created_at = str(date_tag.get("datetime") or date_tag.get_text(" ", strip=True) or "").strip()
        last_posted_at = ""
        last_post_date_tag = row.select_one("td.activity .post-date, td.activity time, .last-posted-at time")
        if last_post_date_tag is not None:
            last_posted_at = str(
                last_post_date_tag.get("datetime") or last_post_date_tag.get_text(" ", strip=True) or ""
            ).strip()
        topic_url = f"{source_url.rstrip('/')}/t/{slug.strip('/')}/{topic_id}" if slug else f"{source_url.rstrip('/')}/t/{topic_id}"
        if topic_url == f"{source_url.rstrip('/')}/t//{topic_id}":
            topic_url = f"{source_url.rstrip('/')}/t/{topic_id}"
        results.append(
            {
                "id": topic_id,
                "title": title,
                "url": topic_url,
                "excerpt": excerpt,
                "created_at": created_at,
                "last_posted_at": last_posted_at,
            }
        )
        if len(results) >= max_results:
            break
    return results


def resolve_announcement_web_target(guild_id: int, *, get_effective_guild_setting):
    channel_id = int(get_effective_guild_setting(guild_id, "forum_announcements_channel_id", 0) or 0)
    enabled = int(get_effective_guild_setting(guild_id, "forum_announcements_enabled", 0) or 0) > 0
    return enabled, channel_id


def mark_announcement_topic_posted(conn, guild_id: int, topic_id: int, posted_at: Optional[str] = None) -> None:
    safe_guild_id = int(guild_id or 0)
    safe_topic_id = int(topic_id or 0)
    if safe_guild_id <= 0 or safe_topic_id <= 0:
        return
    now_iso = (posted_at or datetime.now(UTC).isoformat()).strip()
    conn.execute(
        """
        INSERT INTO discourse_announcement_seen (topic_id, guild_id, posted_at)
        VALUES (?, ?, ?)
        ON CONFLICT(topic_id, guild_id) DO UPDATE SET posted_at=excluded.posted_at
        """,
        (safe_topic_id, safe_guild_id, now_iso),
    )


def load_announcement_seen_topic_ids(conn, guild_id: int) -> set[int]:
    safe_guild_id = int(guild_id or 0)
    if safe_guild_id <= 0:
        return set()
    rows = conn.execute(
        "SELECT topic_id FROM discourse_announcement_seen WHERE guild_id = ?", (safe_guild_id,)
    ).fetchall()
    return {int(row["topic_id"]) for row in rows if row["topic_id"]}

"""Persist Discourse forum activity so statistics survive restarts.

Four tables, deliberately raw rather than pre-aggregated: the posts we have seen, the
topic -> category map, and a snapshot of the staff group. Every statistic -- daily,
hourly, weekly, threads handled, first-response time -- is then a SQL query over
that, so a new metric needs no schema change and no re-scan.

The HTTP layer is injected as `fetch_json(path)` so this module has no dependency on
`bot.py` and can be tested without the network.
"""

from __future__ import annotations

import json as _json
from collections.abc import Callable
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta

# How far back the first run reaches. Later runs are incremental: INSERT OR IGNORE on
# a primary key of post_id makes re-scanning the same window idempotent.
DEFAULT_BACKFILL_DAYS = 90
MAX_PAGES = 800
# Discourse throttles around 60 requests/minute per IP. Pacing faster than ~1.05s
# earns a 429 partway through a long walk, which is worse than a slow one.
DEFAULT_PACE_SECONDS = 1.05


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def parse_discourse_ts(value: str) -> datetime:
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _write_lock(lock):
    """The caller's DB lock, or a no-op.

    Ingest must never hold `db_lock` across an HTTP request or a pacing sleep: the
    lock is shared with message handling, so a multi-minute backfill would stall the
    bot. Each write takes it for the duration of the statement only.
    """
    return lock if lock is not None else nullcontext()


def _fetch_group_members(
    *, group: str, fetch_json: Callable[[str], dict], page_size: int = 100, max_pages: int = 50
) -> list[dict]:
    """Walk the group member list.

    `/groups/<group>/members.json` defaults to 100 per page; a single request would
    silently truncate a larger group and the next roster refresh would then treat the
    missing staff as departed.
    """
    members: list[dict] = []
    offset = 0
    for _ in range(max_pages):
        payload = fetch_json(
            f"/groups/{group}/members.json?limit={page_size}&offset={offset}"
        ) or {}
        page = payload.get("members") or []
        members.extend(page)
        offset += len(page)
        total = (payload.get("meta") or {}).get("total")
        if not page or len(page) < page_size:
            break
        if isinstance(total, int) and offset >= total:
            break
    return members


# --------------------------------------------------------------------------- #
#  Ingest
# --------------------------------------------------------------------------- #


def refresh_staff_roster(
    conn, *, group: str, fetch_json: Callable[[str], dict], lock=None
) -> dict:
    """Snapshot current group membership.

    Membership is *current*, not historical: Discourse exposes no record of who was
    in the group when, so a departed member is removed from the roster and their old
    posts stop counting. Keeping them would contradict the page's own caveat and
    credit someone who is no longer staff.

    The roster is only pruned from a complete, non-empty fetch: a truncated response
    must not be read as "everyone left".
    """
    members = _fetch_group_members(group=group, fetch_json=fetch_json)
    now = _utc_now_iso()
    usernames = {
        str(member.get("username") or "").strip().lower()
        for member in members
        if str(member.get("username") or "").strip()
    }
    added = removed = 0
    with _write_lock(lock):
        if usernames:
            existing = {
                row["username"]
                for row in conn.execute("SELECT username FROM discourse_staff").fetchall()
            }
            departed = sorted(existing - usernames)
            for username in departed:
                conn.execute("DELETE FROM discourse_staff WHERE username = ?", (username,))
            removed = len(departed)
        for username in sorted(usernames):
            cursor = conn.execute(
                """
                INSERT INTO discourse_staff (username, first_seen_at, last_seen_at)
                VALUES (?, ?, ?)
                ON CONFLICT(username) DO UPDATE SET last_seen_at = excluded.last_seen_at
                """,
                (username, now, now),
            )
            if cursor.rowcount == 1:
                added += 1
        conn.commit()
    return {"members": len(usernames), "added": added, "removed": removed}


def ingest_topics(
    conn,
    *,
    categories: tuple[str, ...],
    fetch_json: Callable[[str], dict],
    backfill_days: int = DEFAULT_BACKFILL_DAYS,
    now: datetime | None = None,
    pace: Callable[[float], None] | None = None,
    max_pages: int = MAX_PAGES,
    lock=None,
) -> dict:
    """Build the topic -> category map from each category's latest feed.

    `/posts.json` carries no `category_id`, so this pass is what makes category
    scoping possible at all. Writes take the lock per page; the fetch and the pacing
    sleep stay outside it.
    """
    now = now or datetime.now(UTC)
    cutoff = now - timedelta(days=backfill_days)
    total = pages = 0

    for slug in categories:
        page = 0
        while page < max_pages:
            payload = fetch_json(f"/c/{slug}/l/latest.json?page={page}")
            topics = payload.get("topic_list", {}).get("topics") or []
            if not topics:
                break
            pages += 1
            fresh = 0
            rows = []
            for topic in topics:
                try:
                    created = parse_discourse_ts(topic["created_at"])
                except (KeyError, ValueError):
                    continue
                if created < cutoff:
                    continue
                fresh += 1
                rows.append((int(topic["id"]), slug, topic["created_at"]))
            if rows:
                with _write_lock(lock):
                    for row in rows:
                        conn.execute(
                            """
                            INSERT INTO discourse_topics (topic_id, category_slug, created_at)
                            VALUES (?, ?, ?)
                            ON CONFLICT(topic_id) DO UPDATE SET category_slug = excluded.category_slug
                            """,
                            row,
                        )
                    conn.commit()
                total += len(rows)
            if fresh == 0 and parse_discourse_ts(topics[-1]["created_at"]) < cutoff:
                break
            page += 1
            if pace is not None:
                pace(DEFAULT_PACE_SECONDS)

    return {"pages": pages, "topics": total}


def ingest_posts(
    conn,
    *,
    fetch_json: Callable[[str], dict],
    backfill_days: int = DEFAULT_BACKFILL_DAYS,
    now: datetime | None = None,
    pace: Callable[[float], None] | None = None,
    max_pages: int = MAX_PAGES,
    lock=None,
) -> dict:
    """Walk /posts.json backwards and store every post inside the window.

    Each page is fetched and paced outside the lock, then written inside it. A
    backfill is minutes of network time; holding `db_lock` across it would stall every
    other database user, including message handling.
    """
    now = now or datetime.now(UTC)
    cutoff = now - timedelta(days=backfill_days)
    ingested_at = _utc_now_iso()

    seen = stored = pages = 0
    before = None

    while pages < max_pages:
        batch = fetch_json("/posts.json" + (f"?before={before}" if before else "")).get(
            "latest_posts"
        ) or []
        if not batch:
            break
        pages += 1
        rows = []
        for post in batch:
            try:
                created = parse_discourse_ts(post["created_at"])
            except (KeyError, ValueError):
                continue
            if created < cutoff:
                continue
            seen += 1
            rows.append(
                (
                    int(post["id"]),
                    int(post["topic_id"]),
                    str(post.get("username") or "").lower(),
                    int(post.get("post_number") or 0),
                    post["created_at"],
                    ingested_at,
                )
            )
        if rows:
            with _write_lock(lock):
                for row in rows:
                    cursor = conn.execute(
                        """
                        INSERT OR IGNORE INTO discourse_posts
                            (post_id, topic_id, username, post_number, created_at, ingested_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        row,
                    )
                    stored += cursor.rowcount
                conn.commit()

        if parse_discourse_ts(batch[-1]["created_at"]) < cutoff:
            break
        before = batch[-1]["id"]
        if pace is not None:
            pace(DEFAULT_PACE_SECONDS)

    return {"pages": pages, "posts_in_window": seen, "new_rows": stored}


# --------------------------------------------------------------------------- #
#  Queries
#
#  No SQL is built by interpolation. The join lives in the discourse_staff_posts
#  view, and category scoping is a static predicate: pass NULL to query everything,
#  or a JSON array that json_each() expands. That keeps the statements literal, so
#  there is no formatted-SQL surface and nothing to suppress.
# --------------------------------------------------------------------------- #


def _scope_params(categories: tuple[str, ...] | None) -> tuple[object, object]:
    return (None, None) if not categories else (_json.dumps(list(categories)),) * 2


def staff_activity(
    conn,
    *,
    days: int = DEFAULT_BACKFILL_DAYS,
    categories: tuple[str, ...] | None = None,
    now: datetime | None = None,
) -> list[dict]:
    """Posts, distinct topics, and threads handled first, per staff member."""
    now = now or datetime.now(UTC)
    scoped, scoped_again = _scope_params(categories)
    rows = conn.execute(
        """
        WITH staff_posts AS (
            SELECT topic_id, username, post_number, created_at
            FROM discourse_staff_posts
            WHERE created_at >= ?
              AND (? IS NULL OR category_slug IN (SELECT value FROM json_each(?)))
        ),
        handled AS (
            SELECT topic_id, username FROM (
                SELECT topic_id, username,
                       ROW_NUMBER() OVER (PARTITION BY topic_id ORDER BY created_at) AS rn
                FROM staff_posts WHERE post_number > 1
            ) WHERE rn = 1
        )
        SELECT sp.username,
               COUNT(*) AS posts,
               COUNT(DISTINCT sp.topic_id) AS topics,
               (SELECT COUNT(*) FROM handled h WHERE h.username = sp.username) AS handled
        FROM staff_posts sp
        GROUP BY sp.username
        ORDER BY posts DESC
        """,
        ((now - timedelta(days=days)).isoformat(), scoped, scoped_again),
    ).fetchall()
    return [dict(row) for row in rows]


def daily_counts(conn, *, days: int = DEFAULT_BACKFILL_DAYS,
                 categories: tuple[str, ...] | None = None,
                 now: datetime | None = None) -> list[dict]:
    return _counts(conn, "day", days, categories, now)


def hourly_counts(conn, *, days: int = DEFAULT_BACKFILL_DAYS,
                  categories: tuple[str, ...] | None = None,
                  now: datetime | None = None) -> list[dict]:
    """Posts per UTC hour-of-day; created_at is ISO 8601 with a Z suffix."""
    return _counts(conn, "hour", days, categories, now)


def weekly_counts(conn, *, days: int = DEFAULT_BACKFILL_DAYS,
                  categories: tuple[str, ...] | None = None,
                  now: datetime | None = None) -> list[dict]:
    return _counts(conn, "week", days, categories, now)


# The bucket expression is a fixed, module-level choice -- never caller text.
_BUCKET_SQL = {
    "day": """
        SELECT substr(created_at, 1, 10) AS bucket, COUNT(*) AS posts
        FROM discourse_staff_posts
        WHERE created_at >= ?
          AND (? IS NULL OR category_slug IN (SELECT value FROM json_each(?)))
        GROUP BY bucket ORDER BY bucket
    """,
    "hour": """
        SELECT CAST(substr(created_at, 12, 2) AS INTEGER) AS bucket, COUNT(*) AS posts
        FROM discourse_staff_posts
        WHERE created_at >= ?
          AND (? IS NULL OR category_slug IN (SELECT value FROM json_each(?)))
        GROUP BY bucket ORDER BY bucket
    """,
    "week": """
        SELECT strftime('%G-W%V', created_at) AS bucket, COUNT(*) AS posts
        FROM discourse_staff_posts
        WHERE created_at >= ?
          AND (? IS NULL OR category_slug IN (SELECT value FROM json_each(?)))
        GROUP BY bucket ORDER BY bucket
    """,
}


def _counts(conn, kind: str, days: int, categories, now) -> list[dict]:
    now = now or datetime.now(UTC)
    scoped, scoped_again = _scope_params(categories)
    rows = conn.execute(
        _BUCKET_SQL[kind],
        ((now - timedelta(days=days)).isoformat(), scoped, scoped_again),
    ).fetchall()
    return [dict(row) for row in rows]


def first_response_seconds(
    conn,
    *,
    days: int = DEFAULT_BACKFILL_DAYS,
    categories: tuple[str, ...] | None = None,
    now: datetime | None = None,
) -> list[dict]:
    """Per staff member: threads answered and mean seconds to first reply."""
    now = now or datetime.now(UTC)
    scoped, scoped_again = _scope_params(categories)
    rows = conn.execute(
        """
        WITH opened AS (
            SELECT topic_id, created_at FROM discourse_posts WHERE post_number = 1
        ),
        first_staff AS (
            SELECT topic_id, username, created_at FROM (
                SELECT p.topic_id, p.username, p.created_at,
                       ROW_NUMBER() OVER (PARTITION BY p.topic_id ORDER BY p.created_at) AS rn
                FROM discourse_staff_posts p
                WHERE p.post_number > 1
                  AND (? IS NULL OR p.category_slug IN (SELECT value FROM json_each(?)))
            ) WHERE rn = 1
        )
        SELECT f.username,
               COUNT(*) AS handled,
               CAST(AVG((julianday(f.created_at) - julianday(o.created_at)) * 86400) AS INTEGER) AS mean_seconds
        FROM first_staff f
        JOIN opened o ON o.topic_id = f.topic_id
        WHERE julianday(f.created_at) >= julianday(o.created_at)
          AND f.created_at >= ?
        GROUP BY f.username
        ORDER BY handled DESC
        """,
        (scoped, scoped_again, (now - timedelta(days=days)).isoformat()),
    ).fetchall()
    return [dict(row) for row in rows]


def response_gaps(
    conn,
    *,
    days: int = DEFAULT_BACKFILL_DAYS,
    categories: tuple[str, ...] | None = None,
    now: datetime | None = None,
) -> list[int]:
    """Every first-response gap in seconds, for median/p90 in Python.

    SQLite has no percentile function, so the distribution is collected here rather
    than approximated in SQL.
    """
    now = now or datetime.now(UTC)
    scoped, scoped_again = _scope_params(categories)
    rows = conn.execute(
        """
        WITH opened AS (
            SELECT topic_id, created_at FROM discourse_posts WHERE post_number = 1
        ),
        first_staff AS (
            SELECT topic_id, created_at FROM (
                SELECT p.topic_id, p.created_at,
                       ROW_NUMBER() OVER (PARTITION BY p.topic_id ORDER BY p.created_at) AS rn
                FROM discourse_staff_posts p
                WHERE p.post_number > 1
                  AND (? IS NULL OR p.category_slug IN (SELECT value FROM json_each(?)))
            ) WHERE rn = 1
        )
        SELECT CAST((julianday(f.created_at) - julianday(o.created_at)) * 86400 AS INTEGER) AS secs
        FROM first_staff f
        JOIN opened o ON o.topic_id = f.topic_id
        WHERE julianday(f.created_at) >= julianday(o.created_at)
          AND f.created_at >= ?
        """,
        (scoped, scoped_again, (now - timedelta(days=days)).isoformat()),
    ).fetchall()
    return [int(row["secs"]) for row in rows]

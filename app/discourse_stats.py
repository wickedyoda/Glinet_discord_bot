"""Persist Discourse forum activity so statistics survive restarts.

Four tables, deliberately raw rather than pre-aggregated: the posts we have seen, the
topic -> category map, and a snapshot of the staff group. Every statistic -- daily,
hourly, weekly, threads handled, first-response time -- is then a SQL query over
that, so a new metric needs no schema change and no re-scan.

The HTTP layer is injected as `fetch_json(path)` so this module has no dependency on
`bot.py` and can be tested without the network.
"""

from __future__ import annotations

from collections.abc import Callable
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


# --------------------------------------------------------------------------- #
#  Ingest
# --------------------------------------------------------------------------- #


def refresh_staff_roster(conn, *, group: str, fetch_json: Callable[[str], dict]) -> dict:
    """Snapshot current group membership.

    Membership is *current*, not historical: Discourse exposes no record of who was
    in the group when, so staff who left are invisible to later queries.
    """
    payload = fetch_json(f"/groups/{group}/members.json?limit=100")
    members = payload.get("members") or []
    now = _utc_now_iso()
    added = 0
    for member in members:
        username = str(member.get("username") or "").strip().lower()
        if not username:
            continue
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
    return {"members": len(members), "added": added}


def ingest_topics(
    conn,
    *,
    categories: tuple[str, ...],
    fetch_json: Callable[[str], dict],
    backfill_days: int = DEFAULT_BACKFILL_DAYS,
    now: datetime | None = None,
    pace: Callable[[float], None] | None = None,
    max_pages: int = MAX_PAGES,
) -> dict:
    """Build the topic -> category map from each category's latest feed.

    `/posts.json` carries no `category_id`, so this pass is what makes category
    scoping possible at all.
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
            for topic in topics:
                try:
                    created = parse_discourse_ts(topic["created_at"])
                except (KeyError, ValueError):
                    continue
                if created < cutoff:
                    continue
                fresh += 1
                conn.execute(
                    """
                    INSERT INTO discourse_topics (topic_id, category_slug, created_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(topic_id) DO UPDATE SET category_slug = excluded.category_slug
                    """,
                    (int(topic["id"]), slug, topic["created_at"]),
                )
                total += 1
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
) -> dict:
    """Walk /posts.json backwards and store every post inside the window."""
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
        for post in batch:
            try:
                created = parse_discourse_ts(post["created_at"])
            except (KeyError, ValueError):
                continue
            if created < cutoff:
                continue
            seen += 1
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO discourse_posts
                    (post_id, topic_id, username, post_number, created_at, ingested_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    int(post["id"]),
                    int(post["topic_id"]),
                    str(post.get("username") or "").lower(),
                    int(post.get("post_number") or 0),
                    post["created_at"],
                    ingested_at,
                ),
            )
            stored += cursor.rowcount

        if parse_discourse_ts(batch[-1]["created_at"]) < cutoff:
            break
        before = batch[-1]["id"]
        if pace is not None:
            pace(DEFAULT_PACE_SECONDS)

    return {"pages": pages, "posts_in_window": seen, "new_rows": stored}


# --------------------------------------------------------------------------- #
#  Queries -- all optionally scoped to a set of category slugs
# --------------------------------------------------------------------------- #


def _scope(categories: tuple[str, ...] | None, params: list) -> str:
    if not categories:
        return ""
    params.extend(categories)
    return f" AND t.category_slug IN ({','.join('?' for _ in categories)})"


def _staff_posts_cte(categories: tuple[str, ...] | None) -> tuple[str, list]:
    params: list = []
    scope = _scope(categories, params)
    cte = f"""
        SELECT p.topic_id, p.username, p.post_number, p.created_at
        FROM discourse_posts p
        JOIN discourse_staff s ON s.username = p.username
        JOIN discourse_topics t ON t.topic_id = p.topic_id
        WHERE p.created_at >= ?{scope}
    """
    return cte, params


def staff_activity(
    conn,
    *,
    days: int = DEFAULT_BACKFILL_DAYS,
    categories: tuple[str, ...] | None = None,
    now: datetime | None = None,
) -> list[dict]:
    """Posts, distinct topics, and threads handled first, per staff member."""
    now = now or datetime.now(UTC)
    cte, params = _staff_posts_cte(categories)
    params.insert(0, (now - timedelta(days=days)).isoformat())
    rows = conn.execute(
        f"""
        WITH staff_posts AS ({cte}),
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
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _bucket_counts(
    conn,
    *,
    expr: str,
    days: int = DEFAULT_BACKFILL_DAYS,
    categories: tuple[str, ...] | None = None,
    now: datetime | None = None,
) -> list[dict]:
    now = now or datetime.now(UTC)
    cte, params = _staff_posts_cte(categories)
    params.insert(0, (now - timedelta(days=days)).isoformat())
    rows = conn.execute(
        f"""
        WITH staff_posts AS ({cte})
        SELECT {expr} AS bucket, COUNT(*) AS posts
        FROM staff_posts GROUP BY bucket ORDER BY bucket
        """,
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def daily_counts(conn, **kwargs) -> list[dict]:
    return _bucket_counts(conn, expr="substr(created_at, 1, 10)", **kwargs)


def hourly_counts(conn, **kwargs) -> list[dict]:
    """Posts per UTC hour-of-day; created_at is ISO 8601 with a Z suffix."""
    return _bucket_counts(conn, expr="CAST(substr(created_at, 12, 2) AS INTEGER)", **kwargs)


def weekly_counts(conn, **kwargs) -> list[dict]:
    return _bucket_counts(conn, expr="strftime('%Y-W%W', created_at)", **kwargs)


def _first_response_cte(categories: tuple[str, ...] | None) -> tuple[str, list]:
    params: list = []
    scope = _scope(categories, params)
    cte = f"""
        WITH opened AS (
            SELECT topic_id, created_at FROM discourse_posts WHERE post_number = 1
        ),
        first_staff AS (
            SELECT topic_id, username, created_at FROM (
                SELECT p.topic_id, p.username, p.created_at,
                       ROW_NUMBER() OVER (PARTITION BY p.topic_id ORDER BY p.created_at) AS rn
                FROM discourse_posts p
                JOIN discourse_staff s ON s.username = p.username
                JOIN discourse_topics t ON t.topic_id = p.topic_id
                WHERE p.post_number > 1{scope}
            ) WHERE rn = 1
        )
    """
    return cte, params


def first_response_seconds(
    conn,
    *,
    days: int = DEFAULT_BACKFILL_DAYS,
    categories: tuple[str, ...] | None = None,
    now: datetime | None = None,
) -> list[dict]:
    """Per staff member: threads answered and mean seconds to first reply."""
    now = now or datetime.now(UTC)
    cte, params = _first_response_cte(categories)
    params.append((now - timedelta(days=days)).isoformat())
    rows = conn.execute(
        f"""
        {cte}
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
        params,
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
    cte, params = _first_response_cte(categories)
    params.append((now - timedelta(days=days)).isoformat())
    rows = conn.execute(
        f"""
        {cte}
        SELECT CAST((julianday(f.created_at) - julianday(o.created_at)) * 86400 AS INTEGER) AS secs
        FROM first_staff f
        JOIN opened o ON o.topic_id = f.topic_id
        WHERE julianday(f.created_at) >= julianday(o.created_at)
          AND f.created_at >= ?
        """,
        params,
    ).fetchall()
    return [int(row["secs"]) for row in rows if row["secs"] is not None]

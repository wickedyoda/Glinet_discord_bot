"""Query the stored forum archive the way the member-activity page queries Discord.

The stored tables are deliberately thin -- who posted, in which topic, when -- so this
module answers the questions that shape allows: who is posting, how much, in which
category and period, and how quickly people answer. It holds plain SQL and no Flask, so
a test can point it at an in-memory database.

Dates are stored as ISO-8601 strings with a ``Z`` suffix, so range filters compare
lexicographically and no parsing is needed in SQL. Filters are normalised to that shape
here rather than trusting whatever the query string carried.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, date, datetime, timedelta

DEFAULT_RANGE_DAYS = 30
MAX_RANGE_DAYS = 3650
TOPIC_PAGE_SIZE = 50
USER_LIMIT = 25


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _parse_day(raw, default: date) -> date:
    text = str(raw or "").strip()
    if not text:
        return default
    try:
        return datetime.strptime(text[:10], "%Y-%m-%d").date()
    except ValueError:
        return default


def _iso_start(day: date) -> str:
    return f"{day.isoformat()}T00:00:00.000Z"


def _iso_end(day: date) -> str:
    return f"{day.isoformat()}T23:59:59.999Z"


def normalize_filters(raw: dict | None) -> dict:
    """Turn query-string values into a bounded, predictable filter set."""
    raw = raw or {}
    today = _utc_now().date()
    default_from = today - timedelta(days=DEFAULT_RANGE_DAYS)

    date_from = _parse_day(raw.get("from"), default_from)
    date_to = _parse_day(raw.get("to"), today)
    if date_from > date_to:
        date_from, date_to = date_to, date_from
    if (date_to - date_from).days > MAX_RANGE_DAYS:
        date_from = date_to - timedelta(days=MAX_RANGE_DAYS)

    try:
        page = max(1, int(raw.get("page") or 1))
    except (TypeError, ValueError):
        page = 1

    staff_only = str(raw.get("staff_only") or "").strip().lower() in {"1", "true", "yes", "on"}

    return {
        "username": str(raw.get("username") or "").strip()[:80],
        "category": str(raw.get("category") or "").strip()[:40],
        "date_from": date_from,
        "date_to": date_to,
        "staff_only": staff_only,
        "page": page,
        "from_iso": _iso_start(date_from),
        "to_iso": _iso_end(date_to),
    }


def _percentile(values: list[int], fraction: float) -> int:
    """Nearest-rank percentile, with the mean of the middle pair at the median."""
    if not values:
        return 0
    ordered = sorted(values)
    if fraction == 0.5 and len(ordered) % 2 == 0:
        middle = len(ordered) // 2
        return int((ordered[middle - 1] + ordered[middle]) / 2)
    index = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * fraction))))
    return int(ordered[index])


def _where(filters: dict) -> tuple[str, list]:
    """The shared WHERE clause: date range, optional user, optional category."""
    clauses = ["p.created_at >= ?", "p.created_at <= ?"]
    params: list = [filters["from_iso"], filters["to_iso"]]
    if filters.get("category"):
        clauses.append("t.category_slug = ?")
        params.append(filters["category"])
    if filters.get("username"):
        clauses.append("p.username = ?")
        params.append(filters["username"])
    if filters.get("staff_only"):
        clauses.append("s.username IS NOT NULL")
    return " AND ".join(clauses), params


_FROM = (
    "FROM discourse_posts p "
    "LEFT JOIN discourse_topics t ON t.topic_id = p.topic_id "
    "LEFT JOIN discourse_staff s ON s.username = p.username "
)


def query_options(db) -> dict:
    """The values the filter pickers offer."""
    categories = [
        str(row[0])
        for row in db.execute(
            "SELECT DISTINCT category_slug FROM discourse_topics "
            "WHERE category_slug IS NOT NULL AND category_slug <> '' ORDER BY category_slug"
        )
    ]
    staff = [
        str(row[0])
        for row in db.execute("SELECT username FROM discourse_staff ORDER BY username COLLATE NOCASE")
    ]
    bounds = db.execute("SELECT MIN(created_at), MAX(created_at) FROM discourse_posts").fetchone()
    return {
        "categories": categories,
        "staff": staff,
        "earliest": (bounds[0] or "")[:10] if bounds else "",
        "latest": (bounds[1] or "")[:10] if bounds else "",
    }


def _summary(db, filters: dict) -> dict:
    where, params = _where(filters)
    row = db.execute(
        "SELECT COUNT(*) AS posts, COUNT(DISTINCT p.topic_id) AS topics, "
        "COUNT(DISTINCT p.username) AS people " + _FROM + "WHERE " + where,
        params,
    ).fetchone()
    return {
        "posts": int(row["posts"] or 0),
        "topics": int(row["topics"] or 0),
        "people": int(row["people"] or 0),
    }


def _per_user(db, filters: dict) -> list[dict]:
    where, params = _where(filters)
    rows = db.execute(
        "SELECT p.username AS username, COUNT(*) AS posts, "
        "COUNT(DISTINCT p.topic_id) AS topics, MAX(p.created_at) AS last_post, "
        "MAX(CASE WHEN s.username IS NULL THEN 0 ELSE 1 END) AS is_staff "
        + _FROM
        + "WHERE "
        + where
        + " GROUP BY p.username ORDER BY posts DESC, p.username COLLATE NOCASE LIMIT ?",
        params + [USER_LIMIT],
    ).fetchall()
    return [
        {
            "username": str(row["username"] or ""),
            "posts": int(row["posts"] or 0),
            "topics": int(row["topics"] or 0),
            "last_post": str(row["last_post"] or "")[:19].replace("T", " "),
            "is_staff": bool(row["is_staff"]),
        }
        for row in rows
    ]


def _topics(db, filters: dict, page: int) -> tuple[list[dict], int]:
    where, params = _where(filters)
    total = int(
        db.execute(
            "SELECT COUNT(DISTINCT p.topic_id) " + _FROM + "WHERE " + where, params
        ).fetchone()[0]
        or 0
    )
    offset = (page - 1) * TOPIC_PAGE_SIZE
    rows = db.execute(
        "SELECT p.topic_id AS topic_id, t.category_slug AS category_slug, "
        "t.created_at AS topic_created, COUNT(*) AS posts, "
        "SUM(CASE WHEN s.username IS NULL THEN 0 ELSE 1 END) AS staff_posts, "
        "MIN(p.created_at) AS first_post, MAX(p.created_at) AS last_post "
        + _FROM
        + "WHERE "
        + where
        + " GROUP BY p.topic_id ORDER BY last_post DESC LIMIT ? OFFSET ?",
        params + [TOPIC_PAGE_SIZE, offset],
    ).fetchall()
    return (
        [
            {
                "topic_id": int(row["topic_id"] or 0),
                "category_slug": str(row["category_slug"] or ""),
                "topic_created": str(row["topic_created"] or "")[:19].replace("T", " "),
                "posts": int(row["posts"] or 0),
                "staff_posts": int(row["staff_posts"] or 0),
                "first_post": str(row["first_post"] or "")[:19].replace("T", " "),
                "last_post": str(row["last_post"] or "")[:19].replace("T", " "),
            }
            for row in rows
        ],
        total,
    )


def _user_card(db, filters: dict) -> dict | None:
    """For one chosen user: threads started, replies, and how fast they answered."""
    username = filters.get("username")
    if not username:
        return None
    where, params = _where(filters)
    row = db.execute(
        "SELECT COUNT(*) AS posts, "
        "SUM(CASE WHEN p.post_number = 1 THEN 1 ELSE 0 END) AS threads_started, "
        "SUM(CASE WHEN p.post_number > 1 THEN 1 ELSE 0 END) AS replies "
        + _FROM
        + "WHERE "
        + where,
        params,
    ).fetchone()

    # Which topics this user answered inside the range. The delay is measured from the
    # user's earliest reply anywhere in the archive, not the earliest one inside the
    # range: answering an older thread again today would otherwise be scored as a
    # many-day response. The comment sits outside the string chain deliberately -- a
    # statement split by a comment reads as ad-hoc SQL to static analysis.
    reply_rows = db.execute(
        "SELECT p.topic_id AS topic_id, t.created_at AS topic_created, "  # nosec B608 - constant SQL, bound params
        "(SELECT MIN(p2.created_at) FROM discourse_posts p2 "
        " WHERE p2.topic_id = p.topic_id AND p2.username = p.username "
        " AND p2.post_number > 1) AS first_reply "
        + _FROM
        + "WHERE "
        + where
        + " AND p.post_number > 1 GROUP BY p.topic_id",
        params,
    ).fetchall()

    gaps: list[int] = []
    for reply in reply_rows:
        topic_created = str(reply["topic_created"] or "")
        first_reply = str(reply["first_reply"] or "")
        if not topic_created or not first_reply:
            # No topic metadata, or no stored reply to measure: the topic still counts
            # as answered, it just contributes no delay.
            continue
        try:
            started = datetime.fromisoformat(topic_created.replace("Z", "+00:00"))
            answered = datetime.fromisoformat(first_reply.replace("Z", "+00:00"))
        except ValueError:
            continue
        gap = int((answered - started).total_seconds())
        if gap >= 0:
            gaps.append(gap)

    return {
        "username": username,
        "posts": int(row["posts"] or 0),
        "threads_started": int(row["threads_started"] or 0),
        "replies": int(row["replies"] or 0),
        # Every topic with a matching reply counts, whether or not a delay could be
        # computed from it.
        "answered_topics": len(reply_rows),
        "median_seconds": _percentile(gaps, 0.5),
        "p90_seconds": _percentile(gaps, 0.9),
    }


def build_discourse_query(conn, raw_filters: dict | None, base_url: str = "") -> dict:
    """Everything the query page needs, in one place.

    Queries run on a dedicated cursor, so a caller that hands in a shared connection
    gets it back untouched.
    """
    filters = normalize_filters(raw_filters)
    # A cursor carries its own row factory, so a shared connection is never mutated
    # underneath another thread that is mid-query.
    db = conn.cursor()
    db.row_factory = sqlite3.Row
    try:
        summary = _summary(db, filters)
        users = _per_user(db, filters)
        topics, total_topics = _topics(db, filters, filters["page"])
        user_card = _user_card(db, filters)
        options = query_options(db)
    except Exception as exc:  # noqa: BLE001 - reported to the page, not swallowed
        return {"ok": False, "error": str(exc), "filters": _filters_for_page(filters)}
    finally:
        db.close()

    selected_staff = set(options["staff"])
    for row in users:
        row["is_staff"] = row["username"] in selected_staff

    base = str(base_url or "").rstrip("/")
    for topic in topics:
        topic["url"] = f"{base}/t/{topic['topic_id']}" if base else ""

    pages = max(1, (total_topics + TOPIC_PAGE_SIZE - 1) // TOPIC_PAGE_SIZE)
    return {
        "ok": True,
        "error": "",
        "filters": _filters_for_page(filters),
        "summary": {**summary, "topics_total": total_topics, "pages": pages},
        "users": users,
        "topics": topics,
        "user_card": user_card,
        "options": options,
    }


def _filters_for_page(filters: dict) -> dict:
    return {
        "username": filters.get("username", ""),
        "category": filters.get("category", ""),
        "staff_only": bool(filters.get("staff_only")),
        "page": int(filters.get("page") or 1),
        "from": filters["date_from"].isoformat(),
        "to": filters["date_to"].isoformat(),
    }


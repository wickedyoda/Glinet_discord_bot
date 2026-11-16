"""A ban list shared across the servers this bot runs in.

Each guild's own bans are Discord's business; this is the operator's list, for people
who should not be able to move from one of your servers to the next. Membership is
global on purpose -- that is the point -- and every entry records which guild and which
moderator added it, so a mistake can be traced and undone.
"""

from __future__ import annotations

from datetime import UTC, datetime

MAX_REASON_LENGTH = 500


def _now() -> str:
    return datetime.now(UTC).isoformat()


def add_ban(conn, user_id: int, *, reason: str, added_by: str, guild_id: int) -> bool:
    """Add or refresh an entry. Returns True when a new row was created."""
    existing = conn.execute(
        "SELECT user_id FROM shared_bans WHERE user_id = ?", (int(user_id),)
    ).fetchone()
    conn.execute(
        "INSERT INTO shared_bans (user_id, reason, added_by, added_at, source_guild_id) "
        "VALUES (?, ?, ?, ?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET "
        "reason = excluded.reason, added_by = excluded.added_by, "
        "added_at = excluded.added_at, source_guild_id = excluded.source_guild_id",
        (
            int(user_id),
            str(reason or "")[:MAX_REASON_LENGTH],
            str(added_by or "")[:120],
            _now(),
            int(guild_id),
        ),
    )
    return existing is None


def remove_ban(conn, user_id: int) -> bool:
    cursor = conn.execute("DELETE FROM shared_bans WHERE user_id = ?", (int(user_id),))
    return bool(cursor.rowcount)


def get_ban(conn, user_id: int) -> dict | None:
    row = conn.execute(
        "SELECT user_id, reason, added_by, added_at, source_guild_id "
        "FROM shared_bans WHERE user_id = ?",
        (int(user_id),),
    ).fetchone()
    if row is None:
        return None
    return {
        "user_id": str(row["user_id"]),
        "reason": str(row["reason"] or ""),
        "added_by": str(row["added_by"] or ""),
        "added_at": str(row["added_at"] or ""),
        "source_guild_id": str(row["source_guild_id"] or ""),
    }


def is_banned(conn, user_id: int) -> bool:
    return get_ban(conn, user_id) is not None


def list_bans(conn, limit: int = 200) -> list[dict]:
    rows = conn.execute(
        "SELECT user_id, reason, added_by, added_at, source_guild_id "
        "FROM shared_bans ORDER BY added_at DESC LIMIT ?",
        (max(1, min(int(limit), 1000)),),
    ).fetchall()
    return [
        {
            "user_id": str(row["user_id"]),
            "reason": str(row["reason"] or ""),
            "added_by": str(row["added_by"] or ""),
            "added_at": str(row["added_at"] or ""),
            "source_guild_id": str(row["source_guild_id"] or ""),
        }
        for row in rows
    ]

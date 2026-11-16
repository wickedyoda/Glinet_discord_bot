"""Staff moderation records: manual warnings, notes, and one history view.

Warnings used to exist only as a side effect of the bad-word filter, so a moderator had
no way to record "I told them to stop" and no way to see it later. This module owns the
storage for the manual side -- warnings with a reason and an author, staff notes, and a
combined history -- and holds no Discord objects, so it tests against an in-memory
database.

The case number is the warning row's own id. It is stable, it is what a member can quote
in an appeal, and it needs no second table to allocate.
"""

from __future__ import annotations

from datetime import UTC, datetime

MAX_REASON_LENGTH = 500
MAX_NOTE_LENGTH = 1000
MAX_HISTORY_ROWS = 25


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _clip(value, limit: int) -> str:
    text = str(value or "").strip()
    return text[:limit]


def add_warning(
    conn,
    guild_id: int,
    user_id: int,
    *,
    reason: str,
    moderator: str,
    warning_type: str = "manual",
    matched_term: str = "",
    message_excerpt: str = "",
    action_taken: str = "",
) -> int:
    """Record a warning and return its case number."""
    cursor = conn.execute(
        """
        INSERT INTO moderation_warnings (
            guild_id, user_id, created_at, warning_type, matched_term,
            message_excerpt, action_taken, reason, moderator, cleared_at, case_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '', 0)
        """,
        (
            int(guild_id),
            int(user_id),
            _now(),
            _clip(warning_type, 40) or "manual",
            _clip(matched_term, 120),
            _clip(message_excerpt, 500),
            _clip(action_taken, 120),
            _clip(reason, MAX_REASON_LENGTH),
            _clip(moderator, 120),
        ),
    )
    case_id = int(cursor.lastrowid or 0)
    # The case number is the row id, written back so a query can filter on it without
    # knowing that detail.
    conn.execute(
        "UPDATE moderation_warnings SET case_id = ? WHERE id = ?", (case_id, case_id)
    )
    return case_id


def list_warnings(conn, guild_id: int, user_id: int, *, include_cleared: bool = False) -> list[dict]:
    query = (
        "SELECT id, case_id, created_at, warning_type, reason, moderator, "
        "matched_term, action_taken, cleared_at "
        "FROM moderation_warnings WHERE guild_id = ? AND user_id = ?"
    )
    params: list = [int(guild_id), int(user_id)]
    if not include_cleared:
        query += " AND (cleared_at IS NULL OR cleared_at = '')"
    query += " ORDER BY id DESC"
    rows = conn.execute(query, params).fetchall()
    return [
        {
            "case_id": int(row["case_id"] or row["id"]),
            "created_at": str(row["created_at"] or ""),
            "type": str(row["warning_type"] or ""),
            "reason": str(row["reason"] or ""),
            "moderator": str(row["moderator"] or ""),
            "matched_term": str(row["matched_term"] or ""),
            "action_taken": str(row["action_taken"] or ""),
            "cleared_at": str(row["cleared_at"] or ""),
        }
        for row in rows
    ]


def get_case(conn, guild_id: int, case_id: int) -> dict | None:
    """One case by its number, or None when it does not belong to this guild."""
    row = conn.execute(
        "SELECT id, case_id, user_id, created_at, warning_type, reason, moderator, "
        "matched_term, action_taken, cleared_at "
        "FROM moderation_warnings WHERE guild_id = ? AND case_id = ?",
        (int(guild_id), int(case_id)),
    ).fetchone()
    if row is None:
        return None
    return {
        "case_id": int(row["case_id"] or row["id"]),
        "user_id": str(row["user_id"]),
        "created_at": str(row["created_at"] or ""),
        "type": str(row["warning_type"] or ""),
        "reason": str(row["reason"] or ""),
        "moderator": str(row["moderator"] or ""),
        "matched_term": str(row["matched_term"] or ""),
        "action_taken": str(row["action_taken"] or ""),
        "cleared_at": str(row["cleared_at"] or ""),
    }


def update_case_reason(conn, guild_id: int, case_id: int, *, reason: str, editor: str) -> bool:
    """Rewrite a case's reason, keeping who changed it in the text.

    The original reason is not lost: it is appended, because a case that can be quietly
    rewritten is not a record.
    """
    case = get_case(conn, guild_id, case_id)
    if case is None:
        return False
    new_reason = _clip(reason, MAX_REASON_LENGTH)
    if not new_reason:
        return False
    history = f"{case['reason']} [edited by {_clip(editor, 120)}: {new_reason}]"
    conn.execute(
        "UPDATE moderation_warnings SET reason = ? WHERE guild_id = ? AND case_id = ?",
        (_clip(history, MAX_REASON_LENGTH * 2), int(guild_id), int(case_id)),
    )
    return True


def clear_warnings(conn, guild_id: int, user_id: int, *, case_id: int | None = None) -> int:
    """Clear one warning, or all of them, and return how many were cleared."""
    stamp = _now()
    if case_id is not None:
        cursor = conn.execute(
            "UPDATE moderation_warnings SET cleared_at = ? "
            "WHERE guild_id = ? AND user_id = ? AND case_id = ? "
            "AND (cleared_at IS NULL OR cleared_at = '')",
            (stamp, int(guild_id), int(user_id), int(case_id)),
        )
    else:
        cursor = conn.execute(
            "UPDATE moderation_warnings SET cleared_at = ? "
            "WHERE guild_id = ? AND user_id = ? AND (cleared_at IS NULL OR cleared_at = '')",
            (stamp, int(guild_id), int(user_id)),
        )
    return int(cursor.rowcount or 0)


def add_note(conn, guild_id: int, user_id: int, *, note: str, author: str) -> int:
    cursor = conn.execute(
        "INSERT INTO moderation_notes (guild_id, user_id, created_at, author, note) "
        "VALUES (?, ?, ?, ?, ?)",
        (int(guild_id), int(user_id), _now(), _clip(author, 120), _clip(note, MAX_NOTE_LENGTH)),
    )
    return int(cursor.lastrowid or 0)


def list_notes(conn, guild_id: int, user_id: int) -> list[dict]:
    rows = conn.execute(
        "SELECT id, created_at, author, note FROM moderation_notes "
        "WHERE guild_id = ? AND user_id = ? ORDER BY id DESC",
        (int(guild_id), int(user_id)),
    ).fetchall()
    return [
        {
            "id": int(row["id"]),
            "created_at": str(row["created_at"] or ""),
            "author": str(row["author"] or ""),
            "note": str(row["note"] or ""),
        }
        for row in rows
    ]


def build_history(conn, guild_id: int, user_id: int, *, limit: int = MAX_HISTORY_ROWS) -> dict:
    """Warnings, notes and recorded actions for one member, newest first."""
    warnings = list_warnings(conn, guild_id, user_id, include_cleared=True)
    notes = list_notes(conn, guild_id, user_id)
    try:
        action_rows = conn.execute(
            "SELECT created_at, action, status, moderator, reason FROM actions "
            "WHERE guild_id = ? AND target = ? ORDER BY id DESC LIMIT ?",
            (int(guild_id), str(user_id), int(limit)),
        ).fetchall()
    except Exception:  # noqa: BLE001 - the actions table is optional in older databases
        action_rows = []
    actions = [
        {
            "created_at": str(row["created_at"] or ""),
            "action": str(row["action"] or ""),
            "status": str(row["status"] or ""),
            "moderator": str(row["moderator"] or ""),
            "reason": str(row["reason"] or ""),
        }
        for row in action_rows
    ]
    return {
        "warnings": warnings,
        "notes": notes,
        "actions": actions,
        "active_warnings": sum(1 for warning in warnings if not warning["cleared_at"]),
    }


def _short_date(value: str) -> str:
    return str(value or "")[:10]


def format_history_text(history: dict, *, limit: int = MAX_HISTORY_ROWS) -> str:
    """A compact, Discord-sized rendering of one member's history."""
    lines: list[str] = []
    active = history.get("active_warnings", 0)
    lines.append(f"**Active warnings:** {active}")

    warnings = history.get("warnings") or []
    if warnings:
        lines.append("\n**Warnings**")
        for warning in warnings[:limit]:
            state = " (cleared)" if warning["cleared_at"] else ""
            reason = warning["reason"] or warning["matched_term"] or warning["type"]
            lines.append(
                f"• `#{warning['case_id']}` {_short_date(warning['created_at'])} — {reason}{state}"
            )

    notes = history.get("notes") or []
    if notes:
        lines.append("\n**Notes**")
        for note in notes[:limit]:
            lines.append(f"• {_short_date(note['created_at'])} — {note['note']}")

    actions = history.get("actions") or []
    if actions:
        lines.append("\n**Actions**")
        for action in actions[:limit]:
            reason = f" — {action['reason']}" if action["reason"] else ""
            lines.append(f"• {_short_date(action['created_at'])} {action['action']}{reason}")

    if not warnings and not notes and not actions:
        lines.append("\nNothing recorded for this member.")

    text = "\n".join(lines)
    return text[:1900]

from __future__ import annotations

import re
from datetime import UTC, datetime

REACTION_ROLE_STATUSES = {"active", "paused", "disabled"}
_CUSTOM_EMOJI_RE = re.compile(r"^<a?:([A-Za-z0-9_]{2,32}):(\d+)>$")
_CUSTOM_EMOJI_SHORT_RE = re.compile(r"^([A-Za-z0-9_]{2,32}):(\d+)$")


def normalize_reaction_role_status(value: str | None, default: str = "active"):
    normalized = str(value or "").strip().lower()
    if normalized in REACTION_ROLE_STATUSES:
        return normalized
    return default


def normalize_reaction_role_message_id(value):
    try:
        parsed = int(str(value or "").strip())
    except (TypeError, ValueError):
        return None
    if parsed <= 0:
        return None
    return parsed


def normalize_reaction_role_emoji(value):
    raw = str(value or "").strip()
    if not raw:
        return None
    custom_match = _CUSTOM_EMOJI_RE.fullmatch(raw)
    if custom_match:
        name, emoji_id = custom_match.groups()
        return {"emoji_key": f"custom:{emoji_id}", "emoji_text": f"<:{name}:{emoji_id}>"}
    short_custom_match = _CUSTOM_EMOJI_SHORT_RE.fullmatch(raw)
    if short_custom_match:
        name, emoji_id = short_custom_match.groups()
        return {"emoji_key": f"custom:{emoji_id}", "emoji_text": f"<:{name}:{emoji_id}>"}
    if raw.isdigit():
        return {"emoji_key": f"custom:{raw}", "emoji_text": raw}
    return {"emoji_key": f"unicode:{raw}", "emoji_text": raw}


def reaction_role_emoji_key_from_payload(payload_emoji):
    emoji_id = getattr(payload_emoji, "id", None)
    if emoji_id:
        return f"custom:{int(emoji_id)}"
    emoji_name = str(getattr(payload_emoji, "name", "") or "").strip()
    if not emoji_name:
        return ""
    return f"unicode:{emoji_name}"


def save_reaction_role_mapping(
    get_db_connection,
    db_lock,
    guild_id: int,
    *,
    channel_id: int,
    message_id: int,
    emoji: str,
    role_id: int,
    status: str = "active",
    created_at: str | None = None,
    emoji_text: str | None = None,
):
    normalized_emoji = normalize_reaction_role_emoji(emoji)
    if normalized_emoji is None:
        raise ValueError("Emoji is required.")
    safe_channel_id = int(channel_id)
    safe_message_id = int(message_id)
    safe_role_id = int(role_id)
    if safe_channel_id <= 0:
        raise ValueError("Channel must be a valid Discord channel.")
    if safe_message_id <= 0:
        raise ValueError("Message must be a valid Discord message ID.")
    if safe_role_id <= 0:
        raise ValueError("Choose a valid Discord role.")

    safe_guild_id = int(guild_id)
    now_iso = datetime.now(UTC).isoformat()
    created_iso = str(created_at or now_iso)
    conn = get_db_connection()
    with db_lock:
        existing = conn.execute(
            """
            SELECT created_at
            FROM reaction_roles
            WHERE guild_id = ? AND message_id = ? AND emoji_key = ?
            """,
            (safe_guild_id, safe_message_id, normalized_emoji["emoji_key"]),
        ).fetchone()
        created_value = str(existing["created_at"]) if existing else created_iso
        conn.execute(
            """
            INSERT OR REPLACE INTO reaction_roles (
                guild_id, channel_id, message_id, emoji_key, emoji_text, role_id, status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                safe_guild_id,
                safe_channel_id,
                safe_message_id,
                normalized_emoji["emoji_key"],
                str(emoji_text if emoji_text is not None else normalized_emoji["emoji_text"]),
                safe_role_id,
                normalize_reaction_role_status(status),
                created_value,
                now_iso,
            ),
        )
        conn.commit()
    return normalized_emoji["emoji_key"]


def list_reaction_role_mappings(get_db_connection, db_lock, guild_id: int | None = None):
    conn = get_db_connection()
    with db_lock:
        if guild_id is None:
            rows = conn.execute(
                """
                SELECT guild_id, channel_id, message_id, emoji_key, emoji_text, role_id, status, created_at, updated_at
                FROM reaction_roles
                ORDER BY created_at DESC, message_id DESC, emoji_key ASC
                """
            ).fetchall()
        else:
            safe_guild_id = int(guild_id)
            rows = conn.execute(
                """
                SELECT guild_id, channel_id, message_id, emoji_key, emoji_text, role_id, status, created_at, updated_at
                FROM reaction_roles
                WHERE guild_id = ?
                ORDER BY created_at DESC, message_id DESC, emoji_key ASC
                """,
                (safe_guild_id,),
            ).fetchall()

    mappings = []
    for row in rows:
        mappings.append(
            {
                "guild_id": int(row["guild_id"] or 0),
                "channel_id": int(row["channel_id"] or 0),
                "message_id": int(row["message_id"] or 0),
                "emoji_key": str(row["emoji_key"] or ""),
                "emoji_text": str(row["emoji_text"] or ""),
                "role_id": int(row["role_id"] or 0),
                "status": normalize_reaction_role_status(row["status"]),
                "created_at": str(row["created_at"] or ""),
                "updated_at": str(row["updated_at"] or row["created_at"] or ""),
            }
        )
    return mappings


def set_reaction_role_mapping_status(get_db_connection, db_lock, guild_id: int, *, message_id: int, emoji: str, status: str):
    normalized_emoji = normalize_reaction_role_emoji(emoji)
    if normalized_emoji is None:
        return False
    conn = get_db_connection()
    now_iso = datetime.now(UTC).isoformat()
    with db_lock:
        result = conn.execute(
            """
            UPDATE reaction_roles
            SET status = ?, updated_at = ?
            WHERE guild_id = ? AND message_id = ? AND emoji_key = ?
            """,
            (normalize_reaction_role_status(status), now_iso, int(guild_id), int(message_id), normalized_emoji["emoji_key"]),
        )
        conn.commit()
    return bool(result.rowcount)


def delete_reaction_role_mapping(get_db_connection, db_lock, guild_id: int, *, message_id: int, emoji: str):
    normalized_emoji = normalize_reaction_role_emoji(emoji)
    if normalized_emoji is None:
        return False
    conn = get_db_connection()
    with db_lock:
        result = conn.execute(
            """
            DELETE FROM reaction_roles
            WHERE guild_id = ? AND message_id = ? AND emoji_key = ?
            """,
            (int(guild_id), int(message_id), normalized_emoji["emoji_key"]),
        )
        conn.commit()
    return bool(result.rowcount)


def load_reaction_roles(get_db_connection, db_lock, guild_id: int | None = None):
    mappings = list_reaction_role_mappings(get_db_connection, db_lock, guild_id)
    active_by_guild = {}
    for mapping in mappings:
        if normalize_reaction_role_status(mapping.get("status")) != "active":
            continue
        safe_guild_id = int(mapping.get("guild_id") or 0)
        safe_message_id = int(mapping.get("message_id") or 0)
        emoji_key = str(mapping.get("emoji_key") or "")
        active_by_guild.setdefault(safe_guild_id, {}).setdefault(safe_message_id, {})[emoji_key] = int(mapping.get("role_id") or 0)
    return active_by_guild


def find_reaction_role_mapping(get_db_connection, db_lock, guild_id: int, message_id: int, emoji: str):
    normalized_emoji = normalize_reaction_role_emoji(emoji)
    if normalized_emoji is None:
        return None
    conn = get_db_connection()
    with db_lock:
        row = conn.execute(
            """
            SELECT guild_id, channel_id, message_id, emoji_key, emoji_text, role_id, status, created_at, updated_at
            FROM reaction_roles
            WHERE guild_id = ? AND message_id = ? AND emoji_key = ? AND LOWER(COALESCE(status, 'active')) = 'active'
            """,
            (int(guild_id), int(message_id), normalized_emoji["emoji_key"]),
        ).fetchone()
    if row is None:
        return None
    return {
        "guild_id": int(row["guild_id"] or 0),
        "channel_id": int(row["channel_id"] or 0),
        "message_id": int(row["message_id"] or 0),
        "emoji_key": str(row["emoji_key"] or ""),
        "emoji_text": str(row["emoji_text"] or ""),
        "role_id": int(row["role_id"] or 0),
        "status": normalize_reaction_role_status(row["status"]),
        "created_at": str(row["created_at"] or ""),
        "updated_at": str(row["updated_at"] or row["created_at"] or ""),
    }
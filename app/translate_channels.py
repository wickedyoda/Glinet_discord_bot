from __future__ import annotations

import json
import logging
import sqlite3
import threading
from typing import Any

logger = logging.getLogger("invite_bot.translate_channels")

_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS auto_translate_channels (
    guild_id INTEGER NOT NULL,
    source_channel_id INTEGER NOT NULL,
    target_channel_id INTEGER NOT NULL,
    target_language TEXT NOT NULL DEFAULT 'en',
    source_language TEXT NOT NULL DEFAULT 'auto',
    enabled INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (guild_id, source_channel_id, target_channel_id, target_language)
)
"""


class AutoTranslateChannelStore:
    """Persistence layer for auto-translate channel mappings (source -> target language)."""

    def __init__(self, db_path: str, db_lock: threading.Lock):
        self.db_path = db_path
        self.db_lock = db_lock
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        try:
            conn.execute(_CREATE_SQL)
            conn.commit()
        finally:
            conn.close()
        logger.info("Auto-translate channel store initialized at %s", self.db_path)

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path, check_same_thread=False, timeout=10)

    def list_for_guild(self, guild_id: int) -> list[dict[str, Any]]:
        with self.db_lock:
            conn = self._connect()
            try:
                rows = conn.execute(
                    """
                    SELECT guild_id, source_channel_id, target_channel_id, target_language, source_language, enabled
                    FROM auto_translate_channels
                    WHERE guild_id = ?
                    ORDER BY source_channel_id, target_language
                    """,
                    (int(guild_id),),
                ).fetchall()
            finally:
                conn.close()
        return [
            {
                "guild_id": row[0],
                "source_channel_id": row[1],
                "target_channel_id": row[2],
                "target_language": row[3],
                "source_language": row[4],
                "enabled": bool(row[5]),
            }
            for row in rows
        ]

    def list_active_for_source(self, guild_id: int, source_channel_id: int) -> list[dict[str, Any]]:
        with self.db_lock:
            conn = self._connect()
            try:
                rows = conn.execute(
                    """
                    SELECT guild_id, source_channel_id, target_channel_id, target_language, source_language, enabled
                    FROM auto_translate_channels
                    WHERE guild_id = ? AND source_channel_id = ? AND enabled = 1
                    ORDER BY target_language
                    """,
                    (int(guild_id), int(source_channel_id)),
                ).fetchall()
            finally:
                conn.close()
        return [
            {
                "guild_id": row[0],
                "source_channel_id": row[1],
                "target_channel_id": row[2],
                "target_language": row[3],
                "source_language": row[4],
                "enabled": bool(row[5]),
            }
            for row in rows
        ]

    def upsert(
        self,
        guild_id: int,
        source_channel_id: int,
        target_channel_id: int,
        target_language: str = "en",
        source_language: str = "auto",
        enabled: bool = True,
    ) -> None:
        with self.db_lock:
            conn = self._connect()
            try:
                conn.execute(
                    """
                    INSERT INTO auto_translate_channels (
                        guild_id, source_channel_id, target_channel_id, target_language, source_language, enabled, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
                    ON CONFLICT(guild_id, source_channel_id, target_channel_id, target_language) DO UPDATE SET
                        source_language = excluded.source_language,
                        enabled = excluded.enabled,
                        updated_at = excluded.updated_at
                    """,
                    (
                        int(guild_id),
                        int(source_channel_id),
                        int(target_channel_id),
                        str(target_language).strip().lower() or "en",
                        str(source_language).strip().lower() or "auto",
                        1 if enabled else 0,
                    ),
                )
                conn.commit()
            finally:
                conn.close()

    def delete(
        self,
        guild_id: int,
        source_channel_id: int,
        target_channel_id: int,
        target_language: str,
    ) -> bool:
        with self.db_lock:
            conn = self._connect()
            try:
                cur = conn.execute(
                    """
                    DELETE FROM auto_translate_channels
                    WHERE guild_id = ? AND source_channel_id = ? AND target_channel_id = ? AND target_language = ?
                    """,
                    (
                        int(guild_id),
                        int(source_channel_id),
                        int(target_channel_id),
                        str(target_language).strip().lower(),
                    ),
                )
                conn.commit()
                return cur.rowcount > 0
            finally:
                conn.close()

    def set_enabled(
        self,
        guild_id: int,
        source_channel_id: int,
        target_channel_id: int,
        target_language: str,
        enabled: bool,
    ) -> bool:
        with self.db_lock:
            conn = self._connect()
            try:
                cur = conn.execute(
                    """
                    UPDATE auto_translate_channels
                    SET enabled = ?, updated_at = datetime('now')
                    WHERE guild_id = ? AND source_channel_id = ? AND target_channel_id = ? AND target_language = ?
                    """,
                    (
                        1 if enabled else 0,
                        int(guild_id),
                        int(source_channel_id),
                        int(target_channel_id),
                        str(target_language).strip().lower(),
                    ),
                )
                conn.commit()
                return cur.rowcount > 0
            finally:
                conn.close()

    def to_json(self, guild_id: int) -> str:
        return json.dumps(self.list_for_guild(guild_id), indent=2)

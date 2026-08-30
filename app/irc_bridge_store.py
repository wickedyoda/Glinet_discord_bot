from __future__ import annotations

import sqlite3
import threading
from datetime import UTC, datetime
from typing import Any

from app.irc_bridge_types import IRCChannelMapping, IRCServerConfig


class IRCBridgeStore:
    def __init__(self, db_path: str, lock: threading.RLock) -> None:
        self._db_path = db_path
        self._lock = lock
        self._local = threading.local()

    def _conn(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn") or self._local.conn is None:
            self._local.conn = sqlite3.connect(self._db_path, check_same_thread=False)
            self._local.conn.row_factory = sqlite3.Row
        return self._local.conn

    def init_schema(self) -> None:
        with self._lock:
            conn = self._conn()
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS irc_bridge_servers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    host TEXT NOT NULL,
                    port INTEGER NOT NULL DEFAULT 6667,
                    use_tls INTEGER NOT NULL DEFAULT 0,
                    password TEXT NOT NULL DEFAULT '',
                    nickname TEXT NOT NULL DEFAULT 'BridgeBot',
                    username TEXT NOT NULL DEFAULT 'bridgebot',
                    realname TEXT NOT NULL DEFAULT 'BridgeBot',
                    reconnect_delay_seconds INTEGER NOT NULL DEFAULT 10,
                    max_reconnect_delay_seconds INTEGER NOT NULL DEFAULT 300,
                    ping_timeout_seconds INTEGER NOT NULL DEFAULT 120,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS irc_bridge_channels (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    guild_id INTEGER NOT NULL DEFAULT 0,
                    server_id INTEGER NOT NULL DEFAULT 0,
                    irc_channel TEXT NOT NULL,
                    discord_channel_id INTEGER NOT NULL DEFAULT 0,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(guild_id, server_id, irc_channel, discord_channel_id)
                );

                CREATE INDEX IF NOT EXISTS idx_irc_bridge_channels_guild_id
                    ON irc_bridge_channels(guild_id);
                CREATE INDEX IF NOT EXISTS idx_irc_bridge_channels_server_id
                    ON irc_bridge_channels(server_id);
                """
            )
            conn.commit()

    def create_server(self, server: IRCServerConfig) -> IRCServerConfig:
        now = datetime.now(UTC).isoformat()
        with self._lock:
            cursor = self._conn().execute(
                """
                INSERT INTO irc_bridge_servers (
                    name, host, port, use_tls, password, nickname, username, realname,
                    reconnect_delay_seconds, max_reconnect_delay_seconds, ping_timeout_seconds,
                    enabled, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    server.name,
                    server.host,
                    int(server.port),
                    int(server.use_tls),
                    server.password or "",
                    server.nickname,
                    server.username,
                    server.realname,
                    int(server.reconnect_delay_seconds),
                    int(server.max_reconnect_delay_seconds),
                    int(server.ping_timeout_seconds),
                    int(server.enabled),
                    now,
                    now,
                ),
            )
            server.id = int(cursor.lastrowid)
            return server

    def list_servers(self, guild_id: int | None = None) -> list[dict[str, Any]]:
        with self._lock:
            if guild_id is not None:
                rows = self._conn().execute(
                    "SELECT * FROM irc_bridge_servers WHERE id IN (SELECT server_id FROM irc_bridge_channels WHERE guild_id = ?) ORDER BY id",
                    (guild_id,),
                ).fetchall()
            else:
                rows = self._conn().execute("SELECT * FROM irc_bridge_servers ORDER BY id").fetchall()
            return [dict(row) for row in rows]

    def get_server(self, server_id: int) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn().execute("SELECT * FROM irc_bridge_servers WHERE id = ?", (server_id,)).fetchone()
            return dict(row) if row else None

    def update_server(self, server_id: int, **fields: Any) -> dict[str, Any] | None:
        allowed = {
            "name", "host", "port", "use_tls", "password", "nickname", "username", "realname",
            "reconnect_delay_seconds", "max_reconnect_delay_seconds", "ping_timeout_seconds", "enabled",
        }
        updates = {k: v for k, v in fields.items() if k in allowed}
        if not updates:
            return self.get_server(server_id)
        set_clause = ", ".join(f"{k} = ?" for k in updates)
        values = list(updates.values()) + [datetime.now(UTC).isoformat(), server_id]
        with self._lock:
            self._conn().execute(f"UPDATE irc_bridge_servers SET {set_clause}, updated_at = ? WHERE id = ?", values)
            self._conn().commit()
        return self.get_server(server_id)

    def delete_server(self, server_id: int) -> bool:
        with self._lock:
            cursor = self._conn().execute("DELETE FROM irc_bridge_servers WHERE id = ?", (server_id,))
            self._conn().execute("DELETE FROM irc_bridge_channels WHERE server_id = ?", (server_id,))
            self._conn().commit()
            return cursor.rowcount > 0

    def create_channel_mapping(self, mapping: IRCChannelMapping) -> IRCChannelMapping:
        now = datetime.now(UTC).isoformat()
        with self._lock:
            cursor = self._conn().execute(
                """
                INSERT INTO irc_bridge_channels (
                    guild_id, server_id, irc_channel, discord_channel_id, enabled, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(mapping.guild_id),
                    int(mapping.server_id),
                    mapping.irc_channel,
                    int(mapping.discord_channel_id),
                    int(mapping.enabled),
                    now,
                    now,
                ),
            )
            mapping.id = int(cursor.lastrowid)
            return mapping

    def list_channel_mappings(self, guild_id: int) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn().execute(
                "SELECT * FROM irc_bridge_channels WHERE guild_id = ? ORDER BY id",
                (guild_id,),
            ).fetchall()
            return [dict(row) for row in rows]

    def update_channel_mapping(self, mapping_id: int, **fields: Any) -> dict[str, Any] | None:
        allowed = {"irc_channel", "discord_channel_id", "enabled"}
        updates = {k: v for k, v in fields.items() if k in allowed}
        if not updates:
            row = self._conn().execute("SELECT * FROM irc_bridge_channels WHERE id = ?", (mapping_id,)).fetchone()
            return dict(row) if row else None
        set_clause = ", ".join(f"{k} = ?" for k in updates)
        values = list(updates.values()) + [datetime.now(UTC).isoformat(), mapping_id]
        with self._lock:
            self._conn().execute(f"UPDATE irc_bridge_channels SET {set_clause}, updated_at = ? WHERE id = ?", values)
            self._conn().commit()
        row = self._conn().execute("SELECT * FROM irc_bridge_channels WHERE id = ?", (mapping_id,)).fetchone()
        return dict(row) if row else None

    def upsert_server(self, server: IRCServerConfig) -> IRCServerConfig:
        existing = self.get_server(server.id)
        if existing:
            server.id = int(existing["id"])
            self.update_server(server.id, **server._to_updatable())
            return server
        return self.create_server(server)

    def get_mapping(self, mapping_id: int) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn().execute(
                "SELECT * FROM irc_bridge_channels WHERE id = ?", (mapping_id,)
            ).fetchone()
            return dict(row) if row else None

    def upsert_mapping(self, mapping: IRCChannelMapping) -> IRCChannelMapping:
        existing = self.get_mapping(mapping.id)
        if existing:
            mapping.id = int(existing["id"])
            self.update_channel_mapping(
                mapping.id,
                irc_channel=mapping.irc_channel,
                discord_channel_id=mapping.discord_channel_id,
                enabled=mapping.enabled,
            )
            return mapping
        return self.create_channel_mapping(mapping)

    def delete_mapping(self, mapping_id: int) -> bool:
        return self.delete_channel_mapping(mapping_id)

    def list_mappings_for_discord_channel(self, guild_id: int, discord_channel_id: int) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn().execute(
                "SELECT * FROM irc_bridge_channels WHERE guild_id = ? AND discord_channel_id = ? ORDER BY id",
                (guild_id, discord_channel_id),
            ).fetchall()
            return [dict(row) for row in rows]

    def delete_channel_mapping(self, mapping_id: int) -> bool:
        with self._lock:
            cursor = self._conn().execute("DELETE FROM irc_bridge_channels WHERE id = ?", (mapping_id,))
            self._conn().commit()
            return cursor.rowcount > 0

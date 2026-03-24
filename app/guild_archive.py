import gzip
import json
from datetime import UTC, datetime, timedelta

GUILD_ARCHIVE_KV_PREFIXES = (
    "guild_settings_updated_at:{guild_id}",
    "tag_responses_updated_at:{guild_id}",
    "command_permissions_updated_at:{guild_id}",
    "command_permissions_updated_by:{guild_id}",
)
GUILD_ARCHIVE_GUILD_ID_TABLES = {
    "role_codes",
    "invite_roles",
    "tag_responses",
    "guild_settings",
    "actions",
    "command_permissions",
    "reddit_feed_subscriptions",
    "youtube_subscriptions",
    "linkedin_subscriptions",
    "beta_program_subscriptions",
    "member_activity_summary",
    "member_activity_recent_hourly",
    "member_activity_seen_messages",
    "random_choice_history",
}
GUILD_ARCHIVE_SELECT_QUERIES = {
    "actions": "SELECT * FROM actions WHERE guild_id = ?",
    "beta_program_subscriptions": "SELECT * FROM beta_program_subscriptions WHERE guild_id = ?",
    "command_permissions": "SELECT * FROM command_permissions WHERE guild_id = ?",
    "guild_settings": "SELECT * FROM guild_settings WHERE guild_id = ?",
    "invite_roles": "SELECT * FROM invite_roles WHERE guild_id = ?",
    "linkedin_subscriptions": "SELECT * FROM linkedin_subscriptions WHERE guild_id = ?",
    "member_activity_recent_hourly": "SELECT * FROM member_activity_recent_hourly WHERE guild_id = ?",
    "member_activity_seen_messages": "SELECT * FROM member_activity_seen_messages WHERE guild_id = ?",
    "member_activity_summary": "SELECT * FROM member_activity_summary WHERE guild_id = ?",
    "random_choice_history": "SELECT * FROM random_choice_history WHERE guild_id = ?",
    "reddit_feed_subscriptions": "SELECT * FROM reddit_feed_subscriptions WHERE guild_id = ?",
    "role_codes": "SELECT * FROM role_codes WHERE guild_id = ?",
    "tag_responses": "SELECT * FROM tag_responses WHERE guild_id = ?",
    "youtube_subscriptions": "SELECT * FROM youtube_subscriptions WHERE guild_id = ?",
}
GUILD_ARCHIVE_DELETE_QUERIES = {
    "actions": "DELETE FROM actions WHERE guild_id = ?",
    "beta_program_subscriptions": "DELETE FROM beta_program_subscriptions WHERE guild_id = ?",
    "command_permissions": "DELETE FROM command_permissions WHERE guild_id = ?",
    "guild_settings": "DELETE FROM guild_settings WHERE guild_id = ?",
    "invite_roles": "DELETE FROM invite_roles WHERE guild_id = ?",
    "linkedin_subscriptions": "DELETE FROM linkedin_subscriptions WHERE guild_id = ?",
    "member_activity_recent_hourly": "DELETE FROM member_activity_recent_hourly WHERE guild_id = ?",
    "member_activity_seen_messages": "DELETE FROM member_activity_seen_messages WHERE guild_id = ?",
    "member_activity_summary": "DELETE FROM member_activity_summary WHERE guild_id = ?",
    "random_choice_history": "DELETE FROM random_choice_history WHERE guild_id = ?",
    "reddit_feed_subscriptions": "DELETE FROM reddit_feed_subscriptions WHERE guild_id = ?",
    "role_codes": "DELETE FROM role_codes WHERE guild_id = ?",
    "tag_responses": "DELETE FROM tag_responses WHERE guild_id = ?",
    "youtube_subscriptions": "DELETE FROM youtube_subscriptions WHERE guild_id = ?",
}


class GuildArchiveManager:
    def __init__(
        self,
        *,
        get_db_connection,
        db_lock,
        ensure_member_activity_schema_locked,
        clear_guild_runtime_state,
        retention_days: int = 14,
    ):
        self.get_db_connection = get_db_connection
        self.db_lock = db_lock
        self.ensure_member_activity_schema_locked = ensure_member_activity_schema_locked
        self.clear_guild_runtime_state = clear_guild_runtime_state
        self.retention_days = int(retention_days)

    def _compress_payload(self, payload: dict) -> bytes:
        serialized = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return gzip.compress(serialized)

    def _decompress_payload(self, payload_bytes: bytes) -> dict:
        try:
            raw_payload = gzip.decompress(bytes(payload_bytes or b""))
            parsed = json.loads(raw_payload.decode("utf-8"))
        except (OSError, ValueError, TypeError, UnicodeDecodeError):
            return {}
        return parsed if isinstance(parsed, dict) else {}

    def _require_archive_table_name(self, table_name: str) -> str:
        normalized = str(table_name or "").strip()
        if normalized not in GUILD_ARCHIVE_GUILD_ID_TABLES:
            raise ValueError(f"Unsupported guild archive table: {normalized}")
        return normalized

    def _select_table_rows_locked(self, conn, table_name: str, guild_id: int):
        safe_table_name = self._require_archive_table_name(table_name)
        rows = conn.execute(GUILD_ARCHIVE_SELECT_QUERIES[safe_table_name], (int(guild_id),)).fetchall()
        return [dict(row) for row in rows]

    def _select_kv_rows_locked(self, conn, guild_id: int):
        keys = [pattern.format(guild_id=int(guild_id)) for pattern in GUILD_ARCHIVE_KV_PREFIXES]
        rows = []
        for key in keys:
            row = conn.execute("SELECT key, value, updated_at FROM kv_store WHERE key = ?", (key,)).fetchone()
            if row is not None:
                rows.append(dict(row))
        prefix = f"member_activity_backfill:{int(guild_id)}:"
        rows.extend(
            dict(row)
            for row in conn.execute(
                "SELECT key, value, updated_at FROM kv_store WHERE key LIKE ?",
                (f"{prefix}%",),
            ).fetchall()
        )
        return rows

    def _delete_kv_rows_locked(self, conn, guild_id: int):
        keys = [pattern.format(guild_id=int(guild_id)) for pattern in GUILD_ARCHIVE_KV_PREFIXES]
        for key in keys:
            conn.execute("DELETE FROM kv_store WHERE key = ?", (key,))
        conn.execute(
            "DELETE FROM kv_store WHERE key LIKE ?",
            (f"member_activity_backfill:{int(guild_id)}:%",),
        )

    def _delete_live_guild_data_locked(self, conn, guild_id: int):
        safe_guild_id = int(guild_id)
        conn.execute(
            """
            DELETE FROM reddit_feed_seen_posts
            WHERE feed_id IN (
                SELECT id FROM reddit_feed_subscriptions WHERE guild_id = ?
            )
            """,
            (safe_guild_id,),
        )
        for table_name in GUILD_ARCHIVE_GUILD_ID_TABLES:
            safe_table_name = self._require_archive_table_name(table_name)
            conn.execute(GUILD_ARCHIVE_DELETE_QUERIES[safe_table_name], (safe_guild_id,))
        self._delete_kv_rows_locked(conn, safe_guild_id)

    def _insert_table_rows_locked(self, conn, table_name: str, rows: list[dict]):
        if not rows:
            return
        columns = list(rows[0].keys())
        column_sql = ", ".join(columns)
        placeholder_sql = ", ".join("?" for _ in columns)
        values = [tuple(row.get(column) for column in columns) for row in rows]
        conn.executemany(
            f"INSERT OR REPLACE INTO {table_name} ({column_sql}) VALUES ({placeholder_sql})",
            values,
        )

    def archive_guild_data(self, guild_id: int):
        safe_guild_id = int(guild_id)
        now_dt = datetime.now(UTC).replace(microsecond=0)
        purge_after_dt = now_dt + timedelta(days=self.retention_days)
        conn = self.get_db_connection()
        with self.db_lock:
            self.ensure_member_activity_schema_locked(conn)
            payload = {
                "version": 1,
                "guild_id": safe_guild_id,
                "archived_at": now_dt.isoformat(),
                "purge_after_at": purge_after_dt.isoformat(),
                "tables": {
                    "role_codes": self._select_table_rows_locked(conn, "role_codes", safe_guild_id),
                    "invite_roles": self._select_table_rows_locked(conn, "invite_roles", safe_guild_id),
                    "tag_responses": self._select_table_rows_locked(conn, "tag_responses", safe_guild_id),
                    "guild_settings": self._select_table_rows_locked(conn, "guild_settings", safe_guild_id),
                    "actions": self._select_table_rows_locked(conn, "actions", safe_guild_id),
                    "command_permissions": self._select_table_rows_locked(conn, "command_permissions", safe_guild_id),
                    "reddit_feed_subscriptions": self._select_table_rows_locked(conn, "reddit_feed_subscriptions", safe_guild_id),
                    "reddit_feed_seen_posts": [
                        dict(row)
                        for row in conn.execute(
                            """
                            SELECT sp.*
                            FROM reddit_feed_seen_posts sp
                            JOIN reddit_feed_subscriptions fs ON fs.id = sp.feed_id
                            WHERE fs.guild_id = ?
                            """,
                            (safe_guild_id,),
                        ).fetchall()
                    ],
                    "youtube_subscriptions": self._select_table_rows_locked(conn, "youtube_subscriptions", safe_guild_id),
                    "linkedin_subscriptions": self._select_table_rows_locked(conn, "linkedin_subscriptions", safe_guild_id),
                    "beta_program_subscriptions": self._select_table_rows_locked(conn, "beta_program_subscriptions", safe_guild_id),
                    "member_activity_summary": self._select_table_rows_locked(conn, "member_activity_summary", safe_guild_id),
                    "member_activity_recent_hourly": self._select_table_rows_locked(conn, "member_activity_recent_hourly", safe_guild_id),
                    "member_activity_seen_messages": self._select_table_rows_locked(conn, "member_activity_seen_messages", safe_guild_id),
                    "random_choice_history": self._select_table_rows_locked(conn, "random_choice_history", safe_guild_id),
                },
                "kv_store": self._select_kv_rows_locked(conn, safe_guild_id),
            }
            conn.execute(
                """
                INSERT INTO guild_data_archives (guild_id, archived_at, purge_after_at, payload)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(guild_id) DO UPDATE SET
                    archived_at = excluded.archived_at,
                    purge_after_at = excluded.purge_after_at,
                    payload = excluded.payload
                """,
                (
                    safe_guild_id,
                    now_dt.isoformat(),
                    purge_after_dt.isoformat(),
                    self._compress_payload(payload),
                ),
            )
            self._delete_live_guild_data_locked(conn, safe_guild_id)
            conn.commit()
        self.clear_guild_runtime_state(safe_guild_id)
        return {
            "archived_at": now_dt.isoformat(),
            "purge_after_at": purge_after_dt.isoformat(),
        }

    def restore_archived_guild_data(self, guild_id: int):
        safe_guild_id = int(guild_id)
        conn = self.get_db_connection()
        with self.db_lock:
            self.ensure_member_activity_schema_locked(conn)
            row = conn.execute(
                "SELECT archived_at, purge_after_at, payload FROM guild_data_archives WHERE guild_id = ?",
                (safe_guild_id,),
            ).fetchone()
            if row is None:
                return {"ok": False, "restored": False}
            payload = self._decompress_payload(row["payload"])
            tables_payload = payload.get("tables") if isinstance(payload, dict) else {}
            kv_rows = payload.get("kv_store") if isinstance(payload, dict) else []
            if not isinstance(tables_payload, dict):
                tables_payload = {}
            if not isinstance(kv_rows, list):
                kv_rows = []

            self._delete_live_guild_data_locked(conn, safe_guild_id)
            restore_order = (
                "role_codes",
                "invite_roles",
                "tag_responses",
                "guild_settings",
                "actions",
                "command_permissions",
                "reddit_feed_subscriptions",
                "reddit_feed_seen_posts",
                "youtube_subscriptions",
                "linkedin_subscriptions",
                "beta_program_subscriptions",
                "member_activity_summary",
                "member_activity_recent_hourly",
                "member_activity_seen_messages",
                "random_choice_history",
            )
            for table_name in restore_order:
                rows = tables_payload.get(table_name) or []
                if isinstance(rows, list):
                    safe_rows = [row for row in rows if isinstance(row, dict)]
                    self._insert_table_rows_locked(conn, table_name, safe_rows)
            for row_payload in kv_rows:
                if not isinstance(row_payload, dict):
                    continue
                key = str(row_payload.get("key") or "").strip()
                value = str(row_payload.get("value") or "")
                updated_at = str(row_payload.get("updated_at") or datetime.now(UTC).isoformat())
                if not key:
                    continue
                conn.execute(
                    """
                    INSERT INTO kv_store (key, value, updated_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
                    """,
                    (key, value, updated_at),
                )
            conn.execute("DELETE FROM guild_data_archives WHERE guild_id = ?", (safe_guild_id,))
            conn.commit()
        self.clear_guild_runtime_state(safe_guild_id)
        return {
            "ok": True,
            "restored": True,
            "archived_at": str(row["archived_at"] or ""),
            "purge_after_at": str(row["purge_after_at"] or ""),
        }

    def purge_expired_guild_archives(self):
        now_dt = datetime.now(UTC).replace(microsecond=0)
        conn = self.get_db_connection()
        purged_guild_ids = []
        with self.db_lock:
            rows = conn.execute(
                "SELECT guild_id, purge_after_at FROM guild_data_archives WHERE purge_after_at <= ?",
                (now_dt.isoformat(),),
            ).fetchall()
            for row in rows:
                guild_id = int(row["guild_id"] or 0)
                conn.execute("DELETE FROM guild_data_archives WHERE guild_id = ?", (guild_id,))
                purged_guild_ids.append(guild_id)
            if purged_guild_ids:
                conn.commit()
        for guild_id in purged_guild_ids:
            self.clear_guild_runtime_state(guild_id)
        return purged_guild_ids

from __future__ import annotations

import asyncio
import concurrent.futures
import csv
import io
import json
import zipfile
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import discord


class MemberActivityManager:
    def __init__(
        self,
        *,
        get_db_connection,
        db_lock,
        require_managed_guild_id,
        is_managed_guild_id,
        normalize_activity_timestamp,
        encrypt_member_activity_identity,
        decrypt_member_activity_identity,
        clip_text,
        logger,
        bot,
        enable_members_intent: bool,
        member_activity_window_specs,
        member_activity_web_top_limit: int,
        member_activity_recent_retention_days: int,
        has_moderator_access,
        has_allowed_role,
        moderator_role_ids,
        default_allowed_role_names,
    ):
        self.get_db_connection = get_db_connection
        self.db_lock = db_lock
        self.require_managed_guild_id = require_managed_guild_id
        self.is_managed_guild_id = is_managed_guild_id
        self.normalize_activity_timestamp = normalize_activity_timestamp
        self.encrypt_member_activity_identity = encrypt_member_activity_identity
        self.decrypt_member_activity_identity = decrypt_member_activity_identity
        self.clip_text = clip_text
        self.logger = logger
        self.bot = bot
        self.enable_members_intent = bool(enable_members_intent)
        self.member_activity_window_specs = list(member_activity_window_specs)
        self.member_activity_web_top_limit = int(member_activity_web_top_limit)
        self.member_activity_recent_retention_days = int(member_activity_recent_retention_days)
        self.has_moderator_access = has_moderator_access
        self.has_allowed_role = has_allowed_role
        self.moderator_role_ids = tuple(int(role_id) for role_id in moderator_role_ids)
        self.default_allowed_role_names = tuple(str(name) for name in default_allowed_role_names)
        self.recent_prune_marker = ""
        self.encryption_migration_checked = False

    def ensure_member_activity_schema_locked(self, conn):
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS member_activity_summary (
                guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                username TEXT NOT NULL DEFAULT '',
                display_name TEXT NOT NULL DEFAULT '',
                first_message_at TEXT NOT NULL,
                last_message_at TEXT NOT NULL,
                total_messages INTEGER NOT NULL DEFAULT 0,
                total_active_days INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (guild_id, user_id)
            );

            CREATE TABLE IF NOT EXISTS member_activity_recent_hourly (
                guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                hour_bucket TEXT NOT NULL,
                message_count INTEGER NOT NULL DEFAULT 0,
                last_message_at TEXT NOT NULL,
                PRIMARY KEY (guild_id, user_id, hour_bucket)
            );

            CREATE TABLE IF NOT EXISTS member_activity_seen_messages (
                guild_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (guild_id, message_id)
            );
            """
        )

        member_activity_summary_columns = {
            str(row["name"]) for row in conn.execute("PRAGMA table_info(member_activity_summary)").fetchall()
        }
        if member_activity_summary_columns and "guild_id" not in member_activity_summary_columns:
            conn.executescript(
                """
                CREATE TABLE member_activity_summary_new (
                    guild_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    username TEXT NOT NULL DEFAULT '',
                    display_name TEXT NOT NULL DEFAULT '',
                    first_message_at TEXT NOT NULL,
                    last_message_at TEXT NOT NULL,
                    total_messages INTEGER NOT NULL DEFAULT 0,
                    total_active_days INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (guild_id, user_id)
                );
                INSERT INTO member_activity_summary_new (
                    guild_id,
                    user_id,
                    username,
                    display_name,
                    first_message_at,
                    last_message_at,
                    total_messages,
                    total_active_days
                )
                SELECT
                    0,
                    user_id,
                    username,
                    display_name,
                    first_message_at,
                    last_message_at,
                    total_messages,
                    total_active_days
                FROM member_activity_summary;
                DROP TABLE member_activity_summary;
                ALTER TABLE member_activity_summary_new RENAME TO member_activity_summary;
                """
            )

        member_activity_recent_columns = {
            str(row["name"]) for row in conn.execute("PRAGMA table_info(member_activity_recent_hourly)").fetchall()
        }
        if member_activity_recent_columns and "guild_id" not in member_activity_recent_columns:
            conn.executescript(
                """
                CREATE TABLE member_activity_recent_hourly_new (
                    guild_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    hour_bucket TEXT NOT NULL,
                    message_count INTEGER NOT NULL DEFAULT 0,
                    last_message_at TEXT NOT NULL,
                    PRIMARY KEY (guild_id, user_id, hour_bucket)
                );
                INSERT INTO member_activity_recent_hourly_new (
                    guild_id,
                    user_id,
                    hour_bucket,
                    message_count,
                    last_message_at
                )
                SELECT
                    0,
                    user_id,
                    hour_bucket,
                    message_count,
                    last_message_at
                FROM member_activity_recent_hourly;
                DROP TABLE member_activity_recent_hourly;
                ALTER TABLE member_activity_recent_hourly_new RENAME TO member_activity_recent_hourly;
                """
            )

        member_activity_seen_columns = {
            str(row["name"]) for row in conn.execute("PRAGMA table_info(member_activity_seen_messages)").fetchall()
        }
        if member_activity_seen_columns and "guild_id" not in member_activity_seen_columns:
            conn.executescript(
                """
                CREATE TABLE member_activity_seen_messages_new (
                    guild_id INTEGER NOT NULL,
                    message_id INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (guild_id, message_id)
                );
                INSERT INTO member_activity_seen_messages_new (
                    guild_id,
                    message_id,
                    created_at
                )
                SELECT
                    0,
                    message_id,
                    created_at
                FROM member_activity_seen_messages;
                DROP TABLE member_activity_seen_messages;
                ALTER TABLE member_activity_seen_messages_new RENAME TO member_activity_seen_messages;
                """
            )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_member_activity_summary_last_message
                ON member_activity_summary(last_message_at)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_member_activity_recent_hourly_bucket
                ON member_activity_recent_hourly(hour_bucket)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_member_activity_seen_messages_created_at
                ON member_activity_seen_messages(created_at)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_member_activity_summary_guild_id
                ON member_activity_summary(guild_id)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_member_activity_recent_hourly_guild_bucket
                ON member_activity_recent_hourly(guild_id, hour_bucket)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_member_activity_seen_messages_guild_created
                ON member_activity_seen_messages(guild_id, created_at)
            """
        )
        if not self.encryption_migration_checked:
            rows = conn.execute(
                """
                SELECT guild_id, user_id, username, display_name
                FROM member_activity_summary
                """
            ).fetchall()
            updates = []
            for row in rows:
                username = str(row["username"] or "")
                display_name = str(row["display_name"] or "")
                encrypted_username = self.encrypt_member_activity_identity(username)
                encrypted_display_name = self.encrypt_member_activity_identity(display_name)
                if encrypted_username != username or encrypted_display_name != display_name:
                    updates.append(
                        (
                            encrypted_username,
                            encrypted_display_name,
                            int(row["guild_id"] or 0),
                            int(row["user_id"] or 0),
                        )
                    )
            if updates:
                conn.executemany(
                    """
                    UPDATE member_activity_summary
                    SET username = ?,
                        display_name = ?
                    WHERE guild_id = ? AND user_id = ?
                    """,
                    updates,
                )
                self.logger.info("Encrypted %s existing member activity profile row(s).", len(updates))
            self.encryption_migration_checked = True

    def compute_member_activity_metrics(self, message_count: int, active_days: int, period_start: datetime, period_end: datetime):
        total_seconds = max((period_end - period_start).total_seconds(), 3600.0)
        period_days = max(total_seconds / 86400.0, 1 / 24)
        safe_messages = max(int(message_count or 0), 0)
        safe_active_days = max(int(active_days or 0), 0)
        return {
            "period_days": period_days,
            "messages_per_day": (safe_messages / period_days) if safe_messages else 0.0,
            "messages_per_active_day": (safe_messages / safe_active_days) if safe_messages and safe_active_days else 0.0,
            "active_day_ratio": min(1.0, safe_active_days / period_days) if safe_active_days else 0.0,
        }

    def build_member_activity_window_record(
        self,
        key: str,
        label: str,
        message_count: int,
        active_days: int,
        period_start: datetime,
        period_end: datetime,
        *,
        first_message_at: str = "",
        last_message_at: str = "",
    ):
        metrics = self.compute_member_activity_metrics(message_count, active_days, period_start, period_end)
        return {
            "key": key,
            "label": label,
            "message_count": int(message_count or 0),
            "active_days": int(active_days or 0),
            "period_days": metrics["period_days"],
            "messages_per_day": metrics["messages_per_day"],
            "messages_per_active_day": metrics["messages_per_active_day"],
            "active_day_ratio": metrics["active_day_ratio"],
            "first_message_at": str(first_message_at or ""),
            "last_message_at": str(last_message_at or ""),
        }

    def prune_member_activity_recent_hourly(self, conn, current_dt: datetime):
        current_hour_bucket = current_dt.replace(minute=0, second=0, microsecond=0).isoformat()
        if self.recent_prune_marker == current_hour_bucket:
            return
        cutoff_dt = current_dt - timedelta(days=self.member_activity_recent_retention_days)
        cutoff_bucket = cutoff_dt.replace(minute=0, second=0, microsecond=0).isoformat()
        conn.execute(
            "DELETE FROM member_activity_recent_hourly WHERE hour_bucket < ?",
            (cutoff_bucket,),
        )
        conn.execute(
            "DELETE FROM member_activity_seen_messages WHERE created_at < ?",
            (cutoff_dt.isoformat(),),
        )
        conn.execute(
            """
            DELETE FROM member_activity_summary
            WHERE last_message_at IS NULL OR last_message_at < ?
            """,
            (cutoff_dt.isoformat(),),
        )
        conn.execute(
            """
            UPDATE member_activity_summary
            SET total_messages = 0,
                total_active_days = 0
            """
        )
        self.recent_prune_marker = current_hour_bucket

    def record_member_message_activity_locked(
        self,
        conn,
        *,
        guild_id: int,
        user_id: int,
        username: str,
        display_name: str,
        message_id: int,
        message_dt: datetime,
    ):
        safe_guild_id = self.require_managed_guild_id(guild_id, context="member activity guild")
        encrypted_username = self.encrypt_member_activity_identity(username)
        encrypted_display_name = self.encrypt_member_activity_identity(display_name)
        message_iso = message_dt.isoformat()
        hour_bucket = message_dt.replace(minute=0, second=0, microsecond=0).isoformat()
        inserted = conn.execute(
            """
            INSERT OR IGNORE INTO member_activity_seen_messages (
                guild_id,
                message_id,
                created_at
            )
            VALUES (?, ?, ?)
            """,
            (
                safe_guild_id,
                int(message_id),
                message_iso,
            ),
        )
        if inserted.rowcount == 0:
            return False
        self.prune_member_activity_recent_hourly(conn, message_dt)
        summary_row = conn.execute(
            """
            SELECT first_message_at, last_message_at
            FROM member_activity_summary
            WHERE guild_id = ? AND user_id = ?
            """,
            (safe_guild_id, user_id),
        ).fetchone()

        if summary_row is None:
            conn.execute(
                """
                INSERT INTO member_activity_summary (
                    guild_id,
                    user_id,
                    username,
                    display_name,
                    first_message_at,
                    last_message_at,
                    total_messages,
                    total_active_days
                )
                VALUES (?, ?, ?, ?, ?, ?, 0, 0)
                """,
                (
                    safe_guild_id,
                    user_id,
                    encrypted_username,
                    encrypted_display_name,
                    message_iso,
                    message_iso,
                ),
            )
        else:
            conn.execute(
                """
                UPDATE member_activity_summary
                SET username = ?,
                    display_name = ?,
                    last_message_at = ?,
                    total_messages = 0,
                    total_active_days = 0
                WHERE guild_id = ? AND user_id = ?
                """,
                (
                    encrypted_username,
                    encrypted_display_name,
                    message_iso,
                    safe_guild_id,
                    user_id,
                ),
            )

        conn.execute(
            """
            INSERT INTO member_activity_recent_hourly (
                guild_id,
                user_id,
                hour_bucket,
                message_count,
                last_message_at
            )
            VALUES (?, ?, ?, 1, ?)
            ON CONFLICT(guild_id, user_id, hour_bucket) DO UPDATE SET
                message_count = member_activity_recent_hourly.message_count + 1,
                last_message_at = excluded.last_message_at
            """,
            (
                safe_guild_id,
                user_id,
                hour_bucket,
                message_iso,
            ),
        )
        return True

    def record_member_message_activity(self, message: discord.Message):
        if message.guild is None or message.author.bot:
            return False
        if not self.is_managed_guild_id(message.guild.id):
            return False

        conn = self.get_db_connection()
        with self.db_lock:
            self.ensure_member_activity_schema_locked(conn)
            changed = self.record_member_message_activity_locked(
                conn,
                guild_id=message.guild.id,
                user_id=int(message.author.id),
                username=self.clip_text(str(message.author), max_chars=120),
                display_name=self.clip_text(getattr(message.author, "display_name", str(message.author)), max_chars=120),
                message_id=int(message.id),
                message_dt=self.normalize_activity_timestamp(getattr(message, "created_at", None)),
            )
            conn.commit()
        return changed

    def normalize_optional_role_id(self, value) -> int | None:
        try:
            role_id = int(str(value or "").strip())
        except (TypeError, ValueError):
            return None
        return role_id if role_id > 0 else None

    def is_member_activity_ranking_eligible(self, member: discord.Member | None, role_id: int | None = None):
        import discord

        if not isinstance(member, discord.Member):
            return False
        if member.bot:
            return False
        if self.has_moderator_access(member) or self.has_allowed_role(member):
            return False
        if role_id is not None and not any(role.id == role_id for role in member.roles):
            return False
        return True

    async def resolve_member_activity_members_async(self, guild_id: int, user_ids: list[int]):
        import discord

        guild = self.bot.get_guild(int(guild_id))
        if guild is None:
            return {}

        members_by_id = {}
        if self.enable_members_intent and not guild.chunked:
            try:
                await guild.chunk(cache=True)
            except Exception:
                self.logger.debug("Falling back to partial guild member cache for activity rankings", exc_info=True)

        missing_ids = []
        for user_id in user_ids:
            member = guild.get_member(int(user_id))
            if member is not None:
                members_by_id[int(user_id)] = member
            else:
                missing_ids.append(int(user_id))

        for user_id in missing_ids:
            try:
                member = await guild.fetch_member(int(user_id))
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                continue
            members_by_id[int(user_id)] = member
        return members_by_id

    def resolve_member_activity_members(self, guild_id: int, user_ids: list[int]):
        unique_user_ids = []
        seen = set()
        for user_id in user_ids:
            try:
                normalized = int(user_id)
            except (TypeError, ValueError):
                continue
            if normalized <= 0 or normalized in seen:
                continue
            seen.add(normalized)
            unique_user_ids.append(normalized)
        if not unique_user_ids:
            return {}

        loop = getattr(self.bot, "loop", None)
        if loop is None or not loop.is_running():
            guild = self.bot.get_guild(int(guild_id))
            if guild is None:
                return {}
            return {
                user_id: member
                for user_id in unique_user_ids
                if (member := guild.get_member(user_id)) is not None
            }

        future = asyncio.run_coroutine_threadsafe(self.resolve_member_activity_members_async(int(guild_id), unique_user_ids), loop)
        try:
            return future.result(timeout=20)
        except concurrent.futures.TimeoutError:
            future.cancel()
            self.logger.warning("Timed out resolving guild members for activity rankings (guild=%s).", guild_id)
            return {}
        except Exception:
            self.logger.exception("Failed resolving guild members for activity rankings (guild=%s).", guild_id)
            return {}

    def list_member_activity_top_window(
        self,
        guild_id: int | None,
        window_key: str,
        limit: int,
        *,
        role_id: int | None = None,
    ):
        safe_guild_id = self.require_managed_guild_id(guild_id, context="member activity guild")
        safe_limit = max(1, min(int(limit or self.member_activity_web_top_limit), 100))
        safe_role_id = self.normalize_optional_role_id(role_id)
        now_dt = datetime.now(UTC)
        conn = self.get_db_connection()
        with self.db_lock:
            self.ensure_member_activity_schema_locked(conn)
            window_duration = next((duration for key, _label, duration in self.member_activity_window_specs if key == window_key), None)
            if window_duration is None:
                raise ValueError(f"Unsupported member activity window: {window_key}")
            cutoff_dt = now_dt - window_duration
            rows = conn.execute(
                """
                SELECT
                    h.user_id,
                    s.username,
                    s.display_name,
                    MAX(h.last_message_at) AS last_message_at,
                    SUM(h.message_count) AS message_count,
                    COUNT(DISTINCT substr(h.hour_bucket, 1, 10)) AS active_days
                FROM member_activity_recent_hourly h
                LEFT JOIN member_activity_summary s
                  ON s.guild_id = h.guild_id AND s.user_id = h.user_id
                WHERE h.guild_id = ?
                  AND h.hour_bucket >= ?
                GROUP BY h.user_id, s.username, s.display_name
                ORDER BY message_count DESC, last_message_at DESC
                """,
                (
                    safe_guild_id,
                    cutoff_dt.replace(minute=0, second=0, microsecond=0).isoformat(),
                ),
            ).fetchall()
        members = []
        label = next((item_label for key, item_label, _duration in self.member_activity_window_specs if key == window_key), window_key)
        batch_size = max(safe_limit * 5, 100)
        for batch_start in range(0, len(rows), batch_size):
            batch_rows = rows[batch_start : batch_start + batch_size]
            member_map = self.resolve_member_activity_members(
                safe_guild_id,
                [int(row["user_id"] or 0) for row in batch_rows],
            )
            for row in batch_rows:
                user_id = int(row["user_id"] or 0)
                if not self.is_member_activity_ranking_eligible(member_map.get(user_id), role_id=safe_role_id):
                    continue
                stats = self.build_member_activity_window_record(
                    window_key,
                    label,
                    int(row["message_count"] or 0),
                    int(row["active_days"] or 0),
                    cutoff_dt,
                    now_dt,
                    first_message_at="",
                    last_message_at=str(row["last_message_at"] or ""),
                )
                stats.update(
                    {
                        "rank": len(members) + 1,
                        "user_id": user_id,
                        "username": self.decrypt_member_activity_identity(str(row["username"] or "")),
                        "display_name": self.decrypt_member_activity_identity(str(row["display_name"] or "")),
                    }
                )
                members.append(stats)
                if len(members) >= safe_limit:
                    return members
        return members

    def get_member_activity_snapshot(self, guild_id: int | None, user_id: int):
        safe_guild_id = self.require_managed_guild_id(guild_id, context="member activity guild")
        now_dt = datetime.now(UTC)
        conn = self.get_db_connection()
        with self.db_lock:
            self.ensure_member_activity_schema_locked(conn)
            summary_row = conn.execute(
                """
                SELECT
                    user_id,
                    username,
                    display_name,
                    first_message_at,
                    last_message_at,
                    total_messages,
                    total_active_days
                FROM member_activity_summary
                WHERE guild_id = ? AND user_id = ?
                """,
                (safe_guild_id, int(user_id)),
            ).fetchone()
            if summary_row is None:
                return {
                    "ok": True,
                    "user_id": int(user_id),
                    "username": "",
                    "display_name": "",
                    "windows": [],
                }

            windows = []
            for window_key, label, window_duration in self.member_activity_window_specs:
                cutoff_dt = now_dt - window_duration
                row = conn.execute(
                    """
                    SELECT
                        SUM(message_count) AS message_count,
                        COUNT(DISTINCT substr(hour_bucket, 1, 10)) AS active_days,
                        MAX(last_message_at) AS last_message_at
                    FROM member_activity_recent_hourly
                    WHERE guild_id = ?
                      AND user_id = ?
                      AND hour_bucket >= ?
                    """,
                    (
                        safe_guild_id,
                        int(user_id),
                        cutoff_dt.replace(minute=0, second=0, microsecond=0).isoformat(),
                    ),
                ).fetchone()
                windows.append(
                    self.build_member_activity_window_record(
                        window_key,
                        label,
                        int((row["message_count"] or 0) if row is not None else 0),
                        int((row["active_days"] or 0) if row is not None else 0),
                        cutoff_dt,
                        now_dt,
                        first_message_at="",
                        last_message_at=str((row["last_message_at"] or "") if row is not None else ""),
                    )
                )
            return {
                "ok": True,
                "user_id": int(summary_row["user_id"] or 0),
                "username": self.decrypt_member_activity_identity(str(summary_row["username"] or "")),
                "display_name": self.decrypt_member_activity_identity(str(summary_row["display_name"] or "")),
                "windows": windows,
            }

    def build_member_activity_web_payload(self, guild_id: int, role_id: int | None = None):
        safe_guild_id = self.require_managed_guild_id(guild_id, context="member activity guild")
        safe_role_id = self.normalize_optional_role_id(role_id)
        windows = []
        for window_key, label, _duration in self.member_activity_window_specs:
            windows.append(
                {
                    "key": window_key,
                    "label": label,
                    "members": self.list_member_activity_top_window(
                        safe_guild_id,
                        window_key,
                        limit=self.member_activity_web_top_limit,
                        role_id=safe_role_id,
                    ),
                }
            )
        return {
            "ok": True,
            "guild_id": safe_guild_id,
            "generated_at": datetime.now(UTC).isoformat(),
            "top_limit": self.member_activity_web_top_limit,
            "selected_role_id": safe_role_id or 0,
            "excluded_role_ids": sorted(self.moderator_role_ids),
            "excluded_role_names": sorted(self.default_allowed_role_names),
            "windows": windows,
        }

    def export_member_activity_archive(self, guild_id: int, role_id: int | None = None):
        safe_guild_id = self.require_managed_guild_id(guild_id, context="member activity guild")
        safe_role_id = self.normalize_optional_role_id(role_id)
        payload = self.build_member_activity_web_payload(safe_guild_id, role_id=safe_role_id)
        generated_at = datetime.now(UTC).replace(microsecond=0)
        conn = self.get_db_connection()
        with self.db_lock:
            summary_rows = conn.execute(
                """
                SELECT guild_id, user_id, username, display_name, first_message_at, last_message_at
                FROM member_activity_summary
                WHERE guild_id = ?
                ORDER BY last_message_at DESC, user_id ASC
                """,
                (safe_guild_id,),
            ).fetchall()
            hourly_rows = conn.execute(
                """
                SELECT guild_id, user_id, hour_bucket, message_count, last_message_at
                FROM member_activity_recent_hourly
                WHERE guild_id = ?
                ORDER BY hour_bucket DESC, user_id ASC
                """,
                (safe_guild_id,),
            ).fetchall()

        def build_csv_bytes(headers: list[str], rows: list[list[object]]):
            buffer = io.StringIO()
            writer = csv.writer(buffer)
            writer.writerow(headers)
            writer.writerows(rows)
            return buffer.getvalue().encode("utf-8")

        archive_buffer = io.BytesIO()
        with zipfile.ZipFile(archive_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr(
                "summary.json",
                json.dumps(
                    {
                        "guild_id": safe_guild_id,
                        "generated_at": generated_at.isoformat(),
                        "retention_days": self.member_activity_recent_retention_days,
                        "windows": payload.get("windows", []),
                    },
                    indent=2,
                    sort_keys=True,
                ).encode("utf-8"),
            )

            for window in payload.get("windows", []):
                window_key = str(window.get("key") or "window")
                members = window.get("members", []) if isinstance(window, dict) else []
                archive.writestr(
                    f"{window_key}.csv",
                    build_csv_bytes(
                        ["rank", "user_id", "display_name", "username", "message_count", "active_days", "last_message_at"],
                        [
                            [
                                int(member.get("rank") or 0),
                                int(member.get("user_id") or 0),
                                str(member.get("display_name") or ""),
                                str(member.get("username") or ""),
                                int(member.get("message_count") or 0),
                                int(member.get("active_days") or 0),
                                str(member.get("last_message_at") or ""),
                            ]
                            for member in members
                        ],
                    ),
                )

            archive.writestr(
                "member_activity_summary.csv",
                build_csv_bytes(
                    ["guild_id", "user_id", "username", "display_name", "first_message_at", "last_message_at"],
                    [
                        [
                            int(row["guild_id"] or 0),
                            int(row["user_id"] or 0),
                            self.decrypt_member_activity_identity(str(row["username"] or "")),
                            self.decrypt_member_activity_identity(str(row["display_name"] or "")),
                            str(row["first_message_at"] or ""),
                            str(row["last_message_at"] or ""),
                        ]
                        for row in summary_rows
                    ],
                ),
            )
            archive.writestr(
                "member_activity_recent_hourly.csv",
                build_csv_bytes(
                    ["guild_id", "user_id", "hour_bucket", "message_count", "last_message_at"],
                    [
                        [
                            int(row["guild_id"] or 0),
                            int(row["user_id"] or 0),
                            str(row["hour_bucket"] or ""),
                            int(row["message_count"] or 0),
                            str(row["last_message_at"] or ""),
                        ]
                        for row in hourly_rows
                    ],
                ),
            )

        role_suffix = f"_role_{safe_role_id}" if safe_role_id is not None else ""
        file_name = f"member_activity_guild_{safe_guild_id}{role_suffix}_{generated_at.strftime('%Y%m%dT%H%M%SZ')}.zip"
        return {
            "ok": True,
            "filename": file_name,
            "content_type": "application/zip",
            "data": archive_buffer.getvalue(),
            "generated_at": generated_at.isoformat(),
        }

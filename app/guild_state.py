from __future__ import annotations

import base64
import hashlib
import json
import os
from datetime import UTC, datetime, timedelta


class GuildStateManager:
    def __init__(
        self,
        *,
        get_db_connection,
        db_lock,
        normalize_target_guild_id,
        require_managed_guild_id,
        db_kv_get,
        db_kv_set,
        parse_int_setting,
        logger,
        role_file: str,
        bot_log_channel_id: int,
        mod_log_channel_id: int,
        firmware_notify_channel_id: int,
        random_choice_history_retention_days: int,
        member_activity_backfill_guild_id_raw: str,
        default_guild_id: int,
        member_activity_backfill_state_key,
        extract_member_activity_backfill_completed_ranges,
        audit_hash_secret: str,
        invite_roles_by_guild,
        invite_uses_by_guild,
        tag_response_cache,
        tag_command_names_by_guild,
        guild_settings_cache,
        command_permissions_cache,
        discord_catalog_cache,
    ):
        self.get_db_connection = get_db_connection
        self.db_lock = db_lock
        self.normalize_target_guild_id = normalize_target_guild_id
        self.require_managed_guild_id = require_managed_guild_id
        self.db_kv_get = db_kv_get
        self.db_kv_set = db_kv_set
        self.parse_int_setting = parse_int_setting
        self.logger = logger
        self.role_file = role_file
        self.bot_log_channel_id = int(bot_log_channel_id)
        self.mod_log_channel_id = int(mod_log_channel_id)
        self.firmware_notify_channel_id = int(firmware_notify_channel_id)
        self.random_choice_history_retention_days = int(random_choice_history_retention_days)
        self.member_activity_backfill_guild_id_raw = str(member_activity_backfill_guild_id_raw or "").strip()
        self.default_guild_id = int(default_guild_id)
        self.member_activity_backfill_state_key = member_activity_backfill_state_key
        self.extract_member_activity_backfill_completed_ranges = extract_member_activity_backfill_completed_ranges
        self.audit_hash_secret = str(audit_hash_secret or "").strip()
        self.invite_roles_by_guild = invite_roles_by_guild
        self.invite_uses_by_guild = invite_uses_by_guild
        self.tag_response_cache = tag_response_cache
        self.tag_command_names_by_guild = tag_command_names_by_guild
        self.guild_settings_cache = guild_settings_cache
        self.command_permissions_cache = command_permissions_cache
        self.discord_catalog_cache = discord_catalog_cache

    def default_guild_settings(self):
        return {
            "bot_log_channel_id": self.bot_log_channel_id if self.bot_log_channel_id > 0 else 0,
            "mod_log_channel_id": self.mod_log_channel_id if self.mod_log_channel_id > 0 else 0,
            "firmware_notify_channel_id": self.firmware_notify_channel_id if self.firmware_notify_channel_id > 0 else 0,
            "bad_words_enabled": 0,
            "bad_words_list_json": "[]",
            "bad_words_warning_window_hours": 72,
            "bad_words_warning_threshold": 3,
            "bad_words_action": "timeout",
            "bad_words_timeout_minutes": 60,
            "firmware_monitor_enabled": -1,
            "reddit_feed_notify_enabled": -1,
            "youtube_notify_enabled": -1,
            "linkedin_notify_enabled": -1,
            "beta_program_notify_enabled": -1,
            "access_role_id": 0,
            "welcome_channel_id": 0,
            "welcome_dm_enabled": 0,
            "welcome_channel_image_enabled": 0,
            "welcome_dm_image_enabled": 0,
            "welcome_channel_message": "",
            "welcome_dm_message": "",
            "welcome_image_filename": "",
            "welcome_image_media_type": "",
            "welcome_image_size_bytes": 0,
            "welcome_image_width": 0,
            "welcome_image_height": 0,
            "welcome_image_base64": "",
            "updated_at": "",
            "updated_by_email": "",
        }

    def build_web_actor_audit_label(self, actor_email: str):
        normalized = str(actor_email or "").strip().lower()
        if not normalized:
            return "web_user:unknown"
        salt = (self.audit_hash_secret or "glinet-web-audit-label").encode("utf-8")
        digest = hashlib.scrypt(
            normalized.encode("utf-8"),
            salt=salt,
            n=2**14,
            r=8,
            p=1,
            dklen=12,
        ).hex()
        return f"web_user:{digest}"

    def load_guild_settings(self, guild_id: int | None = None):
        safe_guild_id = self.normalize_target_guild_id(guild_id)
        version = self.db_kv_get(f"guild_settings_updated_at:{safe_guild_id}") or "bootstrap"
        cached = self.guild_settings_cache.get(safe_guild_id) or {}
        if cached.get("mtime") == version:
            return dict(cached.get("settings") or self.default_guild_settings())

        settings = self.default_guild_settings()
        conn = self.get_db_connection()
        with self.db_lock:
            row = conn.execute(
                """
                SELECT bot_log_channel_id, mod_log_channel_id, firmware_notify_channel_id,
                       bad_words_enabled, bad_words_list_json,
                       bad_words_warning_window_hours, bad_words_warning_threshold,
                       bad_words_action, bad_words_timeout_minutes,
                       firmware_monitor_enabled, reddit_feed_notify_enabled,
                       youtube_notify_enabled, linkedin_notify_enabled, beta_program_notify_enabled,
                       access_role_id, welcome_channel_id, welcome_dm_enabled,
                       welcome_channel_image_enabled, welcome_dm_image_enabled,
                       welcome_channel_message, welcome_dm_message,
                       welcome_image_filename, welcome_image_media_type,
                       welcome_image_size_bytes, welcome_image_width, welcome_image_height,
                       welcome_image_base64,
                       updated_at, updated_by_email
                FROM guild_settings
                WHERE guild_id = ?
                """,
                (safe_guild_id,),
            ).fetchone()
        if row is not None:
            settings.update(
                {
                    "bot_log_channel_id": int(row["bot_log_channel_id"] or 0),
                    "mod_log_channel_id": int(row["mod_log_channel_id"] or 0),
                    "firmware_notify_channel_id": int(row["firmware_notify_channel_id"] or 0),
                    "bad_words_enabled": 1 if int(row["bad_words_enabled"] or 0) > 0 else 0,
                    "bad_words_list_json": str(row["bad_words_list_json"] or "[]"),
                    "bad_words_warning_window_hours": int(row["bad_words_warning_window_hours"] or 72),
                    "bad_words_warning_threshold": int(row["bad_words_warning_threshold"] or 3),
                    "bad_words_action": str(row["bad_words_action"] or "timeout").strip().lower() or "timeout",
                    "bad_words_timeout_minutes": int(row["bad_words_timeout_minutes"] or 60),
                    "firmware_monitor_enabled": self._normalize_feature_override(row["firmware_monitor_enabled"]),
                    "reddit_feed_notify_enabled": self._normalize_feature_override(row["reddit_feed_notify_enabled"]),
                    "youtube_notify_enabled": self._normalize_feature_override(row["youtube_notify_enabled"]),
                    "linkedin_notify_enabled": self._normalize_feature_override(row["linkedin_notify_enabled"]),
                    "beta_program_notify_enabled": self._normalize_feature_override(row["beta_program_notify_enabled"]),
                    "access_role_id": int(row["access_role_id"] or 0),
                    "welcome_channel_id": int(row["welcome_channel_id"] or 0),
                    "welcome_dm_enabled": 1 if int(row["welcome_dm_enabled"] or 0) > 0 else 0,
                    "welcome_channel_image_enabled": 1 if int(row["welcome_channel_image_enabled"] or 0) > 0 else 0,
                    "welcome_dm_image_enabled": 1 if int(row["welcome_dm_image_enabled"] or 0) > 0 else 0,
                    "welcome_channel_message": str(row["welcome_channel_message"] or ""),
                    "welcome_dm_message": str(row["welcome_dm_message"] or ""),
                    "welcome_image_filename": str(row["welcome_image_filename"] or ""),
                    "welcome_image_media_type": str(row["welcome_image_media_type"] or ""),
                    "welcome_image_size_bytes": int(row["welcome_image_size_bytes"] or 0),
                    "welcome_image_width": int(row["welcome_image_width"] or 0),
                    "welcome_image_height": int(row["welcome_image_height"] or 0),
                    "welcome_image_base64": str(row["welcome_image_base64"] or ""),
                    "updated_at": str(row["updated_at"] or ""),
                    "updated_by_email": str(row["updated_by_email"] or ""),
                }
            )
        else:
            role_id_raw = self.db_kv_get("access_role_id")
            if role_id_raw is None and os.path.exists(self.role_file):
                try:
                    with open(self.role_file, encoding="utf-8") as handle:
                        role_id_raw = handle.read().strip()
                except Exception:
                    self.logger.exception("Failed reading legacy access role from %s", self.role_file)
            settings["access_role_id"] = self.parse_int_setting(role_id_raw, 0, minimum=0)

        self.guild_settings_cache[safe_guild_id] = {"mtime": version, "settings": dict(settings)}
        return settings

    def save_guild_settings(self, guild_id: int | None, payload: dict | None, actor_email: str = ""):
        safe_guild_id = self.normalize_target_guild_id(guild_id)
        current = self.load_guild_settings(safe_guild_id)
        updated_at = datetime.now(UTC).isoformat()
        merged = dict(current)
        source = payload or {}
        for key in (
            "bot_log_channel_id",
            "mod_log_channel_id",
            "firmware_notify_channel_id",
            "access_role_id",
            "welcome_channel_id",
        ):
            merged[key] = self.parse_int_setting(source.get(key, current.get(key, 0)), 0, minimum=0)
        merged["bad_words_enabled"] = 1 if str(source.get("bad_words_enabled", current.get("bad_words_enabled", 0))).strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        } else 0
        merged["bad_words_list_json"] = str(source.get("bad_words_list_json", current.get("bad_words_list_json", "[]")) or "[]").strip() or "[]"
        merged["bad_words_warning_window_hours"] = self.parse_int_setting(
            source.get("bad_words_warning_window_hours", current.get("bad_words_warning_window_hours", 72)),
            72,
            minimum=1,
        )
        merged["bad_words_warning_threshold"] = self.parse_int_setting(
            source.get("bad_words_warning_threshold", current.get("bad_words_warning_threshold", 3)),
            3,
            minimum=1,
        )
        merged["bad_words_action"] = str(source.get("bad_words_action", current.get("bad_words_action", "timeout")) or "timeout").strip().lower() or "timeout"
        merged["bad_words_timeout_minutes"] = self.parse_int_setting(
            source.get("bad_words_timeout_minutes", current.get("bad_words_timeout_minutes", 60)),
            60,
            minimum=1,
        )
        for key in (
            "firmware_monitor_enabled",
            "reddit_feed_notify_enabled",
            "youtube_notify_enabled",
            "linkedin_notify_enabled",
            "beta_program_notify_enabled",
        ):
            merged[key] = self._normalize_feature_override(source.get(key, current.get(key, -1)))
        merged["welcome_dm_enabled"] = 1 if str(source.get("welcome_dm_enabled", current.get("welcome_dm_enabled", 0))).strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        } else 0
        merged["welcome_channel_image_enabled"] = 1 if str(
            source.get("welcome_channel_image_enabled", current.get("welcome_channel_image_enabled", 0))
        ).strip().lower() in {"1", "true", "yes", "on"} else 0
        merged["welcome_dm_image_enabled"] = 1 if str(
            source.get("welcome_dm_image_enabled", current.get("welcome_dm_image_enabled", 0))
        ).strip().lower() in {"1", "true", "yes", "on"} else 0
        merged["welcome_channel_message"] = str(
            source.get("welcome_channel_message", current.get("welcome_channel_message", ""))
        ).strip()
        merged["welcome_dm_message"] = str(source.get("welcome_dm_message", current.get("welcome_dm_message", ""))).strip()
        if str(source.get("welcome_image_remove", "")).strip().lower() in {"1", "true", "yes", "on"}:
            merged["welcome_image_filename"] = ""
            merged["welcome_image_media_type"] = ""
            merged["welcome_image_size_bytes"] = 0
            merged["welcome_image_width"] = 0
            merged["welcome_image_height"] = 0
            merged["welcome_image_base64"] = ""
        elif source.get("welcome_image_bytes") is not None:
            image_bytes = source.get("welcome_image_bytes") or b""
            if isinstance(image_bytes, str):
                image_bytes = image_bytes.encode("utf-8")
            merged["welcome_image_filename"] = str(source.get("welcome_image_filename") or "welcome-image").strip()
            merged["welcome_image_media_type"] = str(source.get("welcome_image_media_type") or "application/octet-stream").strip()
            merged["welcome_image_size_bytes"] = self.parse_int_setting(
                source.get("welcome_image_size_bytes", len(image_bytes)),
                len(image_bytes),
                minimum=0,
            )
            merged["welcome_image_width"] = self.parse_int_setting(source.get("welcome_image_width", 0), 0, minimum=0)
            merged["welcome_image_height"] = self.parse_int_setting(source.get("welcome_image_height", 0), 0, minimum=0)
            merged["welcome_image_base64"] = base64.b64encode(bytes(image_bytes)).decode("ascii") if image_bytes else ""

        conn = self.get_db_connection()
        with self.db_lock:
            conn.execute(
                """
                INSERT INTO guild_settings (
                    guild_id,
                    bot_log_channel_id,
                    mod_log_channel_id,
                    firmware_notify_channel_id,
                    bad_words_enabled,
                    bad_words_list_json,
                    bad_words_warning_window_hours,
                    bad_words_warning_threshold,
                    bad_words_action,
                    bad_words_timeout_minutes,
                    firmware_monitor_enabled,
                    reddit_feed_notify_enabled,
                    youtube_notify_enabled,
                    linkedin_notify_enabled,
                    beta_program_notify_enabled,
                    access_role_id,
                    welcome_channel_id,
                    welcome_dm_enabled,
                    welcome_channel_image_enabled,
                    welcome_dm_image_enabled,
                    welcome_channel_message,
                    welcome_dm_message,
                    welcome_image_filename,
                    welcome_image_media_type,
                    welcome_image_size_bytes,
                    welcome_image_width,
                    welcome_image_height,
                    welcome_image_base64,
                    updated_at,
                    updated_by_email
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(guild_id) DO UPDATE SET
                    bot_log_channel_id=excluded.bot_log_channel_id,
                    mod_log_channel_id=excluded.mod_log_channel_id,
                    firmware_notify_channel_id=excluded.firmware_notify_channel_id,
                    bad_words_enabled=excluded.bad_words_enabled,
                    bad_words_list_json=excluded.bad_words_list_json,
                    bad_words_warning_window_hours=excluded.bad_words_warning_window_hours,
                    bad_words_warning_threshold=excluded.bad_words_warning_threshold,
                    bad_words_action=excluded.bad_words_action,
                    bad_words_timeout_minutes=excluded.bad_words_timeout_minutes,
                    firmware_monitor_enabled=excluded.firmware_monitor_enabled,
                    reddit_feed_notify_enabled=excluded.reddit_feed_notify_enabled,
                    youtube_notify_enabled=excluded.youtube_notify_enabled,
                    linkedin_notify_enabled=excluded.linkedin_notify_enabled,
                    beta_program_notify_enabled=excluded.beta_program_notify_enabled,
                    access_role_id=excluded.access_role_id,
                    welcome_channel_id=excluded.welcome_channel_id,
                    welcome_dm_enabled=excluded.welcome_dm_enabled,
                    welcome_channel_image_enabled=excluded.welcome_channel_image_enabled,
                    welcome_dm_image_enabled=excluded.welcome_dm_image_enabled,
                    welcome_channel_message=excluded.welcome_channel_message,
                    welcome_dm_message=excluded.welcome_dm_message,
                    welcome_image_filename=excluded.welcome_image_filename,
                    welcome_image_media_type=excluded.welcome_image_media_type,
                    welcome_image_size_bytes=excluded.welcome_image_size_bytes,
                    welcome_image_width=excluded.welcome_image_width,
                    welcome_image_height=excluded.welcome_image_height,
                    welcome_image_base64=excluded.welcome_image_base64,
                    updated_at=excluded.updated_at,
                    updated_by_email=excluded.updated_by_email
                """,
                (
                    safe_guild_id,
                    merged["bot_log_channel_id"],
                    merged["mod_log_channel_id"],
                    merged["firmware_notify_channel_id"],
                    merged["bad_words_enabled"],
                    merged["bad_words_list_json"],
                    merged["bad_words_warning_window_hours"],
                    merged["bad_words_warning_threshold"],
                    merged["bad_words_action"],
                    merged["bad_words_timeout_minutes"],
                    merged["firmware_monitor_enabled"],
                    merged["reddit_feed_notify_enabled"],
                    merged["youtube_notify_enabled"],
                    merged["linkedin_notify_enabled"],
                    merged["beta_program_notify_enabled"],
                    merged["access_role_id"],
                    merged["welcome_channel_id"],
                    merged["welcome_dm_enabled"],
                    merged["welcome_channel_image_enabled"],
                    merged["welcome_dm_image_enabled"],
                    merged["welcome_channel_message"],
                    merged["welcome_dm_message"],
                    merged["welcome_image_filename"],
                    merged["welcome_image_media_type"],
                    merged["welcome_image_size_bytes"],
                    merged["welcome_image_width"],
                    merged["welcome_image_height"],
                    merged["welcome_image_base64"],
                    updated_at,
                    actor_email or "unknown",
                ),
            )
            conn.commit()

        self.db_kv_set(f"guild_settings_updated_at:{safe_guild_id}", updated_at)
        self.guild_settings_cache[safe_guild_id] = {
            "mtime": updated_at,
            "settings": {
                **merged,
                "updated_at": updated_at,
                "updated_by_email": actor_email or "unknown",
            },
        }
        return self.load_guild_settings(safe_guild_id)

    def get_effective_guild_setting(self, guild_id: int | None, key: str, fallback_value: int = 0):
        settings = self.load_guild_settings(guild_id)
        value = self.parse_int_setting(settings.get(key, 0), 0, minimum=0)
        if value > 0:
            return value
        return self.parse_int_setting(fallback_value, 0, minimum=0)

    def _normalize_feature_override(self, raw_value):
        try:
            value = int(str(raw_value).strip())
        except (TypeError, ValueError):
            return -1
        if value > 0:
            return 1
        if value == 0:
            return 0
        return -1

    def get_effective_guild_feature_enabled(self, guild_id: int | None, key: str, fallback_value: bool = False):
        settings = self.load_guild_settings(guild_id)
        override = self._normalize_feature_override(settings.get(key, -1))
        if override == 1:
            return True
        if override == 0:
            return False
        return bool(fallback_value)

    def get_effective_logging_channel_id(self, guild_id: int | None):
        safe_guild_id = self.normalize_target_guild_id(guild_id)
        bot_log_channel_id = self.get_effective_guild_setting(safe_guild_id, "bot_log_channel_id", self.bot_log_channel_id)
        if bot_log_channel_id > 0:
            return bot_log_channel_id
        return self.get_effective_guild_setting(safe_guild_id, "mod_log_channel_id", self.mod_log_channel_id)

    def record_action_safe(
        self,
        action: str,
        status: str,
        moderator: str = "",
        target: str = "",
        reason: str = "",
        guild_id: int | None = None,
    ):
        safe_guild_id = self.normalize_target_guild_id(guild_id)
        conn = self.get_db_connection()
        with self.db_lock:
            conn.execute(
                """
                INSERT INTO actions (guild_id, created_at, action, status, moderator, target, reason)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    safe_guild_id,
                    datetime.now(UTC).isoformat(),
                    str(action or "").strip(),
                    str(status or "").strip(),
                    str(moderator or "").strip(),
                    str(target or "").strip(),
                    str(reason or "").strip(),
                ),
            )
            conn.commit()

    def list_recent_actions(self, guild_id: int | None, limit: int = 200):
        safe_guild_id = self.normalize_target_guild_id(guild_id)
        safe_limit = max(1, min(500, int(limit)))
        conn = self.get_db_connection()
        with self.db_lock:
            rows = conn.execute(
                """
                SELECT id, guild_id, created_at, action, status, moderator, target, reason
                FROM actions
                WHERE guild_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (safe_guild_id, safe_limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def ensure_random_choice_history_schema_locked(self, conn):
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS random_choice_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                selected_at TEXT NOT NULL,
                selected_by_user_id INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_random_choice_history_guild_selected_at
                ON random_choice_history(guild_id, selected_at);
            CREATE INDEX IF NOT EXISTS idx_random_choice_history_guild_user_selected_at
                ON random_choice_history(guild_id, user_id, selected_at);
            """
        )

    def parse_iso_datetime_utc(self, raw_value) -> datetime | None:
        text = str(raw_value or "").strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)

    def normalize_activity_timestamp(self, raw_value=None) -> datetime:
        if isinstance(raw_value, datetime):
            parsed = raw_value
        else:
            parsed = self.parse_iso_datetime_utc(raw_value)
        if parsed is None:
            parsed = datetime.now(UTC)
        elif parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        else:
            parsed = parsed.astimezone(UTC)
        return parsed

    def prune_random_choice_history_locked(self, conn, current_dt: datetime):
        cutoff_dt = self.normalize_activity_timestamp(current_dt) - timedelta(days=self.random_choice_history_retention_days)
        conn.execute(
            "DELETE FROM random_choice_history WHERE selected_at < ?",
            (cutoff_dt.isoformat(),),
        )

    def list_recent_random_choice_user_ids(self, guild_id: int | None, since_dt: datetime):
        safe_guild_id = self.require_managed_guild_id(guild_id, context="random choice guild")
        safe_since_dt = self.normalize_activity_timestamp(since_dt)
        conn = self.get_db_connection()
        with self.db_lock:
            self.ensure_random_choice_history_schema_locked(conn)
            self.prune_random_choice_history_locked(conn, safe_since_dt)
            rows = conn.execute(
                """
                SELECT DISTINCT user_id
                FROM random_choice_history
                WHERE guild_id = ? AND selected_at >= ?
                """,
                (safe_guild_id, safe_since_dt.isoformat()),
            ).fetchall()
        return {int(row["user_id"]) for row in rows}

    def record_random_choice_selection(
        self,
        guild_id: int | None,
        user_id: int,
        *,
        selected_by_user_id: int = 0,
        selected_at: datetime | None = None,
    ):
        safe_guild_id = self.require_managed_guild_id(guild_id, context="random choice guild")
        safe_selected_at = self.normalize_activity_timestamp(selected_at)
        conn = self.get_db_connection()
        with self.db_lock:
            self.ensure_random_choice_history_schema_locked(conn)
            self.prune_random_choice_history_locked(conn, safe_selected_at)
            conn.execute(
                """
                INSERT INTO random_choice_history (
                    guild_id,
                    user_id,
                    selected_at,
                    selected_by_user_id
                )
                VALUES (?, ?, ?, ?)
                """,
                (
                    safe_guild_id,
                    int(user_id),
                    safe_selected_at.isoformat(),
                    int(selected_by_user_id or 0),
                ),
            )
            conn.commit()

    def get_member_activity_backfill_target_guild_id(self) -> int:
        raw_value = self.member_activity_backfill_guild_id_raw
        if not raw_value:
            return self.default_guild_id
        try:
            parsed = int(raw_value)
        except ValueError as exc:
            raise RuntimeError("MEMBER_ACTIVITY_BACKFILL_GUILD_ID must be a numeric guild ID.") from exc
        if parsed <= 0:
            raise RuntimeError("MEMBER_ACTIVITY_BACKFILL_GUILD_ID must be a positive guild ID.")
        return parsed

    def load_member_activity_backfill_state(self, guild_id: int, since_dt: datetime) -> dict:
        raw_value = self.db_kv_get(self.member_activity_backfill_state_key(guild_id, since_dt))
        if not raw_value:
            return {}
        try:
            payload = json.loads(raw_value)
        except ValueError:
            return {}
        return payload if isinstance(payload, dict) else {}

    def save_member_activity_backfill_state(self, guild_id: int, since_dt: datetime, payload: dict):
        self.db_kv_set(
            self.member_activity_backfill_state_key(guild_id, since_dt),
            json.dumps(payload, sort_keys=True),
        )

    def clear_guild_runtime_state(self, guild_id: int):
        safe_guild_id = int(guild_id)
        self.invite_roles_by_guild.pop(safe_guild_id, None)
        self.invite_uses_by_guild.pop(safe_guild_id, None)
        self.tag_response_cache.pop(safe_guild_id, None)
        self.tag_command_names_by_guild.pop(safe_guild_id, None)
        self.guild_settings_cache.pop(safe_guild_id, None)
        self.command_permissions_cache.pop(safe_guild_id, None)
        self.discord_catalog_cache.pop(safe_guild_id, None)

    def list_member_activity_backfill_completed_ranges(self, guild_id: int):
        safe_guild_id = int(guild_id)
        conn = self.get_db_connection()
        with self.db_lock:
            rows = conn.execute(
                """
                SELECT key, value
                FROM kv_store
                WHERE key LIKE ?
                """,
                (f"member_activity_backfill:{safe_guild_id}:%",),
            ).fetchall()
        kv_rows = []
        for row in rows:
            try:
                payload = json.loads(str(row["value"] or ""))
            except ValueError:
                continue
            kv_rows.append({"payload": payload})
        return self.extract_member_activity_backfill_completed_ranges(kv_rows, self.parse_iso_datetime_utc)

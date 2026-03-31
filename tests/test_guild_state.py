import sqlite3
import threading
from datetime import UTC, datetime

from app.guild_state import GuildStateManager


def build_manager(**overrides):
    state = {
        "invite_roles_by_guild": {},
        "invite_uses_by_guild": {},
        "tag_response_cache": {},
        "tag_command_names_by_guild": {},
        "guild_settings_cache": {},
        "command_permissions_cache": {},
        "discord_catalog_cache": {},
    }
    params = {
        "get_db_connection": None,
        "db_lock": None,
        "normalize_target_guild_id": lambda guild_id: int(guild_id or 123),
        "require_managed_guild_id": lambda guild_id, context="": int(guild_id or 123),
        "db_kv_get": lambda key: None,
        "db_kv_set": lambda key, value: None,
        "parse_int_setting": lambda value, default=0, minimum=0: max(int(value or default), minimum),
        "logger": None,
        "role_file": "/tmp/role.txt",
        "bot_log_channel_id": 10,
        "mod_log_channel_id": 20,
        "firmware_notify_channel_id": 30,
        "random_choice_history_retention_days": 7,
        "member_activity_backfill_guild_id_raw": "",
        "default_guild_id": 123,
        "member_activity_backfill_state_key": lambda guild_id, since_dt: f"{guild_id}:{since_dt.isoformat()}",
        "extract_member_activity_backfill_completed_ranges": lambda rows, parser: rows,
        "audit_hash_secret": "test-audit-secret",
        **state,
    }
    params.update(overrides)
    return GuildStateManager(**params)


def test_build_web_actor_audit_label_is_stable_and_hides_email():
    manager = build_manager()

    first = manager.build_web_actor_audit_label("User@example.com")
    second = manager.build_web_actor_audit_label("user@example.com")

    assert first == second
    assert first.startswith("web_user:")
    assert "example.com" not in first


def test_normalize_activity_timestamp_keeps_utc():
    manager = build_manager()

    normalized = manager.normalize_activity_timestamp("2026-03-20T12:34:56+00:00")

    assert normalized == datetime(2026, 3, 20, 12, 34, 56, tzinfo=UTC)


def test_default_guild_settings_uses_configured_global_fallbacks():
    manager = build_manager()

    settings = manager.default_guild_settings()

    assert settings["bot_log_channel_id"] == 10
    assert settings["mod_log_channel_id"] == 20
    assert settings["firmware_notify_channel_id"] == 30


def test_load_guild_settings_uses_global_fallbacks_for_all_guilds_without_rows():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE guild_settings (
            guild_id INTEGER PRIMARY KEY,
            bot_log_channel_id INTEGER NOT NULL DEFAULT 0,
            mod_log_channel_id INTEGER NOT NULL DEFAULT 0,
            firmware_notify_channel_id INTEGER NOT NULL DEFAULT 0,
            bad_words_enabled INTEGER NOT NULL DEFAULT 0,
            bad_words_list_json TEXT NOT NULL DEFAULT '[]',
            bad_words_warning_window_hours INTEGER NOT NULL DEFAULT 72,
            bad_words_warning_threshold INTEGER NOT NULL DEFAULT 3,
            bad_words_action TEXT NOT NULL DEFAULT 'timeout',
            bad_words_timeout_minutes INTEGER NOT NULL DEFAULT 60,
            firmware_monitor_enabled INTEGER NOT NULL DEFAULT -1,
            reddit_feed_notify_enabled INTEGER NOT NULL DEFAULT -1,
            youtube_notify_enabled INTEGER NOT NULL DEFAULT -1,
            linkedin_notify_enabled INTEGER NOT NULL DEFAULT -1,
            beta_program_notify_enabled INTEGER NOT NULL DEFAULT -1,
            access_role_id INTEGER NOT NULL DEFAULT 0,
            welcome_channel_id INTEGER NOT NULL DEFAULT 0,
            welcome_dm_enabled INTEGER NOT NULL DEFAULT 0,
            welcome_channel_image_enabled INTEGER NOT NULL DEFAULT 0,
            welcome_dm_image_enabled INTEGER NOT NULL DEFAULT 0,
            welcome_channel_message TEXT NOT NULL DEFAULT '',
            welcome_dm_message TEXT NOT NULL DEFAULT '',
            welcome_image_filename TEXT NOT NULL DEFAULT '',
            welcome_image_media_type TEXT NOT NULL DEFAULT '',
            welcome_image_size_bytes INTEGER NOT NULL DEFAULT 0,
            welcome_image_width INTEGER NOT NULL DEFAULT 0,
            welcome_image_height INTEGER NOT NULL DEFAULT 0,
            welcome_image_base64 TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL,
            updated_by_email TEXT NOT NULL DEFAULT ''
        )
        """
    )
    manager = build_manager(get_db_connection=lambda: conn, db_lock=threading.Lock())

    settings = manager.load_guild_settings(999)

    assert settings["bot_log_channel_id"] == 10
    assert settings["mod_log_channel_id"] == 20
    assert settings["firmware_notify_channel_id"] == 30


def test_save_and_load_guild_settings_persists_welcome_image():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE guild_settings (
            guild_id INTEGER PRIMARY KEY,
            bot_log_channel_id INTEGER NOT NULL DEFAULT 0,
            mod_log_channel_id INTEGER NOT NULL DEFAULT 0,
            firmware_notify_channel_id INTEGER NOT NULL DEFAULT 0,
            bad_words_enabled INTEGER NOT NULL DEFAULT 0,
            bad_words_list_json TEXT NOT NULL DEFAULT '[]',
            bad_words_warning_window_hours INTEGER NOT NULL DEFAULT 72,
            bad_words_warning_threshold INTEGER NOT NULL DEFAULT 3,
            bad_words_action TEXT NOT NULL DEFAULT 'timeout',
            bad_words_timeout_minutes INTEGER NOT NULL DEFAULT 60,
            firmware_monitor_enabled INTEGER NOT NULL DEFAULT -1,
            reddit_feed_notify_enabled INTEGER NOT NULL DEFAULT -1,
            youtube_notify_enabled INTEGER NOT NULL DEFAULT -1,
            linkedin_notify_enabled INTEGER NOT NULL DEFAULT -1,
            beta_program_notify_enabled INTEGER NOT NULL DEFAULT -1,
            access_role_id INTEGER NOT NULL DEFAULT 0,
            welcome_channel_id INTEGER NOT NULL DEFAULT 0,
            welcome_dm_enabled INTEGER NOT NULL DEFAULT 0,
            welcome_channel_image_enabled INTEGER NOT NULL DEFAULT 0,
            welcome_dm_image_enabled INTEGER NOT NULL DEFAULT 0,
            welcome_channel_message TEXT NOT NULL DEFAULT '',
            welcome_dm_message TEXT NOT NULL DEFAULT '',
            welcome_image_filename TEXT NOT NULL DEFAULT '',
            welcome_image_media_type TEXT NOT NULL DEFAULT '',
            welcome_image_size_bytes INTEGER NOT NULL DEFAULT 0,
            welcome_image_width INTEGER NOT NULL DEFAULT 0,
            welcome_image_height INTEGER NOT NULL DEFAULT 0,
            welcome_image_base64 TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL,
            updated_by_email TEXT NOT NULL DEFAULT ''
        )
        """
    )
    kv_state = {}
    manager = build_manager(
        get_db_connection=lambda: conn,
        db_lock=threading.Lock(),
        db_kv_get=lambda key: kv_state.get(key),
        db_kv_set=lambda key, value: kv_state.__setitem__(key, value),
        logger=type("Logger", (), {"exception": lambda *args, **kwargs: None})(),
    )

    manager.save_guild_settings(
        123,
        {
            "welcome_channel_id": "9999",
            "welcome_dm_enabled": "1",
            "welcome_channel_image_enabled": "1",
            "welcome_dm_image_enabled": "1",
            "welcome_channel_message": "Welcome {member_mention}",
            "welcome_dm_message": "Hi {member_name}",
            "welcome_image_filename": "welcome.png",
            "welcome_image_media_type": "image/png",
            "welcome_image_size_bytes": 15,
            "welcome_image_width": 640,
            "welcome_image_height": 360,
            "welcome_image_bytes": b"\x89PNG\r\n\x1a\nfakepng",
        },
        actor_email="admin@example.com",
    )

    settings = manager.load_guild_settings(123)

    assert settings["welcome_channel_id"] == 9999
    assert settings["welcome_dm_enabled"] == 1
    assert settings["welcome_channel_image_enabled"] == 1
    assert settings["welcome_dm_image_enabled"] == 1
    assert settings["welcome_image_filename"] == "welcome.png"
    assert settings["welcome_image_media_type"] == "image/png"
    assert settings["welcome_image_size_bytes"] == 15
    assert settings["welcome_image_width"] == 640
    assert settings["welcome_image_height"] == 360
    assert settings["welcome_image_base64"]


def test_effective_guild_feature_enabled_uses_override_or_global():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE guild_settings (
            guild_id INTEGER PRIMARY KEY,
            bot_log_channel_id INTEGER NOT NULL DEFAULT 0,
            mod_log_channel_id INTEGER NOT NULL DEFAULT 0,
            firmware_notify_channel_id INTEGER NOT NULL DEFAULT 0,
            bad_words_enabled INTEGER NOT NULL DEFAULT 0,
            bad_words_list_json TEXT NOT NULL DEFAULT '[]',
            bad_words_warning_window_hours INTEGER NOT NULL DEFAULT 72,
            bad_words_warning_threshold INTEGER NOT NULL DEFAULT 3,
            bad_words_action TEXT NOT NULL DEFAULT 'timeout',
            bad_words_timeout_minutes INTEGER NOT NULL DEFAULT 60,
            firmware_monitor_enabled INTEGER NOT NULL DEFAULT -1,
            reddit_feed_notify_enabled INTEGER NOT NULL DEFAULT -1,
            youtube_notify_enabled INTEGER NOT NULL DEFAULT -1,
            linkedin_notify_enabled INTEGER NOT NULL DEFAULT -1,
            beta_program_notify_enabled INTEGER NOT NULL DEFAULT -1,
            access_role_id INTEGER NOT NULL DEFAULT 0,
            welcome_channel_id INTEGER NOT NULL DEFAULT 0,
            welcome_dm_enabled INTEGER NOT NULL DEFAULT 0,
            welcome_channel_image_enabled INTEGER NOT NULL DEFAULT 0,
            welcome_dm_image_enabled INTEGER NOT NULL DEFAULT 0,
            welcome_channel_message TEXT NOT NULL DEFAULT '',
            welcome_dm_message TEXT NOT NULL DEFAULT '',
            welcome_image_filename TEXT NOT NULL DEFAULT '',
            welcome_image_media_type TEXT NOT NULL DEFAULT '',
            welcome_image_size_bytes INTEGER NOT NULL DEFAULT 0,
            welcome_image_width INTEGER NOT NULL DEFAULT 0,
            welcome_image_height INTEGER NOT NULL DEFAULT 0,
            welcome_image_base64 TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL,
            updated_by_email TEXT NOT NULL DEFAULT ''
        )
        """
    )
    manager = build_manager(get_db_connection=lambda: conn, db_lock=threading.Lock())

    assert manager.get_effective_guild_feature_enabled(123, "youtube_notify_enabled", True) is True
    assert manager.get_effective_guild_feature_enabled(123, "youtube_notify_enabled", False) is False

    manager.save_guild_settings(123, {"youtube_notify_enabled": "1"}, actor_email="admin@example.com")
    assert manager.get_effective_guild_feature_enabled(123, "youtube_notify_enabled", False) is True

    manager.save_guild_settings(123, {"youtube_notify_enabled": "0"}, actor_email="admin@example.com")
    assert manager.get_effective_guild_feature_enabled(123, "youtube_notify_enabled", True) is False

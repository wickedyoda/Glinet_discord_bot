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


def test_default_guild_settings_uses_configured_defaults():
    manager = build_manager()

    settings = manager.default_guild_settings()

    assert settings["bot_log_channel_id"] == 10
    assert settings["mod_log_channel_id"] == 20
    assert settings["firmware_notify_channel_id"] == 30

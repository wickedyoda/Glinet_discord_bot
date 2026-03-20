from datetime import UTC, datetime, timedelta

from app.member_activity import MemberActivityManager


class DummyBot:
    loop = None

    def get_guild(self, guild_id):
        return None


def build_manager():
    return MemberActivityManager(
        get_db_connection=None,
        db_lock=None,
        require_managed_guild_id=lambda guild_id, context="": int(guild_id or 1),
        is_managed_guild_id=lambda guild_id: True,
        normalize_activity_timestamp=lambda value=None: value if isinstance(value, datetime) else datetime(2026, 3, 20, tzinfo=UTC),
        encrypt_member_activity_identity=lambda value: f"enc:{value}",
        decrypt_member_activity_identity=lambda value: value.removeprefix("enc:"),
        clip_text=lambda value, max_chars=120: str(value)[:max_chars],
        logger=None,
        bot=DummyBot(),
        enable_members_intent=False,
        member_activity_window_specs=[("last_24h", "Last 24 Hours", timedelta(days=1))],
        member_activity_web_top_limit=20,
        member_activity_recent_retention_days=90,
        has_moderator_access=lambda member: False,
        has_allowed_role=lambda member: False,
        moderator_role_ids=[1, 2],
        default_allowed_role_names={"Admin", "Employee"},
    )


def test_compute_member_activity_metrics_returns_expected_values():
    manager = build_manager()

    metrics = manager.compute_member_activity_metrics(
        48,
        3,
        datetime(2026, 3, 19, tzinfo=UTC),
        datetime(2026, 3, 20, tzinfo=UTC),
    )

    assert metrics["period_days"] == 1.0
    assert metrics["messages_per_day"] == 48.0
    assert metrics["messages_per_active_day"] == 16.0
    assert metrics["active_day_ratio"] == 1.0


def test_build_member_activity_window_record_preserves_counts():
    manager = build_manager()

    record = manager.build_member_activity_window_record(
        "last_24h",
        "Last 24 Hours",
        12,
        2,
        datetime(2026, 3, 19, tzinfo=UTC),
        datetime(2026, 3, 20, tzinfo=UTC),
        last_message_at="2026-03-20T12:00:00+00:00",
    )

    assert record["key"] == "last_24h"
    assert record["message_count"] == 12
    assert record["active_days"] == 2
    assert record["last_message_at"] == "2026-03-20T12:00:00+00:00"


def test_normalize_optional_role_id_filters_invalid_values():
    manager = build_manager()

    assert manager.normalize_optional_role_id("42") == 42
    assert manager.normalize_optional_role_id("0") is None
    assert manager.normalize_optional_role_id("abc") is None

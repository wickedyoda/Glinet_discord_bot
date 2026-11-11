import sqlite3
import zipfile
from datetime import UTC, datetime, timedelta
from io import BytesIO
from types import SimpleNamespace

from app.member_activity import MemberActivityManager


class DummyBot:
    loop = None

    def get_guild(self, guild_id):
        return None


class DummyLock:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class DummyRole:
    def __init__(self, role_id=1, name="Member"):
        self.id = role_id
        self.name = name


class DummyAuthor:
    def __init__(self, user_id=10, name="tester", display_name="Tester"):
        self.id = user_id
        self.name = name
        self.display_name = display_name
        self.bot = False
        self.roles = [DummyRole()]

    def __str__(self):
        return self.name


class DummyGuild:
    def __init__(self, guild_id=123):
        self.id = guild_id


class DummyMessage:
    def __init__(self, message_id: int, created_at: datetime):
        self.id = message_id
        self.created_at = created_at
        self.guild = DummyGuild()
        self.author = DummyAuthor()


def _normalize_timestamp(value=None):
    if isinstance(value, datetime):
        return value
    if isinstance(value, str) and value:
        return datetime.fromisoformat(value)
    return datetime(2026, 3, 20, tzinfo=UTC)


def build_manager(*, conn=None):
    connection = conn
    manager = MemberActivityManager(
        get_db_connection=lambda: connection,
        db_lock=DummyLock(),
        require_managed_guild_id=lambda guild_id, context="": int(guild_id or 1),
        is_managed_guild_id=lambda guild_id: True,
        normalize_activity_timestamp=_normalize_timestamp,
        encrypt_member_activity_identity=lambda value: f"enc:{value}",
        decrypt_member_activity_identity=lambda value: value.removeprefix("enc:"),
        clip_text=lambda value, max_chars=120: str(value)[:max_chars],
        logger=SimpleNamespace(warning=lambda *args, **kwargs: None, exception=lambda *args, **kwargs: None, debug=lambda *args, **kwargs: None),
        bot=DummyBot(),
        enable_members_intent=False,
        member_activity_window_specs=[("last_24h", "Last 24 Hours", timedelta(days=1))],
        member_activity_web_top_limit=20,
        member_activity_recent_retention_days=90,
    )
    if connection is not None:
        # Mirror production startup: ensure the schema once (initialize_storage
        # in bot.py) before any message activity is recorded.
        manager.ensure_member_activity_schema_locked(connection)
    return manager


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


def test_record_member_message_activity_updates_summary_and_hourly_counts():
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    manager = build_manager(conn=conn)

    first_dt = datetime(2026, 3, 24, 12, 0, tzinfo=UTC)
    second_dt = datetime(2026, 3, 24, 12, 5, tzinfo=UTC)
    assert manager.record_member_message_activity(DummyMessage(1001, first_dt)) is True
    assert manager.record_member_message_activity(DummyMessage(1002, second_dt)) is True

    summary_row = conn.execute(
        """
        SELECT total_messages, total_active_days, last_message_at
        FROM member_activity_summary
        WHERE guild_id = 123 AND user_id = 10
        """
    ).fetchone()
    hourly_row = conn.execute(
        """
        SELECT message_count, last_message_at
        FROM member_activity_recent_hourly
        WHERE guild_id = 123 AND user_id = 10
        """
    ).fetchone()

    assert int(summary_row["total_messages"]) == 2
    assert int(summary_row["total_active_days"]) == 1
    assert str(summary_row["last_message_at"]) == second_dt.isoformat()
    assert int(hourly_row["message_count"]) == 2
    assert str(hourly_row["last_message_at"]) == second_dt.isoformat()


def test_record_member_message_activity_increments_active_days_on_new_date():
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    manager = build_manager(conn=conn)

    first_dt = datetime(2026, 3, 24, 23, 55, tzinfo=UTC)
    second_dt = datetime(2026, 3, 25, 0, 5, tzinfo=UTC)
    assert manager.record_member_message_activity(DummyMessage(1001, first_dt)) is True
    assert manager.record_member_message_activity(DummyMessage(1002, second_dt)) is True

    summary_row = conn.execute(
        """
        SELECT total_messages, total_active_days
        FROM member_activity_summary
        WHERE guild_id = 123 AND user_id = 10
        """
    ).fetchone()

    assert int(summary_row["total_messages"]) == 2
    assert int(summary_row["total_active_days"]) == 2


def test_export_member_activity_archive_escapes_formula_like_cells():
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    manager = build_manager(conn=conn)

    manager.record_member_message_activity(
        DummyMessage(1001, datetime(2026, 3, 24, 12, 0, tzinfo=UTC))
    )
    conn.execute(
        """
        UPDATE member_activity_summary
        SET username = ?, display_name = ?
        WHERE guild_id = 123 AND user_id = 10
        """,
        ("enc:=cmd", "enc:+display"),
    )
    conn.commit()
    manager.build_member_activity_web_payload = lambda guild_id, role_id=None: {
        "ok": True,
        "guild_id": int(guild_id),
        "windows": [
            {
                "key": "last_24h",
                "label": "Last 24 Hours",
                "members": [
                    {
                        "rank": 1,
                        "user_id": 10,
                        "display_name": "+display",
                        "username": "=cmd",
                        "message_count": 1,
                        "active_days": 1,
                        "last_message_at": "2026-03-24T12:00:00+00:00",
                    }
                ],
            }
        ],
    }

    payload = manager.export_member_activity_archive(123)

    with zipfile.ZipFile(BytesIO(payload["data"])) as archive:
        summary_csv = archive.read("member_activity_summary.csv").decode("utf-8")
        window_csv = archive.read("last_24h.csv").decode("utf-8")

    assert "'=cmd" in summary_csv
    assert "'+display" in summary_csv
    assert "'+display" in window_csv

# ---------------------------------------------------------------------------
# Role eligibility: every role is monitored unless explicitly excluded
# ---------------------------------------------------------------------------

from unittest.mock import patch  # noqa: E402


class DummyMember:
    def __init__(self, role_ids=(7,), is_bot=False):
        self.bot = is_bot
        self.roles = [DummyRole(role_id=rid, name=f"role-{rid}") for rid in role_ids]


def _eligible(role_ids=(7,), excluded=(), role_id=None):
    manager = build_manager()
    manager.excluded_role_ids = tuple(excluded)
    with patch("discord.Member", DummyMember):
        return manager.is_member_activity_ranking_eligible(
            DummyMember(role_ids=role_ids), role_id=role_id
        )


def test_staff_role_is_monitored_by_default():
    """Employee used to be excluded because "allowed role" implied "not monitored"."""
    assert _eligible(role_ids=(7,)) is True


def test_a_role_is_excluded_only_when_configured():
    assert _eligible(role_ids=(7, 999), excluded=(999,)) is False
    # a different role in the exclusion list must not affect this member
    assert _eligible(role_ids=(7,), excluded=(999,)) is True


def test_bots_are_still_excluded():
    manager = build_manager()
    with patch("discord.Member", DummyMember):
        assert manager.is_member_activity_ranking_eligible(DummyMember(is_bot=True)) is False


def test_role_dropdown_filter_still_applies():
    """Selecting a role keeps only members holding it."""
    assert _eligible(role_ids=(7, 8), role_id=8) is True
    assert _eligible(role_ids=(7,), role_id=8) is False


def test_excluded_role_env_is_parsed_from_ids_and_mentions():
    import bot as bot_module

    parsed = bot_module.normalize_role_ids("111, 222")
    assert parsed == [111, 222]
    assert bot_module.normalize_role_ids("<@&333>") == [333]
    assert bot_module.normalize_role_ids("") == []


def test_excluded_role_constant_defaults_to_empty():
    import bot as bot_module

    assert isinstance(bot_module.MEMBER_ACTIVITY_EXCLUDED_ROLE_IDS, set)

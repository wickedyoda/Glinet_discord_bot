"""Staff moderation records: warnings, notes, history, and the schema that holds them."""

from __future__ import annotations

import sqlite3
import threading

import pytest

import bot as bot_module
from app.moderation_tools import (
    add_note,
    add_warning,
    build_history,
    clear_warnings,
    format_history_text,
    list_notes,
    list_warnings,
)

GUILD = 111
USER = 222


@pytest.fixture()
def db(tmp_path, monkeypatch):
    conn = sqlite3.connect(str(tmp_path / "moderation.db"))
    conn.row_factory = sqlite3.Row
    monkeypatch.setattr(bot_module, "db_connection", conn)
    monkeypatch.setattr(bot_module, "db_lock", threading.RLock())
    bot_module.ensure_db_schema()
    yield conn
    conn.close()


def test_a_warning_gets_a_case_number_and_shows_up(db):
    case_id = add_warning(db, GUILD, USER, reason="Spamming the help channel", moderator="mod#1")
    db.commit()

    assert case_id > 0
    warnings = list_warnings(db, GUILD, USER)
    assert len(warnings) == 1
    assert warnings[0]["case_id"] == case_id
    assert warnings[0]["reason"] == "Spamming the help channel"
    assert warnings[0]["moderator"] == "mod#1"
    assert warnings[0]["cleared_at"] == ""


def test_warnings_are_scoped_to_the_guild_and_member(db):
    add_warning(db, GUILD, USER, reason="one", moderator="mod")
    add_warning(db, GUILD, 999, reason="other member", moderator="mod")
    add_warning(db, 555, USER, reason="other guild", moderator="mod")
    db.commit()

    assert len(list_warnings(db, GUILD, USER)) == 1


def test_clearing_one_warning_leaves_the_others(db):
    first = add_warning(db, GUILD, USER, reason="first", moderator="mod")
    add_warning(db, GUILD, USER, reason="second", moderator="mod")
    db.commit()

    assert clear_warnings(db, GUILD, USER, case_id=first) == 1
    db.commit()

    active = list_warnings(db, GUILD, USER)
    assert [warning["reason"] for warning in active] == ["second"]
    assert len(list_warnings(db, GUILD, USER, include_cleared=True)) == 2


def test_clearing_everything_reports_the_count(db):
    add_warning(db, GUILD, USER, reason="a", moderator="mod")
    add_warning(db, GUILD, USER, reason="b", moderator="mod")
    db.commit()

    assert clear_warnings(db, GUILD, USER) == 2
    db.commit()
    assert list_warnings(db, GUILD, USER) == []
    assert clear_warnings(db, GUILD, USER) == 0


def test_notes_are_separate_from_warnings(db):
    add_note(db, GUILD, USER, note="Asked about a refund, handled", author="mod#1")
    db.commit()

    notes = list_notes(db, GUILD, USER)
    assert len(notes) == 1
    assert notes[0]["note"] == "Asked about a refund, handled"
    assert list_warnings(db, GUILD, USER) == [], "a note must never count as a warning"


def test_history_combines_warnings_notes_and_actions(db):
    add_warning(db, GUILD, USER, reason="warned once", moderator="mod")
    add_note(db, GUILD, USER, note="context", author="mod")
    db.execute(
        "INSERT INTO actions (guild_id, created_at, action, status, moderator, target, reason) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (GUILD, "2026-01-01T00:00:00+00:00", "timeout", "success", "mod", str(USER), "cool off"),
    )
    db.commit()

    history = build_history(db, GUILD, USER)

    assert history["active_warnings"] == 1
    assert len(history["warnings"]) == 1
    assert len(history["notes"]) == 1
    assert len(history["actions"]) == 1


def test_history_text_is_readable_and_bounded(db):
    for index in range(40):
        add_warning(db, GUILD, USER, reason=f"reason {index}", moderator="mod")
    db.commit()

    text = format_history_text(build_history(db, GUILD, USER))

    assert "Active warnings:** 40" in text
    assert len(text) <= 1900


def test_history_text_says_so_when_there_is_nothing(db):
    text = format_history_text(build_history(db, GUILD, USER))
    assert "Nothing recorded" in text


def test_schema_upgrades_a_legacy_warnings_table(tmp_path, monkeypatch):
    """An existing database gains the manual-warning columns without a rebuild."""
    legacy = sqlite3.connect(str(tmp_path / "legacy.db"))
    legacy.row_factory = sqlite3.Row
    legacy.executescript(
        """
        CREATE TABLE moderation_warnings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER NOT NULL DEFAULT 0,
            user_id INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            warning_type TEXT NOT NULL DEFAULT 'bad_word',
            matched_term TEXT NOT NULL DEFAULT '',
            message_excerpt TEXT NOT NULL DEFAULT '',
            action_taken TEXT NOT NULL DEFAULT ''
        );
        """
    )
    legacy.commit()

    monkeypatch.setattr(bot_module, "db_connection", legacy)
    bot_module.ensure_db_schema()

    columns = {row[1] for row in legacy.execute("PRAGMA table_info(moderation_warnings)")}
    assert {"reason", "moderator", "cleared_at", "case_id"} <= columns
    tables = {row[0] for row in legacy.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert "moderation_notes" in tables
    legacy.close()


# --- the case list and its export ---------------------------------------------


def test_case_list_filters_by_status_and_member(db):
    from app.moderation_cases import query_cases

    first = add_warning(db, GUILD, USER, reason="active one", moderator="mod")
    second = add_warning(db, GUILD, USER, reason="cleared one", moderator="mod")
    add_warning(db, GUILD, 999, reason="someone else", moderator="mod")
    clear_warnings(db, GUILD, USER, case_id=second)
    db.commit()

    active = query_cases(db, GUILD, {"status": "active"})
    assert active["total"] == 2, "one active for this member, one for the other"
    assert all(not case["cleared_at"] for case in active["cases"])

    cleared = query_cases(db, GUILD, {"status": "cleared"})
    assert [case["case_id"] for case in cleared["cases"]] == [second]

    mine = query_cases(db, GUILD, {"status": "all", "user_id": str(USER)})
    assert mine["total"] == 2
    assert {case["case_id"] for case in mine["cases"]} == {first, second}


def test_case_list_rejects_a_nonsense_member_id(db):
    from app.moderation_cases import query_cases

    add_warning(db, GUILD, USER, reason="x", moderator="mod")
    db.commit()

    payload = query_cases(db, GUILD, {"user_id": "not-a-number"})
    assert payload["ok"] is True
    assert payload["total"] == 0


def test_csv_export_has_a_header_and_one_row_per_case(db):
    from app.moderation_cases import cases_to_csv, query_cases

    add_warning(db, GUILD, USER, reason="spam, with a comma", moderator="mod#1")
    db.commit()

    payload = query_cases(db, GUILD, {"status": "all"})
    text = cases_to_csv(payload["cases"])
    lines = text.strip().splitlines()

    assert lines[0].startswith("case,user_id,created_at,type,reason")
    assert len(lines) == 2
    assert '"spam, with a comma"' in lines[1], "a comma in the reason must be quoted"


def test_the_cases_page_renders_and_exports(tmp_path):
    import sqlite3

    from tests.test_web_admin import _login, _make_app, _select_guild

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE moderation_warnings (
            id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER, user_id INTEGER,
            created_at TEXT, warning_type TEXT, matched_term TEXT, message_excerpt TEXT,
            action_taken TEXT, reason TEXT, moderator TEXT, cleared_at TEXT, case_id INTEGER
        );
        """
    )
    from datetime import UTC, datetime

    conn.execute(
        "INSERT INTO moderation_warnings (guild_id, user_id, created_at, warning_type, reason, moderator, cleared_at, case_id) "
        "VALUES (1234567890, 555, ?, 'manual', 'spamming', 'mod#1', '', 7)",
        (datetime.now(UTC).isoformat(),),
    )
    conn.commit()

    from app.moderation_cases import cases_to_csv, query_cases

    def get_cases(guild_id, filters):
        return query_cases(conn, int(guild_id), filters)

    def export_cases(guild_id, filters):
        payload = query_cases(conn, int(guild_id), {**filters, "page": 1}, page_size=10000)
        return {
            "ok": True,
            "filename": "cases.csv",
            "content_type": "text/csv",
            "data": cases_to_csv(payload["cases"]).encode(),
        }

    app = _make_app(tmp_path, moderation_cases=get_cases, moderation_cases_export=export_cases)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    page = client.get("/admin/cases?status=all", base_url="https://docker.example:8443")
    assert page.status_code == 200
    html = page.get_data(as_text=True)
    assert "#7" in html and "spamming" in html

    export = client.get("/admin/cases.csv?status=all", base_url="https://docker.example:8443")
    assert export.status_code == 200
    assert export.headers["Content-Type"].startswith("text/csv")
    assert b"spamming" in export.data

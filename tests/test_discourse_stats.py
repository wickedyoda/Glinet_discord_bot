"""Discourse statistics: ingest and the aggregate queries.

The HTTP layer is a fake fetch_json, so these run offline and assert the SQL, which
is where the mistakes live (category scoping, the handled/first-response windows).
"""
from __future__ import annotations

import sqlite3
import threading
from datetime import UTC, datetime, timedelta

import pytest

import bot
from app import discourse_stats

NOW = datetime(2026, 11, 13, 12, 0, tzinfo=UTC)




@pytest.fixture()
def db(tmp_path, monkeypatch):
    conn = sqlite3.connect(str(tmp_path / "discourse.db"))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    monkeypatch.setattr(bot, "db_connection", conn)
    monkeypatch.setattr(bot, "db_lock", threading.RLock())
    bot.ensure_db_schema()
    yield conn
    conn.close()


def _iso(days_ago: float = 0) -> str:
    return (NOW - timedelta(days=days_ago)).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _after(days_ago: float, plus_hours: float) -> str:
    """A timestamp `plus_hours` LATER than the `days_ago` mark.

    `_iso(days_ago=n, hours=h)` subtracted both, which silently put replies before
    the opening post and made every response-time assertion meaningless.
    """
    return (NOW - timedelta(days=days_ago) + timedelta(hours=plus_hours)).strftime(
        "%Y-%m-%dT%H:%M:%S.000Z"
    )


def test_schema_creates_the_discourse_tables(db):
    tables = {
        row["name"]
        for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }
    assert {"discourse_posts", "discourse_topics", "discourse_staff"} <= tables


def test_staff_roster_is_upserted_not_duplicated(db):
    def fetch(_path):
        return {"members": [{"username": "Alice"}, {"username": "bob"}]}

    first = discourse_stats.refresh_staff_roster(db, group="G", fetch_json=fetch)
    second = discourse_stats.refresh_staff_roster(db, group="G", fetch_json=fetch)

    assert first["members"] == 2
    assert second["members"] == 2
    rows = db.execute("SELECT username FROM discourse_staff ORDER BY username").fetchall()
    assert [r["username"] for r in rows] == ["alice", "bob"]


def test_topic_ingest_maps_categories(db):
    def fetch(path):
        if "page=0" not in path:
            return {"topic_list": {"topics": []}}
        if "/c/glrouter/" in path:
            return {"topic_list": {"topics": [
                {"id": 1, "created_at": _iso(days_ago=1)},
                {"id": 2, "created_at": _iso(days_ago=200)},
            ]}}
        if "/c/glkvm/" in path:
            return {"topic_list": {"topics": [{"id": 3, "created_at": _iso(days_ago=5)}]}}
        return {"topic_list": {"topics": []}}

    result = discourse_stats.ingest_topics(
        db, categories=("glrouter", "glkvm"), fetch_json=fetch,
        backfill_days=90, now=NOW, pace=None,
    )
    assert result["topics"] == 2  # the 200-day-old topic is outside the window
    rows = {r["topic_id"]: r["category_slug"]
            for r in db.execute("SELECT topic_id, category_slug FROM discourse_topics")}
    assert rows == {1: "glrouter", 3: "glkvm"}


def test_post_ingest_is_idempotent(db):
    def fetch(_path):
        return {"latest_posts": [
            {"id": 10, "topic_id": 1, "username": "alice", "post_number": 1,
             "created_at": _iso(days_ago=2)},
            {"id": 11, "topic_id": 1, "username": "bob", "post_number": 2,
             "created_at": _iso(days_ago=1)},
        ]}

    first = discourse_stats.ingest_posts(db, fetch_json=fetch, backfill_days=90, now=NOW, pace=None)
    second = discourse_stats.ingest_posts(db, fetch_json=fetch, backfill_days=90, now=NOW, pace=None)

    assert first["new_rows"] == 2
    assert second["new_rows"] == 0, "re-running the same window must not duplicate"
    assert db.execute("SELECT COUNT(*) c FROM discourse_posts").fetchone()["c"] == 2


def _seed(db, *, category="glrouter"):
    db.execute("INSERT INTO discourse_staff VALUES ('alice', '', '')")
    db.execute("INSERT INTO discourse_staff VALUES ('bob', '', '')")
    db.execute("INSERT INTO discourse_topics VALUES (1, ?, ?)", (category, _iso(days_ago=3)))
    db.execute("INSERT INTO discourse_topics VALUES (2, ?, ?)", (category, _iso(days_ago=2)))
    rows = [
        # topic 1: member opens, alice answers 2h later, bob replies 5h after the OP
        (100, 1, "someone", 1, _iso(days_ago=3)),
        (101, 1, "alice", 2, _after(3, 2)),
        (102, 1, "bob", 3, _after(3, 5)),
        # topic 2: member opens, bob answers 30h later
        (103, 2, "someone", 1, _iso(days_ago=2)),
        (104, 2, "bob", 2, _after(2, 30)),
    ]
    db.executemany(
        "INSERT INTO discourse_posts (post_id, topic_id, username, post_number, created_at, ingested_at)"
        " VALUES (?,?,?,?,?,'')", rows,
    )
    db.commit()


def test_staff_activity_counts_posts_topics_and_handled(db):
    _seed(db)
    rows = {r["username"]: r for r in
            discourse_stats.staff_activity(db, days=90, now=NOW)}
    assert rows["alice"]["posts"] == 1
    assert rows["alice"]["handled"] == 1  # first staff reply in topic 1
    assert rows["bob"]["posts"] == 2
    assert rows["bob"]["handled"] == 1    # first staff reply in topic 2
    assert rows["bob"]["topics"] == 2


def test_category_scope_excludes_other_categories(db):
    _seed(db, category="feedback")  # same data, but filed under a category not queried
    rows = discourse_stats.staff_activity(
        db, days=90, categories=("glrouter",), now=NOW
    )
    assert rows == []

    unfiltered = discourse_stats.staff_activity(db, days=90, now=NOW)
    assert unfiltered, "the same rows must be visible when no scope is applied"


def test_first_response_is_measured_from_the_opening_post(db):
    _seed(db)
    gaps = sorted(discourse_stats.response_gaps(db, days=90, now=NOW))
    # topic 1: 2h = 7200s (alice). topic 2: 30h = 108000s (bob).
    assert gaps == [7200, 108000]

    per_user = {r["username"]: r for r in
                discourse_stats.first_response_seconds(db, days=90, now=NOW)}
    assert per_user["alice"]["handled"] == 1
    assert per_user["alice"]["mean_seconds"] == 7200


def test_daily_hourly_weekly_buckets(db):
    _seed(db)
    daily = {r["bucket"]: r["posts"] for r in discourse_stats.daily_counts(db, days=90, now=NOW)}
    # alice + bob reply on the day the first topic was opened; bob's second reply is
    # 30h later and may fall on the following day, so assert the total and spread.
    assert sum(daily.values()) == 3
    assert daily[_iso(days_ago=3)[:10]] == 2

    hourly = {int(r["bucket"]): r["posts"] for r in
              discourse_stats.hourly_counts(db, days=90, now=NOW)}
    assert sum(hourly.values()) == 3

    weekly = discourse_stats.weekly_counts(db, days=90, now=NOW)
    assert sum(r["posts"] for r in weekly) == 3

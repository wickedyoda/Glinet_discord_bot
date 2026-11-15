"""The forum query page: filter normalisation, the SQL, and the rendering."""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from pathlib import Path

import pytest

from app import discourse_query
from app.discourse_query import build_discourse_query, normalize_filters
from app.web_discourse_query import render_discourse_query_body
from tests.test_web_admin import _login, _make_app, _select_guild

BASE_URL = "https://docker.example:8443"


@pytest.fixture()
def archive():
    """Three topics, two staff, one month apart -- enough to tell filters apart."""
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE discourse_posts (
            post_id INTEGER PRIMARY KEY,
            topic_id INTEGER,
            username TEXT,
            post_number INTEGER,
            created_at TEXT
        );
        CREATE TABLE discourse_topics (
            topic_id INTEGER PRIMARY KEY,
            category_slug TEXT,
            created_at TEXT
        );
        CREATE TABLE discourse_staff (
            username TEXT PRIMARY KEY,
            first_seen_at TEXT,
            last_seen_at TEXT
        );
        """
    )
    conn.executemany(
        "INSERT INTO discourse_topics VALUES (?,?,?)",
        [
            (1, "glrouter", "2026-01-01T00:00:00.000Z"),
            (2, "iot", "2026-01-02T00:00:00.000Z"),
            (3, "glrouter", "2026-02-01T00:00:00.000Z"),
        ],
    )
    conn.executemany(
        "INSERT INTO discourse_posts VALUES (?,?,?,?,?)",
        [
            (10, 1, "alice", 1, "2026-01-01T00:00:00.000Z"),
            (11, 1, "bob", 2, "2026-01-01T02:00:00.000Z"),
            (12, 1, "bob", 3, "2026-01-01T05:00:00.000Z"),
            (13, 2, "carol", 1, "2026-01-02T00:00:00.000Z"),
            (14, 2, "bob", 2, "2026-01-02T00:30:00.000Z"),
            (15, 3, "carol", 1, "2026-02-01T00:00:00.000Z"),
        ],
    )
    conn.executemany(
        "INSERT INTO discourse_staff VALUES (?,?,?)",
        [("bob", "2026-01-01", "2026-02-01"), ("dana", "2026-01-01", "2026-02-01")],
    )
    conn.commit()
    return conn


# --- filters ------------------------------------------------------------------


def test_filters_default_to_the_last_thirty_days():
    filters = normalize_filters({})
    assert filters["date_to"] - filters["date_from"] == timedelta(days=30)
    assert filters["staff_only"] is False
    assert filters["page"] == 1


def test_dates_are_swapped_when_they_are_backwards():
    filters = normalize_filters({"from": "2026-03-01", "to": "2026-01-01"})
    assert filters["date_from"] == date(2026, 1, 1)
    assert filters["date_to"] == date(2026, 3, 1)


def test_an_absurd_range_is_clamped():
    filters = normalize_filters({"from": "1990-01-01", "to": "2026-01-01"})
    assert (filters["date_to"] - filters["date_from"]).days == discourse_query.MAX_RANGE_DAYS


def test_garbage_dates_fall_back_instead_of_raising():
    filters = normalize_filters({"from": "not-a-date", "to": "2026-13-45"})
    assert filters["date_from"] <= filters["date_to"]


def test_staff_only_accepts_the_shapes_the_form_sends():
    assert normalize_filters({"staff_only": "1"})["staff_only"] is True
    assert normalize_filters({"staff_only": "on"})["staff_only"] is True
    assert normalize_filters({"staff_only": ""})["staff_only"] is False


# --- the query ----------------------------------------------------------------


def test_summary_counts_posts_topics_and_people(archive):
    payload = build_discourse_query(archive, {"from": "2026-01-01", "to": "2026-01-31"}, "")
    assert payload["ok"] is True
    assert payload["summary"]["posts"] == 5
    assert payload["summary"]["topics"] == 2
    assert payload["summary"]["people"] == 3


def test_category_filter_narrows_the_result(archive):
    payload = build_discourse_query(
        archive, {"from": "2026-01-01", "to": "2026-02-28", "category": "iot"}, ""
    )
    assert payload["summary"]["posts"] == 2
    assert [topic["topic_id"] for topic in payload["topics"]] == [2]


def test_username_filter_and_the_user_card(archive):
    payload = build_discourse_query(
        archive, {"from": "2026-01-01", "to": "2026-02-28", "username": "bob"}, ""
    )
    card = payload["user_card"]
    assert card["posts"] == 3
    assert card["threads_started"] == 0
    assert card["replies"] == 3
    assert card["answered_topics"] == 2
    # 2h in topic 1 and 30m in topic 2 -> median 1h 15m
    assert card["median_seconds"] == 75 * 60


def test_staff_only_excludes_everyone_else(archive):
    payload = build_discourse_query(
        archive, {"from": "2026-01-01", "to": "2026-02-28", "staff_only": "1"}, ""
    )
    assert [row["username"] for row in payload["users"]] == ["bob"]


def test_users_are_flagged_as_staff(archive):
    payload = build_discourse_query(archive, {"from": "2026-01-01", "to": "2026-02-28"}, "")
    flags = {row["username"]: row["is_staff"] for row in payload["users"]}
    assert flags == {"alice": False, "bob": True, "carol": False}


def test_topic_urls_point_at_the_forum(archive):
    payload = build_discourse_query(
        archive, {"from": "2026-01-01", "to": "2026-01-31"}, "https://community.example.com/"
    )
    assert all(topic["url"].startswith("https://community.example.com/t/") for topic in payload["topics"])


def test_topics_are_paged(archive, monkeypatch):
    monkeypatch.setattr(discourse_query, "TOPIC_PAGE_SIZE", 2)
    first = build_discourse_query(archive, {"from": "2026-01-01", "to": "2026-02-28", "page": 1}, "")
    second = build_discourse_query(archive, {"from": "2026-01-01", "to": "2026-02-28", "page": 2}, "")

    assert first["summary"]["pages"] == 2
    assert len(first["topics"]) == 2
    assert len(second["topics"]) == 1
    assert {t["topic_id"] for t in first["topics"]}.isdisjoint({t["topic_id"] for t in second["topics"]})


def test_an_empty_range_still_answers(archive):
    payload = build_discourse_query(archive, {"from": "2020-01-01", "to": "2020-01-31"}, "")
    assert payload["ok"] is True
    assert payload["summary"]["posts"] == 0
    assert payload["topics"] == []
    assert payload["user_card"] is None


def test_the_connection_row_factory_is_left_as_it_was(archive):
    archive.row_factory = None
    build_discourse_query(archive, {}, "")
    assert archive.row_factory is None


# --- the page -----------------------------------------------------------------


def test_page_renders_form_and_results(tmp_path: Path):
    archive = sqlite3.connect(":memory:")
    archive.executescript(
        "CREATE TABLE discourse_posts (post_id INTEGER, topic_id INTEGER, username TEXT, post_number INTEGER, created_at TEXT);"
        "CREATE TABLE discourse_topics (topic_id INTEGER, category_slug TEXT, created_at TEXT);"
        "CREATE TABLE discourse_staff (username TEXT, first_seen_at TEXT, last_seen_at TEXT);"
    )
    archive.execute("INSERT INTO discourse_topics VALUES (99,'glrouter','2026-01-01T00:00:00.000Z')")
    archive.execute("INSERT INTO discourse_posts VALUES (1,99,'alice',1,'2026-01-01T00:00:00.000Z')")
    archive.commit()

    def query(filters):
        return build_discourse_query(archive, filters, "https://community.example.com")

    app = _make_app(tmp_path, discourse_query_payload=query)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    response = client.get("/admin/discourse-query?from=2026-01-01&to=2026-01-31", base_url=BASE_URL)

    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "Query the archive" in html
    assert "alice" in html
    assert "https://community.example.com/t/99" in html
    assert "Run query" in html


def test_page_reports_a_missing_callback(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    response = client.get("/admin/discourse-query", base_url=BASE_URL)

    assert response.status_code == 200
    assert "not configured" in response.get_data(as_text=True)


def test_renderer_escapes_usernames():
    payload = {
        "ok": True,
        "filters": {"from": "2026-01-01", "to": "2026-01-02", "page": 1},
        "summary": {"posts": 1, "topics": 1, "people": 1, "pages": 1},
        "users": [
            {
                "username": "<script>alert(1)</script>",
                "posts": 1,
                "topics": 1,
                "last_post": "2026-01-01 00:00",
                "is_staff": False,
            }
        ],
        "topics": [],
        "user_card": None,
        "options": {"categories": [], "staff": []},
    }
    html = render_discourse_query_body(payload, "Guild")
    assert "<script>alert(1)</script>" not in html
    assert "&lt;script&gt;" in html


def test_reply_delay_uses_the_earliest_reply_not_the_first_in_range(archive):
    """A reply today to a thread answered months ago is not a months-long response time."""
    archive.execute(
        "INSERT INTO discourse_posts VALUES (16,1,'bob',4,'2026-01-05T10:00:00.000Z')"
    )
    archive.commit()

    payload = build_discourse_query(
        archive,
        {"from": "2026-01-05", "to": "2026-01-05", "username": "bob"},
        "",
    )

    card = payload["user_card"]
    assert card["posts"] == 1, "only the in-range post counts as activity"
    # Topic 1 was created 2026-01-01T00:00 and bob first replied at 02:00 that day.
    assert card["median_seconds"] == 2 * 3600


def test_answered_topics_counts_a_topic_without_metadata(archive):
    """A post can exist without a topics row; it still counts as a topic answered."""
    archive.execute(
        "INSERT INTO discourse_posts VALUES (17,99,'bob',2,'2026-01-03T00:00:00.000Z')"
    )
    archive.commit()

    payload = build_discourse_query(
        archive,
        {"from": "2026-01-03", "to": "2026-01-03", "username": "bob"},
        "",
    )

    card = payload["user_card"]
    assert card["replies"] == 1
    assert card["answered_topics"] == 1, "a topic with no metadata still counts as answered"
    assert card["median_seconds"] == 0, "no topic timestamp means no delay to report"

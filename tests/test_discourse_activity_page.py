"""The Discourse activity page: payload in, readable page out.

The renderer is pure, so most of this drives it with a fixture. The route tests go
through the real Flask app to prove the callback is wired and the window parameter
actually reaches the payload builder.
"""

from __future__ import annotations

import sqlite3
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from bs4 import BeautifulSoup

import bot
from app.web_discourse_activity import render_discourse_activity_body
from app.web_permissions import ADMIN_ONLY_PENDING_MAPPING
from tests.test_web_admin import _login, _make_app, _select_guild

BASE_URL = "https://docker.example:8443"


def _payload(**overrides):
    payload = {
        "ok": True,
        "days": 90,
        "categories": ["glrouter"],
        "staff_total": 3,
        "staff_active": 2,
        "posts_stored": 120,
        "staff_posts": 46,
        "activity": [
            {"username": "alice", "posts": 30, "topics": 12, "handled": 9},
            {"username": "bob", "posts": 16, "topics": 7, "handled": 5},
        ],
        "responses": [
            {"username": "alice", "mean_seconds": 5400},
            {"username": "bob", "mean_seconds": 90000},
        ],
        "daily": [{"bucket": "2026-03-15", "posts": 8}],
        "hourly": [{"bucket": 9, "posts": 12}, {"bucket": 3, "posts": 2}],
        "weekly": [{"bucket": "2026-W11", "posts": 20}],
        "answered": 14,
        "median_seconds": 7200,
        "p90_seconds": 172800,
        "within_24h": 11,
    }
    payload.update(overrides)
    return payload


def _activity_soup(tmp_path: Path, payload=None):
    fixture = payload if payload is not None else _payload()
    app = _make_app(
        tmp_path,
        discourse_activity_payload=lambda days=90: fixture,
    )
    client = app.test_client()
    _login(client)
    _select_guild(client)
    response = client.get("/admin/discourse-activity", base_url=BASE_URL)
    assert response.status_code == 200
    return BeautifulSoup(response.get_data(as_text=True), "html.parser")


def test_page_renders_staff_rows_and_coverage_numbers(tmp_path: Path):
    soup = _activity_soup(tmp_path)
    text = soup.get_text(" ", strip=True)

    assert "Discourse activity" in text
    assert "alice" in text and "bob" in text
    # 11 of 14 answered within a day -> 79%
    assert "79%" in text
    assert "2.0h" in text  # median 7200s
    assert "2.0d" in text  # p90 172800s


def test_page_states_the_caveats_the_numbers_carry(tmp_path: Path):
    soup = _activity_soup(tmp_path)
    text = soup.get_text(" ", strip=True)

    assert "first staff reply" in text
    assert "live snapshot" in text
    assert "no resolution signal" in text


def test_unavailable_payload_renders_an_error_instead_of_a_500(tmp_path: Path):
    soup = _activity_soup(tmp_path, {"ok": False, "error": "statistics store is empty"})
    assert "statistics store is empty" in soup.get_text(" ", strip=True)


def test_empty_store_renders_the_empty_state(tmp_path: Path):
    payload = _payload(
        staff_active=0,
        staff_posts=0,
        activity=[],
        responses=[],
        daily=[],
        hourly=[],
        weekly=[],
        answered=0,
        median_seconds=0,
        p90_seconds=0,
        within_24h=0,
    )
    soup = _activity_soup(tmp_path, payload)
    text = soup.get_text(" ", strip=True)

    assert "No staff posts stored" in text
    assert "No weekly data yet" in text
    assert "0%" in text


def test_window_filter_reaches_the_payload_builder(tmp_path: Path):
    seen = {}

    def payload(days=90):
        seen["days"] = days
        return _payload(days=days)

    app = _make_app(tmp_path, discourse_activity_payload=payload)
    client = app.test_client()
    _login(client)
    _select_guild(client)
    response = client.get("/admin/discourse-activity?days=180", base_url=BASE_URL)

    assert response.status_code == 200
    assert seen == {"days": 180}
    assert "value='180' selected" in response.get_data(as_text=True)


def test_unknown_window_falls_back_to_the_default(tmp_path: Path):
    seen = {}

    def payload(days=90):
        seen["days"] = days
        return _payload(days=days)

    app = _make_app(tmp_path, discourse_activity_payload=payload)
    client = app.test_client()
    _login(client)
    _select_guild(client)
    response = client.get("/admin/discourse-activity?days=13", base_url=BASE_URL)

    assert response.status_code == 200
    assert seen == {"days": 90}
    assert "value='90' selected" in response.get_data(as_text=True)


def test_dashboard_card_sits_in_the_support_section(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)
    response = client.get("/admin/dashboard", base_url=BASE_URL)
    assert response.status_code == 200
    soup = BeautifulSoup(response.get_data(as_text=True), "html.parser")

    def section_containing(heading: str):
        for section in soup.find_all("section", class_="dashboard-section"):
            h3 = section.find("h3")
            if h3 and h3.get_text(strip=True) == heading:
                return section
        return None

    support = section_containing("🎫 Support & Forum Integrations")
    assert support is not None
    assert "Discourse Activity" in support.get_text(" ", strip=True)
    assert "/admin/discourse-activity" in support.decode()

    for heading in ("📊 User Activity & Statistics", "📡 Notification Feeds & Monitors"):
        other = section_containing(heading)
        assert other is not None
        assert "Discourse Activity" not in other.get_text(" ", strip=True)


def test_nav_offers_the_page_in_both_selects(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)
    response = client.get("/admin/dashboard", base_url=BASE_URL)
    soup = BeautifulSoup(response.get_data(as_text=True), "html.parser")

    for select_id in ("desktop-nav-page-select", "mobile-nav-page-select"):
        select = soup.find("select", id=select_id)
        assert select is not None
        labels = [option.get_text(strip=True) for option in select.find_all("option")]
        assert "Discourse Activity" in labels

    support_group = None
    for optgroup in soup.find("select", id="desktop-nav-page-select").find_all("optgroup"):
        if optgroup.get("label") == "Support & Integrations":
            support_group = [o.get_text(strip=True) for o in optgroup.find_all("option")]
    assert support_group == sorted(support_group, key=str.casefold)


def test_page_is_documented_as_admin_only():
    """Read-only, but not yet mapped to a role area -- mapping it would be a grant."""
    assert "discourse_activity_page" in ADMIN_ONLY_PENDING_MAPPING


def test_renderer_escapes_user_controlled_text():
    payload = _payload(
        activity=[{"username": "<script>alert(1)</script>", "posts": 1, "topics": 0, "handled": 0}],
        responses=[],
    )
    html = render_discourse_activity_body(payload)
    assert "<script>alert(1)</script>" not in html
    assert "&lt;script&gt;" in html


@pytest.fixture()
def discourse_db(tmp_path, monkeypatch):
    conn = sqlite3.connect(str(tmp_path / "discourse.db"))
    conn.row_factory = sqlite3.Row
    monkeypatch.setattr(bot, "db_connection", conn)
    monkeypatch.setattr(bot, "db_lock", threading.RLock())
    bot.ensure_db_schema()
    yield conn
    conn.close()


def _ago(hours: float) -> str:
    return (datetime.now(UTC) - timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _seed_windows(db):
    db.execute("INSERT INTO discourse_staff VALUES ('alice', '', '')")
    db.execute("INSERT INTO discourse_staff VALUES ('bob', '', '')")
    db.execute("INSERT INTO discourse_topics VALUES (1, 'glrouter', ?)", (_ago(50),))
    db.execute("INSERT INTO discourse_topics VALUES (2, 'glrouter', ?)", (_ago(40),))
    db.executemany(
        "INSERT INTO discourse_posts (post_id, topic_id, username, post_number, created_at, ingested_at)"
        " VALUES (?,?,?,?,?,'')",
        [
            (100, 1, "someone", 1, _ago(50)),
            (101, 1, "alice", 2, _ago(48)),  # answered 2h after the opening post
            (103, 2, "someone", 1, _ago(40)),
            (104, 2, "bob", 2, _ago(10)),  # answered 30h after the opening post
        ],
    )
    db.commit()


def test_payload_builder_reads_the_stored_rows(discourse_db):
    _seed_windows(discourse_db)

    payload = bot.build_discourse_activity_payload(days=90)

    assert payload["ok"] is True
    assert payload["staff_total"] == 2
    assert payload["staff_active"] == 2
    assert payload["staff_posts"] == 2
    assert payload["answered"] == 2
    assert payload["within_24h"] == 1
    # nearest-rank over [7200, 108000]: index int(2 * 0.5) == 1
    assert payload["median_seconds"] == 108000
    assert payload["p90_seconds"] == 108000
    assert payload["posts_stored"] == 4


def test_payload_builder_clamps_the_window(discourse_db):
    assert bot.build_discourse_activity_payload(days="nonsense")["days"] == 90
    assert bot.build_discourse_activity_payload(days=0)["days"] == 1
    assert bot.build_discourse_activity_payload(days=9999)["days"] == 365


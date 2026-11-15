"""The Activity section: its option model, its overview, and its save path."""

from __future__ import annotations

from pathlib import Path

import pytest

from app.activity_options import (
    ACTIVITY_OPTIONS,
    OPTION_KEYS,
    OPTIONS_BY_KEY,
    OptionValueError,
    effective_value,
    normalize_option_value,
    resolve_activity_options,
    validate_submission,
)
from tests.test_web_admin import _extract_csrf_token, _login, _make_app, _select_guild

BASE_URL = "https://docker.example:8443"


# --- the option model ---------------------------------------------------------


def test_every_option_has_a_unique_key_and_a_default():
    assert len(OPTION_KEYS) == len(set(OPTION_KEYS))
    for option in ACTIVITY_OPTIONS:
        assert option.key.isupper()
        assert option.label
        assert option.group
        assert option.help_text


def test_role_ids_are_parsed_strictly_and_deduplicated():
    option = OPTIONS_BY_KEY["MEMBER_ACTIVITY_EXCLUDED_ROLE_IDS"]
    assert normalize_option_value(option, " 42, 42,7 ") == "42,7"
    with pytest.raises(OptionValueError):
        normalize_option_value(option, "42,staff")


def test_integers_are_bounded_and_blank_means_default():
    option = OPTIONS_BY_KEY["MEMBER_ACTIVITY_WEB_TOP_LIMIT"]
    assert normalize_option_value(option, "") == "20"
    assert normalize_option_value(option, "35") == "35"
    with pytest.raises(OptionValueError):
        normalize_option_value(option, "0")
    with pytest.raises(OptionValueError):
        normalize_option_value(option, "500")


def test_booleans_normalise_to_true_or_false():
    option = OPTIONS_BY_KEY["DISCOURSE_STATS_ENABLED"]
    assert normalize_option_value(option, "yes") == "true"
    assert normalize_option_value(option, "") == "false"
    with pytest.raises(OptionValueError):
        normalize_option_value(option, "maybe")


def test_schedule_must_be_a_valid_cron_expression():
    option = OPTIONS_BY_KEY["DISCOURSE_STATS_SCHEDULE"]
    assert normalize_option_value(option, "  15   */6 * * *  ") == "15 */6 * * *"
    with pytest.raises(OptionValueError):
        normalize_option_value(option, "every six hours")


def test_categories_are_lowercased_and_trimmed():
    option = OPTIONS_BY_KEY["DISCOURSE_STATS_CATEGORIES"]
    assert normalize_option_value(option, " GLRouter , glkvm ,, ") == "glrouter,glkvm"


def test_effective_value_prefers_the_configured_value():
    option = OPTIONS_BY_KEY["DISCOURSE_STATS_GROUP"]
    assert effective_value(option, {}) == "GL.iNet_Staff"
    assert effective_value(option, {"DISCOURSE_STATS_GROUP": "Staff"}) == "Staff"
    assert effective_value(option, {"DISCOURSE_STATS_GROUP": "  "}) == "GL.iNet_Staff"


def test_validation_reports_every_bad_key_at_once():
    values, errors = validate_submission(
        {
            "MEMBER_ACTIVITY_EXCLUDED_ROLE_IDS": "12,nope",
            "MEMBER_ACTIVITY_WEB_TOP_LIMIT": "999",
            "DISCOURSE_STATS_ENABLED": "true",
        }
    )
    assert "DISCOURSE_STATS_ENABLED" in values
    assert len(errors) == 2
    assert any("Excluded role IDs" in error for error in errors)
    assert any("Rows per ranking" in error for error in errors)


def test_resolution_marks_defaults_and_hides_secrets():
    resolved = resolve_activity_options(
        {"DISCOURSE_STATS_CATEGORIES": "glrouter", "MEMBER_ACTIVITY_WEB_TOP_LIMIT": "10"}
    )
    by_key = {item["key"]: item for item in resolved}
    assert by_key["DISCOURSE_STATS_CATEGORIES"]["configured"] is True
    assert by_key["DISCOURSE_STATS_CATEGORIES"]["value"] == "glrouter"
    assert by_key["MEMBER_ACTIVITY_WEB_TOP_LIMIT"]["value"] == "10"
    assert by_key["DISCOURSE_STATS_GROUP"]["configured"] is False
    assert by_key["DISCOURSE_STATS_GROUP"]["value"] == "GL.iNet_Staff"


# --- the pages ----------------------------------------------------------------


def _client(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)
    return client


def _post_options(client, data):
    page = client.get("/admin/activity/options", base_url=BASE_URL)
    token = _extract_csrf_token(page)
    return client.post(
        "/admin/activity/options",
        data=data,
        base_url=BASE_URL,
        headers={"X-CSRF-Token": token},
        follow_redirects=True,
    )


def test_activity_overview_shows_both_features(tmp_path: Path):
    client = _client(tmp_path)

    response = client.get("/admin/activity", base_url=BASE_URL)

    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "Discord activity" in html
    assert "Forum activity" in html
    assert "/admin/member-activity" in html
    assert "/admin/discourse-activity" in html
    assert "/admin/activity/options" in html


def test_activity_options_page_lists_every_option(tmp_path: Path):
    client = _client(tmp_path)

    response = client.get("/admin/activity/options", base_url=BASE_URL)

    assert response.status_code == 200
    html = response.get_data(as_text=True)
    for key in OPTION_KEYS:
        assert f"name='{key}'" in html, f"{key} is missing its input"
    assert "Applies immediately" in html
    assert "Takes effect after a restart" in html


def test_activity_options_save_writes_the_env_file(tmp_path: Path):
    client = _client(tmp_path)
    env_file = tmp_path / "env.env"

    data = {key: "" for key in OPTION_KEYS}
    data.update(
        {
            "activity_options": "1",
            "confirm": "yes",
            "MEMBER_ACTIVITY_WEB_TOP_LIMIT": "25",
            "MEMBER_ACTIVITY_EXCLUDED_ROLE_IDS": "42, 43",
            "DISCOURSE_STATS_ENABLED": "true",
            "DISCOURSE_STATS_CATEGORIES": "GLRouter, iot",
            "DISCOURSE_STATS_SCHEDULE": "15 */6 * * *",
            "DISCOURSE_STATS_GROUP": "GL.iNet_Staff",
            "DISCOURSE_STATS_BACKFILL_DAYS": "90",
            "DISCOURSE_STATS_FULL_RECONCILE_HOURS": "24",
            "MEMBER_ACTIVITY_RECENT_RETENTION_DAYS": "200",
        }
    )
    response = _post_options(client, data)

    assert response.status_code == 200
    assert "Activity options saved" in response.get_data(as_text=True)
    written = env_file.read_text()
    assert "MEMBER_ACTIVITY_WEB_TOP_LIMIT=25" in written
    assert "MEMBER_ACTIVITY_EXCLUDED_ROLE_IDS=42,43" in written
    assert "DISCOURSE_STATS_CATEGORIES=glrouter,iot" in written
    # The pre-existing settings must survive a partial form.
    assert "WEB_ENFORCE_CSRF=false" in written


def test_activity_options_refuse_a_bad_role_id(tmp_path: Path):
    client = _client(tmp_path)
    env_file = tmp_path / "env.env"
    before = env_file.read_text()

    data = {key: "" for key in OPTION_KEYS}
    data.update(
        {
            "activity_options": "1",
            "confirm": "yes",
            "MEMBER_ACTIVITY_EXCLUDED_ROLE_IDS": "42,not-a-role",
        }
    )
    response = _post_options(client, data)

    assert response.status_code == 200
    assert "Not saved." in response.get_data(as_text=True)
    assert env_file.read_text() == before


def test_activity_options_require_the_confirmation_box(tmp_path: Path):
    client = _client(tmp_path)
    env_file = tmp_path / "env.env"
    before = env_file.read_text()

    data = {key: "" for key in OPTION_KEYS}
    data["activity_options"] = "1"
    response = _post_options(client, data)

    assert response.status_code == 200
    assert "Tick the confirmation box" in response.get_data(as_text=True)
    assert env_file.read_text() == before


def test_activity_page_needs_a_selected_guild(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)

    response = client.get("/admin/activity", base_url=BASE_URL, follow_redirects=False)

    assert response.status_code in {302, 303}


# --- the PR review's findings, each pinned ------------------------------------


def test_schema_upgrades_a_legacy_member_activity_table(tmp_path, monkeypatch):
    """The guild-scoped index must be created after the rebuild that adds guild_id."""
    import sqlite3

    import bot as bot_module

    legacy = sqlite3.connect(str(tmp_path / "legacy.db"))
    legacy.row_factory = sqlite3.Row
    legacy.executescript(
        """
        CREATE TABLE member_activity_summary (
            user_id INTEGER PRIMARY KEY,
            username TEXT NOT NULL DEFAULT '',
            display_name TEXT NOT NULL DEFAULT '',
            first_message_at TEXT NOT NULL,
            last_message_at TEXT NOT NULL,
            total_messages INTEGER NOT NULL DEFAULT 0,
            total_active_days INTEGER NOT NULL DEFAULT 0
        );
        """
    )
    legacy.commit()

    monkeypatch.setattr(bot_module, "db_connection", legacy)
    bot_module.ensure_db_schema()

    columns = {row[1] for row in legacy.execute("PRAGMA table_info(member_activity_summary)")}
    indexes = {row[1] for row in legacy.execute("PRAGMA index_list(member_activity_summary)")}
    assert "guild_id" in columns, "the legacy table was not rebuilt"
    assert "idx_member_activity_summary_guild_total" in indexes
    legacy.close()


def test_env_int_setting_keeps_the_default_when_the_value_is_unusable(monkeypatch):
    import bot as bot_module

    monkeypatch.setenv("HERMES_TEST_INT", "35")
    assert bot_module._env_int_setting("HERMES_TEST_INT", 20, minimum=1, maximum=100) == 35

    for bad in ("0", "500", "abc", ""):
        monkeypatch.setenv("HERMES_TEST_INT", bad)
        assert bot_module._env_int_setting("HERMES_TEST_INT", 20, minimum=1, maximum=100) == 20


def test_ranking_limit_and_retention_are_read_from_the_environment():
    source = (Path(__file__).resolve().parent.parent / "bot.py").read_text()
    assert 'MEMBER_ACTIVITY_WEB_TOP_LIMIT", 20, minimum=1, maximum=100' in source
    assert 'MEMBER_ACTIVITY_RECENT_RETENTION_DAYS", 200, minimum=7' in source


def test_the_full_reconcile_interval_is_configured(monkeypatch):
    import bot as bot_module

    monkeypatch.setattr(bot_module, "DISCOURSE_STATS_FULL_RECONCILE_HOURS", 2)
    monkeypatch.setattr(
        bot_module,
        "db_kv_get",
        lambda key: "2026-01-01T00:00:00+00:00",
        raising=False,
    )
    # The stored mark is far in the past, so it is due under any interval.
    assert bot_module._discourse_stats_full_ingest_due() is True


def test_a_superscript_digit_is_a_validation_error_not_a_crash():
    option = OPTIONS_BY_KEY["MEMBER_ACTIVITY_EXCLUDED_ROLE_IDS"]
    with pytest.raises(OptionValueError):
        normalize_option_value(option, "42,\u00b2")


def test_the_around_index_is_not_hardcoded_any_more():
    source = (Path(__file__).resolve().parent.parent / "bot.py").read_text()
    assert "DISCOURSE_STATS_FULL_RECONCILE_HOURS = _env_int_setting(" in source
    assert "DISCOURSE_STATS_FULL_RECONCILE_HOURS = 24" not in source


def test_two_members_with_the_same_display_name_stay_separate():
    from app.web_activity import summarize_member_activity

    payload = {
        "ok": True,
        "top_limit": 20,
        "excluded_role_ids": [],
        "windows": [
            {
                "key": "last_180_days",
                "label": "Last 180 Days",
                "members": [
                    {"user_id": 1, "display_name": "Alex", "message_count": 50, "active_days": 5},
                    {"user_id": 2, "display_name": "Alex", "message_count": 30, "active_days": 3},
                ],
            },
            {
                "key": "last_7_days",
                "label": "Last 7 Days",
                "members": [
                    {"user_id": 1, "display_name": "Alex", "message_count": 10, "active_days": 2},
                ],
            },
        ],
    }

    summary = summarize_member_activity(payload)

    assert summary["tracked"] == 2, "two different people were merged by display name"
    assert [row["messages"] for row in summary["top"]] == [50, 30]


def test_the_top_list_comes_from_one_window_not_a_sum():
    from app.web_activity import summarize_member_activity

    payload = {
        "ok": True,
        "top_limit": 20,
        "excluded_role_ids": [],
        "windows": [
            {
                "key": "last_180_days",
                "label": "Last 180 Days",
                "members": [{"user_id": 1, "display_name": "Alex", "message_count": 100, "active_days": 20}],
            },
            {
                "key": "last_90_days",
                "label": "Last 90 Days",
                "members": [{"user_id": 1, "display_name": "Alex", "message_count": 60, "active_days": 12}],
            },
        ],
    }

    summary = summarize_member_activity(payload)

    assert summary["top"][0]["messages"] == 100, "overlapping windows were added together"
    assert summary["top_window_label"] == "Last 180 Days"


def test_the_overview_needs_the_activity_permission():
    from app.web_permissions import read_area_for_endpoint

    assert read_area_for_endpoint("activity_page") == "activity"
    assert read_area_for_endpoint("activity_options_page") == "activity"


def test_an_environment_only_option_is_shown_and_survives_a_save(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("MEMBER_ACTIVITY_WEB_TOP_LIMIT", "42")
    client = _client(tmp_path)

    page = client.get("/admin/activity/options", base_url=BASE_URL)
    assert page.status_code == 200
    assert "value='42'" in page.get_data(as_text=True)

    data = {key: "" for key in OPTION_KEYS}
    data.update({"activity_options": "1", "confirm": "yes", "DISCOURSE_STATS_GROUP": "Staff"})
    saved = _post_options(client, data)
    assert "Activity options saved" in saved.get_data(as_text=True)

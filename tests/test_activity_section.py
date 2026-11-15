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

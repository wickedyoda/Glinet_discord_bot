from app.honeypot import (
    HONEYPOT_ACTION_BAN,
    HONEYPOT_ACTION_ROLE,
    HONEYPOT_ACTION_SOFTBAN,
    HONEYPOT_ACTION_TIMEOUT,
    clamp_honeypot_delete_message_days,
    clamp_honeypot_join_account_age_hours,
    clamp_honeypot_timeout_hours,
    format_honeypot_join_guard_summary,
    format_honeypot_summary,
    honeypot_action_label,
    normalize_honeypot_action,
)


def test_normalize_honeypot_action_defaults_to_softban():
    assert normalize_honeypot_action(None) == HONEYPOT_ACTION_SOFTBAN
    assert normalize_honeypot_action("unknown") == HONEYPOT_ACTION_SOFTBAN
    assert normalize_honeypot_action("BAN") == HONEYPOT_ACTION_BAN


def test_clamp_honeypot_delete_message_days_limits_range():
    assert clamp_honeypot_delete_message_days(-1) == 0
    assert clamp_honeypot_delete_message_days(3) == 3
    assert clamp_honeypot_delete_message_days(99) == 5


def test_clamp_honeypot_timeout_hours_limits_range():
    assert clamp_honeypot_timeout_hours(0) == 1
    assert clamp_honeypot_timeout_hours(24) == 24
    assert clamp_honeypot_timeout_hours(9999) == 24 * 28


def test_clamp_honeypot_join_account_age_hours_limits_range():
    assert clamp_honeypot_join_account_age_hours(0) == 1
    assert clamp_honeypot_join_account_age_hours(72) == 72
    assert clamp_honeypot_join_account_age_hours(999999) == 24 * 365


def test_honeypot_action_label_is_human_readable():
    assert honeypot_action_label(HONEYPOT_ACTION_SOFTBAN) == "Soft ban"
    assert honeypot_action_label(HONEYPOT_ACTION_TIMEOUT) == "Timeout"
    assert honeypot_action_label(HONEYPOT_ACTION_ROLE) == "Grant role"


def test_format_honeypot_summary_includes_action_specific_details():
    timeout_summary = format_honeypot_summary(
        {
            "channel_id": 123,
            "action": HONEYPOT_ACTION_TIMEOUT,
            "timeout_hours": 12,
            "enabled": 1,
        }
    )
    assert "Channel: <#123>" in timeout_summary
    assert "Action: Timeout" in timeout_summary
    assert "Timeout Hours: 12" in timeout_summary

    role_summary = format_honeypot_summary(
        {
            "channel_id": 123,
            "action": HONEYPOT_ACTION_ROLE,
            "role_id": 456,
            "enabled": 0,
        }
    )
    assert "Action: Grant role" in role_summary
    assert "Role: <@&456>" in role_summary
    assert "Enabled: No" in role_summary


def test_format_honeypot_join_guard_summary_includes_threshold():
    summary = format_honeypot_join_guard_summary(
        {
            "enabled": 1,
            "action": HONEYPOT_ACTION_TIMEOUT,
            "min_account_age_hours": 48,
            "timeout_hours": 12,
        }
    )
    assert "Enabled: Yes" in summary
    assert "Action: Timeout" in summary
    assert "Minimum Account Age Hours: 48" in summary
    assert "Timeout Hours: 12" in summary

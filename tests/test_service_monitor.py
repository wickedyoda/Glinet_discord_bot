from app.service_monitor import (
    format_service_monitor_transition_message,
    normalize_service_monitor_targets,
    run_service_monitor_check,
    serialize_service_monitor_targets,
)


def test_normalize_service_monitor_targets_parses_entries():
    targets = normalize_service_monitor_targets(
        '[{"name":"Discord Status","url":"https://discordstatus.com","expected_status":200}]',
        default_timeout_seconds=10,
        default_channel_id=123,
    )

    assert len(targets) == 1
    assert targets[0]["name"] == "Discord Status"
    assert targets[0]["method"] == "GET"
    assert targets[0]["channel_id"] == 123
    assert targets[0]["expected_status"] == 200


def test_run_service_monitor_check_marks_unexpected_status_down(monkeypatch):
    class DummyResponse:
        status_code = 503
        text = "service unavailable"

    def fake_request(method, url, timeout, headers, allow_redirects):
        return DummyResponse()

    monkeypatch.setattr("app.service_monitor.requests.request", fake_request)

    result = run_service_monitor_check(
        {
            "name": "Discord Status",
            "url": "https://discordstatus.com",
            "method": "GET",
            "expected_status": 200,
            "timeout_seconds": 10,
            "contains_text": "",
        }
    )

    assert result["state"] == "down"
    assert result["status_code"] == 503


def test_format_service_monitor_transition_message_renders_recovery():
    message = format_service_monitor_transition_message(
        {"name": "GLDDNS", "url": "https://glddns.com", "expected_status": 200},
        "down",
        {"state": "up", "status_code": 200, "checked_at": "2026-03-28T12:00:00+00:00", "error": ""},
    )

    assert "Service recovered" in message
    assert "GLDDNS" in message
    assert "HTTP 200" in message


def test_serialize_service_monitor_targets_preserves_guild_scope():
    serialized = serialize_service_monitor_targets(
        [
            {
                "guild_id": 1234567890,
                "name": "Discord Status",
                "url": "https://discordstatus.com",
                "method": "GET",
                "expected_status": 200,
                "contains_text": "",
                "timeout_seconds": 10,
                "channel_id": 9999,
            }
        ]
    )

    normalized = normalize_service_monitor_targets(
        serialized,
        default_timeout_seconds=10,
        default_channel_id=0,
    )
    assert normalized[0]["guild_id"] == 1234567890

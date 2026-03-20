from app.uptime_status import fetch_uptime_snapshot, format_uptime_summary


def test_fetch_uptime_snapshot_summarizes_monitors():
    payloads = {
        "config": {
            "config": {"title": "GL.iNet Status"},
            "publicGroupList": [
                {
                    "monitorList": [
                        {"id": 1, "name": "API"},
                        {"id": 2, "name": "Website"},
                    ]
                }
            ],
        },
        "heartbeat": {
            "heartbeatList": {
                "1": [{"status": 1, "time": "2026-03-20T10:00:00Z"}],
                "2": [{"status": 0, "time": "2026-03-20T10:05:00Z"}],
            },
            "uptimeList": {"2_24": 0.975},
        },
    }

    snapshot = fetch_uptime_snapshot(
        config_url="config",
        heartbeat_url="heartbeat",
        page_url="https://status.example/status/glinet",
        fetch_json=payloads.__getitem__,
    )

    assert snapshot["title"] == "GL.iNet Status"
    assert snapshot["total"] == 2
    assert snapshot["counts"]["up"] == 1
    assert snapshot["counts"]["down"] == 1
    assert snapshot["last_sample"] == "2026-03-20T10:05:00Z"
    assert snapshot["down_monitors"] == ["Website (97.5% 24h)"]


def test_format_uptime_summary_renders_down_monitors():
    summary = format_uptime_summary(
        {
            "title": "GL.iNet Status",
            "page_url": "https://status.example/status/glinet",
            "total": 2,
            "counts": {"up": 1, "down": 1, "pending": 0, "maintenance": 0, "unknown": 0},
            "down_monitors": ["Website (97.5% 24h)"],
            "last_sample": "2026-03-20T10:05:00Z",
        },
        page_url="https://status.example/status/glinet",
        truncate_text=lambda value, max_length: value[:max_length],
    )

    assert "**GL.iNet Status**" in summary
    assert "Monitors: 2 | Up: 1 | Down: 1" in summary
    assert "Down monitors:" in summary
    assert "Website (97.5% 24h)" in summary

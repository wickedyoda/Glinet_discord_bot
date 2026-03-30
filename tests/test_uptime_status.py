from app.uptime_status import (
    build_uptime_api_urls,
    build_uptime_instance_urls,
    build_uptime_source_config,
    default_uptime_api_key,
    extract_service_monitor_targets_from_uptime_config,
    extract_service_monitor_targets_from_uptime_metrics,
    fetch_uptime_snapshot,
    format_uptime_summary,
    parse_uptime_metrics_snapshot,
)


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
    assert snapshot["monitors"][0]["status"] == "up"
    assert snapshot["monitors"][1]["status"] == "down"


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


def test_build_uptime_api_urls_parses_status_page_url():
    urls = build_uptime_api_urls("https://status.example.com/status/default")

    assert urls["slug"] == "default"
    assert urls["config_url"] == "https://status.example.com/api/status-page/default"
    assert urls["heartbeat_url"] == "https://status.example.com/api/status-page/heartbeat/default"


def test_extract_service_monitor_targets_from_uptime_config_skips_entries_without_public_urls():
    extracted = extract_service_monitor_targets_from_uptime_config(
        {
            "publicGroupList": [
                {
                    "name": "GL DDNS",
                    "monitorList": [
                        {"name": "GLDDNS Update API", "url": "https://api.example.com/health"},
                        {"name": "Nameserver", "url": "https://"},
                    ],
                }
            ]
        },
        guild_id=1234567890,
        channel_id=9999,
        timeout_seconds=10,
    )

    assert len(extracted["targets"]) == 1
    assert extracted["targets"][0]["guild_id"] == 1234567890
    assert extracted["targets"][0]["channel_id"] == 9999
    assert extracted["targets"][0]["url"] == "https://api.example.com/health"
    assert len(extracted["skipped"]) == 1


def test_build_uptime_instance_urls_normalizes_dashboard_path():
    urls = build_uptime_instance_urls("https://kuma.example.com/dashboard")

    assert urls["instance_url"] == "https://kuma.example.com"
    assert urls["metrics_url"] == "https://kuma.example.com/metrics"


def test_build_uptime_source_config_prefers_authenticated_instance():
    config = build_uptime_source_config(
        page_url="https://status.example.com/status/default",
        instance_url="https://kuma.example.com/",
        api_key="secret",
    )

    assert config["mode"] == "metrics"
    assert config["instance_url"] == "https://kuma.example.com"
    assert config["api_key"] == "secret"


def test_default_uptime_api_key_only_applies_to_testing_instance():
    assert default_uptime_api_key("https://randy.wickedyoda.com/") == "uk1_8F5mp7aFThP-bookSOOWQLUWfcVNmHpv5UjdSyZz"
    assert default_uptime_api_key("https://status.example.com/") == ""


def test_parse_uptime_metrics_snapshot_summarizes_monitor_statuses():
    metrics_text = """
# HELP monitor_status Monitor status
# TYPE monitor_status gauge
monitor_status{monitor_name="Website",monitor_url="https://www.example.com",monitor_hostname="null",monitor_port="null"} 1
monitor_status{monitor_name="API",monitor_url="https://api.example.com/health",monitor_hostname="null",monitor_port="null"} 0
"""

    snapshot = parse_uptime_metrics_snapshot(
        metrics_text,
        source_url="https://kuma.example.com",
    )

    assert snapshot["title"] == "Uptime Kuma"
    assert snapshot["total"] == 2
    assert snapshot["counts"]["up"] == 1
    assert snapshot["counts"]["down"] == 1
    assert snapshot["down_monitors"] == ["API"]


def test_extract_service_monitor_targets_from_uptime_metrics_skips_entries_without_public_urls():
    extracted = extract_service_monitor_targets_from_uptime_metrics(
        """
monitor_status{monitor_name="Website",monitor_url="https://www.example.com",monitor_hostname="null",monitor_port="null"} 1
monitor_status{monitor_name="DNS",monitor_url="null",monitor_hostname="resolver.internal",monitor_port="53"} 1
""",
        guild_id=1234567890,
        channel_id=9999,
        timeout_seconds=12,
    )

    assert len(extracted["targets"]) == 1
    assert extracted["targets"][0]["name"] == "Website"
    assert extracted["targets"][0]["url"] == "https://www.example.com"
    assert extracted["targets"][0]["guild_id"] == 1234567890
    assert len(extracted["skipped"]) == 1

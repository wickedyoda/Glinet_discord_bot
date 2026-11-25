"""Per-channel and per-thread activity: the payload carries it, the page shows it."""

from __future__ import annotations

from app.web_activity import render_activity_overview_body, summarize_member_activity


def _payload(channels):
    return {
        "ok": True,
        "windows": [
            {
                "key": "last_24h",
                "label": "Last 24 Hours",
                "members": [
                    {"user_id": 1, "display_name": "Ada", "message_count": 12, "active_days": 1},
                ],
                "channels": channels,
            }
        ],
        "top_limit": 20,
        "excluded_role_ids": [],
    }


def test_summary_keeps_channels_sorted_by_volume():
    summary = summarize_member_activity(
        _payload(
            [
                {"channel_id": 10, "name": "quiet", "message_count": 2, "active_days": 1, "is_thread": False, "parent_channel_id": 0},
                {"channel_id": 20, "name": "busy", "message_count": 40, "active_days": 3, "is_thread": False, "parent_channel_id": 0},
            ]
        )
    )

    assert [row["channel_id"] for row in summary["channels"]] == [20, 10]
    assert summary["channel_count"] == 2
    assert summary["channels"][0]["messages"] == 40


def test_summary_marks_threads_and_their_parent():
    summary = summarize_member_activity(
        _payload(
            [
                {"channel_id": 30, "name": "ticket-42", "message_count": 7, "active_days": 1, "is_thread": True, "parent_channel_id": 20},
            ]
        )
    )

    row = summary["channels"][0]
    assert row["is_thread"] is True
    assert row["parent_channel_id"] == 20


def test_summary_drops_rows_without_a_channel_id():
    summary = summarize_member_activity(_payload([{"name": "nameless", "message_count": 5}]))

    assert summary["channels"] == []
    assert summary["channel_count"] == 0


def test_overview_renders_channels_and_threads():
    body = render_activity_overview_body(
        summarize_member_activity(
            _payload(
                [
                    {"channel_id": 20, "name": "busy", "message_count": 40, "active_days": 3, "is_thread": False, "parent_channel_id": 0},
                    {"channel_id": 30, "name": "ticket-42", "message_count": 7, "active_days": 1, "is_thread": True, "parent_channel_id": 20},
                ]
            )
        ),
        {"ok": False, "error": "unavailable"},
    )

    assert "Busiest channels and threads" in body
    assert "#busy" in body
    assert "#ticket-42" in body
    assert "thread in &lt;#20&gt;" in body or "thread in <#20>" in body
    # The card still closes and still links to the full page.
    assert "Open Discord activity" in body


def test_overview_omits_the_block_when_there_is_no_channel_data():
    body = render_activity_overview_body(summarize_member_activity(_payload([])), {"ok": False, "error": "unavailable"})

    assert "Busiest channels and threads" not in body
    assert "Open Discord activity" in body

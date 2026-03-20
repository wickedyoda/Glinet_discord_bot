from datetime import UTC, datetime

from app.member_activity_backfill import (
    compute_missing_ranges,
    extract_completed_ranges,
    merge_completed_ranges,
    parse_backfill_since,
    state_key,
)


def _parse_iso_datetime_utc(raw_value):
    if not raw_value:
        return None
    parsed = datetime.fromisoformat(str(raw_value))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _normalize_activity_timestamp(raw_value):
    if isinstance(raw_value, datetime):
        parsed = raw_value
    else:
        parsed = _parse_iso_datetime_utc(raw_value)
    return parsed.astimezone(UTC)


def test_parse_backfill_since_accepts_date_only():
    result = parse_backfill_since("2026-02-17", _parse_iso_datetime_utc)

    assert result == datetime(2026, 2, 17, 0, 0, tzinfo=UTC)


def test_state_key_uses_guild_and_timestamp():
    since_dt = datetime(2026, 2, 17, 0, 0, tzinfo=UTC)

    assert state_key(123, since_dt) == "member_activity_backfill:123:2026-02-17T00:00:00+00:00"


def test_extract_and_merge_completed_ranges():
    rows = [
        {
            "payload": {
                "status": "completed",
                "since_at": "2026-02-17T00:00:00+00:00",
                "until_at": "2026-02-20T00:00:00+00:00",
            }
        },
        {
            "payload": {
                "status": "completed",
                "since_at": "2026-02-20T00:00:00+00:00",
                "until_at": "2026-02-25T00:00:00+00:00",
            }
        },
    ]

    ranges = extract_completed_ranges(rows, _parse_iso_datetime_utc)
    merged = merge_completed_ranges(ranges, _normalize_activity_timestamp)

    assert merged == [
        (
            datetime(2026, 2, 17, 0, 0, tzinfo=UTC),
            datetime(2026, 2, 25, 0, 0, tzinfo=UTC),
        )
    ]


def test_compute_missing_ranges_only_returns_uncovered_gaps():
    requested_since = datetime(2026, 2, 17, 0, 0, tzinfo=UTC)
    requested_until = datetime(2026, 3, 1, 0, 0, tzinfo=UTC)
    completed_ranges = [
        (
            datetime(2026, 2, 20, 0, 0, tzinfo=UTC),
            datetime(2026, 2, 25, 0, 0, tzinfo=UTC),
        ),
        (
            datetime(2026, 2, 26, 0, 0, tzinfo=UTC),
            datetime(2026, 3, 1, 0, 0, tzinfo=UTC),
        ),
    ]

    missing = compute_missing_ranges(
        requested_since,
        requested_until,
        completed_ranges,
        _normalize_activity_timestamp,
    )

    assert missing == [
        (
            datetime(2026, 2, 17, 0, 0, tzinfo=UTC),
            datetime(2026, 2, 20, 0, 0, tzinfo=UTC),
        ),
        (
            datetime(2026, 2, 25, 0, 0, tzinfo=UTC),
            datetime(2026, 2, 26, 0, 0, tzinfo=UTC),
        ),
    ]

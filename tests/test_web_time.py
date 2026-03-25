from app.web_time import format_timestamp_display, parse_iso_datetime_utc


def test_parse_iso_datetime_utc_accepts_z_suffix():
    parsed = parse_iso_datetime_utc("2026-03-25T04:51:15Z")

    assert parsed is not None
    assert parsed.isoformat() == "2026-03-25T04:51:15+00:00"


def test_format_timestamp_display_uses_readable_utc_format():
    assert format_timestamp_display("2026-03-25T04:51:15.920110+00:00") == "2026-03-25 04:51:15 UTC"


def test_format_timestamp_display_uses_blank_for_empty_value():
    assert format_timestamp_display("", blank="Never") == "Never"

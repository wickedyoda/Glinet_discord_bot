from __future__ import annotations

from datetime import UTC, datetime


def parse_iso_datetime_utc(raw_value):
    text = str(raw_value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def format_timestamp_display(raw_value, *, blank: str = "n/a") -> str:
    parsed = parse_iso_datetime_utc(raw_value)
    if parsed is None:
        text = str(raw_value or "").strip()
        return text or blank
    return parsed.strftime("%Y-%m-%d %H:%M:%S UTC")

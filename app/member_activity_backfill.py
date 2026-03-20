import re
from datetime import datetime

DATE_ONLY_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}")


def parse_backfill_since(raw_value: str, parse_iso_datetime_utc):
    text = str(raw_value or "").strip()
    if not text:
        return None
    candidate = text
    if DATE_ONLY_PATTERN.fullmatch(candidate):
        candidate = f"{candidate}T00:00:00+00:00"
    parsed = parse_iso_datetime_utc(candidate)
    if parsed is None:
        return None
    return parsed.replace(minute=0, second=0, microsecond=0)


def state_key(guild_id: int, since_dt: datetime):
    return f"member_activity_backfill:{int(guild_id)}:{since_dt.isoformat()}"


def extract_completed_ranges(kv_rows, parse_iso_datetime_utc):
    ranges = []
    for row in kv_rows:
        if not isinstance(row, dict):
            continue
        payload = row.get("payload")
        if not isinstance(payload, dict):
            continue
        if str(payload.get("status") or "").strip().lower() != "completed":
            continue
        since_dt = parse_iso_datetime_utc(payload.get("since_at"))
        until_dt = parse_iso_datetime_utc(payload.get("until_at"))
        if since_dt is None or until_dt is None:
            continue
        if until_dt <= since_dt:
            continue
        ranges.append((since_dt, until_dt))
    return ranges


def merge_completed_ranges(ranges, normalize_activity_timestamp):
    normalized = []
    for start_dt, end_dt in ranges:
        safe_start = normalize_activity_timestamp(start_dt)
        safe_end = normalize_activity_timestamp(end_dt)
        if safe_end <= safe_start:
            continue
        normalized.append((safe_start, safe_end))
    if not normalized:
        return []

    normalized.sort(key=lambda item: item[0])
    merged = [normalized[0]]
    for start_dt, end_dt in normalized[1:]:
        last_start, last_end = merged[-1]
        if start_dt <= last_end:
            merged[-1] = (last_start, max(last_end, end_dt))
            continue
        merged.append((start_dt, end_dt))
    return merged


def compute_missing_ranges(
    requested_since: datetime,
    requested_until: datetime,
    completed_ranges,
    normalize_activity_timestamp,
):
    safe_requested_since = normalize_activity_timestamp(requested_since)
    safe_requested_until = normalize_activity_timestamp(requested_until)
    if safe_requested_until <= safe_requested_since:
        return []

    merged_ranges = merge_completed_ranges(completed_ranges, normalize_activity_timestamp)
    missing_ranges = []
    cursor = safe_requested_since
    for completed_start, completed_end in merged_ranges:
        if completed_end <= safe_requested_since:
            continue
        if completed_start >= safe_requested_until:
            break
        clipped_start = max(completed_start, safe_requested_since)
        clipped_end = min(completed_end, safe_requested_until)
        if clipped_end <= clipped_start:
            continue
        if clipped_start > cursor:
            missing_ranges.append((cursor, clipped_start))
        cursor = max(cursor, clipped_end)
        if cursor >= safe_requested_until:
            break
    if cursor < safe_requested_until:
        missing_ranges.append((cursor, safe_requested_until))
    return missing_ranges

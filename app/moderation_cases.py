"""Case list for the web panel: every warning, filterable, exportable.

The Discord commands write warnings; this reads them back for staff who are not in
Discord at the time. It is deliberately read-only -- clearing a warning is a decision
with a name attached, and that belongs in the command or the member page, not in a
table row that can be clicked by accident.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

from app.csv_utils import build_csv_bytes

DEFAULT_RANGE_DAYS = 90
MAX_RANGE_DAYS = 3650
PAGE_SIZE = 100


def _parse_day(raw, default: date) -> date:
    text = str(raw or "").strip()
    if not text:
        return default
    try:
        return datetime.strptime(text[:10], "%Y-%m-%d").date()
    except ValueError:
        return default


def normalize_filters(raw: dict | None) -> dict:
    raw = raw or {}
    today = datetime.now(UTC).date()
    date_from = _parse_day(raw.get("from"), today - timedelta(days=DEFAULT_RANGE_DAYS))
    date_to = _parse_day(raw.get("to"), today)
    if date_from > date_to:
        date_from, date_to = date_to, date_from
    if (date_to - date_from).days > MAX_RANGE_DAYS:
        date_from = date_to - timedelta(days=MAX_RANGE_DAYS)

    status = str(raw.get("status") or "active").strip().lower()
    if status not in {"active", "cleared", "all"}:
        status = "active"

    try:
        page = max(1, int(raw.get("page") or 1))
    except (TypeError, ValueError):
        page = 1

    return {
        "user_id": str(raw.get("user_id") or "").strip()[:20],
        "status": status,
        "date_from": date_from,
        "date_to": date_to,
        "page": page,
        "from_iso": f"{date_from.isoformat()}T00:00:00",
        # Exclusive next-day midnight: a timestamp like 23:59:59.123 sorts after
        # "23:59:59" as text, so an inclusive bound silently drops the last second.
        "to_iso": f"{(date_to + timedelta(days=1)).isoformat()}T00:00:00",
    }


def _where(filters: dict) -> tuple[str, list]:
    clauses = ["guild_id = ?", "created_at >= ?", "created_at < ?"]
    params: list = [filters["guild_id"], filters["from_iso"], filters["to_iso"]]
    if filters["status"] == "active":
        clauses.append("(cleared_at IS NULL OR cleared_at = '')")
    elif filters["status"] == "cleared":
        clauses.append("cleared_at IS NOT NULL AND cleared_at <> ''")
    if filters["user_id"]:
        clauses.append("user_id = ?")
        params.append(int(filters["user_id"]) if filters["user_id"].isdigit() else -1)
    return " AND ".join(clauses), params


def query_cases(conn, guild_id: int, raw_filters: dict | None, *, page_size: int = PAGE_SIZE) -> dict:
    filters = normalize_filters(raw_filters)
    filters["guild_id"] = int(guild_id)
    where, params = _where(filters)

    # The WHERE clause is assembled from constant fragments; every value is bound
    # through `params`.
    total = int(
        conn.execute(
            f"SELECT COUNT(*) FROM moderation_warnings WHERE {where}",  # nosec B608
            params,
        ).fetchone()[0]
        or 0
    )
    page_size = max(1, min(int(page_size), 10000))
    offset = (filters["page"] - 1) * page_size
    rows = conn.execute(
        "SELECT id, case_id, user_id, created_at, warning_type, reason, moderator, "
        "matched_term, action_taken, cleared_at "
        f"FROM moderation_warnings WHERE {where} ORDER BY id DESC LIMIT ? OFFSET ?",  # nosec B608
        params + [page_size, offset],
    ).fetchall()

    cases = [
        {
            "case_id": int(row["case_id"] or row["id"]),
            "user_id": str(row["user_id"]),
            "created_at": str(row["created_at"] or "")[:19].replace("T", " "),
            "type": str(row["warning_type"] or ""),
            "reason": str(row["reason"] or ""),
            "moderator": str(row["moderator"] or ""),
            "matched_term": str(row["matched_term"] or ""),
            "action_taken": str(row["action_taken"] or ""),
            "cleared_at": str(row["cleared_at"] or "")[:19].replace("T", " "),
        }
        for row in rows
    ]
    return {
        "ok": True,
        "filters": {
            "user_id": filters["user_id"],
            "status": filters["status"],
            "from": filters["date_from"].isoformat(),
            "to": filters["date_to"].isoformat(),
            "page": filters["page"],
        },
        "cases": cases,
        "total": total,
        "pages": max(1, (total + page_size - 1) // page_size),
    }


def cases_to_csv(cases: list[dict]) -> str:
    """CSV with spreadsheet-safe cells.

    A reason beginning with "=" is a formula to Excel, not text, so the shared helper
    prefixes the dangerous ones. Staff open these files on their own machines.
    """
    rows = [
        [
            case.get("case_id", ""),
            case.get("user_id", ""),
            case.get("created_at", ""),
            case.get("type", ""),
            case.get("reason", ""),
            case.get("moderator", ""),
            case.get("cleared_at", ""),
        ]
        for case in cases
    ]
    return build_csv_bytes(
        ["case", "user_id", "created_at", "type", "reason", "moderator", "cleared_at"], rows
    ).decode("utf-8")

"""Rendering for the case list: filters, the table, and the export link."""

from __future__ import annotations

from html import escape
from urllib.parse import urlencode


def _href(filters: dict, page: int | None = None) -> str:
    params = {
        "user_id": filters.get("user_id", ""),
        "status": filters.get("status", "active"),
        "from": filters.get("from", ""),
        "to": filters.get("to", ""),
        "page": str(page if page is not None else filters.get("page", 1)),
    }
    return "/admin/cases?" + urlencode({key: value for key, value in params.items() if value})


def render_cases_body(payload: dict) -> str:
    filters = payload.get("filters") or {}
    parts: list[str] = []

    status_options = []
    for value, label in (("active", "Active"), ("cleared", "Cleared"), ("all", "All")):
        selected = " selected" if str(filters.get("status") or "active") == value else ""
        status_options.append(f"<option value='{value}'{selected}>{label}</option>")

    parts.append(
        f"""
    <div class="card">
      <h3>Filter</h3>
      <form method="get" action="/admin/cases">
        <div class="field">
          <label for="c-user">Member ID</label>
          <input id="c-user" name="user_id" type="text" inputmode="numeric"
                 value="{escape(str(filters.get('user_id') or ''), quote=True)}"
                 placeholder="Leave blank for everyone">
        </div>
        <div class="field">
          <label for="c-status">Status</label>
          <select id="c-status" name="status">{''.join(status_options)}</select>
        </div>
        <div class="field">
          <label for="c-from">From</label>
          <input id="c-from" name="from" type="date" value="{escape(str(filters.get('from') or ''), quote=True)}">
        </div>
        <div class="field">
          <label for="c-to">To</label>
          <input id="c-to" name="to" type="date" value="{escape(str(filters.get('to') or ''), quote=True)}">
        </div>
        <div class="actions">
          <button class="btn" type="submit">Apply</button>
          <a class="btn secondary" href="/admin/cases.csv?{escape(urlencode({k: v for k, v in filters.items() if k != 'page' and v}), quote=True)}">Download CSV</a>
        </div>
      </form>
    </div>
    """
    )

    if not payload.get("ok"):
        parts.append(
            "<div class='alert error' role='alert'>"
            f"<strong>Could not load cases.</strong> {escape(str(payload.get('error') or 'Unknown error.'))}"
            "</div>"
        )
        return "\n".join(parts)

    cases = payload.get("cases") or []
    total = int(payload.get("total") or 0)
    pages = int(payload.get("pages") or 1)
    page = int(filters.get("page") or 1)

    parts.append(f"<div class='card'><h3>{total} case(s)</h3>")
    if not cases:
        parts.append("<p class='muted'>No cases match those filters.</p>")
    else:
        parts.append(
            "<table class='table'><thead><tr><th>Case</th><th>Member</th><th>When</th>"
            "<th>Type</th><th>Reason</th><th>By</th><th>Status</th></tr></thead><tbody>"
        )
        for case in cases:
            user_id = str(case.get("user_id") or "")
            status = "cleared" if case.get("cleared_at") else "active"
            parts.append(
                "<tr><td>"
                + escape(f"#{case.get('case_id')}")
                + "</td><td>"
                + f"<a href='/admin/members?q={escape(user_id, quote=True)}'>{escape(user_id)}</a>"
                + "</td><td>"
                + escape(str(case.get("created_at") or ""))
                + "</td><td>"
                + escape(str(case.get("type") or ""))
                + "</td><td>"
                + escape(str(case.get("reason") or case.get("matched_term") or ""))
                + "</td><td>"
                + escape(str(case.get("moderator") or ""))
                + "</td><td>"
                + escape(status)
                + "</td></tr>"
            )
        parts.append("</tbody></table>")

        nav = []
        if page > 1:
            nav.append(f"<a class='btn secondary' href='{escape(_href(filters, page - 1), quote=True)}'>&larr; Previous</a>")
        nav.append(f"<span class='muted'>Page {page} of {pages}</span>")
        if page < pages:
            nav.append(f"<a class='btn secondary' href='{escape(_href(filters, page + 1), quote=True)}'>Next &rarr;</a>")
        parts.append("<div class='actions'>" + " ".join(nav) + "</div>")
    parts.append("</div>")

    return "\n".join(parts)

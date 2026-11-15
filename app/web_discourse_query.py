"""Rendering for the forum query page.

The page is a form plus its results: summary tiles, an optional card for one person,
a table of who posted and a paged table of topics. Everything comes from
``app.discourse_query``; this module only arranges it, and escapes every value that
came out of the archive.
"""

from __future__ import annotations

from html import escape
from urllib.parse import urlencode


def _seconds_text(seconds) -> str:
    try:
        value = int(seconds or 0)
    except (TypeError, ValueError):
        return "—"
    if value <= 0:
        return "—"
    days, remainder = divmod(value, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes = remainder // 60
    if days:
        return f"{days}d {hours:02d}h"
    if hours:
        return f"{hours}h {minutes:02d}m"
    return f"{minutes}m"


def _query_href(filters: dict, page: int | None = None) -> str:
    params = {
        "username": filters.get("username", ""),
        "category": filters.get("category", ""),
        "from": filters.get("from", ""),
        "to": filters.get("to", ""),
        "page": str(page if page is not None else filters.get("page", 1)),
    }
    if filters.get("staff_only"):
        params["staff_only"] = "1"
    return "/admin/discourse-query?" + urlencode({k: v for k, v in params.items() if v})


def render_discourse_query_form(filters: dict, options: dict | None = None) -> str:
    options = options or {}
    categories = options.get("categories") or []
    staff = options.get("staff") or []

    category_options = ["<option value=''>All categories</option>"]
    for slug in categories:
        selected = " selected" if str(slug) == str(filters.get("category") or "") else ""
        category_options.append(
            f"<option value='{escape(str(slug), quote=True)}'{selected}>{escape(str(slug))}</option>"
        )

    staff_hint = ", ".join(escape(str(name)) for name in staff[:40]) if staff else ""
    datalist = ""
    if staff_hint:
        datalist = (
            "<datalist id='discourse-staff-names'>"
            + "".join(f"<option value='{escape(str(name), quote=True)}'></option>" for name in staff)
            + "</datalist>"
        )

    checked = " checked" if filters.get("staff_only") else ""
    earliest = str(options.get("earliest") or "")
    latest = str(options.get("latest") or "")

    return f"""
    <div class="card">
      <h3>Query the archive</h3>
      <form method="get" action="/admin/discourse-query">
        <div class="field">
          <label for="q-username">Forum username</label>
          <input id="q-username" name="username" type="text" list="discourse-staff-names"
                 value="{escape(str(filters.get('username') or ''), quote=True)}"
                 placeholder="Anyone, or leave blank for everyone">
          {datalist}
          <div class="muted">Matches exactly. Staff names are offered; any stored user works.</div>
        </div>
        <div class="field">
          <label for="q-category">Category</label>
          <select id="q-category" name="category">{''.join(category_options)}</select>
        </div>
        <div class="field">
          <label for="q-from">From</label>
          <input id="q-from" name="from" type="date" value="{escape(str(filters.get('from') or ''), quote=True)}">
        </div>
        <div class="field">
          <label for="q-to">To</label>
          <input id="q-to" name="to" type="date" value="{escape(str(filters.get('to') or ''), quote=True)}">
        </div>
        <label class="checkbox"><input type="checkbox" name="staff_only" value="1"{checked}> Staff posts only</label>
        <div class="actions"><button class="btn" type="submit">Run query</button></div>
        <div class="muted">Archive holds posts from {escape(earliest or '—')} to {escape(latest or '—')}.</div>
      </form>
    </div>
    """


def render_discourse_query_body(payload: dict, guild_name: str = "") -> str:
    filters = payload.get("filters") or {}
    parts = [render_discourse_query_form(filters, payload.get("options"))]

    if not payload.get("ok"):
        parts.append(
            "<div class='alert error' role='alert'>"
            f"<strong>Query failed.</strong> {escape(str(payload.get('error') or 'Unknown error.'))}"
            "</div>"
        )
        return "\n".join(parts)

    summary = payload.get("summary") or {}
    parts.append("<div class='card'><h3>Results</h3><div class='stat-row'>")
    for label, value in (
        ("Posts", summary.get("posts", 0)),
        ("Topics", summary.get("topics_total", summary.get("topics", 0))),
        ("People", summary.get("people", 0)),
    ):
        parts.append(
            "<div class='stat'><div class='stat-label'>"
            + escape(label)
            + "</div><div class='stat-value'>"
            + escape(str(value))
            + "</div></div>"
        )
    parts.append("</div>")
    parts.append(
        "<p class='muted'>"
        + escape(str(filters.get("from") or ""))
        + " to "
        + escape(str(filters.get("to") or ""))
        + (f" · category {escape(str(filters.get('category')))}" if filters.get("category") else "")
        + (f" · user {escape(str(filters.get('username')))}" if filters.get("username") else "")
        + (" · staff only" if filters.get("staff_only") else "")
        + "</p></div>"
    )

    card = payload.get("user_card")
    if card:
        parts.append("<div class='card'><h3>" + escape(str(card.get("username") or "")) + "</h3><div class='stat-row'>")
        for label, value in (
            ("Posts", card.get("posts", 0)),
            ("Threads started", card.get("threads_started", 0)),
            ("Replies", card.get("replies", 0)),
            ("Topics answered", card.get("answered_topics", 0)),
            ("Median reply", _seconds_text(card.get("median_seconds"))),
            ("90th percentile", _seconds_text(card.get("p90_seconds"))),
        ):
            parts.append(
                "<div class='stat'><div class='stat-label'>"
                + escape(label)
                + "</div><div class='stat-value'>"
                + escape(str(value))
                + "</div></div>"
            )
        parts.append("</div><p class='muted'>Reply delay is measured from each topic's creation to this person's first reply in it.</p></div>")

    users = payload.get("users") or []
    parts.append("<div class='card'><h3>Who posted</h3>")
    if not users:
        parts.append("<p class='muted'>Nobody matched those filters.</p>")
    else:
        parts.append("<table class='table'><thead><tr><th>User</th><th>Posts</th><th>Topics</th><th>Last post</th></tr></thead><tbody>")
        for row in users:
            name = str(row.get("username") or "")
            badge = " <span class='tag'>staff</span>" if row.get("is_staff") else ""
            parts.append(
                "<tr><td>"
                + f"<a href='{escape(_query_href({**filters, 'username': name, 'page': 1}), quote=True)}'>{escape(name)}</a>"
                + badge
                + "</td><td>"
                + escape(str(row.get("posts", 0)))
                + "</td><td>"
                + escape(str(row.get("topics", 0)))
                + "</td><td>"
                + escape(str(row.get("last_post") or ""))
                + "</td></tr>"
            )
        parts.append("</tbody></table>")
    parts.append("</div>")

    topics = payload.get("topics") or []
    pages = int(summary.get("pages") or 1)
    page = int(filters.get("page") or 1)
    parts.append("<div class='card'><h3>Topics</h3>")
    if not topics:
        parts.append("<p class='muted'>No topics in that range.</p>")
    else:
        parts.append(
            "<table class='table'><thead><tr><th>Topic</th><th>Category</th><th>Posts</th>"
            "<th>Staff posts</th><th>First post</th><th>Last post</th></tr></thead><tbody>"
        )
        for row in topics:
            topic_id = row.get("topic_id")
            label = f"#{topic_id}"
            url = str(row.get("url") or "")
            link = (
                f"<a href='{escape(url, quote=True)}' target='_blank' rel='noopener'>{escape(label)}</a>"
                if url
                else escape(label)
            )
            parts.append(
                "<tr><td>"
                + link
                + "</td><td>"
                + escape(str(row.get("category_slug") or ""))
                + "</td><td>"
                + escape(str(row.get("posts", 0)))
                + "</td><td>"
                + escape(str(row.get("staff_posts", 0)))
                + "</td><td>"
                + escape(str(row.get("first_post") or ""))
                + "</td><td>"
                + escape(str(row.get("last_post") or ""))
                + "</td></tr>"
            )
        parts.append("</tbody></table>")
        nav = []
        if page > 1:
            nav.append(f"<a class='btn secondary' href='{escape(_query_href(filters, page - 1), quote=True)}'>&larr; Previous</a>")
        nav.append(f"<span class='muted'>Page {page} of {pages}</span>")
        if page < pages:
            nav.append(f"<a class='btn secondary' href='{escape(_query_href(filters, page + 1), quote=True)}'>Next &rarr;</a>")
        parts.append("<div class='actions'>" + " ".join(nav) + "</div>")
    parts.append("</div>")

    return "\n".join(parts)

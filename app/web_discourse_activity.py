"""Render the Discourse activity page.

Pure rendering: the payload comes from `build_discourse_activity_payload`, so this
module holds no queries and is testable against a fixture.

Bars are plain divs sized by percentage -- no JS, no chart library, and it degrades
to readable numbers if the stylesheet is unavailable.
"""

from __future__ import annotations

from html import escape

_MAX_BAR = 100.0


def _human_duration(seconds: int | float) -> str:
    seconds = max(0, int(seconds or 0))
    if seconds == 0:
        return "0m"
    if seconds < 3600:
        return f"{seconds / 60:.0f}m"
    if seconds < 86400:
        return f"{seconds / 3600:.1f}h"
    return f"{seconds / 86400:.1f}d"


def _bar_row(label: str, value: int, peak: int, suffix: str = "") -> str:
    width = 0.0 if peak <= 0 else (value / peak) * _MAX_BAR
    return (
        f"<div style='display:flex;align-items:center;gap:8px;margin:2px 0;'>"
        f"<span class='mono muted' style='width:74px;flex:none;'>{escape(label)}</span>"
        f"<span style='flex:1;background:rgba(255,255,255,.06);border-radius:3px;height:14px;'>"
        f"<span style='display:block;width:{width:.1f}%;height:14px;border-radius:3px;"
        f"background:var(--accent,#4da3ff);'></span></span>"
        f"<span class='mono' style='width:64px;text-align:right;flex:none;'>{value}{escape(suffix)}</span>"
        f"</div>"
    )


def _stat_card(label: str, value: str, note: str = "") -> str:
    note_html = f"<div class='muted' style='font-size:.85em;'>{escape(note)}</div>" if note else ""
    return (
        "<div class='card' style='padding:12px;'>"
        f"<div class='muted' style='font-size:.8em;text-transform:uppercase;letter-spacing:.04em;'>{escape(label)}</div>"
        f"<div style='font-size:1.6em;font-weight:600;line-height:1.2;'>{escape(value)}</div>"
        f"{note_html}</div>"
    )


def render_discourse_activity_body(payload: dict) -> str:
    payload = payload or {}
    if not payload.get("ok"):
        reason = str(payload.get("error") or "Discourse activity is unavailable.")
        return f"<div class='card'><p class='muted'>{escape(reason)}</p></div>"

    days = int(payload.get("days") or 90)
    categories = payload.get("categories") or []
    staff_total = int(payload.get("staff_total") or 0)
    staff_active = int(payload.get("staff_active") or 0)
    staff_posts = int(payload.get("staff_posts") or 0)
    answered = int(payload.get("answered") or 0)
    within_24h = int(payload.get("within_24h") or 0)
    pct_24h = (within_24h / answered * 100) if answered else 0

    hours = {int(row["bucket"]): int(row["posts"]) for row in payload.get("hourly") or []}
    hour_peak = max(hours.values()) if hours else 0
    hour_rows = "".join(
        _bar_row(f"{hour:02d}:00", hours.get(hour, 0), hour_peak) for hour in range(24)
    )

    weeks = payload.get("weekly") or []
    week_peak = max((int(row["posts"]) for row in weeks), default=0)
    week_rows = "".join(
        _bar_row(str(row["bucket"]), int(row["posts"]), week_peak) for row in weeks
    ) or "<p class='muted'>No weekly data yet.</p>"

    responses = {str(row["username"]): row for row in payload.get("responses") or []}
    table_rows = []
    for row in payload.get("activity") or []:
        username = str(row.get("username") or "")
        mean = _human_duration(responses.get(username, {}).get("mean_seconds") or 0)
        table_rows.append(
            "<tr>"
            f"<td class='mono'>{escape(username)}</td>"
            f"<td class='mono'>{int(row.get('posts') or 0)}</td>"
            f"<td class='mono'>{int(row.get('topics') or 0)}</td>"
            f"<td class='mono'>{int(row.get('handled') or 0)}</td>"
            f"<td class='mono'>{escape(mean)}</td>"
            "</tr>"
        )
    table_html = (
        "<table><thead><tr><th>Staff member</th><th>Posts</th><th>Topics</th>"
        "<th>Handled</th><th>Mean reply</th></tr></thead><tbody>"
        + "".join(table_rows)
        + "</tbody></table>"
        if table_rows
        else "<p class='muted'>No staff posts stored for this window yet.</p>"
    )

    scope_text = ", ".join(escape(str(c)) for c in categories) if categories else "all categories"

    return f"""
<div class="grid">
  <div class="card" style="grid-column:1/-1;">
    <h2>Discourse activity</h2>
    <p class="muted">
      Last {days} days, categories: <span class="mono">{scope_text}</span>.
      Staff are the current <span class="mono">GL.iNet_Staff</span> group &mdash; membership
      is a live snapshot, so people who left the group are not credited for older posts.
      &ldquo;Handled&rdquo; means the first staff reply after the opening post; Discourse has
      no resolution signal.
    </p>
  </div>

  <div class="grid" style="grid-column:1/-1;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));">
    {_stat_card("Staff group", str(staff_total))}
    {_stat_card("Staff who posted", str(staff_active), f"of {staff_total}")}
    {_stat_card("Staff posts", str(staff_posts))}
    {_stat_card("Threads answered", str(answered))}
    {_stat_card("Median reply", _human_duration(payload.get("median_seconds") or 0))}
    {_stat_card("p90 reply", _human_duration(payload.get("p90_seconds") or 0))}
    {_stat_card("Within 24h", f"{pct_24h:.0f}%", f"{within_24h} of {answered}")}
  </div>

  <div class="card">
    <h3>Posts by hour of day (UTC)</h3>
    <p class="muted">Where the staff day actually sits. An empty evening block means an uncovered queue.</p>
    {hour_rows}
  </div>

  <div class="card">
    <h3>Posts by week</h3>
    {week_rows}
  </div>

  <div class="card" style="grid-column:1/-1;">
    <h3>Per staff member</h3>
    {table_html}
  </div>
</div>
"""


__all__ = ["render_discourse_activity_body"]

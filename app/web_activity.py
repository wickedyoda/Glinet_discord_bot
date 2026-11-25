"""Rendering for the Activity section: one place for Discord and forum activity.

The Discord numbers come from the member-activity payload, the forum numbers from the
Discourse activity payload, and the options from ``app.activity_options``. This module
only arranges them -- it holds no state and touches no database, so the section can be
tested with plain dicts.
"""

from __future__ import annotations

from html import escape

from app.activity_options import OPTION_GROUPS


def _int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _seconds_to_text(seconds) -> str:
    value = _int(seconds, 0)
    if value <= 0:
        return "—"
    hours, remainder = divmod(value, 3600)
    minutes = remainder // 60
    if hours:
        return f"{hours}h {minutes:02d}m"
    return f"{minutes}m"


def summarize_member_activity(payload) -> dict:
    """Pick the headline numbers out of the member-activity payload.

    Two things the payload makes easy to get wrong:

    * Display names are not unique, so entries are keyed by ``user_id`` and the name is
      only ever shown, never used to merge two people.
    * The windows overlap by design (180, 90, 30 and 7 days, plus 24 hours). Adding
      them together counts the same message several times and ranks by recency, so the
      top list comes from one explicitly named window.
    """
    if not isinstance(payload, dict) or not payload.get("ok"):
        return {
            "ok": False,
            "error": str(payload.get("error") if isinstance(payload, dict) else "") or "Member activity is unavailable.",
            "windows": [],
            "top": [],
            "top_window_label": "",
            "tracked": 0,
        }

    windows = []
    for window in payload.get("windows", []) or []:
        members = window.get("members", []) if isinstance(window, dict) else []
        windows.append(
            {
                "key": str(window.get("key") or ""),
                "label": str(window.get("label") or window.get("key") or ""),
                "count": len(members),
                "messages": sum(_int(row.get("message_count")) for row in members if isinstance(row, dict)),
            }
        )

    # The widest window the payload carries: it is the honest answer to "who talks most",
    # and the label travels with the list so the page cannot imply it covers everything.
    chosen = None
    if isinstance(payload.get("windows"), list) and payload["windows"]:
        first = payload["windows"][0]
        if isinstance(first, dict):
            chosen = first

    top: list[dict] = []
    seen: set[str] = set()
    for row in (chosen or {}).get("members", []) or []:
        if not isinstance(row, dict):
            continue
        identity = str(row.get("user_id") or "").strip()
        if not identity:
            identity = "name:" + str(row.get("username") or row.get("display_name") or "").strip()
        if not identity or identity in seen:
            continue
        seen.add(identity)
        name = str(row.get("display_name") or row.get("username") or identity).strip()
        top.append(
            {
                "name": name,
                "messages": _int(row.get("message_count")),
                "active_days": _int(row.get("active_days")),
            }
        )
    top.sort(key=lambda entry: entry["messages"], reverse=True)

    channels: list[dict] = []
    for row in (chosen or {}).get("channels", []) or []:
        if not isinstance(row, dict):
            continue
        channel_id = _int(row.get("channel_id"))
        if channel_id <= 0:
            continue
        channels.append(
            {
                "channel_id": channel_id,
                "name": str(row.get("name") or f"#{channel_id}"),
                "messages": _int(row.get("message_count")),
                "active_days": _int(row.get("active_days")),
                "is_thread": bool(row.get("is_thread")),
                "parent_channel_id": _int(row.get("parent_channel_id")),
            }
        )
    channels.sort(key=lambda entry: entry["messages"], reverse=True)

    return {
        "ok": True,
        "error": "",
        "windows": windows,
        "top": top[:10],
        "top_window_label": str((chosen or {}).get("label") or ""),
        "channels": channels[:10],
        "channel_count": len(channels),
        "tracked": len(seen),
        "excluded_role_count": len(payload.get("excluded_role_ids") or []),
        "top_limit": _int(payload.get("top_limit"), 20),
    }


def summarize_discourse_activity(payload) -> dict:
    """Pick the headline numbers out of the Discourse activity payload."""
    if not isinstance(payload, dict) or not payload.get("ok"):
        return {
            "ok": False,
            "error": str(payload.get("error") if isinstance(payload, dict) else "") or "Forum statistics are unavailable.",
            "staff_total": 0,
            "staff_active": 0,
            "staff_posts": 0,
            "posts_stored": 0,
            "answered": 0,
            "median": "—",
            "p90": "—",
            "within_24h": 0,
            "categories": [],
            "days": 0,
            "top": [],
        }

    activity = [row for row in (payload.get("activity") or []) if isinstance(row, dict)]
    ordered = sorted(activity, key=lambda row: _int(row.get("posts")), reverse=True)
    answered = _int(payload.get("answered"))
    within = _int(payload.get("within_24h"))
    return {
        "ok": True,
        "error": "",
        "staff_total": _int(payload.get("staff_total")),
        "staff_active": _int(payload.get("staff_active")),
        "staff_posts": _int(payload.get("staff_posts")),
        "posts_stored": _int(payload.get("posts_stored")),
        "answered": answered,
        "median": _seconds_to_text(payload.get("median_seconds")),
        "p90": _seconds_to_text(payload.get("p90_seconds")),
        "within_24h": round((within / answered) * 100) if answered else 0,
        "categories": [str(item) for item in (payload.get("categories") or [])],
        "days": _int(payload.get("days")),
        "top": [
            {"name": str(row.get("username") or row.get("name") or ""), "posts": _int(row.get("posts"))}
            for row in ordered[:8]
        ],
    }


def _stat_tile(label: str, value, hint: str = "") -> str:
    hint_html = f"<div class='muted'>{escape(hint)}</div>" if hint else ""
    return (
        "<div class='stat'>"
        f"<div class='stat-label'>{escape(label)}</div>"
        f"<div class='stat-value'>{escape(str(value))}</div>"
        f"{hint_html}"
        "</div>"
    )


def render_activity_overview_body(discord_summary: dict, forum_summary: dict) -> str:
    """Two cards of headline numbers plus a way into each full page."""
    parts: list[str] = []

    parts.append("<div class='card'><h3>Discord activity</h3>")
    if not discord_summary.get("ok"):
        parts.append(f"<p class='muted'>{escape(str(discord_summary.get('error') or 'Unavailable.'))}</p>")
    else:
        parts.append("<div class='stat-row'>")
        parts.append(_stat_tile("Members ranked", discord_summary.get("tracked", 0)))
        parts.append(_stat_tile("Rows per ranking", discord_summary.get("top_limit", 0)))
        parts.append(_stat_tile("Excluded roles", discord_summary.get("excluded_role_count", 0)))
        parts.append("</div>")
        if discord_summary.get("windows"):
            parts.append("<table class='table'><thead><tr><th>Window</th><th>Members</th><th>Messages</th></tr></thead><tbody>")
            for window in discord_summary["windows"]:
                parts.append(
                    "<tr><td>"
                    + escape(str(window.get("label") or ""))
                    + "</td><td>"
                    + str(_int(window.get("count")))
                    + "</td><td>"
                    + str(_int(window.get("messages")))
                    + "</td></tr>"
                )
            parts.append("</tbody></table>")
        if discord_summary.get("top"):
            window_label = str(discord_summary.get("top_window_label") or "").strip()
            heading = f"Most messages ({window_label})" if window_label else "Most messages"
            parts.append(f"<h4>{escape(heading)}</h4><ol class='compact-list'>")
            for row in discord_summary["top"]:
                parts.append(
                    "<li>"
                    + escape(str(row.get("name") or ""))
                    + " — "
                    + str(_int(row.get("messages")))
                    + " messages over "
                    + str(_int(row.get("active_days")))
                    + " active days</li>"
                )
            parts.append("</ol>")
        if discord_summary.get("channels"):
            parts.append("<h4>Busiest channels and threads</h4><ol class='compact-list'>")
            for row in discord_summary["channels"]:
                label = "#" + str(row.get("name") or row.get("channel_id"))
                if row.get("is_thread"):
                    parent = row.get("parent_channel_id") or 0
                    label = "🧵 " + label + (f" (thread in <#{parent}>)" if parent else " (thread)")
                parts.append(
                    "<li>"
                    + escape(label)
                    + " — "
                    + str(_int(row.get("messages")))
                    + " messages over "
                    + str(_int(row.get("active_days")))
                    + " active days</li>"
                )
            parts.append("</ol>")

    parts.append("<p><a class='btn secondary' href='/admin/member-activity'>Open Discord activity</a></p></div>")

    parts.append("<div class='card'><h3>Forum activity</h3>")
    if not forum_summary.get("ok"):
        parts.append(f"<p class='muted'>{escape(str(forum_summary.get('error') or 'Unavailable.'))}</p>")
    else:
        parts.append("<div class='stat-row'>")
        parts.append(_stat_tile("Staff", forum_summary.get("staff_total", 0), f"{forum_summary.get('staff_active', 0)} posted"))
        parts.append(_stat_tile("Staff posts", forum_summary.get("staff_posts", 0), f"{forum_summary.get('posts_stored', 0)} stored"))
        parts.append(_stat_tile("Median first reply", forum_summary.get("median", "—")))
        parts.append(_stat_tile("Answered in 24h", f"{forum_summary.get('within_24h', 0)}%"))
        parts.append("</div>")
        if forum_summary.get("top"):
            parts.append("<h4>Most posts</h4><ol class='compact-list'>")
            for row in forum_summary["top"]:
                parts.append("<li>" + escape(str(row.get("name") or "")) + " — " + str(_int(row.get("posts"))) + " posts</li>")
            parts.append("</ol>")
        if forum_summary.get("categories"):
            parts.append(
                "<p class='muted'>Categories: "
                + escape(", ".join(forum_summary["categories"]))
                + f" · last {_int(forum_summary.get('days'))} days</p>"
            )
    parts.append("<p><a class='btn secondary' href='/admin/discourse-activity'>Open forum activity</a></p></div>")

    return "\n".join(parts)


def render_activity_options_body(options: list[dict], errors: list[str] | None = None, saved: bool = False) -> str:
    """One form covering every option, grouped, with live-vs-restart marked."""
    errors = errors or []
    parts: list[str] = ["<input type='hidden' name='activity_options' value='1'>"]

    if saved and not errors:
        parts.append("<div class='alert success' role='status'>Activity options saved.</div>")
    if errors:
        parts.append("<div class='alert error' role='alert'><strong>Not saved.</strong><ul>")
        for error in errors:
            parts.append(f"<li>{escape(error)}</li>")
        parts.append("</ul></div>")

    for group in OPTION_GROUPS:
        group_options = [option for option in options if option.get("group") == group]
        if not group_options:
            continue
        parts.append(f"<div class='card'><h3>{escape(group)}</h3>")
        for option in group_options:
            key = str(option.get("key") or "")
            label = str(option.get("label") or key)
            value = str(option.get("value") or "")
            help_text = str(option.get("help_text") or "")
            kind = str(option.get("kind") or "text")
            applies = "Applies immediately" if option.get("live") else "Takes effect after a restart"
            parts.append("<div class='field'>")
            parts.append(f"<label for='{escape(key, quote=True)}'>{escape(label)}</label>")
            if kind == "bool":
                checked = " checked" if value.lower() in {"1", "true", "yes", "on"} else ""
                parts.append(
                    f"<input id='{escape(key, quote=True)}' name='{escape(key, quote=True)}' type='checkbox' value='true'{checked}>"
                )
            else:
                input_type = "number" if kind == "int" else "text"
                bounds = ""
                if kind == "int" and option.get("minimum") is not None:
                    bounds += f" min='{int(option['minimum'])}'"
                if kind == "int" and option.get("maximum") is not None:
                    bounds += f" max='{int(option['maximum'])}'"
                placeholder = escape(str(option.get("default") or ""), quote=True)
                parts.append(
                    f"<input id='{escape(key, quote=True)}' name='{escape(key, quote=True)}' "
                    f"type='{input_type}' value='{escape(value, quote=True)}' "
                    f"placeholder='{placeholder}'{bounds}>"
                )
            default_text = str(option.get("default") or "")
            source = "set here" if option.get("configured") else f"default: {default_text or 'none'}"
            parts.append(
                f"<div class='muted'>{escape(help_text)}</div>"
                f"<div class='muted'><code>{escape(key)}</code> · {escape(source)} · {escape(applies)}</div>"
            )
            parts.append("</div>")
        parts.append("</div>")

    parts.append("<div class='card'><h3>Save</h3>")
    parts.append(
        "<p class='muted'>These are written to the web settings file. Values marked "
        "&ldquo;applies immediately&rdquo; are picked up by the running bot; the rest take "
        "effect at the next restart.</p>"
    )
    parts.append("<label class='checkbox'><input type='checkbox' name='confirm' value='yes'> I understand these change activity tracking</label>")
    parts.append("<div class='actions'><button class='btn' type='submit'>Save activity options</button></div>")
    parts.append("</div>")

    return "\n".join(parts)

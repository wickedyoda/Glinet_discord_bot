"""The automod settings form: rules, raid guard and the escalation ladder."""

from __future__ import annotations

from html import escape

from app.automod_settings import ladder_for_form


def _field(label: str, name: str, value, *, kind: str = "text", hint: str = "", minimum=None, maximum=None) -> str:
    if kind == "checkbox":
        checked = " checked" if str(value).lower() in {"1", "true", "yes", "on"} else ""
        return (
            f"<label class='checkbox'><input type='checkbox' name='{escape(name, quote=True)}' "
            f"value='1'{checked}> {escape(label)}</label>"
            + (f"<div class='muted'>{escape(hint)}</div>" if hint else "")
        )
    bounds = ""
    if minimum is not None:
        bounds += f" min='{minimum}'"
    if maximum is not None:
        bounds += f" max='{maximum}'"
    input_type = "number" if kind == "number" else "text"
    return (
        "<div class='field'>"
        f"<label for='{escape(name, quote=True)}'>{escape(label)}</label>"
        f"<input id='{escape(name, quote=True)}' name='{escape(name, quote=True)}' type='{input_type}' "
        f"value='{escape(str(value if value is not None else ''), quote=True)}'{bounds}>"
        + (f"<div class='muted'>{escape(hint)}</div>" if hint else "")
        + "</div>"
    )


def render_automod_body(automod: dict, raid: dict, ladder: dict, *, errors=None, saved: bool = False) -> str:
    errors = errors or []
    automod = automod or {}
    raid = raid or {}
    ladder_fields = ladder_for_form(ladder or {})

    parts: list[str] = []
    if saved and not errors:
        parts.append("<div class='alert success' role='status'>Automod settings saved.</div>")
    if errors:
        parts.append("<div class='alert error' role='alert'><strong>Not saved.</strong><ul>")
        for error in errors:
            parts.append(f"<li>{escape(error)}</li>")
        parts.append("</ul></div>")

    regex_text = "\n".join(
        f"{rule.get('pattern', '')} => {rule.get('reason', '')}"
        for rule in (automod.get("regex_rules") or [])
        if isinstance(rule, dict)
    )

    parts.append("<div class='card'><h3>Message rules</h3>")
    parts.append(_field("Automod enabled", "enabled", automod.get("enabled"), kind="checkbox",
                        hint="Off by default. Nothing below runs until this is ticked."))
    parts.append(_field("Delete the offending message", "delete_on_violation",
                        automod.get("delete_on_violation", True), kind="checkbox"))
    parts.append(_field("Messages per window", "max_messages", automod.get("max_messages", 8),
                        kind="number", minimum=2, maximum=60,
                        hint="A member sending more than this inside the window is flooding."))
    parts.append(_field("Window (seconds)", "window_seconds", automod.get("window_seconds", 10),
                        kind="number", minimum=3, maximum=120))
    parts.append(_field("Repeats before a duplicate is flagged", "duplicate_count",
                        automod.get("duplicate_count", 4), kind="number", minimum=2, maximum=20))
    parts.append(_field("Mentions per message", "max_mentions", automod.get("max_mentions", 6),
                        kind="number", minimum=1, maximum=50))
    parts.append(_field("Capital-letter ratio", "max_caps_ratio", automod.get("max_caps_ratio", 0.75),
                        hint="0.75 means three quarters of the letters are capitals."))
    parts.append(_field("Emoji per message", "max_emoji", automod.get("max_emoji", 15),
                        kind="number", minimum=1, maximum=100))
    parts.append(_field("Combining characters (zalgo)", "max_zalgo", automod.get("max_zalgo", 12),
                        kind="number", minimum=1, maximum=100))
    parts.append("</div>")

    parts.append("<div class='card'><h3>Links and attachments</h3>")
    parts.append(_field("Block Discord invites", "block_invites", automod.get("block_invites", True),
                        kind="checkbox"))
    parts.append(_field("Block all other links", "block_links", automod.get("block_links", False),
                        kind="checkbox", hint="When on, only the allowed domains below get through."))
    parts.append(_field("Allowed domains", "allowed_domains",
                        ", ".join(automod.get("allowed_domains") or []),
                        hint="Comma-separated, for example gl-inet.com, docs.gl-inet.com."))
    parts.append(_field("Blocked extensions", "blocked_extensions",
                        ", ".join(automod.get("blocked_extensions") or []),
                        hint="Comma-separated, without the dot."))
    parts.append(_field("Maximum attachment size (MB)", "max_attachment_mb",
                        automod.get("max_attachment_mb", 25), kind="number", minimum=1, maximum=500))
    parts.append(_field("Exempt role IDs", "exempt_role_ids",
                        ", ".join(str(item) for item in (automod.get("exempt_role_ids") or [])),
                        hint="Staff roles that are never checked."))
    parts.append(_field("Exempt channel IDs", "exempt_channel_ids",
                        ", ".join(str(item) for item in (automod.get("exempt_channel_ids") or []))))
    parts.append("</div>")

    parts.append("<div class='card'><h3>Regex rules</h3>")
    parts.append(
        "<div class='field'><label for='regex_rules'>One rule per line</label>"
        f"<textarea id='regex_rules' name='regex_rules' rows='5' "
        f"placeholder='free\\\\s+nitro => nitro scam'>{escape(regex_text)}</textarea>"
        "<div class='muted'>Write <code>pattern =&gt; reason</code>. An invalid pattern is "
        "rejected with its line number instead of being stored and skipped.</div></div>"
    )
    parts.append("</div>")

    parts.append("<div class='card'><h3>Quiet hours</h3>")
    parts.append(_field("Quiet start hour (UTC)", "quiet_start_hour", automod.get("quiet_start_hour"),
                        kind="number", minimum=0, maximum=23, hint="Leave both blank to disable."))
    parts.append(_field("Quiet end hour (UTC)", "quiet_end_hour", automod.get("quiet_end_hour"),
                        kind="number", minimum=0, maximum=23))
    parts.append(_field("Tightening multiplier", "quiet_multiplier", automod.get("quiet_multiplier", 1.0),
                        hint="2.0 halves the flood, duplicate, mention and emoji thresholds."))
    parts.append("</div>")

    parts.append("<div class='card'><h3>Raid guard</h3>")
    parts.append(_field("Raid guard enabled", "raid_enabled", raid.get("enabled"), kind="checkbox"))
    parts.append(_field("Joins per minute before a raid", "max_joins_per_minute",
                        raid.get("max_joins_per_minute", 8), kind="number", minimum=2, maximum=100))
    parts.append(_field("Minimum account age (hours)", "min_account_age_hours",
                        raid.get("min_account_age_hours", 0), kind="number", minimum=0, maximum=8760,
                        hint="0 disables the age gate."))
    parts.append(_field("Quarantine role ID", "quarantine_role_id", raid.get("quarantine_role_id") or "",
                        hint="New or suspicious accounts get this role until verified."))
    parts.append(_field("Lock down on a raid", "auto_lockdown", raid.get("auto_lockdown"),
                        kind="checkbox"))
    parts.append(_field("Lockdown length (minutes)", "lockdown_minutes",
                        raid.get("lockdown_minutes", 30), kind="number", minimum=5, maximum=1440))
    parts.append("</div>")

    parts.append("<div class='card'><h3>Escalation ladder</h3>")
    parts.append(_field("Timeout at strikes", "ladder_timeout_strikes", ladder_fields["timeout_strikes"],
                        kind="number", minimum=1, maximum=50))
    parts.append(_field("Timeout minutes", "ladder_timeout_minutes", ladder_fields["timeout_minutes"],
                        kind="number", minimum=1, maximum=10080))
    parts.append(_field("Kick at strikes", "ladder_kick_strikes", ladder_fields["kick_strikes"],
                        kind="number", minimum=2, maximum=50))
    parts.append(_field("Ban at strikes", "ladder_ban_strikes", ladder_fields["ban_strikes"],
                        kind="number", minimum=3, maximum=50))
    parts.append(
        "<div class='muted'>Strikes are active warnings; clearing a warning removes a strike. "
        "The thresholds must increase.</div>"
    )
    parts.append("</div>")

    parts.append("<div class='card'><h3>Save</h3>")
    parts.append("<label class='checkbox'><input type='checkbox' name='confirm' value='yes'> "
                 "I understand these rules act on members automatically</label>")
    parts.append("<div class='actions'><button class='btn' type='submit'>Save automod settings</button></div>")
    parts.append("</div>")
    return "\n".join(parts)

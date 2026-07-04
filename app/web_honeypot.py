from __future__ import annotations

from html import escape

from app.honeypot import (
    HONEYPOT_ACTION_BAN,
    HONEYPOT_ACTION_ROLE,
    HONEYPOT_ACTION_SOFTBAN,
    HONEYPOT_ACTION_TIMEOUT,
    format_honeypot_join_guard_summary,
    format_honeypot_summary,
    normalize_honeypot_action,
)

HONEYPOT_ACTION_OPTIONS = [
    {"value": HONEYPOT_ACTION_SOFTBAN, "label": "Soft ban"},
    {"value": HONEYPOT_ACTION_BAN, "label": "Ban"},
    {"value": HONEYPOT_ACTION_TIMEOUT, "label": "Timeout"},
    {"value": HONEYPOT_ACTION_ROLE, "label": "Grant role"},
]

HONEYPOT_ENABLED_OPTIONS = [
    {"value": "1", "label": "Enabled"},
    {"value": "0", "label": "Disabled"},
]

HONEYPOT_DELETE_MESSAGE_DAY_OPTIONS = [
    {"value": str(value), "label": f"{value} day(s)"} for value in range(0, 6)
]

HONEYPOT_TIMEOUT_HOUR_OPTIONS = [
    {"value": str(value), "label": f"{value} hour(s)"} for value in (1, 2, 4, 8, 12, 24, 48, 72, 168, 336, 672)
]

HONEYPOT_JOIN_AGE_OPTIONS = [
    {"value": str(value), "label": f"{value} hour(s)"} for value in (1, 6, 12, 24, 48, 72, 168, 336, 720, 1440, 2160, 4320, 8760)
]


def process_honeypot_submission(
    *,
    form,
    on_manage_honeypot,
    actor_email: str,
    selected_guild_id: str,
):
    messages: list[tuple[str, str]] = []
    if not callable(on_manage_honeypot):
        messages.append(("Honeypot management callback is not configured.", "error"))
        return None, messages
    action = str(form.get("action") or "").strip()
    payload = {"action": action}
    if action in {"create_entry", "update_entry"}:
        payload.update(
            {
                "channel_id": form.get("channel_id", ""),
                "honeypot_action": form.get("honeypot_action", ""),
                "delete_message_days": form.get("delete_message_days", ""),
                "timeout_hours": form.get("timeout_hours", ""),
                "role_id": form.get("role_id", ""),
                "enabled": form.get("enabled", "1"),
            }
        )
    elif action == "delete_entry":
        payload["channel_id"] = form.get("channel_id", "")
    elif action == "delete_all":
        payload["confirm"] = form.get("confirm", "")
    elif action == "save_logging":
        payload["log_channel_id"] = form.get("log_channel_id", "")
        payload["log_role_id"] = form.get("log_role_id", "")
    elif action == "save_join_guard":
        payload.update(
            {
                "join_guard_enabled": form.get("join_guard_enabled", "0"),
                "join_guard_action": form.get("join_guard_action", ""),
                "join_guard_min_account_age_hours": form.get("join_guard_min_account_age_hours", ""),
                "join_guard_delete_message_days": form.get("join_guard_delete_message_days", ""),
                "join_guard_timeout_hours": form.get("join_guard_timeout_hours", ""),
                "join_guard_role_id": form.get("join_guard_role_id", ""),
            }
        )
    elif action == "disable_join_guard":
        payload["join_guard_enabled"] = "0"
    else:
        messages.append(("Unknown honeypot action.", "error"))
        return None, messages

    response = on_manage_honeypot(payload, actor_email, selected_guild_id)
    if not isinstance(response, dict):
        messages.append(("Invalid response from honeypot handler.", "error"))
        return None, messages
    if not response.get("ok"):
        messages.append((str(response.get("error") or "Failed to update honeypot settings."), "error"))
        return None, messages
    messages.append((str(response.get("message") or "Honeypot settings updated."), "success"))
    return response, messages


def render_honeypot_body(
    *,
    guild_name: str,
    payload: dict,
    text_channel_options: list[dict],
    role_options: list[dict],
    catalog_error: str,
    render_select_input,
    render_fixed_select_input,
):
    entries = list(payload.get("entries") or [])
    logging_settings = dict(payload.get("logging_settings") or {})
    join_guard = dict(payload.get("join_guard") or {})
    catalog_note = ""
    if text_channel_options or role_options:
        catalog_note = (
            f"<p class='muted'>Loaded live Discord options from <strong>{escape(guild_name)}</strong>. "
            f"Text channels: {len(text_channel_options)}; Roles: {len(role_options)}.</p>"
        )
    elif catalog_error:
        catalog_note = f"<p class='muted'>Could not load Discord options: {escape(catalog_error)}</p>"

    create_channel_select = render_select_input("channel_id", "", text_channel_options, placeholder="Choose honeypot channel")
    create_role_select = render_select_input("role_id", "", role_options, placeholder="No role")
    create_action_select = render_fixed_select_input(
        "honeypot_action",
        HONEYPOT_ACTION_SOFTBAN,
        HONEYPOT_ACTION_OPTIONS,
        placeholder="Choose action",
    )
    create_delete_days_select = render_fixed_select_input(
        "delete_message_days",
        "1",
        HONEYPOT_DELETE_MESSAGE_DAY_OPTIONS,
        placeholder="Delete days",
    )
    create_timeout_hours_select = render_fixed_select_input(
        "timeout_hours",
        "24",
        HONEYPOT_TIMEOUT_HOUR_OPTIONS,
        placeholder="Timeout length",
    )
    create_enabled_select = render_fixed_select_input(
        "enabled",
        "1",
        HONEYPOT_ENABLED_OPTIONS,
        placeholder="Enabled?",
    )

    entry_cards = []
    for entry in entries:
        channel_id = str(int(entry.get("channel_id") or 0))
        update_action_select = render_fixed_select_input(
            "honeypot_action",
            normalize_honeypot_action(entry.get("action")),
            HONEYPOT_ACTION_OPTIONS,
            placeholder="Choose action",
        )
        update_delete_days_select = render_fixed_select_input(
            "delete_message_days",
            str(int(entry.get("delete_message_days") or 1)),
            HONEYPOT_DELETE_MESSAGE_DAY_OPTIONS,
            placeholder="Delete days",
        )
        update_timeout_hours_select = render_fixed_select_input(
            "timeout_hours",
            str(int(entry.get("timeout_hours") or 24)),
            HONEYPOT_TIMEOUT_HOUR_OPTIONS,
            placeholder="Timeout length",
        )
        update_role_select = render_select_input(
            "role_id",
            str(int(entry.get("role_id") or 0)) if int(entry.get("role_id") or 0) > 0 else "",
            role_options,
            placeholder="No role",
        )
        update_enabled_select = render_fixed_select_input(
            "enabled",
            "1" if int(entry.get("enabled") or 0) > 0 else "0",
            HONEYPOT_ENABLED_OPTIONS,
            placeholder="Enabled?",
        )
        entry_cards.append(
            f"""
            <div class='card' style='margin-top:14px;'>
              <h4 style='margin-top:0;'>Channel {escape(channel_id)}</h4>
              <p class='muted'>{escape(format_honeypot_summary(entry))}</p>
              <form method='post'>
                <input type='hidden' name='action' value='update_entry' />
                <input type='hidden' name='channel_id' value='{escape(channel_id, quote=True)}' />
                <table>
                  <thead><tr><th>Action</th><th>Delete Days</th><th>Timeout Hours</th><th>Role</th><th>Enabled</th></tr></thead>
                  <tbody>
                    <tr>
                      <td>{update_action_select}</td>
                      <td>{update_delete_days_select}</td>
                      <td>{update_timeout_hours_select}</td>
                      <td>{update_role_select}</td>
                      <td>{update_enabled_select}</td>
                    </tr>
                  </tbody>
                </table>
                <div style='margin-top:14px; display:flex; gap:10px; flex-wrap:wrap;'>
                  <button class='btn secondary' type='submit'>Save Honeypot</button>
                </div>
              </form>
              <form method='post' style='margin-top:10px;'>
                <input type='hidden' name='action' value='delete_entry' />
                <input type='hidden' name='channel_id' value='{escape(channel_id, quote=True)}' />
                <button class='btn danger' type='submit' onclick="return confirm('Delete this honeypot?');">Delete Honeypot</button>
              </form>
            </div>
            """
        )
    configured_entries_html = "".join(entry_cards) if entry_cards else "<p class='muted'>No honeypot channels configured yet.</p>"

    logging_channel_select = render_select_input(
        "log_channel_id",
        str(int(logging_settings.get("channel_id") or 0)) if int(logging_settings.get("channel_id") or 0) > 0 else "",
        text_channel_options,
        placeholder="Disabled",
    )
    logging_role_select = render_select_input(
        "log_role_id",
        str(int(logging_settings.get("role_id") or 0)) if int(logging_settings.get("role_id") or 0) > 0 else "",
        role_options,
        placeholder="No role ping",
    )

    join_enabled_select = render_fixed_select_input(
        "join_guard_enabled",
        "1" if int(join_guard.get("enabled") or 0) > 0 else "0",
        HONEYPOT_ENABLED_OPTIONS,
        placeholder="Enabled?",
    )
    join_action_select = render_fixed_select_input(
        "join_guard_action",
        normalize_honeypot_action(join_guard.get("action")),
        HONEYPOT_ACTION_OPTIONS,
        placeholder="Choose action",
    )
    join_age_select = render_fixed_select_input(
        "join_guard_min_account_age_hours",
        str(int(join_guard.get("min_account_age_hours") or 72)),
        HONEYPOT_JOIN_AGE_OPTIONS,
        placeholder="Choose age",
    )
    join_delete_days_select = render_fixed_select_input(
        "join_guard_delete_message_days",
        str(int(join_guard.get("delete_message_days") or 1)),
        HONEYPOT_DELETE_MESSAGE_DAY_OPTIONS,
        placeholder="Delete days",
    )
    join_timeout_hours_select = render_fixed_select_input(
        "join_guard_timeout_hours",
        str(int(join_guard.get("timeout_hours") or 24)),
        HONEYPOT_TIMEOUT_HOUR_OPTIONS,
        placeholder="Timeout length",
    )
    join_role_select = render_select_input(
        "join_guard_role_id",
        str(int(join_guard.get("role_id") or 0)) if int(join_guard.get("role_id") or 0) > 0 else "",
        role_options,
        placeholder="No role",
    )

    return f"""
    <div class='card'>
      <h2>Honeypot</h2>
      <p class='muted'>Manage spam-trap channels, honeypot action logging, and join-time screening for <strong>{escape(guild_name)}</strong>.</p>
      <p class='muted'>Use trap channels to catch flood bots after they speak, and use the join guard to act on suspicious new accounts before they can spam.</p>
      {catalog_note}
    </div>

    <div class='card'>
      <h3>Create Honeypot Channel</h3>
      <form method='post'>
        <input type='hidden' name='action' value='create_entry' />
        <table>
          <thead><tr><th>Channel</th><th>Action</th><th>Delete Days</th><th>Timeout Hours</th><th>Role</th><th>Enabled</th></tr></thead>
          <tbody>
            <tr>
              <td>{create_channel_select}</td>
              <td>{create_action_select}</td>
              <td>{create_delete_days_select}</td>
              <td>{create_timeout_hours_select}</td>
              <td>{create_role_select}</td>
              <td>{create_enabled_select}</td>
            </tr>
          </tbody>
        </table>
        <div style='margin-top:14px;'>
          <button class='btn' type='submit'>Create Honeypot</button>
        </div>
      </form>
    </div>

    <div class='card'>
      <h3>Configured Honeypot Channels</h3>
      {configured_entries_html}
      <form method='post' style='margin-top:14px;'>
        <input type='hidden' name='action' value='delete_all' />
        <input type='hidden' name='confirm' value='yes' />
        <button class='btn danger' type='submit' onclick="return confirm('Delete every honeypot in this server?');">Delete All Honeypots</button>
      </form>
    </div>

    <div class='card'>
      <h3>Honeypot Logging</h3>
      <form method='post'>
        <input type='hidden' name='action' value='save_logging' />
        <table>
          <thead><tr><th>Setting</th><th>Value</th></tr></thead>
          <tbody>
            <tr>
              <td><strong>Log Channel</strong></td>
              <td>{logging_channel_select}</td>
            </tr>
            <tr>
              <td><strong>Role Ping</strong></td>
              <td>{logging_role_select}</td>
            </tr>
          </tbody>
        </table>
        <div style='margin-top:14px;'>
          <button class='btn' type='submit'>Save Logging Settings</button>
        </div>
      </form>
    </div>

    <div class='card'>
      <h3>Join Guard</h3>
      <p class='muted'>Catch suspiciously new accounts at join time before they can flood channels.</p>
      <form method='post'>
        <input type='hidden' name='action' value='save_join_guard' />
        <table>
          <thead><tr><th>Setting</th><th>Value</th></tr></thead>
          <tbody>
            <tr>
              <td><strong>Enabled</strong></td>
              <td>{join_enabled_select}</td>
            </tr>
            <tr>
              <td><strong>Minimum Account Age</strong></td>
              <td>{join_age_select}</td>
            </tr>
            <tr>
              <td><strong>Action</strong></td>
              <td>{join_action_select}</td>
            </tr>
            <tr>
              <td><strong>Delete Message Days</strong></td>
              <td>{join_delete_days_select}</td>
            </tr>
            <tr>
              <td><strong>Timeout Hours</strong></td>
              <td>{join_timeout_hours_select}</td>
            </tr>
            <tr>
              <td><strong>Role</strong></td>
              <td>{join_role_select}</td>
            </tr>
          </tbody>
        </table>
        <div class='muted' style='margin-top:10px;'>Current summary: {escape(format_honeypot_join_guard_summary(join_guard))}</div>
        <div style='margin-top:14px; display:flex; gap:10px; flex-wrap:wrap;'>
          <button class='btn' type='submit'>Save Join Guard</button>
        </div>
      </form>
      <form method='post' style='margin-top:14px;'>
        <input type='hidden' name='action' value='disable_join_guard' />
        <button class='btn danger' type='submit'>Disable Join Guard</button>
      </form>
    </div>
    """

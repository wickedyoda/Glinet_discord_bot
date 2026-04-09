from __future__ import annotations

from html import escape

MEMBER_PAGE_SIZE = 50
MEMBER_TIMEOUT_OPTIONS = (
    ("15m", "15 minutes"),
    ("30m", "30 minutes"),
    ("1h", "1 hour"),
    ("6h", "6 hours"),
    ("12h", "12 hours"),
    ("1d", "1 day"),
    ("7d", "7 days"),
)


def process_member_action_submission(
    *,
    form,
    on_manage_member,
    actor_email: str,
    selected_guild_id: str,
):
    messages: list[tuple[str, str]] = []
    if not callable(on_manage_member):
        messages.append(("Member management callback is not configured.", "error"))
        return None, messages

    action = str(form.get("action") or "").strip().lower()
    member_id = str(form.get("member_id") or "").strip()
    role_id = str(form.get("role_id") or "").strip()
    duration = str(form.get("duration") or "").strip()
    reason = str(form.get("reason") or "").strip()
    if not action or not member_id:
        messages.append(("Member action is missing required fields.", "error"))
        return None, messages

    payload = {
        "action": action,
        "member_id": member_id,
        "role_id": role_id,
        "duration": duration,
        "reason": reason,
    }
    response = on_manage_member(payload, actor_email, selected_guild_id)
    if not isinstance(response, dict):
        messages.append(("Invalid response from member management handler.", "error"))
        return None, messages
    if not response.get("ok"):
        messages.append((str(response.get("error") or "Failed to update member."), "error"))
        return response, messages
    messages.append((str(response.get("message") or "Member updated."), "success"))
    return response, messages


def render_members_body(
    *,
    guild_name: str,
    members_payload: dict,
    role_options: list[dict],
    catalog_error: str,
    current_query: str,
    current_role_id: str,
):
    if not isinstance(members_payload, dict) or not members_payload.get("ok"):
        error_text = (
            str(members_payload.get("error") or "Unable to load members.")
            if isinstance(members_payload, dict)
            else "Unable to load members."
        )
        return f"<div class='card'><h2>Members</h2><p class='muted'>{escape(error_text)}</p></div>"

    members = members_payload.get("members", []) or []
    page = max(1, int(members_payload.get("page") or 1))
    total_count = max(0, int(members_payload.get("total_count") or 0))
    page_size = max(1, int(members_payload.get("page_size") or MEMBER_PAGE_SIZE))
    total_pages = max(1, int(members_payload.get("total_pages") or 1))
    start_index = max(0, int(members_payload.get("start_index") or 0))
    end_index = max(0, int(members_payload.get("end_index") or 0))
    has_prev = bool(members_payload.get("has_prev"))
    has_next = bool(members_payload.get("has_next"))

    role_filter_options = ["<option value=''>All roles</option>"]
    role_action_options = ["<option value=''>Select role</option>"]
    for option in role_options:
        value = str(option.get("id") or "").strip()
        label = str(option.get("label") or option.get("name") or value).strip()
        if not value or not label:
            continue
        selected = " selected" if value == current_role_id else ""
        role_filter_options.append(f"<option value='{escape(value, quote=True)}'{selected}>{escape(label)}</option>")
        role_action_options.append(f"<option value='{escape(value, quote=True)}'>{escape(label)}</option>")
    role_filter_select_html = "".join(role_filter_options)
    role_action_select_html = "".join(role_action_options)
    duration_select_html = "".join(
        f"<option value='{escape(value, quote=True)}'>{escape(label)}</option>"
        for value, label in MEMBER_TIMEOUT_OPTIONS
    )

    catalog_note = ""
    if role_options:
        catalog_note = (
            f"<p class='muted'>Loaded live Discord role options from <strong>{escape(guild_name)}</strong>. "
            f"Roles available for assignment: {len(role_options)}.</p>"
        )
    elif catalog_error:
        catalog_note = f"<p class='muted'>Could not load Discord role options: {escape(catalog_error)}</p>"

    member_rows = []
    for entry in members:
        member_id = str(entry.get("id") or "").strip()
        display_name = str(entry.get("display_name") or entry.get("name") or member_id).strip()
        account_name = str(entry.get("account_name") or "").strip()
        joined_label = str(entry.get("joined_at_label") or "Unknown").strip()
        roles_label = str(entry.get("roles_label") or "No roles").strip()
        state_bits = []
        if entry.get("is_owner"):
            state_bits.append("Owner")
        if entry.get("is_bot"):
            state_bits.append("Bot")
        if entry.get("timed_out"):
            until_label = str(entry.get("timed_out_until_label") or "").strip()
            state_bits.append(f"Timed out until {until_label}" if until_label else "Timed out")
        status_label = " | ".join(state_bits) if state_bits else "Active"
        member_rows.append(
            f"""
            <tr>
              <td>
                <strong>{escape(display_name)}</strong>
                <div class='muted'>{escape(account_name or 'Unknown account')}</div>
                <div class='muted mono'>{escape(member_id)}</div>
              </td>
              <td class='muted'>{escape(joined_label)}</td>
              <td class='muted'>{escape(roles_label)}</td>
              <td class='muted'>{escape(status_label)}</td>
              <td>
                <form method='post' style='display:grid; gap:8px;'>
                  <input type='hidden' name='member_id' value='{escape(member_id, quote=True)}' />
                  <input type='hidden' name='q' value='{escape(current_query, quote=True)}' />
                  <input type='hidden' name='role_filter_id' value='{escape(current_role_id, quote=True)}' />
                  <input type='hidden' name='page' value='{page}' />
                  <input type='text' name='reason' placeholder='Reason (optional)' />
                  <select name='role_id'>{role_action_select_html}</select>
                  <select name='duration'>{duration_select_html}</select>
                  <div style='display:flex; flex-wrap:wrap; gap:8px;'>
                    <button class='btn secondary' type='submit' name='action' value='add_role'>Add Role</button>
                    <button class='btn secondary' type='submit' name='action' value='remove_role'>Remove Role</button>
                    <button class='btn secondary' type='submit' name='action' value='timeout'>Timeout</button>
                    <button class='btn secondary' type='submit' name='action' value='untimeout'>Remove Timeout</button>
                    <button class='btn danger' type='submit' name='action' value='kick'
                      onclick="return confirm('Kick this member now?');">Kick</button>
                  </div>
                </form>
              </td>
            </tr>
            """
        )

    if not member_rows:
        member_rows.append(
            "<tr><td colspan='5' class='muted'>No members matched the current filters.</td></tr>"
        )

    page_links = []
    page_window_start = max(1, page - 2)
    page_window_end = min(total_pages, page + 2)
    if page_window_start > 1:
        page_links.append(
            f"<a class='btn secondary' href='?q={escape(current_query, quote=True)}&role_id={escape(current_role_id, quote=True)}&page=1'>1</a>"
        )
        if page_window_start > 2:
            page_links.append("<span class='muted'>...</span>")
    for page_number in range(page_window_start, page_window_end + 1):
        if page_number == page:
            page_links.append(f"<button class='btn' type='button' disabled>{page_number}</button>")
        else:
            page_links.append(
                f"<a class='btn secondary' href='?q={escape(current_query, quote=True)}&role_id={escape(current_role_id, quote=True)}&page={page_number}'>{page_number}</a>"
            )
    if page_window_end < total_pages:
        if page_window_end < total_pages - 1:
            page_links.append("<span class='muted'>...</span>")
        page_links.append(
            f"<a class='btn secondary' href='?q={escape(current_query, quote=True)}&role_id={escape(current_role_id, quote=True)}&page={total_pages}'>{total_pages}</a>"
        )

    prev_link = (
        f"<a class='btn secondary' href='?q={escape(current_query, quote=True)}&role_id={escape(current_role_id, quote=True)}&page={page - 1}'>Previous</a>"
        if has_prev
        else "<button class='btn secondary' type='button' disabled>Previous</button>"
    )
    next_link = (
        f"<a class='btn secondary' href='?q={escape(current_query, quote=True)}&role_id={escape(current_role_id, quote=True)}&page={page + 1}'>Next</a>"
        if has_next
        else "<button class='btn secondary' type='button' disabled>Next</button>"
    )

    return f"""
    <div class='card'>
      <h2>Guild Members</h2>
      <p class='muted'>Browse members for <strong>{escape(guild_name)}</strong>, kick members, and add or remove roles without leaving the web admin.</p>
      <p class='muted'>Role actions and kicks still respect the bot's Discord permissions and role hierarchy.</p>
      {catalog_note}
      <form method='get' style='display:flex; flex-wrap:wrap; gap:12px; align-items:end; margin-bottom:16px;'>
        <div style='flex:1 1 280px;'>
          <label for='member-search'><strong>Search Members</strong></label>
          <input id='member-search' type='text' name='q' value='{escape(current_query, quote=True)}' placeholder='Name, username, or member ID' />
        </div>
        <div style='flex:1 1 220px;'>
          <label for='member-role-filter'><strong>Filter By Role</strong></label>
          <select id='member-role-filter' name='role_id'>{role_filter_select_html}</select>
        </div>
        <input type='hidden' name='page' value='1' />
        <button class='btn' type='submit'>Apply Filters</button>
      </form>

      <div class='muted' style='margin-bottom:10px;'>
        Showing {start_index}-{end_index} of {total_count} matching members. Page {page} of {total_pages}. Page size: {page_size}.
      </div>

      <table>
        <thead>
          <tr><th>Member</th><th>Joined</th><th>Current Roles</th><th>Status</th><th>Actions</th></tr>
        </thead>
        <tbody>
          {''.join(member_rows)}
        </tbody>
      </table>

      <div style='display:flex; gap:8px; margin-top:14px; flex-wrap:wrap; align-items:center;'>
        {prev_link}
        {''.join(page_links)}
        {next_link}
        <form method='get' style='display:flex; gap:8px; align-items:center; margin-left:auto;'>
          <input type='hidden' name='q' value='{escape(current_query, quote=True)}' />
          <input type='hidden' name='role_id' value='{escape(current_role_id, quote=True)}' />
          <label for='members-page-jump' class='muted'><strong>Jump to page</strong></label>
          <input id='members-page-jump' type='number' name='page' value='{page}' min='1' max='{total_pages}' style='width:90px;' />
          <button class='btn secondary' type='submit'>Go</button>
        </form>
      </div>
    </div>
    """

from __future__ import annotations

from html import escape

ROLE_ACCESS_STATUS_OPTIONS = (
    {"value": "active", "label": "Active"},
    {"value": "paused", "label": "Paused"},
    {"value": "disabled", "label": "Disabled"},
)


def process_role_access_submission(*, form, on_manage_role_access_mappings, actor_email: str, selected_guild_id: str):
    messages: list[tuple[str, str]] = []
    if not callable(on_manage_role_access_mappings):
        messages.append(("Role access update callback is not configured.", "error"))
        return None, messages

    payload = {
        "action": str(form.get("action") or "").strip(),
        "code": str(form.get("code") or "").strip(),
        "invite": str(form.get("invite") or "").strip(),
        "role_id": str(form.get("role_id") or "").strip(),
        "status": str(form.get("status") or "").strip().lower(),
    }
    response = on_manage_role_access_mappings(payload, actor_email, selected_guild_id)
    if not isinstance(response, dict):
        messages.append(("Invalid response from role access handler.", "error"))
        return None, messages
    if not response.get("ok"):
        messages.append((str(response.get("error") or "Failed to update role access mappings."), "error"))
        return None, messages

    messages.append((str(response.get("message") or "Role access mappings updated."), "success"))
    return response, messages


def render_role_access_body(
    *,
    guild_name: str,
    mappings: list[dict],
    role_options: list[dict],
    catalog_error: str,
    render_select_input,
    render_fixed_select_input,
):
    role_picker_note = ""
    if catalog_error:
        role_picker_note = f"<p class='muted'>Could not load Discord role list: {escape(catalog_error)}</p>"
    elif role_options:
        role_picker_note = f"<p class='muted'>Loaded live Discord roles from <strong>{escape(guild_name)}</strong>.</p>"
    else:
        role_picker_note = "<p class='muted'>Discord role list is unavailable right now. Existing role IDs can still be viewed.</p>"

    rows = []
    role_labels = {
        str(option.get("id") or "").strip(): str(option.get("label") or option.get("name") or option.get("id") or "").strip()
        for option in role_options
        if str(option.get("id") or "").strip()
    }
    for mapping in mappings:
        code = str(mapping.get("code") or "")
        invite_code = str(mapping.get("invite_code") or "")
        invite_url = str(mapping.get("invite_url") or "")
        role_id = str(mapping.get("role_id") or "")
        status = str(mapping.get("status") or "active").strip().lower()
        created_at = str(mapping.get("created_at") or "")
        updated_at = str(mapping.get("updated_at") or "")
        role_label = role_labels.get(role_id, role_id or "n/a")
        rows.append(
            f"""
            <tr>
              <td><span class="mono">{escape(code or 'n/a')}</span></td>
              <td>{f"<a href='{escape(invite_url, quote=True)}' target='_blank' rel='noopener noreferrer'>{escape(invite_url)}</a>" if invite_url else "<span class='muted'>n/a</span>"}</td>
              <td class="mono">{escape(invite_code or 'n/a')}</td>
              <td>{escape(role_label)}<div class="muted mono">{escape(role_id or 'n/a')}</div></td>
              <td>{escape(status.title())}</td>
              <td class="muted">
                <div class="mono">{escape(created_at or 'n/a')}</div>
                <div>{escape(updated_at or 'n/a')}</div>
              </td>
              <td>
                <form method="post">
                  <input type="hidden" name="code" value="{escape(code, quote=True)}" />
                  <input type="hidden" name="invite" value="{escape(invite_code, quote=True)}" />
                  <div class="dash-actions">
                    <button class="btn secondary" type="submit" name="action" value="set_status" formaction="" onclick="this.form.status.value='active';">Activate</button>
                    <button class="btn secondary" type="submit" name="action" value="set_status" formaction="" onclick="this.form.status.value='paused';">Pause</button>
                    <button class="btn danger" type="submit" name="action" value="set_status" formaction="" onclick="this.form.status.value='disabled';">Disable</button>
                  </div>
                  <input type="hidden" name="status" value="{escape(status, quote=True)}" />
                </form>
              </td>
            </tr>
            """
        )

    add_role_input = render_select_input(
        "role_id",
        "",
        role_options,
        placeholder="Choose role...",
    )
    add_status_input = render_fixed_select_input(
        "status",
        "active",
        list(ROLE_ACCESS_STATUS_OPTIONS),
        placeholder="Select status...",
    )

    return f"""
        <div class="card">
          <h2>Role Access Mappings</h2>
          <p class="muted">Manage invite links and 6-digit access codes for <strong>{escape(guild_name)}</strong>. Paused and disabled mappings stop working immediately for both join-by-invite and <span class="mono">/enter_role</span>.</p>
          <p class="muted">Use the form at the bottom to manually restore an invite/code/role mapping if an entry was deleted from storage and needs to be re-added.</p>
          {role_picker_note}
          <table>
            <thead>
              <tr><th>Code</th><th>Invite Link</th><th>Invite Code</th><th>Role</th><th>Status</th><th>Timestamps</th><th>Quick Actions</th></tr>
            </thead>
            <tbody>
              {"".join(rows) if rows else "<tr><td colspan='7' class='muted'>No role access mappings are configured yet.</td></tr>"}
            </tbody>
          </table>
        </div>
        <div class="card">
          <h2>Manual Restore / Add</h2>
          <p class="muted">Enter an existing Discord invite and 6-digit code to restore or recreate a mapping. The invite must belong to this server.</p>
          <form method="post">
            <input type="hidden" name="action" value="save" />
            <label>6-digit code</label>
            <input type="text" name="code" placeholder="531580" maxlength="6" />
            <label>Discord invite URL or code</label>
            <input type="text" name="invite" placeholder="https://discord.gg/example or example" />
            <label>Role</label>
            {add_role_input}
            <label>Status</label>
            {add_status_input}
            <div style="margin-top:14px;">
              <button class="btn" type="submit">Save Role Access Mapping</button>
            </div>
          </form>
        </div>
        """

from __future__ import annotations

from html import escape

from app.web_time import format_timestamp_display

REACTION_ROLE_STATUS_OPTIONS = (
    {"value": "active", "label": "Active"},
    {"value": "paused", "label": "Paused"},
    {"value": "disabled", "label": "Disabled"},
)


def process_reaction_roles_submission(*, form, on_manage_reaction_roles, actor_email: str, selected_guild_id: str):
    messages: list[tuple[str, str]] = []
    if not callable(on_manage_reaction_roles):
        messages.append(("Reaction role update callback is not configured.", "error"))
        return None, messages

    action = str(form.get("action") or "").strip().lower()

    if action == "post_message":
        channel_id = str(form.get("post_channel_id") or form.get("channel_id") or "").strip()
        message_content = str(form.get("post_message_content") or form.get("message_content") or "").strip()

        pairs = []
        for idx in range(1, 11):
            emoji_val = str(form.get(f"post_emoji_{idx}") or form.get(f"emoji_{idx}") or "").strip()
            role_val = str(form.get(f"post_role_id_{idx}") or form.get(f"role_id_{idx}") or "").strip()
            if emoji_val and role_val:
                pairs.append({"emoji": emoji_val, "role_id": role_val})
            elif emoji_val and not role_val:
                messages.append((f"Row {idx}: Emoji '{emoji_val}' was provided without selecting a role. Please select a role or clear the emoji.", "error"))
                return None, messages
            elif role_val and not emoji_val:
                messages.append((f"Row {idx}: A role was selected without providing an emoji. Please enter an emoji or clear the role selection.", "error"))
                return None, messages

        # Also support list inputs if provided
        if not pairs and hasattr(form, "getlist"):
            emoji_list = form.getlist("post_emoji") or form.getlist("emoji")
            role_list = form.getlist("post_role_id") or form.getlist("role_id")
            if len(emoji_list) != len(role_list):
                messages.append(("Emoji list and role list counts do not match.", "error"))
                return None, messages
            for row_idx, (em, ro) in enumerate(zip(emoji_list, role_list, strict=True), 1):
                em_str = str(em or "").strip()
                ro_str = str(ro or "").strip()
                if em_str and ro_str:
                    pairs.append({"emoji": em_str, "role_id": ro_str})
                elif em_str and not ro_str:
                    messages.append((f"Row {row_idx}: Emoji '{em_str}' was provided without selecting a role.", "error"))
                    return None, messages
                elif ro_str and not em_str:
                    messages.append((f"Row {row_idx}: A role was selected without providing an emoji.", "error"))
                    return None, messages

        if not channel_id:
            messages.append(("Please select a target channel to post the message in.", "error"))
            return None, messages
        if not message_content:
            messages.append(("Message content cannot be empty.", "error"))
            return None, messages
        if not pairs:
            messages.append(("Please configure at least one emoji and role connection.", "error"))
            return None, messages

        payload = {
            "action": "post_message",
            "channel_id": channel_id,
            "message_content": message_content,
            "pairs": pairs,
        }
    else:
        payload = {
            "action": action,
            "channel_id": str(form.get("channel_id") or "").strip(),
            "message_id": str(form.get("message_id") or "").strip(),
            "emoji": str(form.get("emoji") or "").strip(),
            "role_id": str(form.get("role_id") or "").strip(),
            "status": str(form.get("status") or "").strip().lower(),
            "original_message_id": str(form.get("original_message_id") or "").strip(),
            "original_emoji": str(form.get("original_emoji") or "").strip(),
        }

    response = on_manage_reaction_roles(payload, actor_email, selected_guild_id)
    if not isinstance(response, dict):
        messages.append(("Invalid response from reaction role handler.", "error"))
        return None, messages
    if not response.get("ok"):
        messages.append((str(response.get("error") or "Failed to update reaction role mappings."), "error"))
        return None, messages

    messages.append((str(response.get("message") or "Reaction role mappings updated."), "success"))
    return response, messages


def render_reaction_roles_body(
    *,
    guild_name: str,
    mappings: list[dict],
    channel_options: list[dict],
    role_options: list[dict],
    catalog_error: str,
    render_select_input,
    render_fixed_select_input,
):
    picker_note = ""
    if catalog_error:
        picker_note = f"<p class='muted'>Could not load Discord catalog: {escape(catalog_error)}</p>"
    elif channel_options and role_options:
        picker_note = f"<p class='muted'>Loaded live Discord channels and roles from <strong>{escape(guild_name)}</strong>.</p>"
    else:
        picker_note = "<p class='muted'>Discord catalog is unavailable right now. Existing mappings can still be edited.</p>"

    channel_labels = {
        str(option.get("id") or "").strip(): str(option.get("label") or option.get("name") or option.get("id") or "").strip()
        for option in channel_options
        if str(option.get("id") or "").strip()
    }
    role_labels = {
        str(option.get("id") or "").strip(): str(option.get("label") or option.get("name") or option.get("id") or "").strip()
        for option in role_options
        if str(option.get("id") or "").strip()
    }

    rows = []
    for mapping in mappings:
        channel_id = str(mapping.get("channel_id") or "")
        message_id = str(mapping.get("message_id") or "")
        emoji_text = str(mapping.get("emoji_text") or mapping.get("emoji_key") or "")
        role_id = str(mapping.get("role_id") or "")
        status = str(mapping.get("status") or "active").strip().lower()
        created_at = format_timestamp_display(mapping.get("created_at"))
        updated_at = format_timestamp_display(mapping.get("updated_at"))
        channel_label = channel_labels.get(channel_id, channel_id or "n/a")
        role_labels.get(role_id, role_id or "n/a")
        rows.append(
            f"""
            <tr>
              <td>{render_select_input("channel_id", channel_id, channel_options, placeholder="Choose channel...")}</td>
              <td><input type="text" name="message_id" value="{escape(message_id, quote=True)}" inputmode="numeric" /></td>
              <td><input type="text" name="emoji" value="{escape(emoji_text, quote=True)}" placeholder="😀 or <:custom:123>" /></td>
              <td>{render_select_input("role_id", role_id, role_options, placeholder="Choose role...")}</td>
              <td>{render_fixed_select_input("status", status, list(REACTION_ROLE_STATUS_OPTIONS), placeholder="Select status...")}</td>
              <td>
                <input type="hidden" name="original_message_id" value="{escape(message_id, quote=True)}" />
                <input type="hidden" name="original_emoji" value="{escape(emoji_text, quote=True)}" />
                <div class="muted mono">{escape(channel_label)}</div>
                <div class="muted mono">{escape(created_at)}</div>
                <div class="muted">{escape(updated_at)}</div>
              </td>
              <td>
                <div class="dash-actions">
                  <button class="btn" type="submit" name="action" value="save">Save</button>
                  <button class="btn secondary" type="submit" name="action" value="set_status" onclick="this.form.status.value='active';">Activate</button>
                  <button class="btn secondary" type="submit" name="action" value="set_status" onclick="this.form.status.value='paused';">Pause</button>
                  <button class="btn danger" type="submit" name="action" value="delete">Delete</button>
                </div>
              </td>
            </tr>
            """
        )

    # Post builder inputs
    post_channel_input = render_select_input(
        "post_channel_id",
        "",
        channel_options,
        placeholder="Select channel to post message in...",
    )

    pairing_rows = []
    for idx in range(1, 9):
        pairing_role_select = render_select_input(
            f"post_role_id_{idx}",
            "",
            role_options,
            placeholder=f"Select role for emoji {idx}...",
        )
        pairing_rows.append(
            f"""
            <tr>
              <td class="muted mono" style="width:40px;text-align:center;">{idx}</td>
              <td style="width:160px;"><input type="text" name="post_emoji_{idx}" placeholder="e.g. 🎮 or <:custom:123>" style="width:100%;box-sizing:border-box;" /></td>
              <td>{pairing_role_select}</td>
            </tr>
            """
        )

    # Manual add inputs
    add_channel_input = render_select_input("channel_id", "", channel_options, placeholder="Choose channel...")
    add_role_input = render_select_input("role_id", "", role_options, placeholder="Choose role...")
    add_status_input = render_fixed_select_input(
        "status",
        "active",
        list(REACTION_ROLE_STATUS_OPTIONS),
        placeholder="Select status...",
    )

    return f"""
        <div class="card">
          <h2>Post New Reaction Role Message</h2>
          <p class="muted">Compose a message, attach emojis to Discord roles, and have the bot post it directly to a channel. The bot will automatically add the reactions so server members can click them to gain or remove roles.</p>
          <form method="post">
            <input type="hidden" name="action" value="post_message" />
            <label>Target Channel</label>
            {post_channel_input}
            <label style="margin-top:12px;display:block;">Message Content</label>
            <textarea name="post_message_content" rows="4" placeholder="React to this message to get your roles:&#10;🎮 = Gamer&#10;📢 = Announcements&#10;💻 = Developer" style="width:100%;box-sizing:border-box;resize:vertical;" required></textarea>
            <div style="margin-top:16px;">
              <h3 style="margin-bottom:6px;">Emoji & Role Connections</h3>
              <p class="muted" style="margin-top:0;">Connect emojis to server roles below. Leave unused rows blank.</p>
              <table>
                <thead>
                  <tr><th style="width:40px;text-align:center;">#</th><th style="width:160px;">Emoji</th><th>Assigned Role</th></tr>
                </thead>
                <tbody>
                  {"".join(pairing_rows)}
                </tbody>
              </table>
            </div>
            <div style="margin-top:16px;">
              <button class="btn primary" type="submit">Post Message & Setup Reaction Roles</button>
            </div>
          </form>
        </div>

        <div class="card">
          <h2>Active Reaction Role Mappings</h2>
          <p class="muted">Manage message reactions for <strong>{escape(guild_name)}</strong>. When a member reacts with the selected emoji, the assigned role is granted; removing the reaction removes the role.</p>
          {picker_note}
          <table>
            <thead>
              <tr><th>Channel</th><th>Message ID</th><th>Emoji</th><th>Role</th><th>Status</th><th>Timestamps</th><th>Actions</th></tr>
            </thead>
            <tbody>
              {"".join(rows) if rows else "<tr><td colspan='7' class='muted'>No reaction role mappings are configured yet.</td></tr>"}
            </tbody>
          </table>
        </div>

        <div class="card">
          <h2>Manual Add / Update by Message ID</h2>
          <p class="muted">Connect an emoji and role to an already existing Discord message ID. Editing a row in the table above updates that mapping.</p>
          <form method="post">
            <input type="hidden" name="action" value="save" />
            <label>Channel</label>
            {add_channel_input}
            <label>Message ID</label>
            <input type="text" name="message_id" placeholder="123456789012345678" inputmode="numeric" />
            <label>Emoji</label>
            <input type="text" name="emoji" placeholder="😀 or <:custom:123>" />
            <label>Role</label>
            {add_role_input}
            <label>Status</label>
            {add_status_input}
            <div style="margin-top:14px;">
              <button class="btn" type="submit">Save Reaction Role</button>
            </div>
          </form>
        </div>
        """

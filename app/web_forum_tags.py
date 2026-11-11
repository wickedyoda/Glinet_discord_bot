from __future__ import annotations

from html import escape

from app.web_time import format_timestamp_display

FORUM_TAG_STATUS_OPTIONS = (
    {"value": "1", "label": "Enabled"},
    {"value": "0", "label": "Disabled"},
)


def process_forum_tags_submission(*, form, on_manage_forum_tags, actor_email: str, selected_guild_id: str):
    """Translate a forum tag form POST into a callback payload.

    Returns ``(response, messages)`` where *response* is the callback's dict (or
    ``None`` when the callback was never called) and *messages* is a list of
    ``(text, category)`` pairs to flash.
    """
    messages: list[tuple[str, str]] = []
    if not callable(on_manage_forum_tags):
        messages.append(("Forum tag callbacks are not configured.", "error"))
        return None, messages

    action = str(form.get("action") or "").strip().lower()
    if action not in {"add", "edit", "toggle", "delete"}:
        messages.append(("Invalid forum tag action.", "error"))
        return None, messages

    # The picker submits one combined "feed" value ("tag:kb", "category:announcement").
    # Fall back to separate fields so a hand-posted form still works.
    raw_feed = str(form.get("feed") or "").strip()
    if raw_feed:
        raw_type, _, raw_value = raw_feed.partition(":")
        feed_type = raw_type.strip().casefold()
        tag_value = raw_value.strip()
    else:
        feed_type = str(form.get("feed_type") or "tag").strip().casefold()
        tag_value = str(form.get("tag") or "").strip()
    if feed_type not in {"tag", "category"}:
        feed_type = "tag"

    payload = {
        "action": action,
        "tag": tag_value,
        "feed_type": feed_type,
        "channel_id": str(form.get("channel_id") or "").strip(),
        "subscription_id": str(form.get("subscription_id") or "").strip(),
        "enabled": str(form.get("enabled") or "").strip().lower(),
    }

    response = on_manage_forum_tags(payload, actor_email, selected_guild_id)
    if not isinstance(response, dict):
        messages.append(("Invalid response from forum tag handler.", "error"))
        return None, messages
    if not response.get("ok"):
        messages.append((str(response.get("error") or "Failed to update forum tag subscriptions."), "error"))
        return None, messages

    messages.append((str(response.get("message") or "Forum tag subscriptions updated."), "success"))
    return response, messages


def render_forum_tags_body(
    *,
    guild_name: str,
    subscriptions: list[dict],
    channel_options: list[dict],
    render_select_input,
    tag_placeholder: str = "",
    available_tags: list[dict] | None = None,
    tag_options_error: str = "",
    available_categories: list[dict] | None = None,
    category_options_error: str = "",
):
    """Render the forum tag cross-post management page."""
    channel_labels = {
        str(option.get("id") or "").strip(): str(option.get("label") or option.get("name") or option.get("id") or "").strip()
        for option in channel_options
        if str(option.get("id") or "").strip()
    }

    # A single grouped dropdown of real tags and categories. The value carries the
    # feed type ("tag:kb", "category:announcement") so one field is enough and no
    # JavaScript is needed to keep a type selector in sync.
    picker_groups = []
    if available_tags:
        rows = "".join(
            '<option value="tag:{}">{} ({} topics)</option>'.format(
                escape(str(entry.get("slug") or entry.get("name") or "").strip(), quote=True),
                escape(str(entry.get("name") or entry.get("slug") or "").strip()),
                entry.get("count", 0),
            )
            for entry in available_tags
            if str(entry.get("slug") or entry.get("name") or "").strip()
        )
        if rows:
            picker_groups.append(f'<optgroup label="Tags">{rows}</optgroup>')

    if available_categories:
        rows = "".join(
            '<option value="category:{}">{} ({} topics)</option>'.format(
                escape(str(entry.get("slug") or "").strip(), quote=True),
                escape(str(entry.get("name") or entry.get("slug") or "").strip()),
                entry.get("topic_count", entry.get("count", 0)),
            )
            for entry in available_categories
            if str(entry.get("slug") or "").strip()
        )
        if rows:
            picker_groups.append(f'<optgroup label="Categories">{rows}</optgroup>')

    picker_errors = " ".join(
        escape(part) for part in (tag_options_error, category_options_error) if part
    ).strip()

    if picker_groups:
        tag_picker = (
            '<select name="feed" required>'
            "<option value=''>Select a forum tag or category...</option>"
            + "".join(picker_groups)
            + "</select>"
        )
    else:
        tag_picker = (
            f'<input type="text" name="tag" placeholder="{escape(tag_placeholder or "e.g. KB")}" '
            'style="width:260px;" required />'
        )
    if picker_errors:
        tag_picker += f'<div class="warning">{picker_errors}</div>'

    rows = []
    for subscription in subscriptions:
        subscription_id = str(subscription.get("id") or "")
        tag = str(subscription.get("tag") or "")
        channel_id = str(subscription.get("channel_id") or "")
        enabled = bool(subscription.get("enabled"))
        last_checked = format_timestamp_display(subscription.get("last_checked_at"))
        last_posted = format_timestamp_display(subscription.get("last_posted_at"))
        last_error = str(subscription.get("last_error") or "").strip()
        channel_label = channel_labels.get(channel_id, channel_id or "n/a")

        status = "Enabled" if enabled else "Disabled"
        error_note = f"<div class='warning'>{escape(last_error)}</div>" if last_error else ""

        rows.append(
            f"""
            <tr>
              <td class="mono">#{escape(tag)}</td>
              <td>{escape(channel_label)}</td>
              <td>{escape(status)}</td>
              <td>
                <div class="muted mono">{escape(last_checked)}</div>
                <div class="muted mono">{escape(last_posted)}</div>
                {error_note}
              </td>
              <td>
                <form method="post" style="display:flex;gap:8px;flex-wrap:wrap;align-items:center;">
                  <input type="hidden" name="subscription_id" value="{escape(subscription_id, quote=True)}" />
                  <input type="hidden" name="enabled" value="{'0' if enabled else '1'}" />
                  <button class="btn" type="submit" name="action" value="toggle"
                          formaction="">{'Disable' if enabled else 'Enable'}</button>
                  <button class="btn danger" type="submit" name="action" value="delete">Delete</button>
                </form>
              </td>
            </tr>
            """
        )

    channel_select = render_select_input(
        "channel_id",
        "",
        channel_options,
        placeholder="Select a Discord channel...",
    )

    return f"""
        <div class="card">
          <h2>Forum Tag Cross-Posts</h2>
          <p class="muted">Cross-post GL.iNet forum topics carrying a chosen tag into a Discord channel.
          The forum post body becomes the Discord message, with a link back to the original topic.</p>
          <p class="muted">The first check for a tag records what already exists and stays quiet, so enabling a
          tag does not flood the channel with every historical topic.</p>
          <form method="post">
            <input type="hidden" name="action" value="add" />
            <label>Forum Tag</label>
            {tag_picker}
            <label>Discord Channel</label>
            {channel_select}
            <div style="margin-top:14px;">
              <button class="btn primary" type="submit">Add Tag Cross-Post</button>
            </div>
          </form>
        </div>

        <div class="card">
          <h2>Active Forum Tag Cross-Posts</h2>
          <p class="muted">Managing cross-posts for <strong>{escape(guild_name)}</strong>.</p>
          <table>
            <thead>
              <tr><th>Tag</th><th>Channel</th><th>Status</th><th>Last Activity</th><th>Actions</th></tr>
            </thead>
            <tbody>
              {"".join(rows) if rows else "<tr><td colspan='5' class='muted'>No forum tag cross-posts are configured yet.</td></tr>"}
            </tbody>
          </table>
        </div>
        """

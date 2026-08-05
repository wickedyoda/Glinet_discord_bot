from __future__ import annotations

from html import escape

from app.moderation import (
    BAD_WORD_ACTION_OPTIONS,
    BAD_WORD_ACTION_TIMEOUT,
    BAD_WORD_THRESHOLD_OPTIONS,
    BAD_WORD_TIMEOUT_MINUTE_OPTIONS,
    BAD_WORD_WINDOW_HOUR_OPTIONS,
    normalize_bad_word_action,
    parse_bad_word_list,
    parse_bad_word_list_text,
    serialize_bad_word_list,
)


def process_moderation_submission(
    *,
    form,
    on_save_guild_settings,
    actor_email: str,
    selected_guild_id: str,
):
    messages: list[tuple[str, str]] = []
    if not callable(on_save_guild_settings):
        messages.append(("Moderation save callback is not configured.", "error"))
        return None, messages
    payload = {
        "mod_log_channel_id": form.get("mod_log_channel_id", ""),
        "bad_words_enabled": form.get("bad_words_enabled", "0"),
        "bad_words_list_json": serialize_bad_word_list(form.get("bad_words_list_json", "")),
        "bad_words_warning_window_hours": form.get("bad_words_warning_window_hours", "72"),
        "bad_words_warning_threshold": form.get("bad_words_warning_threshold", "3"),
        "bad_words_action": normalize_bad_word_action(form.get("bad_words_action", BAD_WORD_ACTION_TIMEOUT)),
        "bad_words_timeout_minutes": form.get("bad_words_timeout_minutes", "60"),
    }
    response = on_save_guild_settings(payload, actor_email, selected_guild_id)
    if not isinstance(response, dict):
        messages.append(("Invalid response from moderation handler.", "error"))
        return None, messages
    if not response.get("ok"):
        messages.append((str(response.get("error") or "Failed to update moderation settings."), "error"))
        return None, messages
    messages.append((str(response.get("message") or "Moderation settings updated."), "success"))
    return response, messages


def render_moderation_body(
    *,
    guild_name: str,
    current_settings: dict,
    effective_settings: dict,
    text_channel_options: list[dict],
    catalog_error: str,
    render_select_input,
    render_fixed_select_input,
):
    catalog_note = ""
    if text_channel_options:
        catalog_note = (
            f"<p class='muted'>Loaded live Discord options from <strong>{escape(guild_name)}</strong>. "
            f"Text channels: {len(text_channel_options)}.</p>"
        )
    elif catalog_error:
        catalog_note = f"<p class='muted'>Could not load Discord options: {escape(catalog_error)}</p>"

    bad_words_enabled = 1 if int(current_settings.get("bad_words_enabled") or 0) > 0 else 0
    bad_words_text = parse_bad_word_list_text(current_settings.get("bad_words_list_json"))
    mod_log_select = render_select_input(
        "mod_log_channel_id",
        str(current_settings.get("mod_log_channel_id") or ""),
        text_channel_options,
        placeholder="Use global fallback",
    )
    enabled_select = render_fixed_select_input(
        "bad_words_enabled",
        "1" if bad_words_enabled else "0",
        [
            {"value": "1", "label": "Enabled"},
            {"value": "0", "label": "Disabled"},
        ],
        placeholder="Select state",
    )
    warning_window_select = render_fixed_select_input(
        "bad_words_warning_window_hours",
        str(int(current_settings.get("bad_words_warning_window_hours") or 72)),
        [{"value": str(item), "label": f"{item} hour(s)"} for item in BAD_WORD_WINDOW_HOUR_OPTIONS],
        placeholder="Choose window",
    )
    warning_threshold_select = render_fixed_select_input(
        "bad_words_warning_threshold",
        str(int(current_settings.get("bad_words_warning_threshold") or 3)),
        [{"value": str(item), "label": f"{item} warning(s)"} for item in BAD_WORD_THRESHOLD_OPTIONS],
        placeholder="Choose threshold",
    )
    action_select = render_fixed_select_input(
        "bad_words_action",
        normalize_bad_word_action(current_settings.get("bad_words_action")),
        list(BAD_WORD_ACTION_OPTIONS),
        placeholder="Choose action",
    )
    timeout_select = render_fixed_select_input(
        "bad_words_timeout_minutes",
        str(int(current_settings.get("bad_words_timeout_minutes") or 60)),
        [{"value": str(item), "label": f"{item} minute(s)"} for item in BAD_WORD_TIMEOUT_MINUTE_OPTIONS],
        placeholder="Choose timeout",
    )
    effective_enabled = "enabled" if int(effective_settings.get("bad_words_enabled") or 0) > 0 else "disabled"
    effective_action = normalize_bad_word_action(effective_settings.get("bad_words_action"))
    effective_action_label = "Timeout / mute" if effective_action == BAD_WORD_ACTION_TIMEOUT else "Warning only"

    return f"""
    <div class='card'>
      <h2>Moderation</h2>
      <p class='muted'>Manage bad-word filtering, warning escalation, and moderation logging for <strong>{escape(guild_name)}</strong>.</p>
      <p class='muted'>When a member trips the filter, the bot deletes the message, sends a private warning by DM, and can escalate after repeated offenses in the configured time window.</p>
      {catalog_note}
      <form method='post'>
        <table>
          <thead><tr><th>Setting</th><th>Configured Value</th><th>Effective Value</th></tr></thead>
          <tbody>
            <tr>
              <td><strong>Moderation Log Channel</strong><div class='muted mono'>mod_log_channel_id</div></td>
              <td>{mod_log_select}</td>
              <td class='muted mono'>{escape(str(effective_settings.get("mod_log_channel_id") or "")) or 'Use global default'}</td>
            </tr>
            <tr>
              <td><strong>Bad Word Filter</strong><div class='muted mono'>bad_words_enabled</div></td>
              <td>{enabled_select}</td>
              <td class='muted mono'>{effective_enabled}</td>
            </tr>
            <tr>
              <td><strong>Blocked Words / Phrases</strong><div class='muted mono'>bad_words_list_json</div></td>
              <td>
                <textarea name='bad_words_list_json' rows='10' placeholder='one term per line'>{escape(bad_words_text)}</textarea>
                <div class='muted' style='margin-top:8px;'>One word or phrase per line. Matching is case-insensitive.</div>
              </td>
              <td class='muted'>{escape(', '.join(parse_bad_word_list(effective_settings.get("bad_words_list_json"))) or 'No blocked words configured.')}</td>
            </tr>
            <tr>
              <td><strong>Warning Window</strong><div class='muted mono'>bad_words_warning_window_hours</div></td>
              <td>{warning_window_select}</td>
              <td class='muted mono'>{escape(str(int(effective_settings.get("bad_words_warning_window_hours") or 72)))} hour(s)</td>
            </tr>
            <tr>
              <td><strong>Warnings Before Action</strong><div class='muted mono'>bad_words_warning_threshold</div></td>
              <td>{warning_threshold_select}</td>
              <td class='muted mono'>{escape(str(int(effective_settings.get("bad_words_warning_threshold") or 3)))} warning(s)</td>
            </tr>
            <tr>
              <td><strong>Escalation Action</strong><div class='muted mono'>bad_words_action</div></td>
              <td>{action_select}</td>
              <td class='muted mono'>{escape(effective_action_label)}</td>
            </tr>
            <tr>
              <td><strong>Timeout Length</strong><div class='muted mono'>bad_words_timeout_minutes</div></td>
              <td>{timeout_select}</td>
              <td class='muted mono'>{escape(str(int(effective_settings.get("bad_words_timeout_minutes") or 60)))} minute(s)</td>
            </tr>
          </tbody>
        </table>
        <div style='margin-top:14px;'>
          <button class='btn' type='submit'>Save Moderation Settings</button>
        </div>
      </form>
    </div>
    """

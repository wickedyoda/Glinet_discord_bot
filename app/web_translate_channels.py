from __future__ import annotations

from html import escape

LANGUAGE_OPTIONS = [
    {"value": "auto", "label": "Auto-detect"},
    {"value": "en",   "label": "English"},
    {"value": "es",   "label": "Spanish"},
    {"value": "fr",   "label": "French"},
    {"value": "de",   "label": "German"},
    {"value": "it",   "label": "Italian"},
    {"value": "pt",   "label": "Portuguese"},
    {"value": "ru",   "label": "Russian"},
    {"value": "ja",   "label": "Japanese"},
    {"value": "ko",   "label": "Korean"},
    {"value": "zh-cn", "label": "Chinese (Simplified)"},
    {"value": "zh-tw", "label": "Chinese (Traditional)"},
    {"value": "ar",   "label": "Arabic"},
    {"value": "hi",   "label": "Hindi"},
    {"value": "tr",   "label": "Turkish"},
    {"value": "nl",   "label": "Dutch"},
    {"value": "sv",   "label": "Swedish"},
    {"value": "pl",   "label": "Polish"},
    {"value": "th",   "label": "Thai"},
    {"value": "vi",   "label": "Vietnamese"},
    {"value": "uk",   "label": "Ukrainian"},
    {"value": "el",   "label": "Greek"},
    {"value": "he",   "label": "Hebrew"},
    {"value": "fa",   "label": "Persian"},
    {"value": "id",   "label": "Indonesian"},
    {"value": "ms",   "label": "Malay"},
]

ENABLED_OPTIONS = [
    {"value": "1", "label": "Enabled"},
    {"value": "0", "label": "Disabled"},
]


def process_translate_channels_submission(
    *,
    form,
    on_get_translate_channels_channels=None,
    actor_email: str,
    selected_guild_id: str,
):
    """Process POST submission for auto-translate channel management.

    Delegates to the bot-provided callback. Returns (response_dict, messages).
    """
    messages: list[tuple[str, str]] = []
    action = str(form.get("action") or "").strip()

    if not callable(on_get_translate_channels_channels):
        messages.append(("Translate channels management callback is not configured.", "error"))
        return None, messages

    payload: dict = {"action": action}

    if action in {"create_entry", "update_entry"}:
        payload.update({
            "source_channel_id": str(form.get("source_channel_id", "") or ""),
            "target_channel_id": str(form.get("target_channel_id", "") or ""),
            "source_language": str(form.get("source_language", "auto") or "auto"),
            "target_language": str(form.get("target_language", "en") or "en"),
            "enabled": str(form.get("enabled", "1") or "1"),
        })
    elif action == "delete_entry":
        payload.update({
            "source_channel_id": str(form.get("source_channel_id", "") or ""),
            "target_channel_id": str(form.get("target_channel_id", "") or ""),
            "target_language": str(form.get("target_language", "") or ""),
        })
    elif action == "toggle_enabled":
        payload.update({
            "source_channel_id": str(form.get("source_channel_id", "") or ""),
            "target_channel_id": str(form.get("target_channel_id", "") or ""),
            "target_language": str(form.get("target_language", "") or ""),
            "enabled": str(form.get("enabled", "1") or "1"),
        })
    else:
        messages.append(("Unknown translate channel action.", "error"))
        return None, messages

    response = on_get_translate_channels_channels(payload, actor_email, selected_guild_id)
    if not isinstance(response, dict):
        messages.append(("Invalid response from translate channel handler.", "error"))
        return None, messages
    if not response.get("ok"):
        messages.append((str(response.get("error") or "Failed to update translate channel settings."), "error"))
        return None, messages
    messages.append((str(response.get("message") or "Translate channel settings updated."), "success"))
    return response, messages


def render_translate_channels_body(
    *,
    guild_name: str,
    payload: dict,
    text_channel_options: list[dict],
    catalog_error: str,
    render_select_input,
    render_fixed_select_input,
):
    """Render the HTML body for the auto-translate channels web admin panel."""
    entries = list(payload.get("entries") or [])
    catalog_note = ""
    if text_channel_options:
        catalog_note = (
            f"<p class='muted'>Loaded live Discord options from "
            f"<strong>{escape(guild_name)}</strong>. "
            f"Text channels: {len(text_channel_options)}.</p>"
        )
    elif catalog_error:
        catalog_note = f"<p class='muted'>Could not load Discord options: {escape(catalog_error)}</p>"

    # --- Create / Update form ---
    source_channel_select = render_select_input(
        "source_channel_id", "", text_channel_options, placeholder="Choose source channel",
    )
    target_channel_select = render_select_input(
        "target_channel_id", "", text_channel_options, placeholder="Choose target channel",
    )
    source_lang_select = render_fixed_select_input(
        "source_language", "auto", LANGUAGE_OPTIONS, placeholder="Source language",
    )
    target_lang_select = render_fixed_select_input(
        "target_language", "en", LANGUAGE_OPTIONS, placeholder="Target language",
    )
    enabled_select = render_fixed_select_input(
        "enabled", "1", ENABLED_OPTIONS, placeholder="Enabled?",
    )

    # --- Configured entries ---
    entry_cards = []
    for entry in entries:
        src_id = str(int(entry.get("source_channel_id") or 0))
        tgt_id = str(int(entry.get("target_channel_id") or 0))

        entry_source_select = render_select_input(
            "source_channel_id", src_id, text_channel_options, placeholder="Choose source channel",
        )
        entry_target_select = render_select_input(
            "target_channel_id", tgt_id, text_channel_options, placeholder="Choose target channel",
        )
        entry_source_lang = render_fixed_select_input(
            "source_language", str(entry.get("source_language") or "auto"),
            LANGUAGE_OPTIONS, placeholder="Source language",
        )
        entry_target_lang = render_fixed_select_input(
            "target_language", str(entry.get("target_language") or "en"),
            LANGUAGE_OPTIONS, placeholder="Target language",
        )
        entry_enabled = render_fixed_select_input(
            "enabled", "1" if int(entry.get("enabled") or 0) > 0 else "0",
            ENABLED_OPTIONS, placeholder="Enabled?",
        )

        entry_cards.append(f"""
        <div class='card' style='margin-top:14px;'>
          <h4 style='margin-top:0;'>{escape(src_id)} → {escape(tgt_id)} ({escape(entry.get('target_language', ''))})</h4>
          <p class='muted'>Source lang: {escape(entry.get('source_language', 'auto'))} | Status: {'enabled' if int(entry.get('enabled') or 0) > 0 else 'disabled'}</p>
          <form method='post'>
            <input type='hidden' name='action' value='update_entry' />
            <input type='hidden' name='source_channel_id' value='{escape(src_id, quote=True)}' />
            <input type='hidden' name='target_channel_id' value='{escape(tgt_id, quote=True)}' />
            <input type='hidden' name='target_language' value='{escape(entry.get('target_language', ''), quote=True)}' />
            <table>
              <thead><tr><th>Source Channel</th><th>Target Channel</th><th>Source Lang</th><th>Target Lang</th><th>Enabled</th></tr></thead>
              <tbody>
                <tr>
                  <td>{entry_source_select}</td>
                  <td>{entry_target_select}</td>
                  <td>{entry_source_lang}</td>
                  <td>{entry_target_lang}</td>
                  <td>{entry_enabled}</td>
                </tr>
              </tbody>
            </table>
            <div style='margin-top:14px; display:flex; gap:10px; flex-wrap:wrap;'>
              <button class='btn secondary' type='submit'>Save Mapping</button>
            </div>
          </form>
          <form method='post' style='margin-top:10px;'>
            <input type='hidden' name='action' value='delete_entry' />
            <input type='hidden' name='source_channel_id' value='{escape(src_id, quote=True)}' />
            <input type='hidden' name='target_channel_id' value='{escape(tgt_id, quote=True)}' />
            <input type='hidden' name='target_language' value='{escape(entry.get('target_language', ''), quote=True)}' />
            <button class='btn danger' type='submit' onclick="return confirm('Delete this auto-translate mapping?');\">Delete Mapping</button>
          </form>
        </div>
        """)

    configured_entries_html = "".join(entry_cards) if entry_cards else (
        "<p class='muted'>No auto-translate channel mappings configured yet.</p>"
    )

    return f"""
    <div class='card'>
      <h2>Auto-Translate Channels</h2>
      <p class='muted'>Configure channels where messages are automatically translated. Messages in the <strong>source channel</strong> will be translated and posted in the <strong>target channel</strong>.</p>
      {catalog_note}
    </div>

    <div class='card'>
      <h3>Create Mapping</h3>
      <form method='post'>
        <input type='hidden' name='action' value='create_entry' />
        <table>
          <thead><tr><th>Source Channel</th><th>Target Channel</th><th>Source Lang</th><th>Target Lang</th><th>Enabled</th></tr></thead>
          <tbody>
            <tr>
              <td>{source_channel_select}</td>
              <td>{target_channel_select}</td>
              <td>{source_lang_select}</td>
              <td>{target_lang_select}</td>
              <td>{enabled_select}</td>
            </tr>
          </tbody>
        </table>
        <div style='margin-top:14px;'>
          <button class='btn' type='submit'>Create Mapping</button>
        </div>
      </form>
    </div>

    <div class='card'>
      <h3>Configured Mappings</h3>
      {configured_entries_html}
    </div>
    """

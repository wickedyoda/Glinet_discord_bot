from __future__ import annotations

from html import escape

from app.discourse_integration import (
    DISCOURSE_FEATURE_CATEGORIES,
    DISCOURSE_FEATURE_CREATE_TOPIC,
    DISCOURSE_FEATURE_OPTIONS,
    DISCOURSE_FEATURE_REPLY,
    DISCOURSE_FEATURE_SEARCH,
    DISCOURSE_FEATURE_TOPIC_LOOKUP,
    DISCOURSE_STATE_OPTIONS,
    DISCOURSE_TIMEOUT_SECOND_OPTIONS,
    FEATURE_TOGGLE_OPTIONS,
    discourse_feature_enabled,
    discourse_features_summary,
    format_discourse_override_label,
    serialize_discourse_features,
)


def process_discourse_submission(*, form, on_save_guild_settings, actor_email: str, selected_guild_id: str):
    messages: list[tuple[str, str]] = []
    if not callable(on_save_guild_settings):
        messages.append(("Discourse settings save callback is not configured.", "error"))
        return None, messages

    enabled_features = []
    feature_form_map = {
        DISCOURSE_FEATURE_SEARCH: "discourse_feature_search",
        DISCOURSE_FEATURE_TOPIC_LOOKUP: "discourse_feature_topic_lookup",
        DISCOURSE_FEATURE_CATEGORIES: "discourse_feature_categories",
        DISCOURSE_FEATURE_CREATE_TOPIC: "discourse_feature_create_topic",
        DISCOURSE_FEATURE_REPLY: "discourse_feature_reply",
    }
    for feature_key, field_name in feature_form_map.items():
        if str(form.get(field_name) or "0").strip() == "1":
            enabled_features.append(feature_key)

    payload = {
        "discourse_enabled": form.get("discourse_enabled", "-1"),
        "discourse_base_url": form.get("discourse_base_url", ""),
        "discourse_api_username": form.get("discourse_api_username", ""),
        "discourse_profile_name": form.get("discourse_profile_name", ""),
        "discourse_request_timeout_seconds": form.get("discourse_request_timeout_seconds", "15"),
        "discourse_features_json": serialize_discourse_features(enabled_features),
    }
    api_key_value = str(form.get("discourse_api_key") or "").strip()
    if api_key_value:
        payload["discourse_api_key"] = api_key_value
    elif str(form.get("discourse_api_key_clear") or "").strip().lower() in {"1", "true", "yes", "on"}:
        payload["discourse_api_key_clear"] = "1"

    response = on_save_guild_settings(payload, actor_email, selected_guild_id)
    if not isinstance(response, dict):
        messages.append(("Invalid response from Discourse settings handler.", "error"))
        return None, messages
    if not response.get("ok"):
        messages.append((str(response.get("error") or "Failed to update Discourse settings."), "error"))
        return None, messages

    messages.append((str(response.get("message") or "Discourse settings updated."), "success"))
    return response, messages


def render_discourse_body(
    *,
    guild_name: str,
    current_settings: dict,
    effective_settings: dict,
    render_fixed_select_input,
):
    enabled_select = render_fixed_select_input(
        "discourse_enabled",
        str(current_settings.get("discourse_enabled", -1)),
        list(DISCOURSE_STATE_OPTIONS),
        placeholder="Choose state",
    )
    timeout_select = render_fixed_select_input(
        "discourse_request_timeout_seconds",
        str(int(current_settings.get("discourse_request_timeout_seconds") or 15)),
        [{"value": str(item), "label": f"{item} second(s)"} for item in DISCOURSE_TIMEOUT_SECOND_OPTIONS],
        placeholder="Choose timeout",
    )

    feature_rows = []
    for option in DISCOURSE_FEATURE_OPTIONS:
        feature_key = option["key"]
        field_name = f"discourse_feature_{feature_key}"
        feature_select = render_fixed_select_input(
            field_name,
            "1" if discourse_feature_enabled(current_settings.get("discourse_features_json"), feature_key) else "0",
            list(FEATURE_TOGGLE_OPTIONS),
            placeholder="Choose state",
        )
        feature_rows.append(
            f"""
            <tr>
              <td><strong>{escape(option['label'])}</strong><div class='muted mono'>{escape(feature_key)}</div></td>
              <td>{feature_select}</td>
              <td class='muted mono'>{'enabled' if discourse_feature_enabled(effective_settings.get('discourse_features_json'), feature_key) else 'disabled'}</td>
            </tr>
            """
        )

    api_key_status = (
        "Configured for this guild"
        if int(current_settings.get("discourse_api_key_configured") or 0) > 0
        else "Using global fallback"
        if int(effective_settings.get("discourse_api_key_configured") or 0) > 0
        else "Not configured"
    )
    effective_profile_name = str(effective_settings.get("discourse_profile_name") or effective_settings.get("discourse_api_username") or "")
    effective_base_url = str(effective_settings.get("discourse_base_url") or "")
    effective_timeout = int(effective_settings.get("discourse_request_timeout_seconds") or 15)
    current_base_url = str(current_settings.get("discourse_base_url") or "")
    current_username = str(current_settings.get("discourse_api_username") or "")
    current_profile_name = str(current_settings.get("discourse_profile_name") or "")

    return f"""
    <div class='card'>
      <h2>Discourse Integration</h2>
      <p class='muted'>Manage the Discourse forum integration used by <strong>{escape(guild_name)}</strong>. Guild-scoped settings here override the global forum defaults used by <span class='mono'>/search_forum</span>.</p>
      <p class='muted'>Search uses this configuration today. Topic browsing, create-topic, and reply toggles are stored here so those capabilities can be enabled per guild as the bot grows.</p>
      <form method='post'>
        <table>
          <thead><tr><th>Setting</th><th>Configured Value</th><th>Effective Value</th></tr></thead>
          <tbody>
            <tr>
              <td><strong>Integration State</strong><div class='muted mono'>discourse_enabled</div></td>
              <td>{enabled_select}</td>
              <td class='muted mono'>{escape(format_discourse_override_label(current_settings.get('discourse_enabled')))}</td>
            </tr>
            <tr>
              <td><strong>Forum Base URL</strong><div class='muted mono'>discourse_base_url</div></td>
              <td><input type='text' name='discourse_base_url' value='{escape(current_base_url, quote=True)}' placeholder='https://forum.gl-inet.com' /></td>
              <td class='muted mono'>{escape(effective_base_url or 'Not configured')}</td>
            </tr>
            <tr>
              <td><strong>API Username / Profile</strong><div class='muted mono'>discourse_api_username</div></td>
              <td><input type='text' name='discourse_api_username' value='{escape(current_username, quote=True)}' placeholder='forum-bot' /></td>
              <td class='muted mono'>{escape(str(effective_settings.get('discourse_api_username') or '') or 'Not configured')}</td>
            </tr>
            <tr>
              <td><strong>Profile Label</strong><div class='muted mono'>discourse_profile_name</div></td>
              <td><input type='text' name='discourse_profile_name' value='{escape(current_profile_name, quote=True)}' placeholder='GL.iNet Forum Bot' /></td>
              <td class='muted mono'>{escape(effective_profile_name or 'Not configured')}</td>
            </tr>
            <tr>
              <td><strong>API Key</strong><div class='muted mono'>discourse_api_key</div></td>
              <td>
                <input type='password' name='discourse_api_key' value='' placeholder='{'Stored key will be kept if blank' if int(effective_settings.get('discourse_api_key_configured') or 0) > 0 else 'Paste a Discourse API key'}' />
                <label class='checkbox' style='margin-top:8px;'><input type='checkbox' name='discourse_api_key_clear' value='1' /> Clear stored API key</label>
              </td>
              <td class='muted mono'>{escape(api_key_status)}</td>
            </tr>
            <tr>
              <td><strong>Request Timeout</strong><div class='muted mono'>discourse_request_timeout_seconds</div></td>
              <td>{timeout_select}</td>
              <td class='muted mono'>{effective_timeout} second(s)</td>
            </tr>
          </tbody>
        </table>

        <div class='card' style='margin:16px 0 0 0;'>
          <h3 style='margin-top:0;'>Integration Features</h3>
          <p class='muted'>These toggles are stored per guild. Search is enforced now; the others gate future Discourse actions so permissions are already modeled cleanly.</p>
          <table>
            <thead><tr><th>Feature</th><th>Configured Value</th><th>Effective Value</th></tr></thead>
            <tbody>
              {''.join(feature_rows)}
            </tbody>
          </table>
          <p class='muted' style='margin-top:10px;'>Effective feature set: {escape(discourse_features_summary(effective_settings.get('discourse_features_json')))}</p>
        </div>

        <div style='margin-top:14px;'>
          <button class='btn' type='submit'>Save Discourse Settings</button>
        </div>
      </form>
    </div>
    """

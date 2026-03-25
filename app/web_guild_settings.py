from __future__ import annotations

from html import escape

from app.image_metadata import (
    WELCOME_IMAGE_ALLOWED_EXTENSIONS,
    WELCOME_IMAGE_MAX_HEIGHT,
    WELCOME_IMAGE_MAX_WIDTH,
    WELCOME_IMAGE_MIN_HEIGHT,
    WELCOME_IMAGE_MIN_WIDTH,
    detect_image_metadata,
)


def format_byte_size(value: int | str | None) -> str:
    size_bytes = max(0, int(value or 0))
    if size_bytes < 1024:
        return f"{size_bytes} bytes"
    return f"{size_bytes} bytes ({size_bytes / 1024:.1f} KiB)"


def format_override_state(value: int | str | None) -> str:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = -1
    if parsed > 0:
        return "enabled"
    if parsed == 0:
        return "disabled"
    return "use global"


def process_guild_settings_submission(
    *,
    form,
    files,
    on_save_guild_settings,
    actor_email: str,
    selected_guild_id: str,
    max_welcome_image_upload_bytes: int,
):
    messages: list[tuple[str, str]] = []
    if not callable(on_save_guild_settings):
        messages.append(("Guild settings save callback is not configured.", "error"))
        return None, messages

    upload_valid = True
    payload = {
        "bot_log_channel_id": form.get("bot_log_channel_id", ""),
        "mod_log_channel_id": form.get("mod_log_channel_id", ""),
        "firmware_notify_channel_id": form.get("firmware_notify_channel_id", ""),
        "firmware_monitor_enabled": -1 if not form.get("firmware_monitor_enabled__override") else (1 if form.get("firmware_monitor_enabled") else 0),
        "reddit_feed_notify_enabled": -1 if not form.get("reddit_feed_notify_enabled__override") else (1 if form.get("reddit_feed_notify_enabled") else 0),
        "youtube_notify_enabled": -1 if not form.get("youtube_notify_enabled__override") else (1 if form.get("youtube_notify_enabled") else 0),
        "linkedin_notify_enabled": -1 if not form.get("linkedin_notify_enabled__override") else (1 if form.get("linkedin_notify_enabled") else 0),
        "beta_program_notify_enabled": -1 if not form.get("beta_program_notify_enabled__override") else (1 if form.get("beta_program_notify_enabled") else 0),
        "access_role_id": form.get("access_role_id", ""),
        "welcome_channel_id": form.get("welcome_channel_id", ""),
        "welcome_dm_enabled": form.get("welcome_dm_enabled", ""),
        "welcome_channel_image_enabled": form.get("welcome_channel_image_enabled", ""),
        "welcome_dm_image_enabled": form.get("welcome_dm_image_enabled", ""),
        "welcome_channel_message": form.get("welcome_channel_message", ""),
        "welcome_dm_message": form.get("welcome_dm_message", ""),
        "welcome_image_remove": form.get("welcome_image_remove", ""),
    }
    welcome_image_file = files.get("welcome_image_file")
    allowed_welcome_extensions_label = ", ".join(ext.upper().lstrip(".") for ext in WELCOME_IMAGE_ALLOWED_EXTENSIONS)
    if welcome_image_file is not None and welcome_image_file.filename:
        payload_bytes = welcome_image_file.read()
        lowered_name = welcome_image_file.filename.lower()
        if not lowered_name.endswith(WELCOME_IMAGE_ALLOWED_EXTENSIONS):
            messages.append((f"Welcome image must be one of: {allowed_welcome_extensions_label}.", "error"))
            upload_valid = False
        elif len(payload_bytes) > max_welcome_image_upload_bytes:
            messages.append(
                (
                    f"Welcome image is too large ({len(payload_bytes)} bytes). Max allowed is {max_welcome_image_upload_bytes} bytes.",
                    "error",
                )
            )
            upload_valid = False
        else:
            metadata = detect_image_metadata(payload_bytes)
            if metadata is None:
                messages.append(("Welcome image content is not a valid PNG, JPEG, GIF, or WEBP file.", "error"))
                upload_valid = False
            elif (
                int(metadata.get("width") or 0) < WELCOME_IMAGE_MIN_WIDTH
                or int(metadata.get("height") or 0) < WELCOME_IMAGE_MIN_HEIGHT
            ):
                messages.append(
                    (
                        "Welcome image is too small "
                        f"({int(metadata.get('width') or 0)}x{int(metadata.get('height') or 0)}). "
                        f"Minimum is {WELCOME_IMAGE_MIN_WIDTH}x{WELCOME_IMAGE_MIN_HEIGHT}.",
                        "error",
                    )
                )
                upload_valid = False
            elif (
                int(metadata.get("width") or 0) > WELCOME_IMAGE_MAX_WIDTH
                or int(metadata.get("height") or 0) > WELCOME_IMAGE_MAX_HEIGHT
            ):
                messages.append(
                    (
                        "Welcome image dimensions are too large "
                        f"({int(metadata.get('width') or 0)}x{int(metadata.get('height') or 0)}). "
                        f"Maximum is {WELCOME_IMAGE_MAX_WIDTH}x{WELCOME_IMAGE_MAX_HEIGHT}.",
                        "error",
                    )
                )
                upload_valid = False
            else:
                payload["welcome_image_size_bytes"] = int(metadata.get("size_bytes") or len(payload_bytes))
                payload["welcome_image_width"] = int(metadata.get("width") or 0)
                payload["welcome_image_height"] = int(metadata.get("height") or 0)
                payload["welcome_image_media_type"] = str(metadata.get("media_type") or "application/octet-stream")
        if upload_valid:
            payload["welcome_image_bytes"] = payload_bytes
            payload["welcome_image_filename"] = welcome_image_file.filename

    if not upload_valid:
        return None, messages

    response = on_save_guild_settings(payload, actor_email, selected_guild_id)
    if not isinstance(response, dict):
        messages.append(("Invalid response from guild settings handler.", "error"))
        return None, messages
    if not response.get("ok"):
        messages.append((str(response.get("error") or "Failed to update guild settings."), "error"))
        return None, messages

    messages.append((str(response.get("message") or "Guild settings updated."), "success"))
    return response, messages


def render_guild_settings_body(
    *,
    guild_name: str,
    current_settings: dict,
    effective_settings: dict,
    text_channel_options: list[dict],
    role_options: list[dict],
    catalog_error: str,
    max_welcome_image_upload_bytes: int,
    render_select_input,
):
    catalog_note = ""
    if text_channel_options or role_options:
        catalog_note = (
            f"<p class='muted'>Loaded live Discord options from <strong>{escape(guild_name)}</strong>. "
            f"Text channels: {len(text_channel_options)}; Roles: {len(role_options)}.</p>"
        )
    elif catalog_error:
        catalog_note = f"<p class='muted'>Could not load Discord options: {escape(catalog_error)}</p>"

    bot_log_select = render_select_input(
        "bot_log_channel_id",
        str(current_settings.get("bot_log_channel_id") or ""),
        text_channel_options,
        placeholder="Use global fallback",
    )
    mod_log_select = render_select_input(
        "mod_log_channel_id",
        str(current_settings.get("mod_log_channel_id") or ""),
        text_channel_options,
        placeholder="Use global fallback",
    )
    firmware_select = render_select_input(
        "firmware_notify_channel_id",
        str(current_settings.get("firmware_notify_channel_id") or ""),
        text_channel_options,
        placeholder="Use global fallback",
    )
    access_role_select = render_select_input(
        "access_role_id",
        str(current_settings.get("access_role_id") or ""),
        role_options,
        placeholder="No self-assign role",
    )
    welcome_channel_select = render_select_input(
        "welcome_channel_id",
        str(current_settings.get("welcome_channel_id") or ""),
        text_channel_options,
        placeholder="Disabled",
    )
    welcome_dm_enabled = 1 if int(current_settings.get("welcome_dm_enabled") or 0) > 0 else 0
    firmware_monitor_override = 1 if format_override_state(current_settings.get("firmware_monitor_enabled")) == "enabled" else 0 if format_override_state(current_settings.get("firmware_monitor_enabled")) == "disabled" else -1
    reddit_feed_override = 1 if format_override_state(current_settings.get("reddit_feed_notify_enabled")) == "enabled" else 0 if format_override_state(current_settings.get("reddit_feed_notify_enabled")) == "disabled" else -1
    youtube_notify_override = 1 if format_override_state(current_settings.get("youtube_notify_enabled")) == "enabled" else 0 if format_override_state(current_settings.get("youtube_notify_enabled")) == "disabled" else -1
    linkedin_notify_override = 1 if format_override_state(current_settings.get("linkedin_notify_enabled")) == "enabled" else 0 if format_override_state(current_settings.get("linkedin_notify_enabled")) == "disabled" else -1
    beta_program_notify_override = 1 if format_override_state(current_settings.get("beta_program_notify_enabled")) == "enabled" else 0 if format_override_state(current_settings.get("beta_program_notify_enabled")) == "disabled" else -1
    welcome_channel_image_enabled = 1 if int(current_settings.get("welcome_channel_image_enabled") or 0) > 0 else 0
    welcome_dm_image_enabled = 1 if int(current_settings.get("welcome_dm_image_enabled") or 0) > 0 else 0
    welcome_channel_message = str(current_settings.get("welcome_channel_message") or "")
    welcome_dm_message = str(current_settings.get("welcome_dm_message") or "")
    welcome_image_filename = str(current_settings.get("welcome_image_filename") or "")
    welcome_image_media_type = str(effective_settings.get("welcome_image_media_type") or current_settings.get("welcome_image_media_type") or "")
    welcome_image_size_bytes = int(effective_settings.get("welcome_image_size_bytes") or current_settings.get("welcome_image_size_bytes") or 0)
    welcome_image_width = int(effective_settings.get("welcome_image_width") or current_settings.get("welcome_image_width") or 0)
    welcome_image_height = int(effective_settings.get("welcome_image_height") or current_settings.get("welcome_image_height") or 0)
    welcome_image_configured = bool(effective_settings.get("welcome_image_configured"))
    welcome_image_dimensions = (
        f"{welcome_image_width}x{welcome_image_height}" if welcome_image_width > 0 and welcome_image_height > 0 else "Unknown"
    )
    welcome_image_size_label = format_byte_size(welcome_image_size_bytes) if welcome_image_configured else "n/a"
    welcome_image_dimensions_label = welcome_image_dimensions if welcome_image_configured else "n/a"
    welcome_image_details = (
        f"{escape(welcome_image_filename or 'Unnamed image')} | "
        f"{escape(welcome_image_media_type or 'Unknown type')} | "
        f"{escape(format_byte_size(welcome_image_size_bytes))} | "
        f"{escape(welcome_image_dimensions)}"
        if welcome_image_configured
        else "No image uploaded"
    )
    allowed_welcome_extensions_label = ", ".join(ext.upper().lstrip(".") for ext in WELCOME_IMAGE_ALLOWED_EXTENSIONS)

    return f"""
        <div class="card">
          <h2>Guild Settings</h2>
          <p class="muted">These values apply only to <strong>{escape(guild_name)}</strong>. Leave a field blank to use the global fallback.</p>
          <p class="muted">Welcome message placeholders: <span class="mono">{{member_mention}}</span>, <span class="mono">{{member_name}}</span>, <span class="mono">{{display_name}}</span>, <span class="mono">{{guild_name}}</span>, <span class="mono">{{member_count}}</span>, <span class="mono">{{account_created_at}}</span>.</p>
          <p class="muted">Welcome image uploads accept {escape(allowed_welcome_extensions_label)}. Max size: {format_byte_size(max_welcome_image_upload_bytes)}. Allowed dimensions: {WELCOME_IMAGE_MIN_WIDTH}x{WELCOME_IMAGE_MIN_HEIGHT} up to {WELCOME_IMAGE_MAX_WIDTH}x{WELCOME_IMAGE_MAX_HEIGHT}. Recommended: landscape artwork around 1200x675 for clearer in-chat presentation.</p>
          {catalog_note}
          <form method="post" enctype="multipart/form-data">
            <table>
              <thead><tr><th>Setting</th><th>Configured Value</th><th>Effective Value</th></tr></thead>
              <tbody>
                <tr>
                  <td><strong>Bot Log Channel</strong><div class="muted mono">bot_log_channel_id</div></td>
                  <td>{bot_log_select}</td>
                  <td class="muted mono">{escape(str(effective_settings.get("bot_log_channel_id") or ""))}</td>
                </tr>
                <tr>
                  <td><strong>Moderation Log Channel</strong><div class="muted mono">mod_log_channel_id</div></td>
                  <td>{mod_log_select}</td>
                  <td class="muted mono">{escape(str(effective_settings.get("mod_log_channel_id") or ""))}</td>
                </tr>
                <tr>
                  <td><strong>Firmware Notify Channel</strong><div class="muted mono">firmware_notify_channel_id</div></td>
                  <td>{firmware_select}</td>
                  <td class="muted mono">{escape(str(effective_settings.get("firmware_notify_channel_id") or ""))}</td>
                </tr>
                <tr>
                  <td><strong>Firmware Monitor</strong><div class="muted mono">firmware_monitor_enabled</div></td>
                  <td>
                    <label><input type="checkbox" name="firmware_monitor_enabled__override" value="1"{' checked' if firmware_monitor_override >= 0 else ''} /> Override global setting</label>
                    <label style="display:block; margin-top:8px;"><input type="checkbox" name="firmware_monitor_enabled" value="1"{' checked' if firmware_monitor_override > 0 else ''} /> Enabled for this guild</label>
                  </td>
                  <td class="muted mono">{'enabled' if int(effective_settings.get("firmware_monitor_enabled") or 0) > 0 else 'disabled'}<div class="muted">{escape(format_override_state(firmware_monitor_override))}</div></td>
                </tr>
                <tr>
                  <td><strong>Reddit Feed Monitor</strong><div class="muted mono">reddit_feed_notify_enabled</div></td>
                  <td>
                    <label><input type="checkbox" name="reddit_feed_notify_enabled__override" value="1"{' checked' if reddit_feed_override >= 0 else ''} /> Override global setting</label>
                    <label style="display:block; margin-top:8px;"><input type="checkbox" name="reddit_feed_notify_enabled" value="1"{' checked' if reddit_feed_override > 0 else ''} /> Enabled for this guild</label>
                  </td>
                  <td class="muted mono">{'enabled' if int(effective_settings.get("reddit_feed_notify_enabled") or 0) > 0 else 'disabled'}<div class="muted">{escape(format_override_state(reddit_feed_override))}</div></td>
                </tr>
                <tr>
                  <td><strong>YouTube Notifications</strong><div class="muted mono">youtube_notify_enabled</div></td>
                  <td>
                    <label><input type="checkbox" name="youtube_notify_enabled__override" value="1"{' checked' if youtube_notify_override >= 0 else ''} /> Override global setting</label>
                    <label style="display:block; margin-top:8px;"><input type="checkbox" name="youtube_notify_enabled" value="1"{' checked' if youtube_notify_override > 0 else ''} /> Enabled for this guild</label>
                  </td>
                  <td class="muted mono">{'enabled' if int(effective_settings.get("youtube_notify_enabled") or 0) > 0 else 'disabled'}<div class="muted">{escape(format_override_state(youtube_notify_override))}</div></td>
                </tr>
                <tr>
                  <td><strong>LinkedIn Notifications</strong><div class="muted mono">linkedin_notify_enabled</div></td>
                  <td>
                    <label><input type="checkbox" name="linkedin_notify_enabled__override" value="1"{' checked' if linkedin_notify_override >= 0 else ''} /> Override global setting</label>
                    <label style="display:block; margin-top:8px;"><input type="checkbox" name="linkedin_notify_enabled" value="1"{' checked' if linkedin_notify_override > 0 else ''} /> Enabled for this guild</label>
                  </td>
                  <td class="muted mono">{'enabled' if int(effective_settings.get("linkedin_notify_enabled") or 0) > 0 else 'disabled'}<div class="muted">{escape(format_override_state(linkedin_notify_override))}</div></td>
                </tr>
                <tr>
                  <td><strong>Beta Program Notifications</strong><div class="muted mono">beta_program_notify_enabled</div></td>
                  <td>
                    <label><input type="checkbox" name="beta_program_notify_enabled__override" value="1"{' checked' if beta_program_notify_override >= 0 else ''} /> Override global setting</label>
                    <label style="display:block; margin-top:8px;"><input type="checkbox" name="beta_program_notify_enabled" value="1"{' checked' if beta_program_notify_override > 0 else ''} /> Enabled for this guild</label>
                  </td>
                  <td class="muted mono">{'enabled' if int(effective_settings.get("beta_program_notify_enabled") or 0) > 0 else 'disabled'}<div class="muted">{escape(format_override_state(beta_program_notify_override))}</div></td>
                </tr>
                <tr>
                  <td><strong>Self-Assign Access Role</strong><div class="muted mono">access_role_id</div></td>
                  <td>{access_role_select}</td>
                  <td class="muted mono">{escape(str(effective_settings.get("access_role_id") or ""))}</td>
                </tr>
                <tr>
                  <td><strong>Welcome Channel</strong><div class="muted mono">welcome_channel_id</div></td>
                  <td>{welcome_channel_select}</td>
                  <td class="muted mono">{escape(str(effective_settings.get("welcome_channel_id") or ""))}</td>
                </tr>
                <tr>
                  <td><strong>Welcome Channel Message</strong><div class="muted mono">welcome_channel_message</div></td>
                  <td><textarea name="welcome_channel_message" rows="4" placeholder="Welcome to {{guild_name}}, {{member_mention}}.">{escape(welcome_channel_message)}</textarea></td>
                  <td class="muted">{escape(str(effective_settings.get("welcome_channel_message") or "")) or "Default welcome channel message"}</td>
                </tr>
                <tr>
                  <td><strong>Send Welcome DM</strong><div class="muted mono">welcome_dm_enabled</div></td>
                  <td><label><input type="checkbox" name="welcome_dm_enabled" value="1"{' checked' if welcome_dm_enabled else ''} /> Enable DM on join</label></td>
                  <td class="muted mono">{'enabled' if int(effective_settings.get("welcome_dm_enabled") or 0) > 0 else 'disabled'}</td>
                </tr>
                <tr>
                  <td><strong>Welcome DM Message</strong><div class="muted mono">welcome_dm_message</div></td>
                  <td><textarea name="welcome_dm_message" rows="4" placeholder="Welcome to {{guild_name}}, {{member_name}}. We&#39;re glad you&#39;re here.">{escape(welcome_dm_message)}</textarea></td>
                  <td class="muted">{escape(str(effective_settings.get("welcome_dm_message") or "")) or "Default welcome DM message"}</td>
                </tr>
                <tr>
                  <td><strong>Welcome Image</strong><div class="muted mono">welcome_image_file</div></td>
                  <td>
                    <input type="file" name="welcome_image_file" accept=".png,.jpg,.jpeg,.webp,.gif,image/*" />
                    <div class="muted" style="margin-top:8px;">Current: {welcome_image_details}</div>
                    <label style="display:block; margin-top:8px;"><input type="checkbox" name="welcome_image_remove" value="1" /> Remove current image</label>
                  </td>
                  <td class="muted">
                    {'configured' if welcome_image_configured else 'not configured'}
                    <div class="mono" style="margin-top:6px;">{escape(welcome_image_media_type or 'n/a')}</div>
                    <div class="mono">{escape(welcome_image_size_label)}</div>
                    <div class="mono">{escape(welcome_image_dimensions_label)}</div>
                  </td>
                </tr>
                <tr>
                  <td><strong>Attach Image In Channel</strong><div class="muted mono">welcome_channel_image_enabled</div></td>
                  <td><label><input type="checkbox" name="welcome_channel_image_enabled" value="1"{' checked' if welcome_channel_image_enabled else ''} /> Attach uploaded image to channel welcome</label></td>
                  <td class="muted mono">{'enabled' if int(effective_settings.get("welcome_channel_image_enabled") or 0) > 0 else 'disabled'}</td>
                </tr>
                <tr>
                  <td><strong>Attach Image In DM</strong><div class="muted mono">welcome_dm_image_enabled</div></td>
                  <td><label><input type="checkbox" name="welcome_dm_image_enabled" value="1"{' checked' if welcome_dm_image_enabled else ''} /> Attach uploaded image to welcome DM</label></td>
                  <td class="muted mono">{'enabled' if int(effective_settings.get("welcome_dm_image_enabled") or 0) > 0 else 'disabled'}</td>
                </tr>
              </tbody>
            </table>
            <div style="margin-top:14px;">
              <button class="btn" type="submit">Save Guild Settings</button>
            </div>
          </form>
        </div>
        """

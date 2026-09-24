"""Web-side helpers for Freshdesk viewer rendering (read-only).

Contains the HTML body builders and config normalizers used by
``app/web_freshdesk_routes.py`` so that all Freshdesk web logic
lives in one module — mirroring the ``app/web_discourse.py`` pattern.
"""
from __future__ import annotations

from html import escape
from typing import Any


def build_freshdesk_config_from_env(env_values: dict[str, Any]) -> dict[str, Any]:
    """Extract Freshdesk API config from environment values.

    Re-exports ``freshdesk_api.build_freshdesk_config`` for use
    by web-side helpers (mirrors the ``web_discourse`` pattern).
    """
    from app.freshdesk_api import build_freshdesk_config
    return build_freshdesk_config(env_values)


def freshdesk_configured(config: dict[str, Any]) -> bool:
    """Check if Freshdesk is enabled and properly configured (base URL + API key)."""
    from app.freshdesk_api import freshdesk_configured as _real
    return _real(config)


# --------------------------------------------------------------------------- #
#  Status / priority labels
# --------------------------------------------------------------------------- #
FRESHDESK_STATUS_LABELS = {
    2: "Open", 3: "Pending", 4: "Resolved", 5: "Closed",
    6: "Waiting on Customer", 7: "Waiting on External",
    8: "Archived", 9: "Locked",
}

FRESHDESK_PRIORITY_LABELS = {1: "Low", 2: "Medium", 3: "High", 4: "Urgent"}


def format_freshdesk_status(status_code: Any) -> str:
    try:
        return FRESHDESK_STATUS_LABELS.get(int(status_code), str(status_code))
    except (ValueError, TypeError):
        return str(status_code)


def format_freshdesk_priority(priority_code: Any) -> str:
    try:
        return FRESHDESK_PRIORITY_LABELS.get(int(priority_code), str(priority_code))
    except (ValueError, TypeError):
        return str(priority_code)


# --------------------------------------------------------------------------- #
#  Viewer page body
# --------------------------------------------------------------------------- #
def render_freshdesk_viewer_body(
    *,
    guild_name: str,
    effective_settings: dict[str, Any],
    command_permissions: list[dict[str, Any]] | None = None,
    allowed_role_names: list[str] | None = None,
    moderator_role_ids: list[int] | None = None,
    discord_role_options: list[dict[str, Any]] | None = None,
) -> str:
    """Render the Freshdesk viewer page showing integration settings, live ticket data,
    and Freshdesk command role restrictions.

    Parameters
    ----------
    guild_name
        Name of the currently selected guild (for display).
    effective_settings
        Dict with keys: ``FRESHDESK_ENABLED``, ``FRESHDESK_BASE_URL``, ``FRESHDESK_API_KEY``,
        ``FRESHDESK_REQUEST_TIMEOUT_SECONDS``.
    command_permissions
        List of command permission dicts from ``build_command_permissions_web_payload``,
        filtered to Freshdesk command keys. Each dict has keys: key, label, description,
        default_policy, default_policy_label, mode, role_ids.
    allowed_role_names
        List of allowed role names (from the payload).
    discord_role_options
        List of role option dicts from the Discord catalog, each with ``id`` and ``name``.
    """
    enabled = str(effective_settings.get("FRESHDESK_ENABLED", "true")).strip().lower() in {"1", "true", "yes", "on"}
    base_url = str(effective_settings.get("FRESHDESK_BASE_URL", "")).strip()
    api_key = str(effective_settings.get("FRESHDESK_API_KEY", "")).strip()
    api_key_configured = bool(api_key)
    timeout = int(str(effective_settings.get("FRESHDESK_REQUEST_TIMEOUT_SECONDS", "15") or "15").strip())

    enabled_checked = " checked" if enabled else ""
    api_key_placeholder = "••••••••••••••••" if api_key_configured else ""

    integration_items = f"""
        <tr>
          <td><strong>Enabled</strong></td>
          <td><label><input type="checkbox" name="FRESHDESK_ENABLED" value="true"{enabled_checked} /> Enable Freshdesk</label></td>
        </tr>
        <tr>
          <td><strong>Helpdesk URL</strong></td>
          <td><input type="text" name="FRESHDESK_BASE_URL" value="{escape(base_url, quote=True)}" placeholder="https://glinetservice.freshdesk.com" style="width:350px;" /></td>
        </tr>
        <tr>
          <td><strong>API Key</strong></td>
          <td><input type="password" name="FRESHDESK_API_KEY" value="" placeholder="{api_key_placeholder}" style="width:350px;" /> <small class="muted">Leave blank to keep existing</small></td>
        </tr>
        <tr>
          <td><strong>Request Timeout</strong></td>
          <td><input type="number" name="FRESHDESK_REQUEST_TIMEOUT_SECONDS" value="{escape(str(timeout), quote=True)}" min="5" max="120" style="width:80px;" /> seconds</td>
        </tr>
    """

    search_form = """
    <div class='card'>
      <h3 style='margin-top:0;'>Search Tickets</h3>
      <p class='muted'>Search GL.iNet Freshdesk tickets by query (e.g. <code>status:2</code>).</p>
      <form method='get' action='/admin/freshdesk/viewer/search' style='margin-bottom:8px;'>
        <input type='text' name='q' placeholder='Search query...' style='width:400px;' />
        <button class='btn' type='submit'>Search</button>
      </form>
    </div>
    """

    categories_card = f"""
    <div class='card'>
      <h3 style='margin-top:0;'>Solution Categories</h3>
      <p class='muted'>Solution/knowledge-base categories from {escape(base_url or 'Freshdesk')}</p>
      <div id='freshdesk-categories' class='muted'>Loading categories...</div>
    </div>
    """

    available_options = """
    <div class='card'>
      <h3 style='margin-top:0;'>Available Actions</h3>
      <p class='muted'>Use <a href='/admin/command-permissions'>Command Permissions</a> to restrict role access for these commands.</p>
      <ul>
        <li><strong>/support-ticket-search &lt;query&gt;</strong> — Discord command to search Freshdesk tickets</li>
        <li><strong>/support-ticket-view &lt;id&gt;</strong> — Discord command to view ticket details</li>
        <li><strong>/support-ticket-categories</strong> — Discord command to list solution categories</li>
        <li><strong>/create-ticket</strong> — Discord command to create a new ticket (requires role permission)</li>
      </ul>
    </div>
    """

    forum_html = "".join([search_form, categories_card, available_options])

    # Freshdesk command permissions section
    permissions_rows = ""
    if command_permissions:
        for entry in command_permissions:
            command_key = str(entry.get("key") or "").strip()
            if not command_key:
                continue
            label = str(entry.get("label") or command_key)
            description = str(entry.get("description") or "").strip()
            default_policy_label = str(entry.get("default_policy_label") or "").strip()
            mode = str(entry.get("mode") or "default").strip()
            role_ids = entry.get("role_ids", []) or []
            role_ids_value = ",".join(str(v) for v in role_ids)
            default_selected = " selected" if mode == "default" else ""
            public_selected = " selected" if mode == "public" else ""
            custom_selected = " selected" if mode == "custom_roles" else ""
            enabled_checked = "" if mode == "disabled" else " checked"

            # Build multi-select role dropdown from Discord catalog
            if discord_role_options:
                option_tags = []
                for opt in discord_role_options:
                    opt_id = str(opt.get("id", "")).strip()
                    opt_label = str(opt.get("name", opt.get("label", opt_id))).strip()
                    if not opt_id:
                        continue
                    selected = " selected" if any(str(rid) == opt_id for rid in role_ids) else ""
                    option_tags.append(f"<option value='{escape(opt_id, quote=True)}'{selected}>{escape(opt_label)} ({escape(opt_id)})</option>")
                # Always include a "no selection" placeholder option
                role_selector = (
                    f"<select name='role_ids__{escape(command_key, quote=True)}' "
                    f"multiple size='6' style='width:100%;'>"
                    f"<option value=''>— none —</option>"
                    + "".join(option_tags)
                    + "</select>"
                )
            else:
                # Fallback: text input for role IDs
                role_selector = (
                    f"<input type='text' name='role_ids_text__{escape(command_key, quote=True)}' "
                    f"value='{escape(role_ids_value, quote=True)}' "
                    f"placeholder='Comma-separated role IDs' style='width:180px;' />"
                )
            permissions_rows += f"""
            <tr>
              <td>
                <strong>{escape(label)}</strong>
                <div class="muted mono">{escape(command_key)}</div>
                <div class="muted">{escape(description)}</div>
                <input type="hidden" name="command_key" value="{escape(command_key, quote=True)}" />
              </td>
              <td class="muted">{escape(default_policy_label)}</td>
              <td>
                <label><input type="checkbox" name="enabled__{escape(command_key, quote=True)}" value="1"{enabled_checked} /> Enabled</label>
              </td>
              <td>
                <select name="mode__{escape(command_key, quote=True)}">
                  <option value="default"{default_selected}>Default rule</option>
                  <option value="public"{public_selected}>Public (any member)</option>
                  <option value="custom_roles"{custom_selected}>Custom roles</option>
                </select>
              </td>
              <td>
                {role_selector}
              </td>
            </tr>
            """
    permissions_card = ""
    if command_permissions:
        permissions_card = f"""
    <div class="card" style="margin-top:16px;">
      <h3 style="margin-top:0;">Freshdesk Command Permissions</h3>
      <p class="muted">Restrict which Discord roles can use Freshdesk commands. Changes are saved to the bot's command permission store.</p>
      <form method="post" action="/admin/freshdesk/viewer/">
        <table class="table-scroll">
          <thead>
            <tr>
              <th>Command</th>
              <th>Default</th>
              <th>Enabled</th>
              <th>Mode</th>
              <th>Role IDs</th>
            </tr>
          </thead>
          <tbody>
            {permissions_rows}
          </tbody>
        </table>
        <div style="margin-top:10px;">
          <button class="btn" type="submit">Save Command Permissions</button>
        </div>
      </form>
    </div>
    """

    return f"""
    <div class='card'>
      <h2>Freshdesk Ticket Viewer</h2>
      <p class='muted'>Read-only view of GL.iNet support tickets via Freshdesk for <strong>{escape(guild_name)}</strong>.
         This page allows searching and viewing Freshdesk tickets without modifying any data.
         {'' if api_key_configured else ' <span class="warning">API key is not configured — live data may be unavailable.</span>'}
      </p>

      <form method="post" action="/admin/freshdesk/viewer/">
        <div class='card' style='margin:16px 0 0 0;'>
          <h3 style="margin-top:0;">Integration Settings</h3>
          <table>
            <tbody>
              {integration_items}
            </tbody>
          </table>
          <button class="btn" type="submit" style="margin-top:12px;">Save Integration Settings</button>
        </div>
      </form>

      {permissions_card}

      {forum_html}
    </form>

    <script>
    (function() {{
      var baseUrl = {repr(base_url or '')};
      if (!baseUrl) return;
      fetch('/admin/freshdesk/viewer/api/categories')
        .then(function(r) {{ return r.json(); }})
        .then(function(data) {{
          var el = document.getElementById('freshdesk-categories');
          if (data && data.categories && data.categories.length > 0) {{
            el.innerHTML = data.categories.map(function(c) {{
              return '<a href="' + escape(c.url) + '" target="_blank">' + escape(c.name) + '</a>';
            }}).join('<br>');
          }} else {{
            el.textContent = 'No categories found or unable to fetch.';
          }}
        }})
        .catch(function(e) {{
          var el = document.getElementById('freshdesk-categories');
          el.textContent = 'Error fetching categories: ' + e.message;
        }});
    }})();
    </script>
    """


def render_freshdesk_search_results(
    *,
    search_query: str,
    tickets: list[dict[str, Any]],
    base_url: str,
) -> str:
    """Render search results as HTML links."""
    if not tickets:
        return "<p class='muted'>No tickets found for your search.</p>"

    items = "".join(
        f'<li><a href="{escape(str(t.get("url", "")))}" target="_blank">'
        f"#{t.get('id', '?')} — {escape(str(t.get('subject', 'No subject'))[:200])}</a>"
        f'<div class="muted">{escape(str(t.get("status", "")))} · '
        f"{escape(str(t.get('type', '')))}</div></li>"
        for t in tickets
    )
    return f"<ul>{items}</ul>"

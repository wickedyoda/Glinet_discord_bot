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
    """Check if Freshdesk is properly configured (base URL + API key)."""
    return bool(str(config.get("base_url", "")).strip()) and bool(str(config.get("api_key", "")).strip())


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
) -> str:
    """Render the read-only Freshdesk viewer page showing integration settings and live ticket data.

    Parameters
    ----------
    guild_name
        Name of the currently selected guild (for display).
    effective_settings
        Dict with keys: ``freshdesk_base_url``, ``freshdesk_api_key``,
        ``freshdesk_request_timeout_seconds``.
    """
    config = build_freshdesk_config_from_env(effective_settings)
    base_url = config["base_url"]
    api_key_configured = bool(config["api_key"])
    timeout = config["timeout"]

    integration_rows = [
        ("Helpdesk URL", base_url or "Not configured"),
        ("API Key", "Configured" if api_key_configured else "Not configured"),
        ("Request Timeout", f"{timeout} second(s)"),
        ("Scope", "Read-only (search, view, browse)"),
    ]

    integration_items = "".join(
        f"""
        <tr>
          <td><strong>{escape(label)}</strong></td>
          <td class='muted mono'>{escape(value)}</td>
        </tr>
        """
        for label, value in integration_rows
    )

    search_form = f"""
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
      <ul>
        <li><strong>/freshdesk search &lt;query&gt;</strong> — Discord command to search Freshdesk tickets</li>
        <li><strong>/freshdesk ticket &lt;id&gt;</strong> — Discord command to view ticket details</li>
        <li><strong>/freshdesk categories</strong> — Discord command to list solution categories</li>
      </ul>
    </div>
    """

    forum_html = "".join([search_form, categories_card, available_options])

    return f"""
    <div class='card'>
      <h2>Freshdesk Ticket Viewer</h2>
      <p class='muted'>Read-only view of GL.iNet support tickets via Freshdesk for <strong>{escape(guild_name)}</strong>.
         This page allows searching and viewing Freshdesk tickets without modifying any data.
         {'' if api_key_configured else ' <span class="warning">API key is not configured — live data may be unavailable.</span>'}
      </p>

      <div class='card' style='margin:16px 0 0 0;'>
        <h3 style='margin-top:0;'>Integration Settings</h3>
        <table>
          <thead><tr><th>Setting</th><th>Value</th></tr></thead>
          <tbody>
            {integration_items}
          </tbody>
        </table>
      </div>
    </div>

    {forum_html}

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

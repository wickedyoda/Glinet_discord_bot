"""Freshdesk viewer routes — read-only Flask Blueprint.

Extracted from ``web_admin.py``'s ``create_web_app`` to keep the
monolith manageable and Freshdesk viewer logic self-contained,
following the same pattern as the Discourse viewer blueprint.

The blueprint is registered from ``create_web_app`` via
``register_freshdesk_viewer_blueprint(app, **helpers)`` where each
keyword argument is a closure defined inside ``create_web_app``
(e.g. ``_current_user``, ``_selected_guild``, ``_render_page`` …).
"""
from __future__ import annotations

from html import escape
from typing import Any

from flask import Blueprint, jsonify, request

from app.freshdesk_api import (
    FreshdeskApiError,
    FreshdeskRateLimitError,
    fetch_freshdesk_ticket,
    list_freshdesk_solution_categories,
    search_freshdesk_tickets,
)
from app.freshdesk_web_helpers import render_freshdesk_viewer_body

bp = Blueprint("freshdesk_viewer", __name__, url_prefix="/admin/freshdesk/viewer")

# These are injected by ``register_freshdesk_viewer_blueprint``.
_h = {}  # type: dict[str, object]


# --------------------------------------------------------------------------- #
#  Helpers
# --------------------------------------------------------------------------- #
def _current_user():
    return _h["current_user"]()  # type: ignore[operator]


def _selected_guild():
    return _h["selected_guild"]() or {}  # type: ignore[operator]


def _require_selected_guild_redirect():
    return _h["require_selected_guild_redirect"]()  # type: ignore[operator]


def _render_page(title: str, body: str, email: str, is_admin: bool):
    return _h["render_page"](title, body, email, is_admin)  # type: ignore[operator]


# --------------------------------------------------------------------------- #
#  Page routes
# --------------------------------------------------------------------------- #
@bp.route("/", methods=["GET"], endpoint="freshdesk_viewer_page")
def viewer_page():
    user = _current_user()
    selection_redirect = _require_selected_guild_redirect()
    if selection_redirect is not None:
        return selection_redirect
    selected_guild = _selected_guild()
    guild_name = str(selected_guild.get("name") or "Unknown")
    selected_guild_id = str(selected_guild.get("id") or "")

    env_values = _resolve_env_values()

    body = render_freshdesk_viewer_body(
        guild_name=guild_name,
        effective_settings=env_values,
    )
    return _render_page("Freshdesk Viewer", body, user["email"], bool(user.get("is_admin")))


@bp.route("/search", methods=["GET"], endpoint="freshdesk_viewer_search")
def search():
    user = _current_user()
    selection_redirect = _require_selected_guild_redirect()
    if selection_redirect is not None:
        return selection_redirect

    search_query = str(request.args.get("q", "")).strip()
    env_values = _resolve_env_values()

    base_url = str(env_values.get("freshdesk_base_url", "")).rstrip("/")
    api_key = str(env_values.get("freshdesk_api_key", "")).strip()
    timeout = int(env_values.get("freshdesk_request_timeout_seconds", 15))

    search_results_html = ""
    if search_query and base_url and api_key:
        try:
            tickets = search_freshdesk_tickets(
                base_url=base_url,
                query=search_query,
                max_results=20,
                timeout_seconds=timeout,
                api_key=api_key,
            )
            if tickets:
                result_items = "".join(
                    f'<li><a href="{escape(str(t["url"]))}" target="_blank">'
                    f"#{t['id']} — {escape(t['subject'])}</a>"
                    f'<div class="muted">{escape(t.get("status", ""))} · {escape(t.get("type", ""))}</div></li>'
                    for t in tickets
                )
                search_results_html = f"<ul>{result_items}</ul>"
            else:
                search_results_html = "<p class='muted'>No tickets found for your search.</p>"
        except (FreshdeskApiError, FreshdeskRateLimitError) as exc:
            search_results_html = f'<p class="warning">Search error: {escape(str(exc))}</p>'
        except Exception as exc:  # noqa: BLE001
            search_results_html = f'<p class="warning">Search error: {escape(str(exc))}</p>'
    elif not search_query:
        search_results_html = "<p class='muted'>Enter a search query above to search Freshdesk.</p>"
    elif not api_key:
        search_results_html = (
            "<p class='warning'>Freshdesk API key is not configured. "
            "Contact an administrator.</p>"
        )

    body = f"""
    <div class='card'>
      <h2>Freshdesk Ticket Search</h2>
      <p class='muted'>Searching <strong>{escape(base_url or 'not configured')}</strong> for <strong>{escape(search_query)}</strong></p>
      <form method='get' action='/admin/freshdesk/viewer/search' style='margin-bottom:12px;'>
        <input type='text' name='q' value='{escape(search_query, quote=True)}' placeholder='Search query...' style='width:400px;' />
        <button class='btn' type='submit'>Search</button>
      </form>
      {search_results_html}
    </div>
    """
    return _render_page("Freshdesk Search", body, user["email"], bool(user.get("is_admin")))


@bp.route("/ticket/<int:ticket_id>", methods=["GET"], endpoint="freshdesk_viewer_ticket")
def view_ticket(ticket_id: int):
    user = _current_user()
    selection_redirect = _require_selected_guild_redirect()
    if selection_redirect is not None:
        return selection_redirect

    env_values = _resolve_env_values()
    base_url = str(env_values.get("freshdesk_base_url", "")).rstrip("/")
    api_key = str(env_values.get("freshdesk_api_key", "")).strip()
    timeout = int(env_values.get("freshdesk_request_timeout_seconds", 15))

    ticket_html = "<p class='muted'>No ticket ID provided.</p>"
    if base_url and api_key:
        try:
            ticket = fetch_freshdesk_ticket(
                base_url=base_url,
                ticket_id=ticket_id,
                timeout_seconds=timeout,
                api_key=api_key,
            )
            if ticket:
                ticket_html = (
                    f"<h3>#{ticket['id']} — {escape(ticket['subject'])}</h3>"
                    f"<p class='muted'>{escape(ticket.get('status', ''))} · "
                    f"{escape(ticket.get('priority', ''))} · "
                    f"{escape(ticket.get('type', ''))}</p>"
                    f"<div class='muted'>{escape(ticket.get('description', ''))}</div>"
                    f"<p class='muted'>Created: {escape(ticket.get('created_at', ''))} | "
                    f"Updated: {escape(ticket.get('updated_at', ''))}</p>"
                    f'<p><a href="{escape(ticket.get("url", ""))}" target="_blank">View in Freshdesk</a></p>'
                )
            else:
                ticket_html = "<p class='muted'>Ticket not found.</p>"
        except (FreshdeskApiError, FreshdeskRateLimitError) as exc:
            ticket_html = f'<p class="warning">Error loading ticket: {escape(str(exc))}</p>'
        except Exception as exc:  # noqa: BLE001
            ticket_html = f'<p class="warning">Error loading ticket: {escape(str(exc))}</p>'

    body = f"""
    <div class='card'>
      <h2>Freshdesk Ticket</h2>
      {ticket_html}
    </div>
    <div class='card'>
      <a class='btn secondary' href='/admin/freshdesk/viewer'>Back to Viewer</a>
    </div>
    """
    return _render_page("Freshdesk Ticket", body, user["email"], bool(user.get("is_admin")))


# --------------------------------------------------------------------------- #
#  API routes
# --------------------------------------------------------------------------- #
@bp.route("/api/categories", endpoint="freshdesk_viewer_api_categories")
def api_categories():
    user = _current_user()
    if not str(user["email"]):
        return jsonify({"ok": False, "error": "Not authenticated"}), 401

    env_values = _resolve_env_values()
    base_url = str(env_values.get("freshdesk_base_url", "")).rstrip("/")
    api_key = str(env_values.get("freshdesk_api_key", "")).strip()
    timeout = int(env_values.get("freshdesk_request_timeout_seconds", 15))

    if not base_url or not api_key:
        return jsonify({"ok": False, "error": "Freshdesk not configured"}), 400

    try:
        categories = list_freshdesk_solution_categories(
            base_url=base_url,
            timeout_seconds=timeout,
            api_key=api_key,
        )
        return jsonify({"ok": True, "categories": categories})
    except (FreshdeskApiError, FreshdeskRateLimitError) as exc:
        return jsonify({"ok": False, "error": str(exc)}), 502
    except Exception as exc:  # noqa: BLE001
        return jsonify({"ok": False, "error": str(exc)}), 500


# --------------------------------------------------------------------------- #
#  Internal: resolve config from closures
# --------------------------------------------------------------------------- #
def _resolve_env_values() -> dict[str, Any]:
    """Build Freshdesk config from env values injected into the blueprint."""
    on_get_env = _h.get("on_get_env")
    if callable(on_get_env):
        result = on_get_env() or {}
        return result if isinstance(result, dict) else {}
    return {}


# --------------------------------------------------------------------------- #
#  Registration
# --------------------------------------------------------------------------- #
def register_freshdesk_viewer_blueprint(app, **helpers):
    """Register the Freshdesk viewer blueprint.

    Parameters
    ----------
    app
        The Flask application.
    **helpers
        Keyword pairs whose values are callables or values defined inside
        ``create_web_app``. Recognised keys:
        - ``current_user`` – callable, returns the logged-in web user dict
        - ``selected_guild`` – callable, returns the active guild dict (or {})
        - ``require_selected_guild_redirect`` – callable, returns redirect or None
        - ``render_page`` – callable(title, body, email, is_admin) -> Response
        - ``on_get_env`` – callable, returns dict of env values for Freshdesk
    """
    global _h
    _h = {
        "current_user": helpers.get("current_user"),
        "selected_guild": helpers.get("selected_guild"),
        "require_selected_guild_redirect": helpers.get("require_selected_guild_redirect"),
        "render_page": helpers.get("render_page"),
        "on_get_env": helpers.get("on_get_env"),
    }
    app.register_blueprint(bp)

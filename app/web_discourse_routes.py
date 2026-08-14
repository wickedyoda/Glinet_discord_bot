"""Discourse Forum Viewer routes — read-only Flask Blueprint.

Extracted from ``web_admin.py``'s ``create_web_app`` to reduce the
6,500-line monolith and keep Discourse viewer logic self-contained.

The blueprint is registered from ``create_web_app`` via
``register_discourse_viewer_blueprint(app, **helpers)`` where each
keyword argument is a closure defined inside ``create_web_app``
(e.g. ``_current_user``, ``_selected_guild``, ``_render_page`` …).
"""
from __future__ import annotations

import logging
from html import escape

from flask import Blueprint, jsonify, request

from app.discourse_api import (
    DiscourseApiError,
    DiscourseRateLimitError,
    fetch_discourse_categories,
    search_discourse_topics,
)
from app.web_discourse import (
    build_discourse_config_from_settings,
    render_discourse_viewer_body,
)

logger = logging.getLogger("invite_bot.discourse_viewer")

bp = Blueprint("discourse_viewer", __name__, url_prefix="/admin/discourse/viewer")

# These are injected by ``register_discourse_viewer_blueprint``.
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


def _get_guild_settings(guild_id: str):
    cb = _h.get("get_guild_settings")
    if callable(cb):
        return cb(guild_id)
    return {"ok": False, "error": "Discourse callbacks are not configured."}


def _resolve_settings(selected_guild_id: str):
    """Fetch effective Discourse settings for the selected guild.

    Returns ``(effective_settings, error_message)``. On success
    *effective_settings* is a dict and *error_message* is ``None``.
    On failure, *effective_settings* is ``None``.
    """
    payload = _get_guild_settings(selected_guild_id)
    if not isinstance(payload, dict) or not payload.get("ok"):
        err = (
            str(payload.get("error") or "Unable to load Discourse settings.")
            if isinstance(payload, dict)
            else "Unable to load Discourse settings."
        )
        return None, err
    return payload.get("effective", {}), None


# --------------------------------------------------------------------------- #
#  Page routes
# --------------------------------------------------------------------------- #
@bp.route("/", methods=["GET"], endpoint="discourse_viewer_page")
def viewer_page():
    user = _current_user()
    selection_redirect = _require_selected_guild_redirect()
    if selection_redirect is not None:
        return selection_redirect
    selected_guild = _selected_guild()
    guild_name = str(selected_guild.get("name") or "Unknown")
    selected_guild_id = str(selected_guild.get("id") or "")

    effective_settings, error = _resolve_settings(selected_guild_id)
    if error:
        body = (
            "<div class='card'><h2>Discourse Forum Viewer</h2>"
            f"<p class='muted'>Could not load Discourse settings: {escape(error)}</p></div>"
        )
        return _render_page("Discourse Viewer", body, user["email"], bool(user.get("is_admin")))

    body = render_discourse_viewer_body(
        guild_name=guild_name, effective_settings=effective_settings
    )
    return _render_page("Discourse Viewer", body, user["email"], bool(user.get("is_admin")))


@bp.route("/search", methods=["GET"], endpoint="discourse_viewer_search")
def search():
    user = _current_user()
    selection_redirect = _require_selected_guild_redirect()
    if selection_redirect is not None:
        return selection_redirect
    selected_guild = _selected_guild()
    selected_guild_id = str(selected_guild.get("id") or "")

    search_query = str(request.args.get("q", "")).strip()
    effective_settings, error = _resolve_settings(selected_guild_id)

    if error:
        body = (
            "<div class='card'><h2>Discourse Forum Search</h2>"
            f"<p class='muted'>Could not load Discourse settings: {escape(error)}</p></div>"
        )
        return _render_page("Discourse Search", body, user["email"], bool(user.get("is_admin")))

    config = build_discourse_config_from_settings(effective_settings)
    search_results_html = ""

    if search_query and config["base_url"]:
        try:
            topics = search_discourse_topics(
                base_url=config["base_url"],
                query=search_query,
                max_results=20,
                source_name="Discourse search",
                timeout_seconds=config["timeout"],
                user_agent="GLiNetDiscordBot/1.0 (+https://github.com/wickedyoda/Glinet_discord_bot)",
                api_key=config["api_key"],
                api_username=config["api_username"],
            )
            if topics:
                result_items = "".join(
                    f'<li><a href="{escape(t["url"])}" target="_blank">{escape(t["title"])}</a>'
                    f'<div class="muted">{escape(t.get("excerpt", ""))}</div></li>'
                    for t in topics
                )
                search_results_html = f"<ul>{result_items}</ul>"
            else:
                search_results_html = "<p class='muted'>No topics found for your search.</p>"
        except (DiscourseApiError, DiscourseRateLimitError):
            search_results_html = "<p class='warning'>Search error: a Discourse API error occurred.</p>"
        except Exception:  # noqa: BLE001
            logger.exception("Discourse search error")
            search_results_html = "<p class='warning'>Search error: an unexpected error occurred.</p>"
    elif not search_query:
        search_results_html = "<p class='muted'>Enter a search query above to search the forum.</p>"

    body = f"""
    <div class='card'>
      <h2>Discourse Forum Search</h2>
      <p class='muted'>Searching <strong>{escape(config['base_url'] or 'not configured')}</strong> for <strong>{escape(search_query)}</strong></p>
      <form method='get' action='/admin/discourse/viewer/search' style='margin-bottom:12px;'>
        <input type='text' name='q' value='{escape(search_query, quote=True)}' placeholder='Search query...' style='width:400px;' />
        <button class='btn' type='submit'>Search</button>
      </form>
      {search_results_html}
    </div>
    """
    return _render_page("Discourse Search", body, user["email"], bool(user.get("is_admin")))


# --------------------------------------------------------------------------- #
#  API routes
# --------------------------------------------------------------------------- #
@bp.route("/api/categories", endpoint="discourse_viewer_api_categories")
def api_categories():
    user = _current_user()
    if not str(user["email"]):
        return jsonify({"ok": False, "error": "Not authenticated"}), 401
    selected_guild = _selected_guild()
    selected_guild_id = str(selected_guild.get("id") or "")

    effective_settings, error = _resolve_settings(selected_guild_id)
    if error:
        return jsonify({"ok": False, "error": "Discourse settings not available"}), 500

    config = build_discourse_config_from_settings(effective_settings)
    if not config["base_url"]:
        return jsonify({"ok": False, "error": "Discourse base URL not configured"}), 400

    try:
        categories = fetch_discourse_categories(
            base_url=config["base_url"],
            timeout_seconds=config["timeout"],
            user_agent="GLiNetDiscordBot/1.0 (+https://github.com/wickedyoda/Glinet_discord_bot)",
            api_key=config["api_key"],
            api_username=config["api_username"],
        )
        return jsonify({"ok": True, "categories": categories})
    except (DiscourseApiError, DiscourseRateLimitError):
        return jsonify({"ok": False, "error": "Discourse API error — check configuration."}), 502
    except Exception:  # noqa: BLE001
        logger.exception("Discourse API categories error")
        return jsonify({"ok": False, "error": "An unexpected error occurred."}), 500


@bp.route("/api/topics", endpoint="discourse_viewer_api_topics")
def api_topics():
    user = _current_user()
    if not str(user["email"]):
        return jsonify({"ok": False, "error": "Not authenticated"}), 401
    selected_guild = _selected_guild()
    selected_guild_id = str(selected_guild.get("id") or "")

    effective_settings, error = _resolve_settings(selected_guild_id)
    if error:
        return jsonify({"ok": False, "error": "Discourse settings not available"}), 500

    config = build_discourse_config_from_settings(effective_settings)
    if not config["base_url"]:
        return jsonify({"ok": False, "error": "Discourse base URL not configured"}), 400

    try:
        topics = search_discourse_topics(
            base_url=config["base_url"],
            query="",
            max_results=20,
            source_name="Discourse latest",
            timeout_seconds=config["timeout"],
            user_agent="GLiNetDiscordBot/1.0 (+https://github.com/wickedyoda/Glinet_discord_bot)",
            api_key=config["api_key"],
            api_username=config["api_username"],
        )
        return jsonify({"ok": True, "topics": topics})
    except (DiscourseApiError, DiscourseRateLimitError):
        return jsonify({"ok": False, "error": "Discourse API error — check configuration."}), 502
    except Exception:  # noqa: BLE001
        logger.exception("Discourse API topics error")
        return jsonify({"ok": False, "error": "An unexpected error occurred."}), 500


# --------------------------------------------------------------------------- #
#  Registration
# --------------------------------------------------------------------------- #
def register_discourse_viewer_blueprint(app, **helpers):
    """Register the Discourse viewer blueprint and inject ``create_web_app``
    closures as the helpers the route handlers depend on.

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
        - ``get_guild_settings`` – callable(guild_id) -> dict
    """
    global _h
    _h = {
        "current_user": helpers.get("current_user"),
        "selected_guild": helpers.get("selected_guild"),
        "require_selected_guild_redirect": helpers.get("require_selected_guild_redirect"),
        "render_page": helpers.get("render_page"),
        "get_guild_settings": helpers.get("get_guild_settings"),
    }
    app.register_blueprint(bp)

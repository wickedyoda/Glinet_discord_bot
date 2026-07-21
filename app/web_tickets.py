"""
Tickets web admin routes.
Adds ticket management views into the existing Flask web admin.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from flask import Blueprint, jsonify, render_template_string, request, session, url_for

from app.tickets import TicketStore


tickets_bp = Blueprint("tickets", __name__, url_prefix="/admin/tickets")


def _get_store() -> TicketStore:
    db_path = request.ctx.get("db_path") if hasattr(request, "ctx") else None
    if db_path is None:
        from app.guild_state import GuildStateManager
        gsm = GuildStateManager()
        db_path = gsm.db_path
    import sqlite3
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    guild_id = session.get("guild_id") if session else None
    store = TicketStore(conn, guild_id=guild_id)
    store.ensure_schema()
    return store


TEMPLATE = """{% extends "base.html" %}
{% block content %}
<h2>Tickets</h2>
<div class="actions">
  <a class="btn" href="{{ url_for('.create') }}">Cleanse now</a>
</div>
<table class="table">
  <thead><tr><th>#</th><th>Channel</th><th>Category</th><th>Owner</th><th>Status</th><th>Created</th></tr></thead>
  <tbody>
    <tr><td colspan="6">No ticket data exported yet.</td></tr>
  </tbody>
</table>
{% endblock %}


"""


@tickets_bp.route("/", methods=["GET"])
def index() -> str:
    try:
        store = _get_store()
        guild_id = session.get("guild_id") if session else None
        stats = store.stats(guild_id or 0)
        return render_template_string(
            Path(__file__).with_name("templates").joinpath("tickets_index.html").read_text()
            if Path(__file__).with_name("templates").joinpath("tickets_index.html").exists()
            else "<h2>Tickets</h2><pre>" + __import__("json").dumps(stats, indent=2) + "</pre>",
            stats=stats,
        )
    except Exception as exc:
        return f"<h2>Tickets</h2><pre>Error: {exc}</pre>"


@tickets_bp.route("/stats.json", methods=["GET"])
def stats_json() -> Any:
    try:
        store = _get_store()
        guild_id = session.get("guild_id") if session else None
        return jsonify({"ok": True, "stats": store.stats(guild_id or 0)})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


def register_tickets_blueprint(app: Any) -> None:
    app.register_blueprint(tickets_bp)

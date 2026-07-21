"""
Tickets web admin callback wrappers.
Used by web_admin.py routes; implemented in this module to avoid circular imports.
"""
from __future__ import annotations

import json
from typing import Any

import sqlite3
from app.tickets import TicketStore


def _get_ticket_conn():
    from bot import get_db_connection
    return get_db_connection()


def ensure_ticket_schema() -> None:
    store = _store_from_request()
    store.ensure_schema()


def _store_from_request() -> TicketStore:
    import sqlite3
    conn = sqlite3.connect("/dev/null", check_same_thread=False)
    return TicketStore(conn, guild_id=0)


def _get_store_for_guild(guild_id: int | None) -> TicketStore:
    conn = _get_ticket_conn()
    conn.row_factory = sqlite3.Row
    store = TicketStore(conn, guild_id=guild_id or 0)
    store.ensure_schema()
    return store


def run_web_get_tickets(guild_id: int | None = None):
    try:
        store = _get_store_for_guild(guild_id)
        rows = store.conn.execute(
            "SELECT id, channel_id, owner_id, category_id, status, number, created_at, closed_at "
            "FROM tickets WHERE guild_id=? ORDER BY id DESC LIMIT 200",
            (guild_id or 0,),
        ).fetchall()
        return {"ok": True, "tickets": [dict(r) for r in rows]}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def run_web_ticket_stats(guild_id: int | None = None):
    try:
        store = _get_store_for_guild(guild_id)
        return {"ok": True, "stats": store.stats(guild_id or 0)}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}

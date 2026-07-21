"""
Tickets module — ported from discord-tickets/bot (GPLv3).

Single-responsibility helpers for bot.py:
- TicketStore: SQLite-backed ticket state
- Role-tier permission helpers
- UI/render helpers used by slash commands and buttons
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Optional

from sqlite3 import Connection, Row

import discord
from discord import app_commands
from discord.ext import commands

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Ticket category defaults
# ---------------------------------------------------------------------------
TICKET_CATEGORIES_DEFAULT = [
    {"id": "support", "name": "Support", "questions": ["What do you need help with?"]},
    {"id": "sales", "name": "Sales", "questions": ["What product/plan are you interested in?"]},
    {"id": "partnership", "name": "Partnership", "questions": ["Describe the partnership idea."]},
]


# ---------------------------------------------------------------------------
# Ticket store
# ---------------------------------------------------------------------------
class TicketStore:
    def __init__(self, conn: Connection, guild_id: int = 0) -> None:
        self.conn = conn
        self.guild_id = guild_id

    def ensure_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS tickets (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id     INTEGER NOT NULL DEFAULT 0,
                channel_id   INTEGER NOT NULL,
                owner_id     INTEGER NOT NULL,
                category_id  TEXT NOT NULL,
                claimer_id   INTEGER,
                status       TEXT NOT NULL DEFAULT 'open',
                number       INTEGER,
                created_at   TEXT NOT NULL,
                closed_at    TEXT,
                note         TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_tickets_guild ON tickets(guild_id);
            CREATE INDEX IF NOT EXISTS idx_tickets_channel ON tickets(channel_id);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_tickets_guild_number ON tickets(guild_id, number);
            CREATE INDEX IF NOT EXISTS idx_tickets_status ON tickets(status);
            CREATE INDEX IF NOT EXISTS idx_tickets_owner ON tickets(owner_id);
            """
        )
        self.conn.commit()
        logger.debug("Ensured ticket schema for guild=%s", self.guild_id)

    def next_number(self, guild_id: int) -> int:
        row = self.conn.execute(
            "SELECT COALESCE(MAX(number),0) AS n FROM tickets WHERE guild_id=?",
            (guild_id,),
        ).fetchone()
        if row is None:
            return 1
        return int(row["n"]) + 1

    def create(self, *, channel_id: int, owner_id: int, category_id: str, guild_id: int) -> int:
        number = self.next_number(guild_id)
        now = datetime.now(UTC).isoformat()
        cur = self.conn.execute(
            "INSERT INTO tickets(guild_id,channel_id,owner_id,category_id,number,created_at) VALUES(?,?,?,?,?,?)",
            (guild_id, channel_id, owner_id, category_id, number, now),
        )
        self.conn.commit()
        ticket_id = int(cur.lastrowid)
        logger.debug(
            "Created ticket id=%s number=%s category=%s channel=%s owner=%s guild=%s",
            ticket_id, number, category_id, channel_id, owner_id, guild_id,
        )
        return ticket_id

    def close(self, channel_id: int, *, closer_id: int, note: Optional[str] = None) -> bool:
        now = datetime.now(UTC).isoformat()
        cur = self.conn.execute(
            "UPDATE tickets SET status='closed', closed_at=?, claimer_id=?, note=? "
            "WHERE channel_id=? AND status='open'",
            (now, closer_id, note, channel_id),
        )
        self.conn.commit()
        updated = cur.rowcount > 0
        logger.debug("Closed ticket channel=%s by=%s updated=%s", channel_id, closer_id, updated)
        return updated

    def reopen(self, channel_id: int) -> bool:
        cur = self.conn.execute(
            "UPDATE tickets SET status='open', closed_at=NULL, claimer_id=NULL "
            "WHERE channel_id=? AND status='closed'",
            (channel_id,),
        )
        self.conn.commit()
        updated = cur.rowcount > 0
        logger.debug("Reopened ticket channel=%s updated=%s", channel_id, updated)
        return updated

    def get_by_channel(self, channel_id: int) -> Optional[dict]:
        if not channel_id:
            return None
        row = self.conn.execute(
            "SELECT * FROM tickets WHERE channel_id=? LIMIT 1", (channel_id,)
        ).fetchone()
        if row is None:
            return None
        return dict(row)

    def get_by_number(self, guild_id: int, number: int) -> Optional[dict]:
        row = self.conn.execute(
            "SELECT * FROM tickets WHERE guild_id=? AND number=? LIMIT 1",
            (guild_id, number),
        ).fetchone()
        if row is None:
            return None
        return dict(row)

    def search_by_owner_email(self, guild_id: int, email: str) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM tickets WHERE guild_id=? AND owner_id=? ORDER BY created_at DESC",
            (guild_id, email),
        ).fetchall()
        return [dict(r) for r in rows]

    def set_owner_email(self, ticket_id: int, email: str) -> bool:
        cur = self.conn.execute("UPDATE tickets SET owner_id=? WHERE id=?", (email, ticket_id))
        self.conn.commit()
        logger.debug("Set owner email ticket=%s email=%s", ticket_id, email)
        return cur.rowcount > 0

    def reassign(self, ticket_id: int, new_assignee_id: int) -> bool:
        cur = self.conn.execute(
            "UPDATE tickets SET claimer_id=? WHERE id=? AND status='open'",
            (new_assignee_id, ticket_id),
        )
        self.conn.commit()
        updated = cur.rowcount > 0
        logger.debug("Reassigned ticket=%s assignee=%s updated=%s", ticket_id, new_assignee_id, updated)
        return updated

    def set_note(self, ticket_id: int, note: str) -> bool:
        cur = self.conn.execute("UPDATE tickets SET note=? WHERE id=?", (note, ticket_id))
        self.conn.commit()
        return cur.rowcount > 0

    def stats(self, guild_id: int) -> dict:
        rows = self.conn.execute(
            "SELECT category_id, status, COUNT(*) AS c "
            "FROM tickets WHERE guild_id=? GROUP BY category_id, status",
            (guild_id,),
        ).fetchall()
        by_category: dict[str, dict[str, int]] = {}
        for row in rows:
            cat = by_category.setdefault(row["category_id"], {"open": 0, "closed": 0})
            status = row["status"]
            if status in cat:
                cat[status] = int(row["c"])
        logger.debug("Ticket stats guild=%s categories=%d", guild_id, len(by_category))
        return by_category


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------
def get_db_connection() -> Optional[Connection]:
    try:
        from bot import get_db_connection as _bot_get_db  # Local import avoids circular import
        return _bot_get_db()
    except Exception:
        return None


def _ticket_store() -> "TicketStore":
    conn = get_db_connection()
    if conn is None:
        raise RuntimeError("Database connection is not available.")
    conn.row_factory = sqlite3.Row
    store = TicketStore(conn, guild_id=0)
    store.ensure_schema()
    return store


# ---------------------------------------------------------------------------
# Role-tier helpers  (role titles: search, create, reassign, admin)
# ---------------------------------------------------------------------------
def load_ticket_role_map() -> dict[str, list[int]]:
    conn = get_db_connection()
    if not conn:
        logger.debug("DB connection unavailable for ticket role map lookup.")
        return {"search": [], "create": [], "reassign": [], "admin": []}
    raw = {}
    try:
        columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(guild_settings)").fetchall()
        }
        if "ticket_role_map_json" not in columns:
            logger.debug("ticket_role_map_json column missing; using empty role map.")
            return {"search": [], "create": [], "reassign": [], "admin": []}
        row = conn.execute(
            "SELECT ticket_role_map_json FROM guild_settings WHERE guild_id=0 LIMIT 1"
        ).fetchone()
        raw = json.loads(str(row["ticket_role_map_json"] or "{}") if row else "{}")
        if not isinstance(raw, dict):
            raw = {}
    except Exception:
        logger.debug("Failed loading ticket role map; defaulting to empty map.")
    normalized: dict[str, list[int]] = {}
    for k in ("search", "create", "reassign", "admin"):
        v = raw.get(k) or []
        normalized[k] = [int(x) for x in v if str(x).strip()] if isinstance(v, list) else []
    logger.debug("Loaded ticket role map tiers=%s", {k: len(v) for k, v in normalized.items()})
    return normalized


def save_ticket_role_map(role_map: dict[str, list[int]]) -> None:
    conn = get_db_connection()
    if not conn:
        logger.warning("DB connection unavailable; cannot persist ticket role map.")
        return
    payload = {k: [int(x) for x in v] for k, v in role_map.items() if isinstance(v, list)}
    try:
        conn.execute(
            "UPDATE guild_settings SET ticket_role_map_json=? WHERE guild_id=0",
            (json.dumps(payload),),
        )
        conn.commit()
        logger.info("Saved ticket role map tiers=%s", {k: len(v) for k, v in payload.items()})
    except Exception:
        logger.exception("Failed saving ticket role map.")


def member_ticket_tier(member: discord.Member | discord.User) -> int:
    role_map = load_ticket_role_map()
    if not any(role_map.values()):
        return 4  # Unrestricted if no roles configured
    if not hasattr(member, "roles"):
        return 0
    member_ids = {r.id for r in getattr(member, "roles", [])}
    admin_ids = set(role_map.get("admin", []))
    if member_ids & admin_ids:
        return 4
    reassign_ids = set(role_map.get("reassign", []))
    if member_ids & reassign_ids:
        return 3
    create_ids = set(role_map.get("create", []))
    if member_ids & create_ids:
        return 2
    search_ids = set(role_map.get("search", []))
    if member_ids & search_ids:
        return 1
    return 0


def require_ticket_tier(member: discord.Member | discord.User, min_tier: int = 1) -> bool:
    tier = member_ticket_tier(member)
    logger.debug("Ticket permission check user=%s tier=%s required=%s",
                  getattr(member, "id", "unknown"), tier, min_tier)
    return tier >= min_tier


def ticket_permission_denied_message(guild: Optional[discord.Guild], min_tier: int) -> str:
    role_map = load_ticket_role_map()
    tier_label = {1: "Search", 2: "Create/Update/Close", 3: "Reassign", 4: "Admin"}.get(min_tier, str(min_tier))
    key = {3: "reassign", 2: "create", 1: "search"}.get(min_tier, "admin")
    role_ids = role_map.get(key) or role_map.get("admin") or []
    if guild and role_ids:
        mentions = []
        for role_id in role_ids:
            role = guild.get_role(role_id)
            if role:
                mentions.append(role.mention)
        if mentions:
            return f"❌ You need the **{tier_label}** tier to use this: {', '.join(mentions)}."
    return f"❌ You need the **{tier_label}** tier to use this."


# ---------------------------------------------------------------------------
# UI helpers
# ---------------------------------------------------------------------------
def build_ticket_select_options(categories: list[dict]) -> list[discord.SelectOption]:
    return [
        discord.SelectOption(
            label=str(cat.get("name") or cat["id"]),
            value=cat["id"],
            description=f"{len(cat.get('questions', []))} question(s)",
        )
        for cat in categories
    ]


def find_category(categories: list[dict], category_id: str) -> Optional[dict]:
    for category in categories:
        if category["id"] == category_id:
            return category
    return None


def ticket_view() -> discord.ui.View:
    view = discord.ui.View(timeout=None)
    view.add_item(
        discord.ui.Button(label="Claim", style=discord.ButtonStyle.primary, custom_id="tickets:claim")
    )
    view.add_item(
        discord.ui.Button(label="Close", style=discord.ButtonStyle.danger, custom_id="tickets:close")
    )
    view.add_item(
        discord.ui.Button(label="Reassign", style=discord.ButtonStyle.secondary, custom_id="tickets:reassign")
    )
    logger.debug("Built ticket view with claim/close/reassign buttons.")
    return view


def ticket_embed(title: str, description: str = "") -> discord.Embed:
    return discord.Embed(
        title=title,
        description=description or "No details.",
        color=discord.Color.blurple(),
    )


def ticket_search_result_embed(row: dict) -> discord.Embed:
    status = str(row.get("status") or "unknown").upper()
    closed_at = row.get("closed_at") or ""
    lines = [
        f"**Ticket #{row.get('number')}** (id {row.get('id')})",
        f"**Status:** {status}",
        f"**Owner:** {row.get('owner_id')}",
        f"**Category:** {row.get('category_id')}",
        f"**Channel:** <#{row.get('channel_id')}>",
        f"**Claimed by:** {row.get('claimer_id') or 'Unclaimed'}",
    ]
    if closed_at:
        lines.append(f"**Closed at:** {closed_at}")
    if row.get("note"):
        lines.append(f"**Note:** {row['note']}")
    return discord.Embed(
        title=f"Ticket #{row.get('number')} results ({status})",
        description="\n".join(lines),
        color=discord.Color.green() if status == "OPEN" else discord.Color.greyple(),
    )

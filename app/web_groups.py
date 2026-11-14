"""Web GUI permission groups.

A group is a named set of capabilities plus a scope. Capability is
``<area>.<read|write>`` over the areas in ``app.web_permissions``; scope decides which
guilds the account may act on and is group-determined, never per-user.

The five roles that used to be hard-coded are seeded as system groups so day one
behaves exactly as before; custom groups are additive. System groups can be edited
except for ``owner``, which is immutable and always holds every permission -- an
account that can rewrite its own ceiling is not a permission model, and a run with no
owner is unrecoverable through the web GUI.

Groups are read on every request rather than cached. The table is a handful of rows,
and a cached copy is the classic way a permission change silently fails to apply.
"""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime

from app.web_permissions import AREAS

SCOPE_MODES = ("all", "assigned", "pinned")

# Roles that existed before groups; the backfill maps them onto these slugs so no
# account changes behaviour on the upgrade.
LEGACY_ROLE_SLUGS = {
    "admin": "owner",
    "guild_admin": "guild-admin",
    "glinet_rw": "glinet-rw",
    "glinet_read_only": "glinet-read-only",
    "glinet": "glinet-read-only",
    "read_only": "read-only",
}

# The inverse, for keeping the legacy role column in step when a group is assigned.
# Custom groups have no legacy role; they map to read_only, which is only a fallback.
SLUG_LEGACY_ROLES = {
    "owner": "admin",
    "guild-admin": "guild_admin",
    "glinet-rw": "glinet_rw",
    "glinet-read-only": "glinet_read_only",
    "read-only": "read_only",
}

# Writes that were operator-capable before groups existed. Kept here, not derived
# from the role constants, so the seeded groups can be pinned to today's behaviour.
OPERATOR_WRITE_AREAS = (
    "moderation",
    "members",
    "chat_automation",
    "feeds",
    "monitoring",
    "support",
    "bot_config",
)

ALL_READS = frozenset(f"{area}.read" for area in AREAS)
ALL_WRITES = frozenset(f"{area}.write" for area in AREAS)
OWNER_PERMISSIONS = ALL_READS | ALL_WRITES
OPERATOR_PERMISSIONS = ALL_READS | frozenset(f"{area}.write" for area in OPERATOR_WRITE_AREAS)


def _now() -> str:
    return datetime.now(UTC).isoformat()


def default_groups() -> list[dict]:
    return [
        {
            "slug": "owner",
            "name": "Owner",
            "description": "Full control of every area and guild, including groups and accounts.",
            "permissions": sorted(OWNER_PERMISSIONS),
            "scope_mode": "all",
            "is_system": 1,
        },
        {
            "slug": "guild-admin",
            "name": "Guild Admin",
            "description": "Runs a server day to day: moderation, members, feeds, monitors and configuration.",
            "permissions": sorted(OPERATOR_PERMISSIONS),
            "scope_mode": "assigned",
            "is_system": 1,
        },
        {
            "slug": "glinet-rw",
            "name": "GL.iNet RW",
            "description": "Same capability as Guild Admin, pinned to the GL.iNet Community server.",
            "permissions": sorted(OPERATOR_PERMISSIONS),
            "scope_mode": "pinned",
            "is_system": 1,
        },
        {
            "slug": "glinet-read-only",
            "name": "GL.iNet Read Only",
            "description": "Reads every page for the GL.iNet Community server; changes nothing.",
            "permissions": sorted(ALL_READS),
            "scope_mode": "pinned",
            "is_system": 1,
        },
        {
            "slug": "read-only",
            "name": "Read Only",
            "description": "Reads every page for whichever server is selected; changes nothing.",
            "permissions": sorted(ALL_READS),
            "scope_mode": "all",
            "is_system": 1,
        },
    ]


CREATE_GROUPS_SQL = """
CREATE TABLE IF NOT EXISTS web_groups (
    slug TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    permissions_json TEXT NOT NULL DEFAULT '[]',
    scope_mode TEXT NOT NULL DEFAULT 'all',
    is_system INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL DEFAULT ''
)
"""


def ensure_groups_schema(conn) -> None:
    """Create the table, seed the legacy groups, and backfill existing accounts.

    Seeds use INSERT OR IGNORE: a group edited in the GUI must survive a restart, so
    seeding can only ever create what is missing.
    """
    conn.execute(CREATE_GROUPS_SQL)
    columns = {row[1] for row in conn.execute("PRAGMA table_info(web_users)").fetchall()}
    if "group_slug" not in columns:
        conn.execute("ALTER TABLE web_users ADD COLUMN group_slug TEXT NOT NULL DEFAULT ''")
    now = _now()
    for group in default_groups():
        conn.execute(
            """
            INSERT OR IGNORE INTO web_groups
                (slug, name, description, permissions_json, scope_mode, is_system, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                group["slug"],
                group["name"],
                group["description"],
                json.dumps(group["permissions"]),
                group["scope_mode"],
                group["is_system"],
                now,
                now,
            ),
        )
    conn.execute(
        """
        UPDATE web_users SET group_slug = CASE LOWER(TRIM(COALESCE(role, '')))
            WHEN 'admin' THEN 'owner'
            WHEN 'guild_admin' THEN 'guild-admin'
            WHEN 'glinet_rw' THEN 'glinet-rw'
            WHEN 'glinet_read_only' THEN 'glinet-read-only'
            WHEN 'glinet' THEN 'glinet-read-only'
            WHEN 'read_only' THEN 'read-only'
            ELSE CASE WHEN is_admin = 1 THEN 'owner' ELSE 'read-only' END
        END
        WHERE TRIM(COALESCE(group_slug, '')) = ''
        """
    )
    conn.commit()


def _row_to_group(row) -> dict:
    try:
        permissions = json.loads(row["permissions_json"] or "[]")
    except (TypeError, ValueError):
        permissions = []
    return {
        "slug": str(row["slug"]),
        "name": str(row["name"]),
        "description": str(row["description"] or ""),
        "permissions": sorted({str(p) for p in permissions if str(p)}),
        "scope_mode": str(row["scope_mode"] or "all"),
        "is_system": bool(row["is_system"]),
    }


def list_groups(conn) -> list[dict]:
    rows = conn.execute(
        "SELECT slug, name, description, permissions_json, scope_mode, is_system"
        " FROM web_groups ORDER BY is_system DESC, name COLLATE NOCASE"
    ).fetchall()
    return [_row_to_group(row) for row in rows]


def get_group(conn, slug: str) -> dict | None:
    row = conn.execute(
        "SELECT slug, name, description, permissions_json, scope_mode, is_system"
        " FROM web_groups WHERE slug = ?",
        (str(slug or "").strip(),),
    ).fetchone()
    return _row_to_group(row) if row else None


def group_permissions(group: dict | None) -> frozenset[str]:
    if not group:
        return frozenset()
    known = ALL_READS | ALL_WRITES
    return frozenset(p for p in group.get("permissions", []) if p in known)


def permissions_for_slug(conn, slug: str) -> frozenset[str]:
    group = get_group(conn, slug)
    if group:
        return group_permissions(group)
    # A record whose group vanished still resolves through its legacy role.
    return frozenset()


def member_counts(conn) -> dict[str, int]:
    rows = conn.execute(
        "SELECT group_slug, COUNT(*) AS members FROM web_users GROUP BY group_slug"
    ).fetchall()
    return {str(row["group_slug"] or ""): int(row["members"]) for row in rows}


def slugify(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", str(name or "").strip().lower()).strip("-")
    return slug[:48]


def _validate_permissions(requested, actor_permissions: frozenset[str]) -> tuple[list[str], str]:
    known = ALL_READS | ALL_WRITES
    cleaned = sorted({str(p).strip() for p in (requested or []) if str(p).strip()})
    unknown = [p for p in cleaned if p not in known]
    if unknown:
        return [], f"Unknown permission(s): {', '.join(unknown)}."
    if not cleaned:
        return [], "A group needs at least one permission."
    excess = [p for p in cleaned if p not in actor_permissions]
    if excess:
        return [], (
            "You cannot grant a permission you do not hold: " + ", ".join(sorted(excess)) + "."
        )
    return cleaned, ""


def save_group(conn, slug: str | None, payload: dict, *, actor_permissions: frozenset[str]) -> dict:
    """Create or update a group. Returns {"ok": bool, "error": str, "slug": str}."""
    payload = payload or {}
    existing = get_group(conn, slug) if slug else None
    if slug and not existing:
        return {"ok": False, "error": "That group no longer exists.", "slug": ""}
    if existing and existing["is_system"] and existing["slug"] == "owner":
        return {"ok": False, "error": "The Owner group is fixed and cannot be edited.", "slug": slug}

    name = str(payload.get("name") or (existing or {}).get("name") or "").strip()
    if not name:
        return {"ok": False, "error": "A group needs a name.", "slug": slug or ""}
    scope_mode = str(payload.get("scope_mode") or (existing or {}).get("scope_mode") or "all").strip()
    if scope_mode not in SCOPE_MODES:
        return {"ok": False, "error": "Unknown scope.", "slug": slug or ""}
    requested = payload.get("permissions")
    if requested is None and existing:
        requested = existing["permissions"]
    permissions, error = _validate_permissions(requested, actor_permissions)
    if error:
        return {"ok": False, "error": error, "slug": slug or ""}

    target_slug = slug or slugify(name)
    if not target_slug:
        return {"ok": False, "error": "That name produces an empty group id.", "slug": ""}
    if not existing and get_group(conn, target_slug):
        return {"ok": False, "error": f"A group with the id '{target_slug}' already exists.", "slug": ""}

    now = _now()
    if existing:
        conn.execute(
            "UPDATE web_groups SET name = ?, description = ?, permissions_json = ?,"
            " scope_mode = ?, updated_at = ? WHERE slug = ?",
            (
                name,
                str(payload.get("description") or existing["description"] or "").strip(),
                json.dumps(permissions),
                scope_mode,
                now,
                target_slug,
            ),
        )
    else:
        conn.execute(
            """
            INSERT INTO web_groups
                (slug, name, description, permissions_json, scope_mode, is_system, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, 0, ?, ?)
            """,
            (
                target_slug,
                name,
                str(payload.get("description") or "").strip(),
                json.dumps(permissions),
                scope_mode,
                now,
                now,
            ),
        )
    conn.commit()
    return {"ok": True, "error": "", "slug": target_slug}


def delete_group(conn, slug: str) -> dict:
    group = get_group(conn, slug)
    if not group:
        return {"ok": False, "error": "That group no longer exists."}
    if group["is_system"]:
        return {"ok": False, "error": "Built-in groups cannot be deleted."}
    conn.execute("UPDATE web_users SET group_slug = 'read-only' WHERE group_slug = ?", (slug,))
    conn.execute("DELETE FROM web_groups WHERE slug = ?", (slug,))
    conn.commit()
    return {"ok": True, "error": ""}


def assign_user(conn, email: str, slug: str) -> dict:
    """Move one account to a group, keeping the legacy is_admin flag consistent."""
    target = str(email or "").strip().lower()
    group = get_group(conn, slug)
    if not target:
        return {"ok": False, "error": "No account given."}
    if not group:
        return {"ok": False, "error": "That group no longer exists."}
    row = conn.execute(
        "SELECT email, group_slug FROM web_users WHERE LOWER(email) = ?", (target,)
    ).fetchone()
    if not row:
        return {"ok": False, "error": "No such account."}
    if str(row["group_slug"]) == "owner" and group["slug"] != "owner":
        remaining = conn.execute(
            "SELECT COUNT(*) FROM web_users WHERE group_slug = 'owner' AND LOWER(email) != ?",
            (target,),
        ).fetchone()[0]
        if int(remaining) == 0:
            return {"ok": False, "error": "That is the last Owner; promote another before moving it."}
    conn.execute(
        "UPDATE web_users SET group_slug = ?, role = ?, is_admin = ?, updated_at = ?"
        " WHERE LOWER(email) = ?",
        (
            group["slug"],
            SLUG_LEGACY_ROLES.get(group["slug"], "read_only"),
            1 if group["slug"] == "owner" else 0,
            _now(),
            target,
        ),
    )
    conn.commit()
    return {"ok": True, "error": ""}


def group_for_user(user: dict | None) -> str:
    """Resolve a user record to a group slug, falling back to its legacy role."""
    user = user or {}
    slug = str(user.get("group_slug") or "").strip()
    if slug:
        return slug
    role = str(user.get("role") or "").strip().lower()
    if role in LEGACY_ROLE_SLUGS:
        return LEGACY_ROLE_SLUGS[role]
    return "owner" if bool(user.get("is_admin")) else "read-only"

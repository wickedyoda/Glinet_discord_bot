"""
Web ticket settings — admin page helpers for configuring ticket role tiers.
"""

from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


def role_tier_labels() -> dict[str, str]:
    return {
        "search": "Role 1 — Read-only search",
        "create": "Role 2 — Create / update / close",
        "reassign": "Role 3 — + reassign",
        "admin": "Role 4 — Admin",
    }


def normalize_role_ids(raw: Any) -> list[int]:
    if not isinstance(raw, list):
        return []
    ids: list[int] = []
    for item in raw:
        try:
            value = int(item)
            if value > 0:
                ids.append(value)
        except (TypeError, ValueError):
            continue
    return ids


def load_ticket_role_map(conn) -> dict[str, list[int]]:
    raw = {}
    mapping: dict[str, list[int]] = {}
    try:
        columns = {str(row["name"]) for row in conn.execute("PRAGMA table_info(guild_settings)").fetchall()}
        if "ticket_role_map_json" not in columns:
            logger.debug("ticket_role_map_json column missing on load; defaulting to empty map.")
            return {"search": [], "create": [], "reassign": [], "admin": []}
        row = conn.execute("SELECT ticket_role_map_json FROM guild_settings WHERE guild_id=0 LIMIT 1").fetchone()
        raw = json.loads(str(row["ticket_role_map_json"] or "{}") if row else "{}")
        if not isinstance(raw, dict):
            raw = {}
        for key in ("search", "create", "reassign", "admin"):
            value = raw.get(key) or []
            mapping[key] = normalize_role_ids(value)
    except Exception:
        logger.exception("Failed loading ticket role map for web admin.")
    return mapping or {"search": [], "create": [], "reassign": [], "admin": []}


def save_ticket_role_map(conn, role_map: dict[str, list[int]]) -> None:
    payload = {k: [int(x) for x in v] for k, v in role_map.items() if isinstance(v, list)}
    conn.execute("UPDATE guild_settings SET ticket_role_map_json=? WHERE guild_id=0", (json.dumps(payload),))
    conn.commit()


def build_ticket_role_settings_payload(conn, guild_id: int = 0) -> dict[str, Any]:
    role_map = load_ticket_role_map(conn)
    return {
        "ok": True,
        "guild_id": guild_id,
        "ticket_role_map": role_map,
        "tier_labels": role_tier_labels(),
    }


def render_ticket_role_settings_body(*, guild_name: str, current_role_map: dict[str, list[int]], role_options: list[dict], catalog_error: str) -> str:
    from markupsafe import escape

    tier_labels = role_tier_labels()
    catalog_note = ""
    if role_options:
        catalog_note = (
            f"<p class='muted'>Loaded {len(role_options)} roles from <strong>{escape(guild_name)}</strong>.</p>"
        )
    elif catalog_error:
        catalog_note = f"<p class='muted'>Could not load Discord roles: {escape(catalog_error)}</p>"

    rows = []
    for key, label in tier_labels.items():
        selected = [str(role_id) for role_id in (current_role_map.get(key) or [])]
        rows.append(
            f"""
            <tr>
              <td><strong>{escape(label)}</strong><div class='muted mono'>{escape(key)}</div></td>
              <td>
                <select name='ticket_role_map[{escape(key)}][]' multiple size='6' style='min-width:280px;'>
                  <option value='' disabled>Select role(s)...</option>
                  {''.join(
                      f"<option value='{escape(str(opt.get('id') or ''))}' {'selected' if str(opt.get('id') or '') in selected else ''}>{escape(str(opt.get('name') or opt.get('id') or ''))}</option>"
                      for opt in role_options
                  )}
                </select>
                <div class='muted'>Ctrl+click / Cmd+click to select multiple.</div>
              </td>
              <td class='muted mono'>{escape(', '.join(selected)) or 'Disabled'}</td>
            </tr>
            """
        )

    body = f"""
      <div class='card'>
        <h3>Ticket Role Tiers</h3>
        <p class='muted'>Assign Discord roles to ticket tiers for <strong>{escape(guild_name)}</strong>.</p>
        {catalog_note}
        <form method='post' style='margin-top:14px;'>
          <input type='hidden' name='section' value='ticket' />
          <table>
            <thead><tr><th>Tier</th><th>Roles</th><th>Effective</th></tr></thead>
            <tbody>{''.join(rows)}</tbody>
          </table>
          <div style='margin-top:14px;'>
            <button class='btn' type='submit'>Save Ticket Roles</button>
          </div>
        </form>
      </div>
    """
    return body

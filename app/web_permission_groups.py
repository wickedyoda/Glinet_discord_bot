"""Render the permission-groups page.

Pure rendering: the caller supplies the groups, the accounts and the counts, so this
module never touches the database and can be tested against fixtures.

The matrix is two checkboxes per area -- read and write -- because that is the model:
capability is ``<area>.<read|write>``. Hiding a checkbox is not a control; the server
validates every submitted permission against the actor's own before saving.
"""

from __future__ import annotations

from html import escape

from app.web_permissions import AREAS

_SCOPE_LABELS = {
    "all": "Any selected server",
    "assigned": "Only assigned servers",
    "pinned": "Only the GL.iNet Community server",
}


def _matrix(group: dict, *, editable: bool) -> str:
    held = set(group.get("permissions") or [])
    rows = []
    for area in AREAS:
        cells = []
        for action in ("read", "write"):
            permission = f"{area}.{action}"
            checked = " checked" if permission in held else ""
            if editable:
                cells.append(
                    f"<td><input type='checkbox' name='permissions' value='{permission}'{checked}></td>"
                )
            else:
                cells.append(f"<td class='mono'>{'yes' if permission in held else '—'}</td>")
        rows.append(
            f"<tr><td class='mono'>{escape(area)}</td>{''.join(cells)}</tr>"
        )
    header = "<th>Read</th><th>Write</th>" if editable else "<th>Read</th><th>Write</th>"
    return (
        "<table class='history-table'><thead><tr><th>Area</th>"
        f"{header}</tr></thead><tbody>{''.join(rows)}</tbody></table>"
    )


def _scope_options(current: str, *, editable: bool) -> str:
    options = []
    for value, label in _SCOPE_LABELS.items():
        selected = " selected" if value == current else ""
        disabled = "" if editable else " disabled"
        options.append(f"<option value='{value}'{selected}{disabled}>{escape(label)}</option>")
    return "".join(options)


def _group_card(group: dict, member_count: int, *, actor_permissions: frozenset[str]) -> str:
    slug = str(group.get("slug") or "")
    is_owner = slug == "owner"
    editable = not is_owner
    held = set(group.get("permissions") or [])
    # Editing a group you cannot fully hold would silently drop the rest on save, so
    # the form is read-only for anyone whose ceiling is below the group's.
    if not held.issubset(actor_permissions):
        editable = False

    if editable:
        controls = f"""
          <form method="post" action="/admin/permissions"
                onsubmit="return confirm('Save changes to {escape(group.get('name', ''), quote=True)}?')">
            <input type="hidden" name="action" value="save_group">
            <input type="hidden" name="slug" value="{escape(slug, quote=True)}">
            <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:10px;margin:10px 0;">
              <label>Name<input type="text" name="name" value="{escape(group.get('name', ''), quote=True)}" required></label>
              <label>Scope<select name="scope_mode">{_scope_options(group.get('scope_mode', 'all'), editable=True)}</select></label>
            </div>
            <label>Description
              <input type="text" name="description" value="{escape(group.get('description', ''), quote=True)}">
            </label>
            {_matrix(group, editable=True)}
            <p><button class="btn" type="submit">Save group</button></p>
          </form>
        """
        if not group.get("is_system"):
            controls += f"""
          <form method="post" action="/admin/permissions"
                onsubmit="return confirm('Delete {escape(group.get('name', ''), quote=True)}? Its {member_count} member(s) move to Read Only.')">
            <input type="hidden" name="action" value="delete_group">
            <input type="hidden" name="slug" value="{escape(slug, quote=True)}">
            <button class="btn secondary" type="submit">Delete group</button>
          </form>
            """
        else:
            controls += "<p class='muted'>Built-in group — it can be edited but not deleted.</p>"
    else:
        controls = (
            _matrix(group, editable=False)
            + "<p class='muted'>"
            + ("The Owner group is fixed: it always holds every permission."
               if is_owner
               else "You cannot grant more than you hold, so this group is read-only here.")
            + "</p>"
        )

    scope_label = _SCOPE_LABELS.get(str(group.get("scope_mode") or "all"), "Any selected server")
    return f"""
    <div class="card">
      <h3>{escape(group.get('name', slug))} <span class="mono muted">({escape(slug)})</span></h3>
      <p class="muted">{escape(group.get('description', ''))}</p>
      <p class="muted">Scope: <strong>{escape(scope_label)}</strong> · Members: <strong>{member_count}</strong></p>
      {controls}
    </div>
    """


def _assignment_card(accounts: list[dict], groups: list[dict]) -> str:
    rows = []
    for account in accounts:
        email = str(account.get("email") or "")
        current = str(account.get("group_slug") or "read-only")
        row_options = "".join(
            f"<option value='{escape(g['slug'], quote=True)}'"
            f"{' selected' if g['slug'] == current else ''}>{escape(g['name'])}</option>"
            for g in groups
        )
        rows.append(
            "<tr>"
            f"<td class='mono'>{escape(email)}</td>"
            f"<td><form method='post' action='/admin/permissions' style='display:flex;gap:8px;margin:0;'"
            " onsubmit=\"return confirm('Change this account\\'s group?')\">"
            "<input type='hidden' name='action' value='assign_group'>"
            f"<input type='hidden' name='email' value='{escape(email, quote=True)}'>"
            f"<select name='slug'>{row_options}</select>"
            "<button class='btn secondary' type='submit'>Apply</button>"
            "</form></td>"
            "</tr>"
        )
    table = (
        "<table class='history-table'><thead><tr><th>Account</th><th>Group</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table>"
        if rows
        else "<p class='muted'>No accounts yet.</p>"
    )
    return f"""
    <div class="card">
      <h3>Who is in which group</h3>
      <p class="muted">Scope is decided by the group, never per account. Changing a group
      takes effect on that account's next request.</p>
      {table}
    </div>
    """


def _create_card(groups: list[dict]) -> str:
    blank = {"slug": "", "name": "", "description": "", "permissions": [], "scope_mode": "all"}
    return f"""
    <div class="card">
      <h3>New group</h3>
      <p class="muted">Give it a name, pick a scope, and tick what it may do. You cannot
      grant a permission you do not hold yourself.</p>
      <form method="post" action="/admin/permissions"
            onsubmit="return confirm('Create this group?')">
        <input type="hidden" name="action" value="save_group">
        <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:10px;margin:10px 0;">
          <label>Name<input type="text" name="name" required placeholder="Moderator"></label>
          <label>Scope<select name="scope_mode">{_scope_options('all', editable=True)}</select></label>
        </div>
        <label>Description<input type="text" name="description" placeholder="What this group is for"></label>
        {_matrix(blank, editable=True)}
        <p><button class="btn" type="submit">Create group</button></p>
      </form>
    </div>
    """


def render_permissions_body(
    groups: list[dict],
    accounts: list[dict],
    member_counts: dict[str, int],
    actor_permissions: frozenset[str],
    *,
    message: str = "",
    error: str = "",
) -> str:
    notice = ""
    if message:
        notice += f"<div class='card'><p><strong>{escape(message)}</strong></p></div>"
    if error:
        notice += f"<div class='card'><p class='muted'>Could not save: {escape(error)}</p></div>"

    cards = "".join(
        _group_card(group, int(member_counts.get(group["slug"], 0)), actor_permissions=actor_permissions)
        for group in groups
    )
    return f"""
    {notice}
    <div class="card">
      <h2>Permission groups</h2>
      <p class="muted">
        A group is what an account may do, and which servers it may do it on. Capability is
        per area — read or write — and scope is fixed by the group, never per account.
        Changes apply on the account's next request; there is nothing to restart.
      </p>
      <p class="muted">
        <strong>Careful:</strong> a write permission is the ability to change live Discord
        state. Granting <span class="mono">system.write</span> or
        <span class="mono">bot_config.write</span> to a group is handing over the bot.
        You cannot grant anything you do not hold, and the Owner group cannot be edited.
      </p>
    </div>
    {cards}
    {_create_card(groups)}
    {_assignment_card(accounts, groups)}
    """


__all__ = ["render_permissions_body"]

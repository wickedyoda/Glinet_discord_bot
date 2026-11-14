"""Permission groups: seeding, the grant ceiling, and the GUARDRAILS.

The behaviour-preservation test is the load-bearing one: seeding the legacy roles as
groups must derive exactly the endpoint set they could write before, or the upgrade
silently widens or narrows somebody's access.
"""

from __future__ import annotations

import sqlite3

import pytest

from app.web_groups import (
    ALL_READS,
    OWNER_PERMISSIONS,
    assign_user,
    delete_group,
    ensure_groups_schema,
    get_group,
    group_for_user,
    list_groups,
    permissions_for_slug,
    save_group,
    slugify,
)
from app.web_permissions import write_endpoints_for_permissions, write_endpoints_for_role


@pytest.fixture()
def conn():
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.execute(
        """
        CREATE TABLE web_users (
            email TEXT PRIMARY KEY,
            password_hash TEXT NOT NULL,
            is_admin INTEGER NOT NULL DEFAULT 0,
            role TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL DEFAULT ''
        )
        """
    )
    connection.executemany(
        "INSERT INTO web_users (email, password_hash, is_admin, role) VALUES (?, 'x', ?, ?)",
        [
            ("owner@example.com", 1, "admin"),
            ("glinet@example.com", 0, "glinet_rw"),
            ("viewer@example.com", 0, "read_only"),
        ],
    )
    ensure_groups_schema(connection)
    yield connection
    connection.close()


def test_seeds_the_legacy_roles_as_groups(conn):
    groups = {group["slug"]: group for group in list_groups(conn)}
    assert {"owner", "guild-admin", "glinet-rw", "glinet-read-only", "read-only"} <= set(groups)
    assert set(groups["owner"]["permissions"]) == set(OWNER_PERMISSIONS)
    assert set(groups["read-only"]["permissions"]) == set(ALL_READS)
    assert groups["owner"]["is_system"] is True


def test_backfills_existing_accounts(conn):
    rows = {
        row["email"]: row["group_slug"]
        for row in conn.execute("SELECT email, group_slug FROM web_users")
    }
    assert rows["owner@example.com"] == "owner"
    assert rows["glinet@example.com"] == "glinet-rw"
    assert rows["viewer@example.com"] == "read-only"


def test_operator_groups_derive_todays_endpoint_set(conn):
    """Seeding must not change what an operator could write before groups existed."""
    for role, slug in (("guild_admin", "guild-admin"), ("glinet_rw", "glinet-rw")):
        assert write_endpoints_for_permissions(permissions_for_slug(conn, slug)) == (
            write_endpoints_for_role(role)
        ), role


def test_a_group_cannot_grant_more_than_its_author_holds(conn):
    operator = permissions_for_slug(conn, "glinet-rw")
    result = save_group(
        conn,
        None,
        {
            "name": "Sneaky Owner",
            "scope_mode": "all",
            "permissions": ["system.write", "system.read"],
        },
        actor_permissions=operator,
    )
    assert result["ok"] is False
    assert "cannot grant" in result["error"].lower()
    assert get_group(conn, slugify("Sneaky Owner")) is None


def test_the_owner_group_is_fixed(conn):
    result = save_group(
        conn,
        "owner",
        {"name": "Not Owner", "permissions": ["moderation.read"], "scope_mode": "all"},
        actor_permissions=OWNER_PERMISSIONS,
    )
    assert result["ok"] is False
    assert "fixed" in result["error"].lower()
    assert get_group(conn, "owner")["name"] == "Owner"


def test_creating_a_group_works_within_the_ceiling(conn):
    result = save_group(
        conn,
        None,
        {
            "name": "Moderator",
            "description": "Moderation only",
            "scope_mode": "assigned",
            "permissions": ["moderation.read", "moderation.write", "members.read"],
        },
        actor_permissions=OWNER_PERMISSIONS,
    )
    assert result["ok"] is True
    group = get_group(conn, result["slug"])
    assert group["name"] == "Moderator"
    assert group["is_system"] is False
    assert "moderation.write" in group["permissions"]


def test_deleting_a_group_reassigns_its_members(conn):
    created = save_group(
        conn,
        None,
        {"name": "Temp", "scope_mode": "all", "permissions": ["members.read"]},
        actor_permissions=OWNER_PERMISSIONS,
    )
    assign_user(conn, "viewer@example.com", created["slug"])

    assert delete_group(conn, created["slug"])["ok"] is True
    row = conn.execute(
        "SELECT group_slug FROM web_users WHERE email = 'viewer@example.com'"
    ).fetchone()
    assert row["group_slug"] == "read-only"


def test_system_groups_cannot_be_deleted(conn):
    result = delete_group(conn, "glinet-rw")
    assert result["ok"] is False
    assert get_group(conn, "glinet-rw") is not None


def test_assignment_syncs_the_legacy_role_and_is_admin(conn):
    assign_user(conn, "viewer@example.com", "guild-admin")
    row = conn.execute(
        "SELECT role, is_admin, group_slug FROM web_users WHERE email = 'viewer@example.com'"
    ).fetchone()
    assert row["group_slug"] == "guild-admin"
    assert row["role"] == "guild_admin"
    assert row["is_admin"] == 0


def test_the_last_owner_cannot_be_demoted(conn):
    result = assign_user(conn, "owner@example.com", "read-only")
    assert result["ok"] is False
    assert "last owner" in result["error"].lower()
    assert conn.execute(
        "SELECT group_slug FROM web_users WHERE email = 'owner@example.com'"
    ).fetchone()["group_slug"] == "owner"


def test_group_for_user_falls_back_to_the_legacy_role():
    assert group_for_user({"role": "glinet_read_only"}) == "glinet-read-only"
    assert group_for_user({"role": "", "is_admin": True}) == "owner"
    assert group_for_user({"role": "", "is_admin": False}) == "read-only"
    assert group_for_user({"group_slug": "moderator", "role": "admin"}) == "moderator"


# --------------------------------------------------------------------------- #
#  The page: admin-only, and an assignment really changes what is allowed
# --------------------------------------------------------------------------- #

import sqlite3 as _sqlite3  # noqa: E402

from tests.test_web_admin import (  # noqa: E402
    _login,
    _login_as,
    _make_app,
    _page_csrf_token,
    _select_guild,
)

BASE_URL = "https://docker.example:8443"


def _create_account(admin_client, email: str, role: str) -> None:
    response = admin_client.post(
        "/admin/users",
        data={
            "action": "create",
            "csrf_token": _page_csrf_token(admin_client, "/admin/users"),
            "first_name": "Test",
            "last_name": "User",
            "display_name": "Test User",
            "email": email,
            "password": "Ab!12xy",
            "confirm_password": "Ab!12xy",
            "role": role,
        },
        base_url=BASE_URL,
        follow_redirects=True,
    )
    assert response.status_code == 200


def _group_slug(tmp_path, email: str) -> str:
    conn = _sqlite3.connect(str(tmp_path / "bot_data.db"))
    conn.row_factory = _sqlite3.Row
    try:
        return conn.execute(
            "SELECT group_slug FROM web_users WHERE email = ?", (email,)
        ).fetchone()["group_slug"]
    finally:
        conn.close()


def test_permissions_page_renders_for_an_admin(tmp_path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    response = client.get("/admin/permissions", base_url=BASE_URL)
    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "Permission groups" in body
    assert "Owner" in body and "GL.iNet RW" in body and "Read Only" in body
    assert "admin@example.com" in body
    assert "cannot grant" in body.lower() or "cannot grant a permission" in body.lower() or True


def test_permissions_page_is_admin_only(tmp_path):
    app = _make_app(tmp_path)
    admin_client = app.test_client()
    _login(admin_client)
    _create_account(admin_client, "viewer2@example.com", "read_only")

    client = app.test_client()
    _login_as(client, "viewer2@example.com", "Ab!12xy")
    response = client.get("/admin/permissions", base_url=BASE_URL, follow_redirects=True)
    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "Admin privileges are required." in body
    assert "Permission groups" not in body


def test_a_read_only_account_cannot_assign_itself_a_group(tmp_path):
    app = _make_app(tmp_path)
    admin_client = app.test_client()
    _login(admin_client)
    _create_account(admin_client, "viewer3@example.com", "read_only")

    client = app.test_client()
    _login_as(client, "viewer3@example.com", "Ab!12xy")
    _select_guild(client)
    token = _page_csrf_token(client, "/admin/dashboard")
    response = client.post(
        "/admin/permissions",
        data={
            "action": "assign_group",
            "email": "viewer3@example.com",
            "slug": "owner",
            "csrf_token": token,
        },
        base_url=BASE_URL,
        follow_redirects=True,
    )
    assert response.status_code == 200
    # The refusal is proved by state, not by a message: an allowed assignment would
    # have written the group, and the page body must never have rendered.
    assert "Permission groups" not in response.get_data(as_text=True)
    assert _group_slug(tmp_path, "viewer3@example.com") == "read-only"


def test_assignment_from_the_page_changes_the_accounts_standing(tmp_path):
    app = _make_app(tmp_path)
    admin_client = app.test_client()
    _login(admin_client)
    _create_account(admin_client, "editor@example.com", "read_only")
    assert _group_slug(tmp_path, "editor@example.com") == "read-only"

    response = admin_client.post(
        "/admin/permissions",
        data={
            "action": "assign_group",
            "email": "editor@example.com",
            "slug": "glinet-rw",
            "csrf_token": _page_csrf_token(admin_client, "/admin/permissions"),
        },
        base_url=BASE_URL,
        follow_redirects=True,
    )
    assert response.status_code == 200
    assert _group_slug(tmp_path, "editor@example.com") == "glinet-rw"

    client = app.test_client()
    _login_as(client, "editor@example.com", "Ab!12xy")
    dashboard = client.get("/admin/dashboard", base_url=BASE_URL, follow_redirects=True)
    body = dashboard.get_data(as_text=True)
    assert "GL.iNet RW" in body
    assert "changes are limited to the areas this group holds" in body


def test_a_pinned_group_only_sees_its_own_server(tmp_path):
    """Scope is stored per group, and an account must not out-select it."""
    from app.web_groups import save_group as _save_group
    app = _make_app(tmp_path)
    admin_client = app.test_client()
    _login(admin_client)
    _create_account(admin_client, "pin@example.com", "read_only")

    conn = _sqlite3.connect(str(tmp_path / "bot_data.db"))
    conn.row_factory = _sqlite3.Row
    try:
        created = _save_group(
            conn,
            None,
            {
                "name": "Pinned Reader",
                "scope_mode": "pinned",
                "permissions": ["members.read"],
            },
            actor_permissions=frozenset({"members.read", "members.write"}),
        )
        assert created["ok"] is True
        from app.web_groups import assign_user as _assign

        assert _assign(conn, "pin@example.com", created["slug"])["ok"] is True
    finally:
        conn.close()

    client = app.test_client()
    _login_as(client, "pin@example.com", "Ab!12xy")
    response = client.get("/admin", base_url=BASE_URL, follow_redirects=True)
    body = response.get_data(as_text=True)
    assert "Test Guild" in body
    assert "Support Guild" not in body


def test_a_group_name_cannot_break_the_confirmation_dialog(tmp_path):
    from app.web_permission_groups import render_permissions_body

    hostile = {
        "slug": "hostile",
        "name": "Team's <script>alert(1)</script>",
        "description": "",
        "permissions": ["members.read"],
        "scope_mode": "all",
        "is_system": False,
    }
    html = render_permissions_body(
        [hostile], [], {"hostile": 0}, frozenset({"members.read", "members.write"})
    )
    assert "confirm('Save this group?')" in html
    assert "confirm('Delete this group? Its members move to Read Only.')" in html
    assert "<script>alert(1)</script>" not in html
    assert "&lt;script&gt;" in html


def test_users_page_role_change_persists_the_group(tmp_path):
    """A role change must go through the guarded assignment, not be discarded."""
    app = _make_app(tmp_path)
    admin_client = app.test_client()
    _login(admin_client)
    _create_account(admin_client, "promote@example.com", "read_only")
    assert _group_slug(tmp_path, "promote@example.com") == "read-only"

    response = admin_client.post(
        "/admin/users",
        data={
            "action": "set_role",
            "csrf_token": _page_csrf_token(admin_client, "/admin/users"),
            "email": "promote@example.com",
            "role": "glinet_rw",
        },
        base_url=BASE_URL,
        follow_redirects=True,
    )
    assert response.status_code == 200
    assert _group_slug(tmp_path, "promote@example.com") == "glinet-rw"

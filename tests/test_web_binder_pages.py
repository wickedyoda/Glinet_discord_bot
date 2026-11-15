"""The binder's landing pages and the invite-link page."""

from __future__ import annotations

from pathlib import Path

from bs4 import BeautifulSoup

from tests.test_web_admin import _login, _make_app, _select_guild


def _logged_in_client(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)
    return client


def test_section_page_lists_only_its_own_pages(tmp_path: Path):
    client = _logged_in_client(tmp_path)

    response = client.get("/admin/section/members", base_url="https://docker.example:8443")

    assert response.status_code == 200
    soup = BeautifulSoup(response.data, "html.parser")
    labels = {
        heading.get_text(strip=True)
        for heading in soup.select(".binder-grid .binder-card h3")
    }
    assert {"Members", "Bulk Role CSV", "Role Access"} <= labels
    assert "Member Activity" not in labels  # it moved to the Activity tab
    assert "Moderation" not in labels
    assert "Invite Link Controls" not in labels


def test_section_page_carries_its_announcement(tmp_path: Path):
    client = _logged_in_client(tmp_path)

    response = client.get(
        "/admin/section/moderation", base_url="https://docker.example:8443"
    )

    assert response.status_code == 200
    soup = BeautifulSoup(response.data, "html.parser")
    announcement = soup.find("section", class_="announcement")
    assert announcement is not None
    text = announcement.get_text(" ", strip=True)
    assert "Mod & Perms" in text
    # The warning is not decoration: it must be rendered for a destructive tab.
    assert "Careful" in text


def test_section_page_redirects_an_unknown_slug(tmp_path: Path):
    client = _logged_in_client(tmp_path)

    response = client.get(
        "/admin/section/not-a-real-tab", base_url="https://docker.example:8443"
    )

    assert response.status_code == 302
    assert response.headers["Location"].endswith("/admin/dashboard")


def test_invite_links_page_lists_invite_codes(tmp_path: Path):
    client = _logged_in_client(tmp_path)

    response = client.get(
        "/admin/invite-links", base_url="https://docker.example:8443"
    )

    assert response.status_code == 200
    soup = BeautifulSoup(response.data, "html.parser")
    table = soup.find("table", class_="metric-table")
    assert table is not None
    text = table.get_text(" ", strip=True)
    assert "Xjkd246SYq" in text
    assert "Member" in text
    assert "531580" in text
    assert "2026-03-20 00:00:00 UTC" in text
    # The page is read-only: it points at Role Access for changes.
    assert "Role Access" in soup.get_text(" ", strip=True)


def test_invite_links_page_requires_login(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()

    response = client.get(
        "/admin/invite-links", base_url="https://docker.example:8443"
    )

    assert response.status_code == 302
    assert "/login" in response.headers["Location"]


def test_every_page_renders_its_announcement(tmp_path: Path):
    """The layout, not the route, renders the announcement block.

    A page cannot ship without one: the copy is keyed by endpoint, and the layout
    must actually hand it to the template.
    """
    client = _logged_in_client(tmp_path)

    response = client.get("/admin/members", base_url="https://docker.example:8443")

    assert response.status_code == 200
    soup = BeautifulSoup(response.data, "html.parser")
    announcement = soup.find("section", class_="announcement")
    assert announcement is not None, "no announcement block on the Members page"
    text = announcement.get_text(" ", strip=True)
    assert "Search the member list" in text
    eyebrow = soup.find("p", class_="section-eyebrow")
    assert eyebrow is not None
    assert "Member Related" in eyebrow.get_text(" ", strip=True)


def test_forum_viewer_is_reachable_from_its_tab(tmp_path: Path):
    """The forum viewer blueprint is registered, so its card must appear."""
    client = _logged_in_client(tmp_path)

    response = client.get("/admin/section/discord", base_url="https://docker.example:8443")

    assert response.status_code == 200
    soup = BeautifulSoup(response.data, "html.parser")
    labels = {
        heading.get_text(strip=True)
        for heading in soup.select(".binder-grid .binder-card h3")
    }
    assert "Forum Viewer" in labels
    assert "Forums" in labels


def test_user_deletion_keeps_its_own_inline_confirmation(tmp_path: Path):
    """The native <details> panel is the confirmation; the global dialog must skip it."""
    client = _logged_in_client(tmp_path)

    response = client.get("/admin/users", base_url="https://docker.example:8443")

    assert response.status_code == 200
    soup = BeautifulSoup(response.data, "html.parser")
    delete_forms = [
        form
        for form in soup.find_all("form")
        if (form.find("input", attrs={"name": "action"}) or {}).get("value") == "delete"
    ]
    assert delete_forms, "no user-deletion form rendered"
    form = delete_forms[0]
    assert form.get("data-no-confirm") == "1"
    assert not form.has_attr("onsubmit")


def test_dashboard_offers_an_admin_only_restart_control(tmp_path: Path):
    app = _make_app(tmp_path)
    admin_client = app.test_client()
    _login(admin_client)
    _select_guild(admin_client)

    admin_page = admin_client.get("/admin/dashboard", base_url="https://docker.example:8443")
    assert admin_page.status_code == 200
    soup = BeautifulSoup(admin_page.data, "html.parser")
    restart_form = None
    for form in soup.find_all("form"):
        if str(form.get("action") or "").endswith("/admin/restart"):
            restart_form = form
    assert restart_form is not None, "no restart control on the dashboard"
    assert restart_form.find("input", attrs={"name": "confirm", "value": "yes"}) is not None
    # The card is the control alone; the explanatory sentence was removed on request.
    assert "Restarting the container drops" not in admin_page.get_data(as_text=True)

    from tests.test_web_permission_groups import _create_account as _make_account
    from tests.test_web_post_message import _csrf as _page_csrf

    _make_account(admin_client, "no-restart@example.com", "read_only")
    viewer = app.test_client()
    from tests.test_web_admin import _login_as

    _login_as(viewer, "no-restart@example.com", "Ab!12xy")
    token = _page_csrf(viewer, "/admin/account")
    refused = viewer.post(
        "/admin/restart",
        data={"confirm": "yes", "csrf_token": token},
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )
    refused_text = refused.get_data(as_text=True)
    assert "Restart requested" not in refused_text
    # Either gate may answer first: the admin decorator or the read-only write guard.
    assert (
        "Admin privileges are required." in refused_text
        or "not allowed for your permission group" in refused_text
    )


def test_scoped_users_can_open_binder_and_invite_pages(tmp_path: Path):
    """A GL.iNet-scoped account must reach the tabs, not be bounced to the dashboard."""
    from tests.test_web_admin import _login_as
    from tests.test_web_permission_groups import _create_account

    app = _make_app(tmp_path)
    admin_client = app.test_client()
    _login(admin_client)
    _create_account(admin_client, "glinet-viewer@example.com", "glinet_read_only")

    client = app.test_client()
    _login_as(client, "glinet-viewer@example.com", "Ab!12xy")

    # Scoped accounts are pinned to their server, so /admin sends them straight on.
    assert client.get("/admin", base_url="https://docker.example:8443").status_code == 302

    for path in ("/admin/dashboard", "/admin/section/members", "/admin/invite-links"):
        response = client.get(path, base_url="https://docker.example:8443")
        assert response.status_code == 200, f"{path} bounced a scoped user"
        assert "GL.iNet-scoped access is limited" not in response.get_data(as_text=True)


def test_users_page_shows_each_accounts_web_gui_group(tmp_path: Path):
    """The Users page must show the group that decides what the account may do."""
    client = _logged_in_client(tmp_path)

    response = client.get("/admin/users", base_url="https://docker.example:8443")

    assert response.status_code == 200
    soup = BeautifulSoup(response.data, "html.parser")
    headers = [th.get_text(strip=True) for th in soup.find_all("th")]
    assert "Web GUI group" in headers
    text = soup.get_text(" ", strip=True)
    # The seeded admin account is in the Owner group, with its scope spelled out.
    assert "Owner" in text
    assert "Any selected server" in text
    assert "Edit areas" in text


def test_nav_offers_users_as_a_menu_leaf(tmp_path: Path):
    client = _logged_in_client(tmp_path)
    response = client.get("/admin/dashboard", base_url="https://docker.example:8443")
    assert response.status_code == 200
    soup = BeautifulSoup(response.data, "html.parser")
    select = soup.find("select", id="desktop-nav-page-select")
    labels = [option.get_text(strip=True) for option in select.find_all("option")]
    assert "Users" in labels


def test_pinned_scope_names_no_specific_server(tmp_path: Path):
    """Pinned resolves to the primary or the only guild, not necessarily GL.iNet."""
    from tests.test_web_permission_groups import _create_account

    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)
    _create_account(client, "scoped@example.com", "glinet_read_only")

    response = client.get("/admin/users", base_url="https://docker.example:8443")

    assert response.status_code == 200
    text = response.get_data(as_text=True)
    assert "Only the preferred server" in text
    assert "Only the GL.iNet Community server" not in text


def test_account_page_does_not_promise_session_revocation(tmp_path: Path):
    client = _logged_in_client(tmp_path)

    response = client.get("/admin/account", base_url="https://docker.example:8443")

    assert response.status_code == 200
    text = BeautifulSoup(response.data, "html.parser").get_text(" ", strip=True)
    assert "signs other sessions out" not in text
    assert "revoke" not in text.lower()


# ---------------------------------------------------------------------------
# Roles separate areas: a group only sees what it was approved for
# ---------------------------------------------------------------------------


def _make_members_only_group(tmp_path, email: str) -> None:
    import sqlite3

    from app.web_groups import assign_user, save_group

    conn = sqlite3.connect(str(tmp_path / "bot_data.db"))
    conn.row_factory = sqlite3.Row
    try:
        created = save_group(
            conn,
            None,
            {
                "name": "Members Only",
                "scope_mode": "all",
                "permissions": ["members.read"],
            },
            actor_permissions=frozenset({"members.read", "members.write"}),
        )
        assert created["ok"] is True, created
        assert assign_user(conn, email, created["slug"])["ok"] is True
    finally:
        conn.close()


def test_a_narrow_group_sees_only_its_own_area(tmp_path: Path):
    from tests.test_web_admin import _login_as
    from tests.test_web_permission_groups import _create_account

    app = _make_app(tmp_path)
    admin_client = app.test_client()
    _login(admin_client)
    _select_guild(admin_client)
    _create_account(admin_client, "members@example.com", "read_only")
    _make_members_only_group(tmp_path, "members@example.com")

    client = app.test_client()
    _login_as(client, "members@example.com", "Ab!12xy")
    _select_guild(client)

    dashboard = client.get("/admin/dashboard", base_url="https://docker.example:8443")
    assert dashboard.status_code == 200
    page = BeautifulSoup(dashboard.data, "html.parser")

    # Only the approved tab is on the cover and in the menu.
    tabs = {card.find("h3").get_text(strip=True) for card in page.select(".binder-grid .binder-card")}
    assert any("Member Related" in tab for tab in tabs)
    assert not any("Mod & Perms" in tab for tab in tabs)
    assert not any("Security" in tab for tab in tabs)

    menu = page.find("select", id="desktop-nav-page-select")
    labels = {option.get_text(strip=True) for option in menu.find_all("option")}
    assert "Members" in labels
    assert "Moderation" not in labels
    assert "Logs" not in labels

    # The page itself is refused, not just hidden.
    blocked = client.get("/admin/moderation", base_url="https://docker.example:8443")
    assert blocked.status_code == 302
    assert blocked.headers["Location"].endswith("/admin/dashboard")

    allowed = client.get("/admin/members", base_url="https://docker.example:8443")
    assert allowed.status_code == 200


def test_users_page_uses_the_group_picker_not_the_legacy_role(tmp_path: Path):
    client = _logged_in_client(tmp_path)

    response = client.get("/admin/users", base_url="https://docker.example:8443")

    assert response.status_code == 200
    soup = BeautifulSoup(response.data, "html.parser")
    assert soup.find("select", attrs={"name": "slug"}) is not None
    assert soup.find("select", attrs={"name": "role"}) is None
    form = soup.find("form", attrs={"action": "/admin/permissions"})
    assert form is not None
    assert form.find("input", attrs={"name": "action", "value": "assign_group"}) is not None


def test_group_can_be_changed_from_the_users_page(tmp_path: Path):
    import sqlite3

    from tests.test_web_permission_groups import _create_account
    from tests.test_web_post_message import _csrf

    app = _make_app(tmp_path)
    admin_client = app.test_client()
    _login(admin_client)
    _select_guild(admin_client)
    _create_account(admin_client, "mover@example.com", "read_only")

    token = _csrf(admin_client, "/admin/users")
    response = admin_client.post(
        "/admin/permissions",
        data={
            "action": "assign_group",
            "email": "mover@example.com",
            "slug": "glinet-read-only",
            "csrf_token": token,
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )
    assert response.status_code == 200

    conn = sqlite3.connect(str(tmp_path / "bot_data.db"))
    conn.row_factory = sqlite3.Row
    try:
        slug = conn.execute(
            "SELECT group_slug FROM web_users WHERE LOWER(email) = 'mover@example.com'"
        ).fetchone()["group_slug"]
    finally:
        conn.close()
    assert slug == "glinet-read-only"


def test_the_last_owner_cannot_be_demoted(tmp_path: Path):
    from tests.test_web_post_message import _csrf

    client = _logged_in_client(tmp_path)
    token = _csrf(client, "/admin/users")

    response = client.post(
        "/admin/permissions",
        data={
            "action": "assign_group",
            "email": "admin@example.com",
            "slug": "read-only",
            "csrf_token": token,
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )

    assert "last Owner" in response.get_data(as_text=True)

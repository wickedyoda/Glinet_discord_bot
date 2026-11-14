from __future__ import annotations

from pathlib import Path

from bs4 import BeautifulSoup

from tests.test_web_admin import _login, _make_app, _select_guild
from web_admin import _discord_channel_option_sort_key, _discord_role_option_sort_key


def test_channel_and_role_sort_keys_order_alphabetically():
    channels = [
        {"id": "3", "name": "#welcome"},
        {"id": "1", "name": "#announcements"},
        {"id": "2", "name": "#general"},
        {"id": "4", "name": "bot-commands"},
    ]
    sorted_channels = sorted(channels, key=_discord_channel_option_sort_key)
    sorted_names = [c["name"].lstrip("#") for c in sorted_channels]
    assert sorted_names == ["announcements", "bot-commands", "general", "welcome"]

    roles = [
        {"id": "3", "name": "@Moderator"},
        {"id": "1", "name": "@Admin"},
        {"id": "2", "name": "Developer"},
        {"id": "4", "name": "@VIP"},
    ]
    sorted_roles = sorted(roles, key=_discord_role_option_sort_key)
    sorted_role_names = [r["name"].lstrip("@") for r in sorted_roles]
    assert sorted_role_names == ["Admin", "Developer", "Moderator", "VIP"]


# Every page the old flat list offered. Regrouping must not drop one.
_EXPECTED_NAV_PAGES = {
    "Action History",
    "Auto-Translate Channels",
    "Bot Profile",
    "Bulk Role CSV",
    "Command Permissions",
    "Command Status",
    "Dashboard",
    "Discourse",
    "Discourse Activity",
    "Documentation",
    "Forum Viewer",
    "Freshdesk",
    "Forum Tag Cross-Posts",
    "GL.iNet Beta Programs",
    "Global Settings",
    "Guild Settings",
    "Honeypot",
    "LinkedIn Profiles",
    "Logs",
    "Member Activity",
    "Members",
    "Moderation",
    "My Account",
    "Observability",
    "Permissions",
    "Reaction Roles",
    "Reddit Auto-Reply",
    "Reddit Feeds",
    "Role Access",
    "Servers",
    "Service Monitors",
    "Tag Responses",
    "Uptime Kuma",
    "Users",
    "YouTube Subscriptions",
}


def _load_dashboard_soup(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)
    response = client.get("/admin/dashboard", base_url="https://docker.example:8443")
    assert response.status_code == 200
    return BeautifulSoup(response.data, "html.parser")


def _nav_select(soup, select_id: str):
    select = soup.find("select", id=select_id)
    assert select is not None, f"no #{select_id} in the rendered page"
    return select


def _group_rows(select):
    """[(group label, [option labels])] in document order, groups only."""
    rows = []
    for optgroup in select.find_all("optgroup"):
        labels = [o.get_text().strip() for o in optgroup.find_all("option")]
        rows.append((str(optgroup.get("label") or "").strip(), labels))
    return rows


def test_nav_dropdown_is_grouped_by_task(tmp_path: Path):
    """A flat A-Z list of ~30 pages is hard to scan; task groups make it findable."""
    soup = _load_dashboard_soup(tmp_path)

    for select_id in ("desktop-nav-page-select", "mobile-nav-page-select"):
        select = _nav_select(soup, select_id)
        rows = _group_rows(select)
        assert len(rows) >= 5, f"{select_id} is still one flat list"
        labels = [label for label, _ in rows]
        assert all(labels), f"{select_id} has an unlabelled group"
        assert len(labels) == len(set(labels)), f"duplicate group labels: {labels}"


def test_nav_options_stay_alphabetical_inside_each_group(tmp_path: Path):
    soup = _load_dashboard_soup(tmp_path)
    select = _nav_select(soup, "desktop-nav-page-select")

    for label, items in _group_rows(select):
        assert items == sorted(items, key=str.casefold), (
            f"group {label!r} is not alphabetical: {items}"
        )


def test_nav_offers_the_github_wiki_link_when_configured(tmp_path: Path, monkeypatch):
    """The wiki entry is conditional and must survive regrouping in both selects."""
    monkeypatch.setenv("WEB_GITHUB_WIKI_URL", "https://example.com/wiki")
    soup = _load_dashboard_soup(tmp_path)

    for select_id in ("desktop-nav-page-select", "mobile-nav-page-select"):
        select = _nav_select(soup, select_id)
        wiki = [
            o for o in select.find_all("option")
            if o.get_text().strip() == "GitHub Wiki"
        ]
        assert wiki, f"{select_id} lost the GitHub Wiki entry"
        assert wiki[0].get("data-external") == "1", (
            "wiki option must open in a new tab via data-external=1"
        )
        assert wiki[0].get("value") == "https://example.com/wiki"


def test_nav_hides_the_github_wiki_link_when_unset(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("WEB_GITHUB_WIKI_URL", "")
    soup = _load_dashboard_soup(tmp_path)

    for select_id in ("desktop-nav-page-select", "mobile-nav-page-select"):
        select = _nav_select(soup, select_id)
        labels = {o.get_text().strip() for o in select.find_all("option")}
        assert "GitHub Wiki" not in labels, f"{select_id} rendered a blank wiki entry"


def test_nav_grouping_loses_no_page(tmp_path: Path):
    """The menu is deliberately short; every other page must stay reachable.

    The operator asked for twelve menu leaves and everything else one level down,
    so this checks both halves: the twelve leaves are in the dropdown, and every
    page in the binder is linked from its section landing page.
    """
    soup = _load_dashboard_soup(tmp_path)
    select = _nav_select(soup, "desktop-nav-page-select")

    rendered = {item for _, items in _group_rows(select) for item in items}
    for label in (
        "My Account",
        "Servers",
        "Member Activity",
        "Members",
        "Moderation",
        "Permissions",
        "Auto-Translate Channels",
        "Forums",
        "Invite Link Controls",
        "Reaction Roles",
        "Honeypot",
        "Logs",
    ):
        assert label in rendered, f"{label!r} missing from the nav"

    # Logout moved into its own group, not lost
    assert "Logout" in rendered

    # the placeholder stays first, outside any group
    first = select.find("option")
    assert first is not None
    assert first.get_text().strip() == "Go to page..."

    from app.webgui_sections import SECTIONS

    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    expected = {
        label
        for section in SECTIONS
        for endpoint, label, _description in section["pages"]
        if "." not in endpoint  # blueprint pages may not be registered here
    }
    seen = set()
    for section in SECTIONS:
        response = client.get(
            f"/admin/section/{section['slug']}",
            base_url="https://docker.example:8443",
        )
        assert response.status_code == 200, f"section {section['slug']} did not render"
        page = BeautifulSoup(response.data, "html.parser")
        for card in page.find_all("a", class_="binder-card"):
            heading = card.find("h3")
            if heading:
                seen.add(heading.get_text(strip=True))

    missing = expected - seen
    assert not missing, f"pages not reachable from any section page: {sorted(missing)}"


def test_dashboard_renders_the_six_binder_sections(tmp_path: Path):
    """The dashboard is the binder's cover: six tabs, not seven card sections."""
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    response = client.get("/admin/dashboard", base_url="https://docker.example:8443")
    assert response.status_code == 200

    html = response.data.decode("utf-8")
    for label in (
        "My Account",
        "Servers",
        "Member Related",
        "Mod &amp; Perms",
        "Discord Server Related",
        "Security &amp; Information",
    ):
        assert label in html, f"dashboard is missing the {label!r} binder tab"

    # The old long-scroll card sections are gone.
    for retired in (
        "Roles &amp; Onboarding",
        "Notification Feeds &amp; Monitors",
        "Bot Configuration &amp; System Administration",
    ):
        assert retired not in html, f"dashboard still renders the retired {retired!r} section"

    # Every tab announces itself in plain language, with a warning where it matters.
    assert "binder-card" in html
    assert "announcement" in html
    assert "Quick Tips &amp; History" in html


def test_member_activity_lives_in_the_member_related_tab(tmp_path: Path):
    """Member activity sits with members, not with role administration."""
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    members = client.get("/admin/section/members", base_url="https://docker.example:8443")
    assert members.status_code == 200
    members_html = members.data.decode("utf-8")
    assert "Member Activity" in members_html
    assert "Members" in members_html

    moderation = client.get(
        "/admin/section/moderation", base_url="https://docker.example:8443"
    )
    assert moderation.status_code == 200
    moderation_soup = BeautifulSoup(moderation.data, "html.parser")
    moderation_labels = {
        heading.get_text(strip=True)
        for heading in moderation_soup.select(".binder-grid .binder-card h3")
    }
    assert "Member Activity" not in moderation_labels
    assert "Moderation" in moderation_labels

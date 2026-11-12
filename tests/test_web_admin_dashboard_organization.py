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
    """Regrouping must not silently drop a page from the menu."""
    soup = _load_dashboard_soup(tmp_path)
    select = _nav_select(soup, "desktop-nav-page-select")

    rendered = {item for _, items in _group_rows(select) for item in items}
    missing = _EXPECTED_NAV_PAGES - rendered
    assert not missing, f"pages missing from the nav: {sorted(missing)}"

    # Logout moved into its own group, not lost
    assert "Logout" in {o.get_text().strip() for o in select.find_all("option")}

    # the placeholder stays first, outside any group
    first = select.find("option")
    assert first is not None
    assert first.get_text().strip() == "Go to page..."


def test_dashboard_renders_reorganized_logical_sections(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    response = client.get("/admin/dashboard", base_url="https://docker.example:8443")
    assert response.status_code == 200

    html = response.data.decode("utf-8")
    assert "Moderation &amp; Security" in html or "Moderation & Security" in html
    assert "Roles &amp; Onboarding" in html or "Roles & Onboarding" in html
    assert "Channels &amp; Chat Automation" in html or "Channels & Chat Automation" in html
    assert "Notification Feeds &amp; Monitors" in html or "Notification Feeds & Monitors" in html
    assert "Support &amp; Forum Integrations" in html or "Support & Forum Integrations" in html
    assert "Bot Configuration &amp; System Administration" in html or "Bot Configuration & System Administration" in html
    assert "User Activity &amp; Statistics" in html or "User Activity & Statistics" in html


def test_member_activity_lives_in_its_own_section(tmp_path: Path):
    """User activity is its own dashboard section, ready for more platforms.

    It used to be the last card under Roles & Onboarding, which conflated
    engagement statistics with role administration.
    """
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    response = client.get("/admin/dashboard", base_url="https://docker.example:8443")
    assert response.status_code == 200
    soup = BeautifulSoup(response.data, "html.parser")

    def section_containing(heading_text: str):
        for section in soup.find_all("section", class_="dashboard-section"):
            heading = section.find("h3")
            if heading and heading.get_text(strip=True) == heading_text:
                return section
        return None

    activity = section_containing("📊 User Activity & Statistics")
    assert activity is not None, "no User Activity & Statistics section on the dashboard"
    assert "Member Activity" in activity.get_text()

    roles = section_containing("👥 Roles & Onboarding")
    assert roles is not None
    assert "Member Activity" not in roles.get_text(), (
        "Member Activity is still listed under Roles & Onboarding"
    )

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


def test_nav_dropdown_options_are_alphabetically_sorted(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    response = client.get("/admin/dashboard", base_url="https://docker.example:8443")
    assert response.status_code == 200

    soup = BeautifulSoup(response.data, "html.parser")
    desktop_select = soup.find("select", id="desktop-nav-page-select")
    assert desktop_select is not None

    options = [opt.get_text().strip() for opt in desktop_select.find_all("option")]
    assert len(options) > 10
    assert options[0] == "Go to page..."
    assert options[-1] == "Logout"

    page_options = options[1:-1]
    sorted_page_options = sorted(page_options, key=lambda s: s.casefold())
    assert page_options == sorted_page_options, f"Nav options not in ABC order: {page_options} vs {sorted_page_options}"


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

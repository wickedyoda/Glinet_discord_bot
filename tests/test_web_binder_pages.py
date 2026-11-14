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
    assert {"Members", "Member Activity"} <= labels
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

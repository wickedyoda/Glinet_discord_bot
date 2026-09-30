"""Tests for alphabetical sorting of role dropdown options.

Discord returns guild roles in hierarchy/position order, which is not A-Z and is
hard to scan in the web GUI's role dropdowns. ``_load_discord_catalog_options``
now sorts them centrally so every consumer benefits.
"""

from __future__ import annotations

from pathlib import Path

from web_admin import _discord_role_option_sort_key


def _sorted_names(options):
    return [
        str(o.get("name") or o.get("label") or o.get("id") or "")
        for o in sorted(options, key=_discord_role_option_sort_key)
    ]


def test_sorts_case_insensitively_alphabetically():
    options = [
        {"id": "1", "name": "zeta"},
        {"id": "2", "name": "Alpha"},
        {"id": "3", "name": "beta"},
        {"id": "4", "name": "Gamma"},
    ]
    # Lowercase must interleave with uppercase, not sort after it.
    assert _sorted_names(options) == ["Alpha", "beta", "Gamma", "zeta"]


def test_sorts_full_alphabet_not_case_sensitive_grouping():
    """A naive `sort(key=str.lower)` on a mixed-case set must interleave.

    Guards against regressing to a plain ASCII sort where all uppercase names
    would cluster before all lowercase ones.
    """
    options = [
        {"id": "1", "name": "apple"},
        {"id": "2", "name": "Banana"},
        {"id": "3", "name": "cherry"},
    ]
    assert _sorted_names(options) == ["apple", "Banana", "cherry"]


def test_leading_at_sign_is_ignored_for_sorting():
    options = [
        {"id": "1", "name": "@Moderator"},
        {"id": "2", "name": "Administrator"},
    ]
    # "@Moderator" should sort as "Moderator" -> after "Administrator".
    assert _sorted_names(options) == ["Administrator", "@Moderator"]


def test_falls_back_to_label_then_id_when_name_missing():
    options = [
        {"id": "9", "label": "Zeta Label"},
        {"id": "3", "label": "Alpha Label"},
    ]
    assert _sorted_names(options) == ["Alpha Label", "Zeta Label"]

    id_only = [{"id": "222"}, {"id": "111"}]
    ordered = sorted(id_only, key=_discord_role_option_sort_key)
    assert [o["id"] for o in ordered] == ["111", "222"]


def test_duplicate_names_are_broken_by_id_deterministically():
    """Ties must resolve identically every call, not depend on input order."""
    options = [
        {"id": "222", "name": "Support"},
        {"id": "111", "name": "Support"},
    ]
    first = [o["id"] for o in sorted(options, key=_discord_role_option_sort_key)]
    second = [o["id"] for o in sorted(list(reversed(options)), key=_discord_role_option_sort_key)]
    assert first == second == ["111", "222"]


def test_non_dict_entries_do_not_raise():
    assert _discord_role_option_sort_key(None) == ("", "")
    assert _discord_role_option_sort_key("not-a-dict") == ("", "")
    mixed = [{"id": "1", "name": "b"}, None, {"id": "2", "name": "a"}]
    assert len(sorted(mixed, key=_discord_role_option_sort_key)) == 3


def test_empty_name_sorts_first():
    """A role with no name has no word to sort on, so it leads the list.

    The renderer falls back to showing the id in that case, so the displayed
    value here is the id.
    """
    options = [{"id": "1", "name": ""}, {"id": "2", "name": "Alpha"}]
    assert _sorted_names(options) == ["1", "Alpha"]


def test_freshdesk_page_role_dropdown_is_sorted():
    """The Freshdesk renderer sorts defensively too.

    It receives discord_role_options as an argument, so it can be called with an
    unsorted list that never passed through the catalog loader. A dropdown that is
    alphabetical on the settings page but not here would be worse than either.
    """
    from app.freshdesk_web_helpers import render_freshdesk_viewer_body

    unsorted_roles = [
        {"id": "111", "name": "Member", "label": "@Member"},
        {"id": "222", "name": "Employee", "label": "@Employee"},
    ]
    html = render_freshdesk_viewer_body(
        guild_name="Test",
        effective_settings={
            "FRESHDESK_ENABLED": "true",
            "FRESHDESK_BASE_URL": "https://example.freshdesk.com",
            "FRESHDESK_API_KEY": "key",
            "FRESHDESK_REQUEST_TIMEOUT_SECONDS": "15",
        },
        discord_role_options=unsorted_roles,
    )

    employee_at = html.find("Employee")
    member_at = html.find("Member")
    assert employee_at != -1 and member_at != -1, "role options not rendered"
    assert employee_at < member_at, "Freshdesk role dropdown is not alphabetical"


# --------------------------------------------------------------------------- #
#  Integration: the catalog loader must apply the sort to a real page
# --------------------------------------------------------------------------- #
def test_settings_page_role_dropdown_lists_roles_alphabetically(tmp_path):
    """End-to-end: a page that renders a *_ROLE_ID dropdown must show A-Z order.

    The shared test fixture returns roles as [Member, Employee] — deliberately not
    alphabetical — so asserting Employee precedes Member proves the central sort in
    _load_discord_catalog_options actually reaches the rendered HTML.
    """

    from test_web_admin import _login, _make_app, _select_guild  # noqa: PLC0415

    app = _make_app(Path(tmp_path))
    client = app.test_client()
    _login(client)
    _select_guild(client)

    response = client.get("/admin/settings", base_url="https://docker.example:8443")
    assert response.status_code == 200
    html = response.get_data(as_text=True)

    # Match the rendered role options specifically. A bare find("Member") would hit
    # the nav link "Members" long before the dropdown and assert nothing useful.
    employee_at = html.find("@Employee (222)")
    member_at = html.find("@Member (111)")
    assert employee_at != -1, "Employee role option not rendered"
    assert member_at != -1, "Member role option not rendered"

    # Both must sit in the same <select>, and Employee must come first.
    assert employee_at < member_at, (
        "role dropdown is not alphabetical: @Employee should be listed before @Member"
    )
    between = html[employee_at:member_at]
    assert "<select" not in between, "options are in different dropdowns"

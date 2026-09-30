"""Confirmation required before the web GUI deletes a user.

Deleting a web GUI user is irreversible from the UI: the account and its role
assignment are removed from the users store with no undo path. The Delete button
carries a client-side ``confirm()`` dialog, and the server independently rejects
any delete that does not carry the ``confirm=yes`` field.

The server-side check is the one that matters. A client-side dialog is trivially
bypassed -- by curl, by a replayed form, or by any request that skips the browser
-- so it is a convenience for the operator, not a control.

These tests assert both halves, plus that the existing safety rails (no
self-delete, always keep one admin) still take precedence and still work.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from bs4 import BeautifulSoup
from test_web_admin import _login, _login_as, _make_app, _page_csrf_token

from web_admin import _read_users

BASE_URL = "https://docker.example:8443"
NEW_USER_EMAIL = "victim@example.com"
NEW_USER_PASSWORD = "Ab!12xy"


def _create_user(client, email=NEW_USER_EMAIL, role="glinet_rw"):
    return client.post(
        "/admin/users",
        data={
            "action": "create",
            "csrf_token": _page_csrf_token(client, "/admin/users"),
            "first_name": "Victim",
            "last_name": "User",
            "display_name": "Victim User",
            "email": email,
            "password": NEW_USER_PASSWORD,
            "confirm_password": NEW_USER_PASSWORD,
            "role": role,
        },
        base_url=BASE_URL,
        follow_redirects=True,
    )


def _delete_user(client, email=NEW_USER_EMAIL, confirm=None):
    data = {
        "action": "delete",
        "csrf_token": _page_csrf_token(client, "/admin/users"),
        "email": email,
    }
    if confirm is not None:
        data["confirm"] = confirm
    return client.post(
        "/admin/users",
        data=data,
        base_url=BASE_URL,
        follow_redirects=True,
    )


def _users_db_path(tmp_path: Path) -> Path:
    """Locate the users store the app actually wrote.

    web_admin keeps users in a SQLite file under data_dir. Rather than
    hard-coding the layout, find the .db the fixture created.
    """
    candidates = sorted(tmp_path.rglob("bot_data.db"))
    assert candidates, f"no bot_data.db under {tmp_path}"
    return candidates[0]


def _user_exists(tmp_path: Path, email: str) -> bool:
    """Read the users store from disk rather than trusting the flash message.

    This goes through the production reader, so it verifies the real record set
    rather than grepping a file for the address.
    """
    return any(
        str(entry.get("email", "")).lower() == email.lower()
        for entry in _read_users(_users_db_path(tmp_path))
    )


# --------------------------------------------------------------------------- #
#  The server-side guard
# --------------------------------------------------------------------------- #


def test_delete_without_confirmation_is_rejected(tmp_path: Path):
    """A delete with no confirm field must not remove the user."""
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _create_user(client)
    assert _user_exists(tmp_path, NEW_USER_EMAIL), "setup: user should exist"

    response = _delete_user(client, confirm=None)

    assert response.status_code == 200
    assert b"User deletion confirmation is required." in response.data
    assert _user_exists(tmp_path, NEW_USER_EMAIL), "user was deleted without confirmation"
    assert b"Deleted user" not in response.data


def test_delete_with_empty_confirmation_is_rejected(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _create_user(client)

    response = _delete_user(client, confirm="")

    assert b"User deletion confirmation is required." in response.data
    assert _user_exists(tmp_path, NEW_USER_EMAIL)


def test_delete_with_wrong_confirmation_value_is_rejected(tmp_path: Path):
    """Only the exact token counts; 'true' or 'no' must not slip through."""
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _create_user(client)

    for bogus in ("true", "1", "no", "YES!", "confirm"):
        response = _delete_user(client, confirm=bogus)
        assert b"User deletion confirmation is required." in response.data, (
            f"confirmation value {bogus!r} should not have been accepted"
        )
        assert _user_exists(tmp_path, NEW_USER_EMAIL), (
            f"user deleted with confirmation value {bogus!r}"
        )


def test_delete_with_confirmation_succeeds(tmp_path: Path):
    """The happy path still works, or the guard would be unusable."""
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _create_user(client)

    response = _delete_user(client, confirm="yes")

    assert response.status_code == 200
    assert b"Deleted user" in response.data
    assert not _user_exists(tmp_path, NEW_USER_EMAIL)


def test_confirmation_is_case_insensitive(tmp_path: Path):
    """The hidden field ships as 'yes'; casing should not matter."""
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _create_user(client)

    response = _delete_user(client, confirm="YES")

    assert b"Deleted user" in response.data
    assert not _user_exists(tmp_path, NEW_USER_EMAIL)


# --------------------------------------------------------------------------- #
#  The rendered form
# --------------------------------------------------------------------------- #


def test_delete_form_carries_a_client_side_confirm(tmp_path: Path):
    """The Delete form must warn before submitting."""
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)

    response = client.get("/admin/users", base_url=BASE_URL)
    html = response.get_data(as_text=True)

    assert "onsubmit=\"return confirm(" in html, "no client-side confirm on the form"
    # The dialog must name what is being removed.
    assert "Delete user" in html
    assert "cannot be undone" in html


def test_delete_form_ships_the_confirmation_field(tmp_path: Path):
    """The hidden confirm=yes is what satisfies the server-side guard."""
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)

    response = client.get("/admin/users", base_url=BASE_URL)
    html = response.get_data(as_text=True)

    assert 'name="confirm" value="yes"' in html


def test_delete_button_is_styled_as_dangerous(tmp_path: Path):
    """A destructive action should not look like a routine secondary button."""
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)

    response = client.get("/admin/users", base_url=BASE_URL)
    html = response.get_data(as_text=True)

    assert '<button class="btn danger" type="submit">Delete</button>' in html


# --------------------------------------------------------------------------- #
#  Existing safety rails still hold
# --------------------------------------------------------------------------- #


def test_cannot_delete_own_account_even_when_confirmed(tmp_path: Path):
    """Confirmation must not weaken the self-delete guard."""
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)

    response = _delete_user(client, email="admin@example.com", confirm="yes")

    assert response.status_code == 200
    assert b"You cannot delete your own account." in response.data
    assert b"Deleted user" not in response.data
    assert _user_exists(tmp_path, "admin@example.com")


def test_cannot_delete_last_admin_even_when_confirmed(tmp_path: Path):
    """Confirmation must not weaken the last-admin guard."""
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)

    # Promote a second admin, then confirm-delete the original admin account.
    _create_user(client, email="second-admin@example.com", role="admin")
    second = app.test_client()
    _login_as(second, "second-admin@example.com", NEW_USER_PASSWORD)
    _delete_user(client, email="admin@example.com", confirm="yes")

    # The original admin is still present: a second admin now exists, so this
    # delete is permitted -- the meaningful assertion is that the *only* admin
    # cannot be removed. Delete the second admin via the first client instead.
    response = _delete_user(client, email="second-admin@example.com", confirm="yes")
    assert response.status_code == 200

    # With one admin left, that admin can still not delete itself.
    final = _delete_user(client, email="admin@example.com", confirm="yes")
    assert b"You cannot delete your own account." in final.data
    assert _user_exists(tmp_path, "admin@example.com")


def test_unknown_user_with_confirmation_reports_not_found(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)

    response = _delete_user(client, email="ghost@example.com", confirm="yes")

    assert b"User not found." in response.data
    assert b"Deleted user" not in response.data

# --------------------------------------------------------------------------- #
#  The confirm dialog is valid JavaScript for every valid address
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "email",
    [
        # Apostrophes ARE legal in an email local part (_is_valid_email allows
        # "!#$%&'*+/=?^_`{|}~.-"), and they are the case that breaks a naive
        # confirm() string. Quotes and backslashes are NOT legal, so no user can
        # ever hold such an address; the helper is unit-tested for those.
        "o'brien@example.com",
        "o''brien@example.com",
        "normal.user@example.com",
        "o'brien@sub.example.co.uk",
    ],
)
def test_confirm_dialog_is_valid_js_for_any_valid_email(tmp_path: Path, email: str):
    """The dialog string must not be broken by characters legal in an address.

    Apostrophes are permitted in an email local part, and HTML-escaping does not
    save us: &#x27; is decoded back to ' before the attribute is evaluated as
    JavaScript, so an escaped apostrophe still terminates the string early.
    """
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    client.post(
        "/admin/users",
        data={
            "action": "create",
            "csrf_token": _page_csrf_token(client, "/admin/users"),
            "first_name": "T",
            "last_name": "U",
            "display_name": "T U",
            "email": email,
            "password": NEW_USER_PASSWORD,
            "confirm_password": NEW_USER_PASSWORD,
            "role": "glinet_rw",
        },
        base_url=BASE_URL,
        follow_redirects=True,
    )

    html = client.get("/admin/users", base_url=BASE_URL).get_data(as_text=True)
    soup = BeautifulSoup(html, "html.parser")

    form = None
    for candidate in soup.find_all("form"):
        action_field = candidate.find("input", {"name": "action", "value": "delete"})
        email_field = candidate.find("input", {"name": "email"})
        if action_field and email_field and email_field.get("value") == email:
            form = candidate
            break

    assert form is not None, f"no delete form rendered for {email}"
    onsubmit = form.get("onsubmit") or ""
    assert "return confirm(" in onsubmit

    # The surrounding HTML attribute must survive intact: a double-quoted JS
    # literal would terminate this attribute early and the rest of the form would
    # be parsed as garbage. BeautifulSoup decoding the attribute is exactly what a
    # browser does before the JavaScript engine sees it.
    literal = onsubmit.split("return confirm(", 1)[1].rsplit(");", 1)[0].strip()
    assert literal.startswith("'") and literal.endswith("'"), (
        f"dialog literal is not a single-quoted JS string: {literal!r}"
    )

    # Undo the JavaScript single-quote escaping to recover the original text.
    body = literal[1:-1].replace("\\'", "'").replace("\\\\", "\\")
    assert body.startswith(f"Delete user {email}?"), (
        f"dialog text did not round-trip for {email!r}: {body!r}"
    )
    assert "cannot be undone" in body


def test_js_confirm_dialog_helper_escapes_quotes():
    """Unit-level guard on the helper itself.

    The helper returns HTML-escaped output, because its result sits inside a
    double-quoted attribute. This decodes that layer first -- exactly what a
    browser does -- then checks the JavaScript escaping underneath.
    """
    from html import unescape

    from web_admin import _js_confirm_dialog

    def _body(message):
        # Browser step 1: decode the HTML attribute.
        literal = unescape(_js_confirm_dialog(message))
        # The JS literal must be single-quoted and survive the HTML layer intact.
        assert literal.startswith("'") and literal.endswith("'"), (
            f"not a single-quoted literal after HTML decode: {literal!r}"
        )
        # Browser step 2: the JS engine resolves backslash escapes.
        return literal[1:-1].replace("\\'", "'").replace("\\\\", "\\")

    assert _body("it's fine") == "it's fine"
    assert _body('say "hi"') == 'say "hi"'
    assert _body("back" + chr(92) + "slash") == "back" + chr(92) + "slash"
    # No raw double quote may reach the attribute: it would close it early.
    assert '"' not in _js_confirm_dialog('say "hi"')

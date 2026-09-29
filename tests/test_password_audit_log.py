"""Guards the audit-log call behind Semgrep alert 275 (CWE-532).

Semgrep flagged ``web_admin.py``'s "Password for %s changed without current
password" warning under
``python.lang.security.audit.logging.logger-credential-leak.python-logger-credential-disclosure``.

The finding is a false positive: the rule matches on the literal word
"Password" in the format string, but the only values interpolated into that
call are the account email and the client IP. No password, hash, or other
credential is ever passed to the logger.

That reasoning is recorded as a ``nosemgrep`` suppression on the call site.
A suppression comment is easy to leave behind when the code underneath it
changes, so these tests assert the actual invariant it claims: that the
password-change audit record names the account and the client, and contains
neither the new password, the old password, nor their hashes.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

import pytest

import web_admin
from web_admin import create_web_app

from test_web_admin import _login, _make_app, _page_csrf_token

NEW_PASSWORD = "Zz!99qq"
CONFIRM_EMAIL = "admin@example.com"


class _RecordingLogger:
    """Logger stand-in that keeps the format string and its actual arguments."""

    def __init__(self):
        self.records: list[tuple[str, str, tuple]] = []

    def _record(self, level):
        def _log(msg, *args, **kwargs):
            self.records.append((level, str(msg), args))

        return _log

    def __getattr__(self, name):
        level = name.split("_", 1)[-1] if name.startswith("_") else name
        return self._record(name)

    @property
    def messages(self) -> list[str]:
        return [message for _level, message, _args in self.records]

    def rendered(self) -> list[str]:
        """Every record with %-substitution applied, as a logger would emit it."""
        return [
            message % args if args else message
            for _level, message, args in self.records
        ]


def _make_app_with_logger(tmp_path: Path):
    """Build the standard test app but capture the logger it is handed."""
    recorder = _RecordingLogger()
    app = _make_app(tmp_path, logger=recorder)
    return app, recorder


def _perform_password_forgotten(client):
    return client.post(
        "/admin/account",
        data={
            "action": "password_forgotten",
            "csrf_token": _page_csrf_token(client, "/admin/account"),
            "confirm_email": CONFIRM_EMAIL,
            "new_password": NEW_PASSWORD,
            "confirm_password": NEW_PASSWORD,
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )


def test_password_forgotten_audit_log_records_actor_and_ip(tmp_path: Path):
    """The audit record must name the account and the client IP."""
    app, recorder = _make_app_with_logger(tmp_path)
    client = app.test_client()
    _login(client)

    _perform_password_forgotten(client)

    joined = "\n".join(recorder.rendered())
    assert "changed without current password" in joined, (
        "expected the password-change audit record in the log"
    )
    assert CONFIRM_EMAIL in joined, "audit record should identify the account"
    assert "ip=" in joined, "audit record should include the client IP"


def test_password_forgotten_audit_log_excludes_credential_material(tmp_path: Path):
    """The invariant behind the Semgrep suppression.

    Neither the new password, the previous password, nor a password hash may
    appear anywhere in the audit log. If a future change starts interpolating
    credential material here, this fails and the nosemgrep comment must go.
    """
    app, recorder = _make_app_with_logger(tmp_path)
    client = app.test_client()
    _login(client)

    _perform_password_forgotten(client)

    joined = "\n".join(recorder.rendered())
    assert NEW_PASSWORD not in joined, "the new password leaked into the log"

    # The seeded admin password, and any bcrypt/sha hash of the new password.
    assert "Ab!12xy" not in joined, "the previous password leaked into the log"

    # A werkzeug hash is the long $2b$ / $scrypt$ / pbkdf2 form.
    assert not re.search(r"\$(?:2[aby]|scrypt|pbkdf2)[\w$]*", joined), (
        "a password hash leaked into the log"
    )


def test_audit_log_retains_warning_severity(tmp_path: Path):
    """The record must stay a warning: it is a security-relevant audit event."""
    app, recorder = _make_app_with_logger(tmp_path)
    client = app.test_client()
    _login(client)

    _perform_password_forgotten(client)

    levels = [
        level
        for level, message, _args in recorder.records
        if "changed without current password" in message
    ]
    assert levels == ["warning"], f"expected exactly one warning record, got {levels}"


def test_suppression_comment_cites_the_alert_and_stays_line_scoped():
    """Keep the nosemgrep honest: it must name alert 275 and carry the rule id."""
    source = Path(web_admin.__file__).read_text()
    assert "nosemgrep" in source, "the Semgrep suppression is gone; re-check alert 275"

    suppression_line = next(
        line for line in source.splitlines() if "nosemgrep" in line
    )
    assert "275" in source, "suppression should reference the Semgrep alert number"
    assert "python-logger-credential-disclosure" in suppression_line, (
        "suppression should name the specific rule it silences"
    )

    # It must be a per-line nosemgrep, not a blanket rule exclusion in CI: that
    # would silently disable credential-leak detection for the whole codebase.
    assert "exclude-rule" not in source, (
        "credential-leak detection must not be disabled repo-wide"
    )

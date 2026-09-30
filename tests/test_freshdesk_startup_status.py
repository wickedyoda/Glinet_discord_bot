"""Freshdesk startup status reporting.

The Freshdesk integration is the one subsystem whose configuration state never
appeared in the startup banner. Every other monitor logs whether it is active
("LinkedIn monitor active: polling every 900 seconds", "Service monitor
disabled via SERVICE_MONITOR_ENABLED"), but Freshdesk was silent -- so a user
hitting

    ❌ Freshdesk integration is not configured or disabled.

left the operator hunting through FRESHDESK_* with nothing in the log saying
which of them was unset. That is exactly the diagnosis this feature removes.

These tests assert the log names the missing settings, and -- critically -- that
it never prints the API key.
"""

from __future__ import annotations

import logging

import pytest

import bot as bot_module
from bot import _log_freshdesk_startup_status


FRESHDESK_ENV_VARS = (
    "FRESHDESK_ENABLED",
    "FRESHDESK_DOMAIN",
    "FRESHDESK_BASE_URL",
    "FRESHDESK_API_KEY",
    "FRESHDESK_TICKET_TARGET_CHANNEL_ID",
)


@pytest.fixture(autouse=True)
def _clean_freshdesk_env(monkeypatch):
    """Start from a known state: no Freshdesk variables set at all."""
    for name in FRESHDESK_ENV_VARS:
        monkeypatch.delenv(name, raising=False)


def _emit(caplog) -> str:
    _log_freshdesk_startup_status()
    return "\n".join(record.getMessage() for record in caplog.records)


# --------------------------------------------------------------------------- #
#  Fully configured
# --------------------------------------------------------------------------- #


def test_reports_enabled_with_base_url_and_intake_channel(monkeypatch, caplog):
    monkeypatch.setenv("FRESHDESK_DOMAIN", "acme.freshdesk.com")
    monkeypatch.setenv("FRESHDESK_API_KEY", "super-secret-key-value")
    monkeypatch.setenv("FRESHDESK_TICKET_TARGET_CHANNEL_ID", "111111111")

    with caplog.at_level(logging.INFO, logger="invite_bot"):
        message = _emit(caplog)

    assert "Freshdesk integration enabled" in message
    # Assert on the labelled field rather than a bare host substring: CodeQL's
    # "Incomplete URL substring sanitization" rule flags any `host in message`
    # check, even in a test, and CI treats it as a blocking failure.
    assert "base_url=https://acme.freshdesk.com" in message
    assert "111111111" in message
    assert "configured" in message


def test_never_logs_the_api_key(monkeypatch, caplog):
    """The key itself must never appear -- only whether one is present."""
    secret = "sk-live-DO-NOT-LOG-THIS-1234567890"
    monkeypatch.setenv("FRESHDESK_DOMAIN", "acme.freshdesk.com")
    monkeypatch.setenv("FRESHDESK_API_KEY", secret)

    with caplog.at_level(logging.INFO, logger="invite_bot"):
        message = _emit(caplog)

    assert secret not in message, "the Freshdesk API key was written to the log"
    assert secret not in caplog.text


def test_reports_when_no_intake_channel_is_set(monkeypatch, caplog):
    """No intake channel is a valid state, not a fault -- say so plainly."""
    monkeypatch.setenv("FRESHDESK_DOMAIN", "acme.freshdesk.com")
    monkeypatch.setenv("FRESHDESK_API_KEY", "key")

    with caplog.at_level(logging.INFO, logger="invite_bot"):
        message = _emit(caplog)

    assert "Freshdesk integration enabled" in message
    assert "intake channel not set" in message


def test_uses_base_url_when_domain_is_absent(monkeypatch, caplog):
    monkeypatch.setenv("FRESHDESK_BASE_URL", "https://alt.freshdesk.com")
    monkeypatch.setenv("FRESHDESK_API_KEY", "key")

    with caplog.at_level(logging.INFO, logger="invite_bot"):
        message = _emit(caplog)

    assert "base_url=https://alt.freshdesk.com" in message


# --------------------------------------------------------------------------- #
#  Disabled / misconfigured -- the cases that actually needed diagnosing
# --------------------------------------------------------------------------- #


def test_reports_when_disabled_by_env(caplog):
    import os

    os.environ["FRESHDESK_ENABLED"] = "false"
    try:
        with caplog.at_level(logging.INFO, logger="invite_bot"):
            message = _emit(caplog)
    finally:
        del os.environ["FRESHDESK_ENABLED"]

    assert "disabled" in message
    assert "FRESHDESK_ENABLED" in message


@pytest.mark.parametrize(
    "flag",
    ["0", "false", "no", "off", "FALSE"],
)
def test_every_disabled_spelling_is_honoured(monkeypatch, caplog, flag):
    """resolve_freshdesk_config treats these as false; the report must agree."""
    monkeypatch.setenv("FRESHDESK_ENABLED", flag)
    monkeypatch.setenv("FRESHDESK_DOMAIN", "acme.freshdesk.com")
    monkeypatch.setenv("FRESHDESK_API_KEY", "key")

    with caplog.at_level(logging.INFO, logger="invite_bot"):
        message = _emit(caplog)

    assert "disabled" in message, f"{flag!r} should read as disabled"


def test_names_the_missing_domain(monkeypatch, caplog):
    """This is the case that cost the original bug its diagnosis time."""
    monkeypatch.setenv("FRESHDESK_API_KEY", "key")

    with caplog.at_level(logging.WARNING, logger="invite_bot"):
        message = _emit(caplog)

    assert "not fully configured" in message
    assert "FRESHDESK_DOMAIN" in message
    assert "FRESHDESK_BASE_URL" in message
    assert "not configured" in message


def test_names_the_missing_api_key(monkeypatch, caplog):
    monkeypatch.setenv("FRESHDESK_DOMAIN", "acme.freshdesk.com")

    with caplog.at_level(logging.WARNING, logger="invite_bot"):
        message = _emit(caplog)

    assert "FRESHDESK_API_KEY" in message


def test_names_both_when_neither_is_set(monkeypatch, caplog):
    with caplog.at_level(logging.WARNING, logger="invite_bot"):
        message = _emit(caplog)

    assert "FRESHDESK_API_KEY" in message
    assert "FRESHDESK_DOMAIN" in message or "FRESHDESK_BASE_URL" in message


def test_misconfiguration_is_a_warning_not_info(monkeypatch, caplog):
    """A broken integration should be visible without raising the log level to ERROR."""
    monkeypatch.setenv("FRESHDESK_API_KEY", "key")

    with caplog.at_level(logging.INFO, logger="invite_bot"):
        _log_freshdesk_startup_status()

    levels = [record.levelno for record in caplog.records]
    assert logging.WARNING in levels
    assert logging.ERROR not in levels


# --------------------------------------------------------------------------- #
#  Robustness
# --------------------------------------------------------------------------- #


def test_never_raises(monkeypatch, caplog):
    """A diagnostic line must not be able to break startup."""
    monkeypatch.setenv("FRESHDESK_DOMAIN", "acme.freshdesk.com")
    monkeypatch.setenv("FRESHDESK_API_KEY", "key")

    def _boom():
        raise RuntimeError("config resolution exploded")

    monkeypatch.setattr(bot_module, "resolve_freshdesk_config", _boom)

    with caplog.at_level(logging.INFO, logger="invite_bot"):
        _log_freshdesk_startup_status()  # must not raise

    assert "Failed to log Freshdesk startup status" in caplog.text


def test_callable_from_the_startup_banner():
    """on_ready must actually invoke it, or the report is never emitted."""
    import inspect

    source = inspect.getsource(bot_module.on_ready)
    assert "_log_freshdesk_startup_status()" in source, (
        "on_ready does not report Freshdesk configuration at startup"
    )

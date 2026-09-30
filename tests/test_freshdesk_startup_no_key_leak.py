"""Semgrep 276: the Freshdesk startup line must never print the API key.

Semgrep flags

    logger.info("Freshdesk integration enabled: base_url=%s api_key=%s %s", ...)

as python-logger-credential-disclosure because the format string contains the
literal token "api_key=".

It is a false positive: the second argument is
``"configured" if has_api_key else "missing"`` -- a boolean rendered as one of
two fixed words. ``has_api_key`` itself is ``bool(str(config.get("api_key")))``,
so the key is reduced to a truth value before it ever reaches the logger.

These tests are the evidence for the line-scoped ``nosemgrep`` suppression. If
someone later "simplifies" the call to pass the key itself, these fail.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

import bot as bot_module


class _CaptureHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records = []

    def emit(self, record):
        self.records.append(record)


@pytest.fixture
def captured():
    """Attach a real handler so records are actually observed.

    Do not stub addHandler -- the fixture must observe genuine LogRecords.
    """
    handler = _CaptureHandler()
    bot_module.logger.addHandler(handler)
    original_level = bot_module.logger.level
    original_propagate = bot_module.logger.propagate
    bot_module.logger.setLevel(logging.INFO)
    bot_module.logger.propagate = False
    yield handler
    bot_module.logger.removeHandler(handler)
    bot_module.logger.setLevel(original_level)
    bot_module.logger.propagate = original_propagate


SECRET = "fd-super-secret-key-value-1234567890"


def _run(monkeypatch, **config):
    base = {
        "enabled": True,
        "base_url": "https://acme.freshdesk.com",
        "api_key": SECRET,
    }
    base.update(config)
    monkeypatch.setattr(bot_module, "resolve_freshdesk_config", lambda: base)
    bot_module._log_freshdesk_startup_status()


def _all_text(handler):
    parts = []
    for r in handler.records:
        parts.append(r.getMessage())
        if r.args:
            parts.extend(str(a) for a in r.args)
    return "\n".join(parts)


def test_startup_line_never_prints_the_api_key(monkeypatch, captured):
    """The headline assertion: the secret must not appear anywhere."""
    _run(monkeypatch)
    text = _all_text(captured)
    assert SECRET not in text, "the Freshdesk API key was written to the log"
    assert "fd-super-secret-key-value" not in text


def test_key_is_reduced_to_a_word_before_logging(monkeypatch, captured):
    _run(monkeypatch)
    text = _all_text(captured)
    assert "api_key=configured" in text, (
        "presence should be reported as the fixed word 'configured'"
    )
    # the boolean-derived words are the only permitted values
    assert "api_key=missing" not in text


def test_missing_key_is_reported_without_a_value(monkeypatch, captured):
    _run(monkeypatch, api_key="")
    text = _all_text(captured)
    assert "FRESHDESK_API_KEY" in text
    assert SECRET not in text


def test_disabled_path_never_reaches_the_enabled_line(monkeypatch, captured):
    _run(monkeypatch, enabled=False)
    text = _all_text(captured)
    assert "disabled" in text
    assert SECRET not in text
    assert "api_key=configured" not in text


def test_source_does_not_pass_the_key_directly():
    """Static guard: the call site must keep the boolean indirection."""
    src = Path(bot_module.__file__).read_text()
    idx = src.index("Freshdesk integration enabled: base_url=")
    window = src[idx:idx+420]
    assert '"configured" if has_api_key else "missing"' in window, (
        "the startup log must pass the presence flag, not the key itself"
    )
    assert "config.get(\"api_key\")" not in window.split(")")[0], (
        "the raw API key must not be an argument to the startup log call"
    )

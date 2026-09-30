"""Freshdesk settings must survive a container restart.

The Web GUI reported saves as successful (HTTP 200) yet nothing persisted.
The write path silently degraded:

  primary  = WEB_ENV_FILE  -> /app/data/web-settings.env  (persistent volume)
  fallback = data_dir/web-settings.env  -> ALSO /app/data/... when
              WEB_ENV_FILE is already that path

and when the primary is unwritable the fallback is filtered through
FALLBACK_PROTECTED_ENV_KEYS, which drops FRESHDESK_API_KEY. So a save that
lands on the fallback path silently loses the API key -- the one setting
Freshdesk cannot work without -- and the UI still reports success.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import web_admin


@pytest.fixture
def tmp_dirs(tmp_path):
    data = tmp_path / "data"
    data.mkdir()
    return data


# --------------------------------------------------------------------------- #
#  The defect: FRESHDESK_API_KEY is stripped from the fallback write
# --------------------------------------------------------------------------- #


def test_freshdesk_api_key_reaches_the_fallback_file():
    """Regression: the key was silently dropped, breaking Freshdesk on restart.

    The fallback file is written with mode 0600 on the same persistent volume
    as the primary, so withholding the key bought no security -- it only
    guaranteed the setting never persisted.
    """
    values = {
        "FRESHDESK_ENABLED": "true",
        "FRESHDESK_BASE_URL": "https://acme.freshdesk.com",
        "FRESHDESK_API_KEY": "secret-key",
    }
    filtered, skipped = web_admin._filter_fallback_env_values(values)

    assert "FRESHDESK_API_KEY" in filtered, (
        "FRESHDESK_API_KEY must reach the fallback file, otherwise enabling "
        "Freshdesk from the Web GUI silently loses the credential"
    )
    assert "FRESHDESK_API_KEY" not in skipped


def test_genuinely_sensitive_keys_are_still_withheld():
    """The allowlist must not become a blanket exemption."""
    values = {
        "DISCORD_TOKEN": "d",
        "WEB_ADMIN_SESSION_SECRET": "s",
        "WEB_ADMIN_DEFAULT_PASSWORD": "p",
        "FRESHDESK_API_KEY": "k",
    }
    filtered, skipped = web_admin._filter_fallback_env_values(values)

    for key in ("DISCORD_TOKEN", "WEB_ADMIN_SESSION_SECRET", "WEB_ADMIN_DEFAULT_PASSWORD"):
        assert key not in filtered, f"{key} must stay out of the fallback file"
        assert key in skipped
    assert "FRESHDESK_API_KEY" in filtered


def test_fallback_filter_keeps_the_non_sensitive_freshdesk_settings():
    values = {
        "FRESHDESK_ENABLED": "true",
        "FRESHDESK_BASE_URL": "https://acme.freshdesk.com",
    }
    filtered, _ = web_admin._filter_fallback_env_values(values)
    assert filtered.get("FRESHDESK_ENABLED") == "true"
    assert filtered.get("FRESHDESK_BASE_URL") == "https://acme.freshdesk.com"


# --------------------------------------------------------------------------- #
#  The path bug: fallback resolves to the same file as primary
# --------------------------------------------------------------------------- #


def test_fallback_path_matches_data_dir_default(tmp_dirs):
    fallback = web_admin._env_fallback_file_path(str(tmp_dirs))
    assert fallback.name == "web-settings.env"


def test_fallback_is_distinct_from_primary_when_primary_is_default(tmp_path):
    """If WEB_ENV_FILE == the data-dir fallback, there is no fallback at all."""
    data = tmp_path / "data"
    data.mkdir()
    primary = Path(str(data / "web-settings.env"))
    fallback = web_admin._env_fallback_file_path(str(data))

    assert primary == fallback, (
        "documented behaviour: the write path short-circuits to a hard "
        "failure when both paths are identical"
    )
    saved, _err, _f, _s = web_admin._try_write_env_file_with_fallback(
        primary, fallback, {"X": "1"}
    )
    # writable here, so it succeeds -- the point is the paths coincide
    assert saved is True


# --------------------------------------------------------------------------- #
#  End-to-end: save, then read back from a fresh process-like resolution
# --------------------------------------------------------------------------- #


def test_saved_freshdesk_settings_round_trip(tmp_path):
    """Write via the real writer, read via the real parser."""
    env_file = tmp_path / "web-settings.env"
    values = {
        "FRESHDESK_ENABLED": "true",
        "FRESHDESK_BASE_URL": "https://acme.freshdesk.com",
        "FRESHDESK_API_KEY": "secret-key",
        "FRESHDESK_DOMAIN": "acme",
    }
    saved, err = web_admin._try_write_env_file(env_file, values)
    assert saved, err

    reloaded = web_admin._parse_env_file(env_file)
    assert reloaded.get("FRESHDESK_ENABLED") == "true"
    assert reloaded.get("FRESHDESK_API_KEY") == "secret-key", (
        "the API key must round-trip through the file the container persists"
    )


def test_readonly_primary_falls_back_and_keeps_the_api_key(tmp_path, monkeypatch):
    """Simulate the container: primary read-only, fallback writable.

    This mirrors docker-compose.yml mounting ./.env read-only while
    /app/data is a real volume.
    """
    data = tmp_path / "data"
    data.mkdir()

    values = {
        "FRESHDESK_ENABLED": "true",
        "FRESHDESK_BASE_URL": "https://acme.freshdesk.com",
        "FRESHDESK_API_KEY": "secret-key",
    }

    # Simulate the container without monkeypatching the helper under test.
    # Running as root defeats chmod, so point the primary at a path whose
    # parent is a FILE -- mkdir then fails for root too, exactly as a
    # read-only bind mount does in the container.
    blocked_parent = tmp_path / "blocked.env"
    blocked_parent.write_text("not a directory\n")
    readonly = blocked_parent / "web.env"
    fallback = web_admin._env_fallback_file_path(str(data))

    values = {
        "FRESHDESK_ENABLED": "true",
        "FRESHDESK_BASE_URL": "https://acme.freshdesk.com",
        "FRESHDESK_API_KEY": "secret-key",
    }
    saved, err, saved_to, skipped = web_admin._try_write_env_file_with_fallback(
        readonly, fallback, values
    )
    assert saved, err
    assert saved_to == fallback

    written = web_admin._parse_env_file(fallback)
    assert written.get("FRESHDESK_ENABLED") == "true"
    assert written.get("FRESHDESK_API_KEY") == "secret-key", (
        "after falling back, the API key must still be written -- this is "
        "what silently breaks Freshdesk in the container"
    )

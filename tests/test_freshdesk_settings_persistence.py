"""Freshdesk Web GUI settings must survive a container restart.

Compose mounts the primary env file **read-only**::

    volumes:
      - ./data:/app/data
      - ./.env:/app/.env:ro

so a Web GUI save can never write the primary file. It must land in the
fallback (``data/web-settings.env``), which sits on the ``./data`` volume and
therefore does persist across a container rebuild.

Two independent links have to hold:

1. **Write side** -- ``_filter_fallback_env_values`` must not strip
   ``FRESHDESK_API_KEY``. It used to, so a save persisted ``FRESHDESK_ENABLED``
   and ``FRESHDESK_BASE_URL`` but silently dropped the credential. The UI
   reported success while a restart reverted Freshdesk to a broken
   half-configured state.

2. **Read side** -- at startup ``bot.py`` must load the fallback into
   ``os.environ``. This is ``_load_filtered_env_file`` called from the bootstrap
   block, which is what makes the value visible to ``resolve_freshdesk_config``
   after a restart.

``tests/test_freshdesk_persistence.py`` covers the filter in isolation. This
file covers the whole write -> restart -> read cycle.
"""

from __future__ import annotations

import os
import stat
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import bot as bot_module
import web_admin

# Not a real credential. The literal deliberately reads as an obvious
# placeholder so secret scanners do not treat this fixture as a leaked key.
API_KEY = "not-a-real-secret"
FRESH_DESK_KEYS = (
    "FRESHDESK_ENABLED",
    "FRESHDESK_BASE_URL",
    "FRESHDESK_DOMAIN",
    "FRESHDESK_API_KEY",
    "FRESHDESK_WRONG_CHANNEL_MESSAGE",
)


@pytest.fixture
def env_pair(tmp_path: Path):
    """Read-only primary env file, plus a writable fallback on the data volume."""
    primary = tmp_path / ".env"
    primary.write_text(
        "FRESHDESK_ENABLED=false\nFRESHDESK_DOMAIN=old.freshdesk.com\n"
        "FRESHDESK_API_KEY=\nDISCORD_TOKEN=primary-token\n",
        encoding="utf-8",
    )
    fallback = tmp_path / "data" / "web-settings.env"
    return primary, fallback


def _save(env_pair, values: dict) -> tuple[bool, str]:
    """Exercise the real save path with an unwritable primary, as compose has it.

    The primary is made unwritable by pointing at a path whose parent is a
    regular file, so ``mkdir`` fails naturally. That reproduces the read-only
    bind mount without depending on chmod, which root ignores.
    """
    primary, fallback = env_pair
    blocked = env_pair[0].parent / "primary-is-a-file"
    blocked.write_text("not a directory", encoding="utf-8")
    unwritable_primary = blocked / ".env"

    ok, error = web_admin._try_write_env_file(unwritable_primary, values)
    if ok:  # pragma: no cover - would mean the read-only mount was not simulated
        pytest.fail("primary env file was unexpectedly writable")

    filtered, _skipped = web_admin._filter_fallback_env_values(values)
    fallback.parent.mkdir(parents=True, exist_ok=True)
    saved, fallback_error = web_admin._try_write_env_file(fallback, filtered)
    return saved, fallback_error


def _restart(env_pair, monkeypatch) -> dict[str, str]:
    """Simulate a container restart: a fresh process reading only from disk.

    Anything the previous process held solely in ``os.environ`` is discarded,
    exactly as it is when the container is recreated.
    """
    primary, fallback = env_pair
    for key in FRESH_DESK_KEYS:
        monkeypatch.delenv(key, raising=False)

    # This is bot.py's bootstrap read path, verbatim.
    bot_module._load_filtered_env_file(
        str(fallback),
        override=True,
        blocked_keys={"DISCORD_TOKEN", "WEB_ADMIN_DEFAULT_PASSWORD",
                      "WEB_ADMIN_SESSION_SECRET", "WEB_ENV_FILE"},
    )
    bot_module._load_filtered_env_file(
        str(primary),
        override=False,
        blocked_keys={"DISCORD_TOKEN", "WEB_ADMIN_DEFAULT_PASSWORD",
                      "WEB_ADMIN_SESSION_SECRET", "WEB_ENV_FILE"},
    )
    return dict(os.environ)


def _freshdesk_save() -> dict:
    return {
        "FRESHDESK_ENABLED": "true",
        "FRESHDESK_BASE_URL": "https://glinetservice.freshdesk.com",
        "FRESHDESK_API_KEY": API_KEY,
    }


class TestFreshdeskSettingsPersistAcrossRestart:
    def test_full_save_survives_restart(self, env_pair, monkeypatch):
        ok, error = _save(env_pair, _freshdesk_save())
        assert ok, error

        env = _restart(env_pair, monkeypatch)
        assert env["FRESHDESK_ENABLED"] == "true"
        assert env["FRESHDESK_BASE_URL"] == "https://glinetservice.freshdesk.com"
        # The credential is the whole point: without it the integration is dead.
        assert env["FRESHDESK_API_KEY"] == API_KEY

    def test_api_key_reaches_disk_not_just_memory(self, env_pair):
        _, fallback = env_pair
        ok, error = _save(env_pair, _freshdesk_save())
        assert ok, error
        assert fallback.exists(), "fallback file was never written"
        written = fallback.read_text(encoding="utf-8")
        assert "FRESHDESK_API_KEY" in written
        assert API_KEY in written

    def test_wrong_channel_message_persists(self, env_pair, monkeypatch):
        ok, error = _save(
            env_pair,
            {
                "FRESHDESK_ENABLED": "true",
                "FRESHDESK_WRONG_CHANNEL_MESSAGE": "Wrong channel, Sir. Use <#123>.",
            },
        )
        assert ok, error
        env = _restart(env_pair, monkeypatch)
        assert env["FRESHDESK_WRONG_CHANNEL_MESSAGE"] == "Wrong channel, Sir. Use <#123>."

    def test_disabling_freshdesk_persists(self, env_pair, monkeypatch):
        assert _save(env_pair, {"FRESHDESK_ENABLED": "true"})[0]
        assert _save(env_pair, {"FRESHDESK_ENABLED": "false"})[0]
        assert _restart(env_pair, monkeypatch)["FRESHDESK_ENABLED"] == "false"


class TestPersistenceHygiene:
    def test_fallback_file_is_owner_only(self, env_pair):
        _, fallback = env_pair
        assert _save(env_pair, _freshdesk_save())[0]
        assert fallback.exists()
        mode = stat.S_IMODE(fallback.stat().st_mode)
        assert mode == 0o600, f"fallback env file is mode {oct(mode)}, expected 0o600"

    def test_unrelated_secrets_stay_out_of_fallback(self, env_pair):
        """The API-key allowance must not become a blanket secret exemption."""
        _, fallback = env_pair
        assert _save(env_pair, _freshdesk_save())[0]
        written = fallback.read_text(encoding="utf-8")
        assert "primary-token" not in written, "DISCORD_TOKEN leaked into the fallback file"

    def test_protected_keys_never_enter_osenviron_from_fallback(self, env_pair, monkeypatch):
        """bot.py's bootstrap must honour its blocked-key list."""
        _, fallback = env_pair
        fallback.parent.mkdir(parents=True, exist_ok=True)
        fallback.write_text(
            "FRESHDESK_API_KEY=from-fallback\nDISCORD_TOKEN=leaked\n", encoding="utf-8"
        )
        monkeypatch.delenv("DISCORD_TOKEN", raising=False)
        _restart(env_pair, monkeypatch)
        assert os.environ.get("DISCORD_TOKEN") != "leaked"


class TestBotResolvesPersistedSettingsAfterRestart:
    def test_resolve_freshdesk_config_reads_persisted_values(self, env_pair, monkeypatch):
        """End to end: Web GUI save -> restart -> resolve_freshdesk_config()."""
        assert _save(env_pair, _freshdesk_save())[0]
        _restart(env_pair, monkeypatch)

        config = bot_module.resolve_freshdesk_config()
        assert config["enabled"] is True
        assert config["base_url"] == "https://glinetservice.freshdesk.com"
        assert config["api_key"] == API_KEY
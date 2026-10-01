"""Guild-id type consistency between the Web GUI and the command runtime.

The Web GUI saves command permissions with a *string* guild id:

    selected_guild_id = str(selected_guild.get("id") or "")
    on_save_command_permissions({...}, user["email"], selected_guild_id)

while the command runtime resolves with an int (interaction.guild.id).

save_command_permission_rules() normalises via normalize_target_guild_id()
before touching SQLite, so the column should always receive an int. These
tests pin that: if a raw string ever reaches the INSERT, SQLite stores TEXT
and the later int-keyed SELECT silently misses the row -- the rule is saved
but never read back, so the command reverts to its default.
"""

from __future__ import annotations

import sqlite3
import sys
import threading
import types
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import app.command_permissions as cp

GUILD = 1136164306522751017


@pytest.fixture
def env(monkeypatch, tmp_path):
    conn = sqlite3.connect(tmp_path / "bot.db", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE command_permissions ("
        "guild_id, command_key, mode, role_ids_json, updated_at)"
    )
    conn.execute("CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT)")
    conn.commit()

    kv = {}

    class Ctx:
        def __init__(self, commit=False):
            self.commit = commit
        def __enter__(self):
            return conn
        def __exit__(self, *a):
            if self.commit:
                conn.commit()
            return False

    def normalize(raw, default=None):
        try:
            gid = int(str(raw).strip())
        except (TypeError, ValueError, AttributeError):
            return None
        return gid if gid > 0 else None

    mod = types.ModuleType("bot")
    mod.COMMAND_PERMISSION_DEFAULTS = {"create_ticket": {"mode": "moderator", "role_ids": []}}
    mod.command_permissions_cache = {}
    mod.command_permissions_lock = threading.RLock()
    mod.with_db = Ctx
    mod.normalize_target_guild_id = normalize
    mod.db_kv_get = kv.get
    mod.db_kv_set = lambda k, v: kv.__setitem__(k, v)
    monkeypatch.setitem(sys.modules, "bot", mod)
    return mod, conn, kv


def test_stored_guild_id_column_is_an_integer(env):
    """A string guild id must be coerced to int before INSERT."""
    mod, conn, _ = env
    cp.save_command_permission_rules(
        {"create_ticket": {"mode": "public", "role_ids": []}},
        actor_email="admin@example.com",
        guild_id=str(GUILD),          # exactly what the Web GUI sends
    )
    rows = conn.execute("SELECT guild_id, typeof(guild_id) AS t FROM command_permissions").fetchall()
    assert rows, "nothing was stored"
    for row in rows:
        assert row["t"] == "integer", (
            f"guild_id stored as {row['t']!r}; an int-keyed SELECT will never "
            "match a TEXT row, so the saved rule is invisible at runtime"
        )


def test_string_write_is_readable_by_int_runtime(env):
    """The end-to-end shape: GUI writes a string, the command reads an int."""
    mod, conn, _ = env
    cp.save_command_permission_rules(
        {"create_ticket": {"mode": "public", "role_ids": []}},
        actor_email="admin@example.com",
        guild_id=str(GUILD),
    )
    mod.command_permissions_cache.clear()

    rules = cp.load_command_permission_rules(GUILD)   # int, as the command passes
    assert rules.get("create_ticket", {}).get("mode") == "public", (
        "rule saved with a string guild id was not readable with an int"
    )


def test_version_key_uses_the_same_normalised_guild_id(env):
    """Reader and writer must derive the identical cache-version key."""
    mod, conn, kv = env
    cp.save_command_permission_rules(
        {"create_ticket": {"mode": "public", "role_ids": []}},
        actor_email="admin@example.com",
        guild_id=str(GUILD),
    )
    assert f"command_permissions_updated_at:{GUILD}" in kv, (
        f"stored keys are {list(kv)} -- reader looks up an int-derived key"
    )


def test_restart_reads_the_row_back(env):
    """Cold cache after restart must still see the public rule."""
    mod, conn, _ = env
    cp.save_command_permission_rules(
        {"create_ticket": {"mode": "public", "role_ids": []}},
        actor_email="admin@example.com",
        guild_id=str(GUILD),
    )
    mod.command_permissions_cache.clear()   # simulates the container restart
    rules = cp.load_command_permission_rules(GUILD)
    assert rules["create_ticket"]["mode"] == "public"

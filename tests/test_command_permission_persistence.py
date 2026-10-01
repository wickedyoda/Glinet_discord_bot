"""Command permissions must survive a container restart.

Symptom: after restarting the container or reloading the bot,
/create-ticket reverts to its default (moderator-only) instead of staying
public.

Persistence itself works: save_command_permission_rules() writes to the
command_permissions table. The suspect is the read-side cache.

load_command_permission_rules() gates its cache on

    cache_entry.get("mtime") == db_kv_get(f"command_permissions_updated_at:{guild_id}")

and the cache is an in-process dict. On restart the dict is empty, so the
cache is cold and the DB is read -- that path is fine.

But the version key is the suspect. If save writes
db_kv_set("command_permissions_updated_at:<guild>") while the DELETE+INSERT
above commits in a separate transaction, or if the key is written with a
different guild-id normalisation than the reader uses, the reader can see a
stale version and return a cached (default) rule set.

These tests pin the observable requirement: a saved non-default rule is
read back after a simulated restart (cache cleared, DB untouched).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import app.command_permissions as cp


@pytest.fixture
def bot_stub(monkeypatch, tmp_path):
    """Minimal in-memory stand-in for the bot module the helpers import."""
    import types

    db_file = tmp_path / "bot.db"
    conn_box = {}

    import sqlite3

    conn = sqlite3.connect(db_file, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE command_permissions (
            guild_id INTEGER,
            command_key TEXT,
            mode TEXT,
            role_ids_json TEXT,
            updated_at TEXT
        )
        """
    )
    conn.execute("CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT)")
    conn.commit()
    conn_box['c'] = conn

    kv = {}

    class _Ctx:
        def __init__(self, commit=False):
            self.commit = commit
        def __enter__(self):
            return conn_box['c']
        def __exit__(self, *a):
            if self.commit:
                conn_box['c'].commit()
            return False

    mod = types.ModuleType("bot")
    mod.COMMAND_PERMISSION_DEFAULTS = {
        "create_ticket": {"mode": "moderator", "role_ids": []},
        "ban_member": {"mode": "administrator", "role_ids": []},
        "search_reddit": {"mode": "moderator", "role_ids": []},
    }
    mod.command_permissions_cache = {}
    mod.command_permissions_lock = __import__("threading").RLock()
    mod.with_db = _Ctx
    mod.normalize_target_guild_id = lambda g: None if g in (None, "", "null") else int(g)
    mod.db_kv_get = lambda k: kv.get(k)
    mod.db_kv_set = lambda k, v: kv.__setitem__(k, v)
    mod.COMMAND_PERMISSION_METADATA = dict(mod.COMMAND_PERMISSION_DEFAULTS)
    mod.COMMAND_PERMISSION_MODE_CUSTOM_ROLES = "custom_roles"
    mod.normalize_permission_mode = cp.normalize_permission_mode
    mod.normalize_role_ids = cp.normalize_role_ids
    mod.logger = __import__("logging").getLogger("stub")

    monkeypatch.setitem(sys.modules, "bot", mod)
    return mod, kv


GUILD = 1136164306522751017


def test_unsaved_command_falls_back_to_its_default_policy(bot_stub):
    """Baseline: with nothing stored, the command uses its declared default.

    load_command_permission_rules() returns only explicitly-saved rules, so an
    absent key means "use COMMAND_PERMISSION_DEFAULTS", which is
    moderator-only for create_ticket.
    """
    mod, _ = bot_stub
    rules = cp.load_command_permission_rules(GUILD)
    assert "create_ticket" not in rules, (
        "an unsaved command must not appear in the stored rule set"
    )
    default_policy = mod.COMMAND_PERMISSION_DEFAULTS["create_ticket"]
    assert default_policy.get("mode") == "moderator", (
        "the declared default is moderator-only, which is what the user sees "
        "when their public setting is lost"
    )


def test_saved_public_rule_survives_a_restart(bot_stub):
    """The regression: save public, simulate restart, must still be public."""
    mod, kv = bot_stub

    cp.save_command_permission_rules(
        {"create_ticket": {"mode": "public", "role_ids": []}},
        actor_email="admin@example.com",
        guild_id=GUILD,
    )

    # Simulate the restart: the in-process cache is gone, the DB is not.
    mod.command_permissions_cache.clear()

    rules = cp.load_command_permission_rules(GUILD)
    assert rules.get("create_ticket", {}).get("mode") == "public", (
        "/create-ticket reverted to its default after a simulated restart"
    )


def test_reader_and_writer_agree_on_the_version_key(bot_stub):
    """The cache version key must be computed identically on both sides."""
    mod, kv = bot_stub
    cp.save_command_permission_rules(
        {"create_ticket": {"mode": "public", "role_ids": []}},
        actor_email="admin@example.com",
        guild_id=GUILD,
    )
    mod.command_permissions_cache.clear()
    cp.load_command_permission_rules(GUILD)
    expected = f"command_permissions_updated_at:{GUILD}"
    assert expected in kv, (
        f"writer stored {list(kv)} -- the reader looks up {expected!r}"
    )


def test_default_mode_is_not_persisted(bot_stub):
    """A rule equal to the default should not be stored at all."""
    mod, _ = bot_stub
    cp.save_command_permission_rules(
        {"create_ticket": {"mode": "default", "role_ids": []}},
        actor_email="admin@example.com",
        guild_id=GUILD,
    )
    mod.command_permissions_cache.clear()
    rules = cp.load_command_permission_rules(GUILD)
    assert "create_ticket" not in rules or rules["create_ticket"].get("mode") == "public"

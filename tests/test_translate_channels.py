from __future__ import annotations

import os
import tempfile
import threading

os.environ.setdefault("DISCORD_TOKEN", "mock-token-for-tests")
os.environ.setdefault("GUILD_ID", "123456789")
os.environ.setdefault("DATA_DIR", "/tmp/bot-test-data")

from app.translate_channels import AutoTranslateChannelStore


def _new_store() -> tuple[AutoTranslateChannelStore, str]:
    tmpdir = tempfile.mkdtemp()
    db_path = os.path.join(tmpdir, "translate_channels.db")
    lock = threading.RLock()
    store = AutoTranslateChannelStore(db_path, lock)
    return store, db_path


def test_create_and_list():
    store, _ = _new_store()
    store.upsert(123, 456, 789, target_language="en")
    mappings = store.list_for_guild(123)
    assert len(mappings) == 1
    assert mappings[0]["source_channel_id"] == 456
    assert mappings[0]["target_channel_id"] == 789
    assert mappings[0]["target_language"] == "en"
    assert mappings[0]["source_language"] == "auto"
    assert mappings[0]["enabled"] is True


def test_upsert_updates_existing():
    store, _ = _new_store()
    store.upsert(123, 456, 789, target_language="en", enabled=True)
    store.upsert(123, 456, 789, target_language="en", enabled=False)
    mappings = store.list_for_guild(123)
    assert len(mappings) == 1
    assert mappings[0]["enabled"] is False


def test_distinct_target_languages_create_distinct_rows():
    store, _ = _new_store()
    store.upsert(123, 456, 789, target_language="en")
    store.upsert(123, 456, 789, target_language="es")
    mappings = store.list_for_guild(123)
    languages = sorted(m["target_language"] for m in mappings)
    assert languages == ["en", "es"]


def test_list_active_filters_disabled():
    store, _ = _new_store()
    store.upsert(123, 456, 789, target_language="en", enabled=True)
    store.upsert(123, 456, 790, target_language="es", enabled=False)
    active = store.list_active_for_source(123, 456)
    assert len(active) == 1
    assert active[0]["target_language"] == "en"


def test_delete():
    store, _ = _new_store()
    store.upsert(123, 456, 789, target_language="en")
    removed = store.delete(123, 456, 789, "en")
    assert removed is True
    assert store.list_for_guild(123) == []
    # Second delete returns False
    assert store.delete(123, 456, 789, "en") is False


def test_guild_isolation():
    store, _ = _new_store()
    store.upsert(111, 100, 200, target_language="en")
    store.upsert(222, 100, 200, target_language="en")
    assert len(store.list_for_guild(111)) == 1
    assert len(store.list_for_guild(222)) == 1
    assert len(store.list_for_guild(333)) == 0


def test_to_json():
    store, _ = _new_store()
    store.upsert(123, 456, 789, target_language="fr", source_language="auto")
    json_str = store.to_json(123)
    assert "456" in json_str
    assert "789" in json_str
    assert "fr" in json_str


def test_persistence_across_instances():
    tmpdir = tempfile.mkdtemp()
    db_path = os.path.join(tmpdir, "translate_channels.db")
    lock = threading.RLock()
    store1 = AutoTranslateChannelStore(db_path, lock)
    store1.upsert(123, 456, 789, target_language="en")
    store2 = AutoTranslateChannelStore(db_path, lock)
    mappings = store2.list_for_guild(123)
    assert len(mappings) == 1
    assert mappings[0]["target_channel_id"] == 789


def test_set_enabled():
    store, _ = _new_store()
    store.upsert(123, 456, 789, target_language="en", enabled=True)
    assert store.set_enabled(123, 456, 789, "en", False) is True
    mappings = store.list_for_guild(123)
    assert mappings[0]["enabled"] is False
    # Setting enabled on non-existent row returns False
    assert store.set_enabled(123, 999, 789, "en", True) is False

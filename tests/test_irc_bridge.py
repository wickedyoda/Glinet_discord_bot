from __future__ import annotations

import os
import tempfile
import threading

from app.irc_bridge_store import IRCBridgeStore
from app.irc_bridge_types import IRCChannelMapping, IRCServerConfig


def test_server_crud():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "bot_data.db")
        store = IRCBridgeStore(path, threading.RLock())
        store.init_schema()
        server = IRCServerConfig(id=1, name="Test", host="irc.example.com", port=6667, nickname="Bot")
        saved = store.upsert_server(server)
        assert saved.id == 1
        got = store.get_server(1)
        assert got is not None
        assert got["host"] == "irc.example.com"
        servers = store.list_servers()
        assert len(servers) == 1


def test_mapping_crud():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "bot_data.db")
        store = IRCBridgeStore(path, threading.RLock())
        store.init_schema()
        store.upsert_server(IRCServerConfig(id=1, name="Test", host="irc.example.com", port=6667, nickname="Bot"))
        mapping = IRCChannelMapping(id=1, server_id=1, guild_id=10, discord_channel_id=11, irc_channel="#test")
        saved = store.upsert_mapping(mapping)
        assert saved.id == 1
        got = store.get_mapping(1)
        assert got is not None
        assert got["discord_channel_id"] == 11
        mappings = store.list_mappings_for_discord_channel(10, 11)
        assert len(mappings) == 1


def test_delete_flow():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "bot_data.db")
        store = IRCBridgeStore(path, threading.RLock())
        store.init_schema()
        store.upsert_server(IRCServerConfig(id=1, name="Test", host="irc.example.com", port=6667, nickname="Bot"))
        mapping = IRCChannelMapping(id=1, server_id=1, guild_id=10, discord_channel_id=11, irc_channel="#test")
        store.upsert_mapping(mapping)
        assert store.get_mapping(1) is not None
        store.delete_mapping(1)
        assert store.get_mapping(1) is None
        assert store.list_mappings_for_discord_channel(10, 11) == []

from __future__ import annotations

import threading

from app.irc_bridge_service import IRCBridgeService


def test_service_initializes_schema():
    service = IRCBridgeService(None, ":memory:", threading.RLock())
    assert service._store is not None

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass


@dataclass
class IRCBridgeConfig:
    enabled: bool = False
    reconnect_delay_seconds: int = 10
    max_reconnect_delay_seconds: int = 300
    ping_timeout_seconds: int = 120


@dataclass
class IRCServerConfig:
    id: int
    name: str
    host: str
    port: int = 6667
    use_tls: bool = False
    password: str | None = None
    nickname: str = "BridgeBot"
    username: str = "bridgebot"
    realname: str = "BridgeBot"
    reconnect_delay_seconds: int = 10
    max_reconnect_delay_seconds: int = 300
    ping_timeout_seconds: int = 120
    enabled: bool = True

    def _to_updatable(self) -> dict[str, object]:
        return {
            "name": self.name,
            "host": self.host,
            "port": self.port,
            "use_tls": self.use_tls,
            "password": self.password,
            "nickname": self.nickname,
            "username": self.username,
            "realname": self.realname,
            "reconnect_delay_seconds": self.reconnect_delay_seconds,
            "max_reconnect_delay_seconds": self.max_reconnect_delay_seconds,
            "ping_timeout_seconds": self.ping_timeout_seconds,
            "enabled": self.enabled,
        }


@dataclass
class IRCChannelMapping:
    id: int | None = None
    guild_id: int = 0
    server_id: int = 0
    irc_channel: str = ""
    discord_channel_id: int = 0
    enabled: bool = True
    created_at: str = ""


@dataclass
class IRCBridgeState:
    servers: dict[int, IRCServerConfig] = field(default_factory=dict)
    mappings: dict[int, IRCChannelMapping] = field(default_factory=dict)
    connections: dict[int, object] = field(default_factory=dict)
    bridge_enabled: bool = False


# Shared bridge state protected by bot-level lock
_bridge_state = IRCBridgeState()
_bridge_lock = threading.RLock()


def get_bridge_state() -> IRCBridgeState:
    with _bridge_lock:
        return _bridge_state


def get_bridge_lock() -> threading.RLock:
    return _bridge_lock

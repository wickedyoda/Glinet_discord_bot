from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections.abc import Callable
from typing import Any

from irc.bot import SingleServerIRCBot

from app.irc_bridge_types import IRCServerConfig, get_bridge_state

logger = logging.getLogger(__name__)


class _IRCClient(SingleServerIRCBot):
    def __init__(self, server: IRCServerConfig, on_message) -> None:
        super().__init__(
            [(server.host, server.port)],
            server.nickname,
            username=server.username,
            realname=server.realname,
        )
        self._server = server
        self._on_message = on_message
        self._connected = False

    def on_welcome(self, connection, event) -> None:
        self._connected = True
        logger.info("IRC connected to %s:%s as %s", self._server.host, self._server.port, self._server.nickname)
        if self._server.password:
            connection.privmsg("NickServ", f"IDENTIFY {self._server.password}")
        for mapping in self._pending_channels():
            channel = mapping.irc_channel.strip()
            if channel:
                connection.join(channel)

    def _pending_channels(self):
        try:
            return [m for m in get_bridge_state().mappings.values() if m.enabled and m.server_id == self._server.id]
        except Exception:  # noqa: BLE001
            return []

    def on_disconnect(self, connection, event) -> None:
        self._connected = False
        logger.warning("IRC disconnected from %s", self._server.host)

    def on_pubmsg(self, connection, event) -> None:
        try:
            source = event.source.nick if event.source else ""
            target = event.target or ""
            text = event.arguments[0] if event.arguments else ""
            self._on_message(server_id=self._server.id, source=source, irc_channel=target, text=text)
        except Exception:  # noqa: BLE001
            logger.exception("Error handling IRC pubmsg")

    def on_privmsg(self, connection, event) -> None:
        try:
            source = event.source.nick if event.source else ""
            text = event.arguments[0] if event.arguments else ""
            self._on_message(server_id=self._server.id, source=source, irc_channel=source, text=text)
        except Exception:  # noqa: BLE001
            logger.exception("Error handling IRC privmsg")

    def on_join(self, connection, event) -> None:
        try:
            nick = event.source.nick if event.source else ""
            if nick == self._server.nickname:
                logger.info("IRC joined %s on %s", event.target, self._server.host)
        except Exception:  # noqa: BLE001
            logger.exception("Error handling IRC join")

    def on_part(self, connection, event) -> None:
        try:
            if event.source and event.source.nick == self._server.nickname:
                logger.info("IRC left %s on %s", event.target, self._server.host)
        except Exception:  # noqa: BLE001
            logger.exception("Error handling IRC part")

    def is_connected(self) -> bool:
        return self._connected


class IRCBridge:
    def __init__(self, send_to_discord: Callable[..., Any] | None = None) -> None:
        self._clients: dict[int, _IRCClient] = {}
        self._monitor_task = None
        self._lock = threading.RLock()
        self._last_connect_attempt: dict[int, float] = {}
        self._backoff: dict[int, float] = {}
        self._send_to_discord = send_to_discord

    async def start(self) -> None:
        get_bridge_state().bridge_enabled = True
        logger.info("IRC bridge starting")
        self._monitor_task = asyncio.create_task(self._monitor_loop(), name="irc_bridge_monitor")

    async def stop(self) -> None:
        get_bridge_state().bridge_enabled = False
        if self._monitor_task:
            self._monitor_task.cancel()
            self._monitor_task = None
        with self._lock:
            for server_id, client in list(self._clients.items()):
                try:
                    client.disconnect("Shutting down")
                except Exception:  # noqa: BLE001
                    logger.debug("Disconnect error for server %s", server_id)
            self._clients.clear()
        logger.info("IRC bridge stopped")

    async def _monitor_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(10)
                if not get_bridge_state().bridge_enabled:
                    break
                self._reconcile_connections()
            except asyncio.CancelledError:
                break
            except Exception:  # noqa: BLE001
                logger.exception("IRC bridge monitor error")

    def _reconcile_connections(self) -> None:
        state = get_bridge_state()
        desired = {s.id for s in state.servers.values() if s.enabled}
        with self._lock:
            for server_id in desired:
                server = state.servers.get(server_id)
                if not server:
                    continue
                client = self._clients.get(server_id)
                if client is None or not client.is_connected():
                    self._connect_server(server)
            for server_id in list(self._clients.keys()):
                if server_id not in desired:
                    try:
                        self._clients[server_id].disconnect("Removed")
                    except Exception:  # noqa: BLE001
                        pass
                    self._clients.pop(server_id, None)

    def _connect_server(self, server: IRCServerConfig) -> None:
        with self._lock:
            old = self._clients.pop(server.id, None)
            if old is not None:
                try:
                    old.disconnect("Reconnecting")
                except Exception:  # noqa: BLE001
                    pass
            now = time.time()
            last = self._last_connect_attempt.get(server.id, 0.0)
            delay = self._next_backoff(server.id)
            if now - last < delay:
                logger.debug("Skipping IRC connect for %s; backoff=%ss", server.host, round(delay, 1))
                return
            self._last_connect_attempt[server.id] = now
            try:
                client = _IRCClient(server, self._on_irc_message)
                thread = threading.Thread(target=client.start, daemon=True)
                thread.start()
                self._clients[server.id] = client
                logger.info("IRC connecting to %s:%s as %s", server.host, server.port, server.nickname)
            except Exception:  # noqa: BLE001
                logger.exception("Failed to start IRC client for %s", server.host)

    def _next_backoff(self, server_id: int) -> float:
        current = self._backoff.get(server_id, 0.0)
        state = get_bridge_state()
        server = state.servers.get(server_id)
        base = float(server.reconnect_delay_seconds if server else 10)
        cap = float(server.max_reconnect_delay_seconds if server else 300)
        new = min(current + base, cap) if current else base
        self._backoff[server_id] = new
        return new

    def _reset_backoff(self, server_id: int) -> None:
        self._backoff.pop(server_id, None)

    def _on_irc_message(self, *, server_id: int, source: str, irc_channel: str, text: str) -> None:
        mapping = next(
            (
                m
                for m in get_bridge_state().mappings.values()
                if m.enabled and m.server_id == server_id and m.irc_channel.lower() == irc_channel.lower()
            ),
            None,
        )
        if mapping is None:
            return
        if self._send_to_discord is None:
            return
        try:
            asyncio.get_running_loop().create_task(
                self._send_to_discord(
                    guild_id=mapping.guild_id,
                    discord_channel_id=mapping.discord_channel_id,
                    author=source,
                    text=text,
                )
            )
        except RuntimeError:
            logger.debug("No running loop for IRC->Discord relay")

    async def _send_to_discord(self, *, guild_id: int, discord_channel_id: int, author: str, text: str) -> None:
        if self._send_to_discord is None:
            return
        await self._send_to_discord(
            guild_id=guild_id,
            discord_channel_id=discord_channel_id,
            author=author,
            text=text,
        )

    async def send_to_irc(self, *, server_id: int, irc_channel: str, author: str, text: str) -> None:
        with self._lock:
            client = self._clients.get(server_id)
        if client is None or not client.is_connected():
            return
        try:
            client.connection.privmsg(irc_channel, f"[{author}] {text}")
            self._reset_backoff(server_id)
        except Exception:  # noqa: BLE001
            logger.exception("Failed to relay Discord message to IRC")
            self._backoff[server_id] = min((self._backoff.get(server_id, 0.0) + 10), 300)

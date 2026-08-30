from __future__ import annotations

import logging
import threading

from app.irc_bridge import IRCBridge
from app.irc_bridge_store import IRCBridgeStore

logger = logging.getLogger(__name__)


class IRCBridgeService:
    def __init__(self, bot, db_path: str, lock: threading.RLock) -> None:
        self._store = IRCBridgeStore(db_path, lock)
        self._store.init_schema()
        self._bridge = IRCBridge(send_to_discord=self._send_to_discord)
        self._bot = bot
        self._lock = lock

    async def _send_to_discord(self, *, guild_id: int, discord_channel_id: int, author: str, text: str) -> None:
        if self._bot is None:
            return
        try:
            guild = self._bot.get_guild(guild_id)
            if guild is None:
                return
            channel = guild.get_channel(discord_channel_id)
            if channel is None or not hasattr(channel, "send"):
                return
            await channel.send(f"[{author}] {text}")
        except Exception:  # noqa: BLE001
            logger.exception("Failed to relay IRC message to Discord")

    async def start(self) -> None:
        await self._bridge.start()

    async def stop(self) -> None:
        await self._bridge.stop()

    async def send_to_irc(self, *, server_id: int, irc_channel: str, author: str, text: str) -> None:
        await self._bridge.send_to_irc(server_id=server_id, irc_channel=irc_channel, author=author, text=text)

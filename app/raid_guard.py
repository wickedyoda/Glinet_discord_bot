"""Join-rate and account-age checks, for the first minute of a raid.

A raid is not one account; it is twenty in a minute. This keeps the join times per guild
in memory and answers two questions cheaply: is this a spike, and is this account too
new to trust. What the bot does about it -- quarantine, kick, lockdown -- is decided in
the bot, not here.
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field

RAID = "raid"
YOUNG_ACCOUNT = "young_account"

DEFAULT_MAX_JOINS_PER_MINUTE = 8
DEFAULT_MIN_ACCOUNT_AGE_HOURS = 24


@dataclass(frozen=True)
class RaidConfig:
    enabled: bool = False
    max_joins_per_minute: int = DEFAULT_MAX_JOINS_PER_MINUTE
    min_account_age_hours: int = DEFAULT_MIN_ACCOUNT_AGE_HOURS
    quarantine_role_id: int = 0
    auto_lockdown: bool = False
    lockdown_minutes: int = 30


@dataclass(frozen=True)
class JoinVerdict:
    code: str
    detail: str


@dataclass
class _GuildJoins:
    timestamps: deque = field(default_factory=lambda: deque(maxlen=256))


class JoinTracker:
    """Join times per guild, bounded and in memory."""

    def __init__(self) -> None:
        self._guilds: dict[int, _GuildJoins] = {}

    def record(self, guild_id: int, *, now: float | None = None) -> int:
        stamp = time.monotonic() if now is None else float(now)
        entry = self._guilds.setdefault(int(guild_id), _GuildJoins())
        entry.timestamps.append(stamp)
        cutoff = stamp - 60.0
        while entry.timestamps and entry.timestamps[0] < cutoff:
            entry.timestamps.popleft()
        return len(entry.timestamps)

    def joins_last_minute(self, guild_id: int, *, now: float | None = None) -> int:
        stamp = time.monotonic() if now is None else float(now)
        entry = self._guilds.get(int(guild_id))
        if entry is None:
            return 0
        cutoff = stamp - 60.0
        return sum(1 for value in entry.timestamps if value >= cutoff)

    def reset(self, guild_id: int) -> None:
        self._guilds.pop(int(guild_id), None)


def evaluate_join(
    tracker: JoinTracker,
    config: RaidConfig,
    *,
    guild_id: int,
    account_age_hours: float,
    now: float | None = None,
) -> JoinVerdict | None:
    """Record the join and return a verdict, or None when it looks ordinary."""
    if not config.enabled:
        return None

    joins = tracker.record(guild_id, now=now)

    if joins > config.max_joins_per_minute:
        return JoinVerdict(RAID, f"{joins} joins in the last minute")

    if config.min_account_age_hours > 0 and account_age_hours < config.min_account_age_hours:
        return JoinVerdict(
            YOUNG_ACCOUNT,
            f"account is {account_age_hours:.1f}h old, minimum {config.min_account_age_hours}h",
        )

    return None

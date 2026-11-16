"""The automod rules engine: decisions only, no Discord objects.

Every rule here answers one question -- "is this message a violation, and of what?" --
so the bot can decide what to *do* about it separately, and so the rules can be tested
without a server. The tracker keeps per-member state (message times, recent text) in
memory; nothing is written to the database on the hot path.

Defaults are deliberately conservative: the engine is off unless a server enables it,
and the thresholds are set so a normal conversation never trips them.
"""

from __future__ import annotations

import re
import time
import unicodedata
from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime

# Violation codes. The bot maps these to actions; the web UI shows them verbatim.
RATE = "rate"
DUPLICATE = "duplicate"
MENTIONS = "mentions"
CAPS = "caps"
EMOJI = "emoji"
ZALGO = "zalgo"
INVITE = "invite"
LINK = "link"
ATTACHMENT = "attachment"
REGEX = "regex"

DEFAULT_WINDOW_SECONDS = 10
DEFAULT_MAX_MESSAGES = 8
DEFAULT_DUPLICATE_COUNT = 4
DEFAULT_MAX_MENTIONS = 6
DEFAULT_MAX_CAPS_RATIO = 0.75
DEFAULT_MAX_EMOJI = 15
DEFAULT_MAX_ZALGO = 12
DEFAULT_MAX_ATTACHMENT_MB = 25

_INVITE_RE = re.compile(r"(?:discord\.(?:gg|com/invite)|discordapp\.com/invite)/[A-Za-z0-9\-_]+", re.I)
_URL_RE = re.compile(r"https?://([^/\s]+)", re.I)
_EMOJI_RE = re.compile(
    "[" "\U0001F300-\U0001FAFF" "\U00002600-\U000027BF" "\U0001F1E6-\U0001F1FF" "\U00002B00-\U00002BFF" "]"
)
_ZALGO_RE = re.compile("[\u0300-\u036f\u0489\u1ab0-\u1aff\u1dc0-\u1dff\u20d0-\u20ff\ufe20-\ufe2f]")

# Extensions that are executable on the platforms people actually use.
DEFAULT_BLOCKED_EXTENSIONS = (
    "exe", "scr", "bat", "cmd", "com", "pif", "msi", "vbs", "vbe", "js", "jse",
    "wsf", "wsh", "ps1", "psm1", "jar", "lnk", "hta", "cpl", "reg", "apk", "dll",
)


@dataclass(frozen=True)
class RegexRule:
    pattern: str
    reason: str = "matched a blocked pattern"

    def compiled(self):
        try:
            return re.compile(self.pattern, re.I)
        except re.error:
            return None


@dataclass(frozen=True)
class AutomodConfig:
    enabled: bool = False
    window_seconds: int = DEFAULT_WINDOW_SECONDS
    max_messages: int = DEFAULT_MAX_MESSAGES
    duplicate_count: int = DEFAULT_DUPLICATE_COUNT
    max_mentions: int = DEFAULT_MAX_MENTIONS
    max_caps_ratio: float = DEFAULT_MAX_CAPS_RATIO
    max_emoji: int = DEFAULT_MAX_EMOJI
    max_zalgo: int = DEFAULT_MAX_ZALGO
    block_invites: bool = True
    block_links: bool = False
    allowed_domains: tuple[str, ...] = ()
    blocked_extensions: tuple[str, ...] = DEFAULT_BLOCKED_EXTENSIONS
    max_attachment_mb: int = DEFAULT_MAX_ATTACHMENT_MB
    regex_rules: tuple[RegexRule, ...] = ()
    exempt_role_ids: tuple[int, ...] = ()
    exempt_channel_ids: tuple[int, ...] = ()
    quiet_start_hour: int | None = None
    quiet_end_hour: int | None = None
    quiet_multiplier: float = 1.0
    delete_on_violation: bool = True

    def is_quiet_hour(self, hour: int) -> bool:
        """True when the stricter overnight window is active."""
        if self.quiet_start_hour is None or self.quiet_end_hour is None:
            return False
        start, end = self.quiet_start_hour, self.quiet_end_hour
        if start == end:
            return False
        if start < end:
            return start <= hour < end
        return hour >= start or hour < end

    def effective(self, hour: int) -> AutomodConfig:
        """The config in force at a given hour: tighter thresholds overnight."""
        if not self.is_quiet_hour(hour) or self.quiet_multiplier <= 0:
            return self
        scale = self.quiet_multiplier
        return AutomodConfig(
            **{
                **self.__dict__,
                "max_messages": max(2, int(self.max_messages / scale)),
                "duplicate_count": max(2, int(self.duplicate_count / scale)),
                "max_mentions": max(1, int(self.max_mentions / scale)),
                "max_emoji": max(2, int(self.max_emoji / scale)),
            }
        )


@dataclass
class Verdict:
    code: str
    detail: str
    severity: int = 1

    def as_text(self) -> str:
        return f"{self.code}: {self.detail}"


@dataclass
class MemberState:
    timestamps: deque = field(default_factory=lambda: deque(maxlen=64))
    texts: deque = field(default_factory=lambda: deque(maxlen=16))


class MessageTracker:
    """Per-member message history, kept in memory and bounded."""

    def __init__(self) -> None:
        self._state: dict[tuple[int, int], MemberState] = {}

    def state_for(self, guild_id: int, user_id: int) -> MemberState:
        key = (int(guild_id), int(user_id))
        state = self._state.get(key)
        if state is None:
            state = MemberState()
            self._state[key] = state
        return state

    def forget(self, guild_id: int, user_id: int) -> None:
        self._state.pop((int(guild_id), int(user_id)), None)

    def size(self) -> int:
        return len(self._state)


def _normalise(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "").strip().lower())


def _caps_ratio(text: str) -> float:
    letters = [char for char in text if char.isalpha()]
    if len(letters) < 12:
        return 0.0
    upper = sum(1 for char in letters if char.isupper())
    return upper / len(letters)


def _emoji_count(text: str) -> int:
    return len(_EMOJI_RE.findall(text))


def _zalgo_count(text: str) -> int:
    return len(_ZALGO_RE.findall(text))


def _domains(text: str) -> list[str]:
    return [match.group(1).lower().split(":")[0] for match in _URL_RE.finditer(text or "")]


def _extension(name: str) -> str:
    _, _, ext = str(name or "").rpartition(".")
    return ext.lower()


def evaluate(
    tracker: MessageTracker,
    config: AutomodConfig,
    *,
    guild_id: int,
    user_id: int,
    text: str,
    mention_count: int = 0,
    attachments: list[dict] | None = None,
    now: float | None = None,
) -> Verdict | None:
    """Return the first violation this message commits, or None.

    Order matters: a flood is more urgent than shouting, and an invite is more urgent
    than a duplicate, so the most actionable reason wins.
    """
    if not config.enabled:
        return None

    stamp = time.monotonic() if now is None else float(now)
    hour = datetime.now(UTC).hour
    config = config.effective(hour)

    state = tracker.state_for(guild_id, user_id)

    # Rate: count messages inside the window, including this one.
    while state.timestamps and stamp - state.timestamps[0] > config.window_seconds:
        state.timestamps.popleft()
    state.timestamps.append(stamp)
    if len(state.timestamps) > config.max_messages:
        return Verdict(RATE, f"{len(state.timestamps)} messages in {config.window_seconds}s", severity=3)

    body = str(text or "")
    normalised = _normalise(body)
    if normalised:
        state.texts.append(normalised)
        repeats = sum(1 for previous in list(state.texts)[-config.duplicate_count :] if previous == normalised)
        if len(state.texts) >= config.duplicate_count and repeats >= config.duplicate_count:
            return Verdict(DUPLICATE, f"the same message {repeats} times", severity=2)

    if mention_count > config.max_mentions:
        return Verdict(MENTIONS, f"{mention_count} mentions", severity=3)

    if config.block_invites and _INVITE_RE.search(body):
        return Verdict(INVITE, "Discord invite link", severity=3)

    if config.block_links:
        allowed = tuple(domain.lower() for domain in config.allowed_domains)
        for domain in _domains(body):
            if not any(domain == ok or domain.endswith("." + ok) for ok in allowed):
                return Verdict(LINK, f"link to {domain}", severity=2)

    for rule in config.regex_rules:
        compiled = rule.compiled()
        if compiled is not None and compiled.search(body):
            return Verdict(REGEX, rule.reason, severity=2)

    if _caps_ratio(body) > config.max_caps_ratio:
        return Verdict(CAPS, "mostly capital letters", severity=1)

    if _emoji_count(body) > config.max_emoji:
        return Verdict(EMOJI, "emoji flood", severity=1)

    if _zalgo_count(body) > config.max_zalgo:
        return Verdict(ZALGO, "combining characters", severity=1)

    for attachment in attachments or []:
        name = str(attachment.get("filename") or "")
        size_mb = int(attachment.get("size") or 0) / (1024 * 1024)
        if size_mb > config.max_attachment_mb:
            return Verdict(ATTACHMENT, f"{name} is {size_mb:.1f} MB", severity=2)
        if _extension(name) in config.blocked_extensions:
            return Verdict(ATTACHMENT, f"{name} is an executable", severity=3)

    return None


def is_exempt(
    config: AutomodConfig, *, role_ids: list[int] | tuple[int, ...], channel_id: int
) -> bool:
    """Exemptions are checked before evaluation: staff and chosen channels are skipped."""
    if channel_id in config.exempt_channel_ids:
        return True
    return bool(set(int(role) for role in role_ids) & set(config.exempt_role_ids))


def normalise_text(value: str) -> str:
    """Strip zero-width and combining noise before a human reads a log line."""
    cleaned = unicodedata.normalize("NFKC", str(value or ""))
    return "".join(char for char in cleaned if unicodedata.category(char)[0] != "C")[:300]

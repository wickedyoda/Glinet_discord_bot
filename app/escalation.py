"""The escalation ladder: how many active warnings become a timeout, a kick, a ban.

Thresholds are data, not code. A server can change them without a release, and the
default ladder is deliberately gentle: three strikes is a timeout, five is a kick, seven
is a ban. Clearing a warning removes the strike, which is the point of clearing it.
"""

from __future__ import annotations

from dataclasses import dataclass

ACTION_NONE = "none"
ACTION_TIMEOUT = "timeout"
ACTION_KICK = "kick"
ACTION_BAN = "ban"

# (strikes, action, seconds) -- the seconds apply to timeouts only.
DEFAULT_LADDER: tuple[tuple[int, str, int], ...] = (
    (3, ACTION_TIMEOUT, 600),
    (5, ACTION_KICK, 0),
    (7, ACTION_BAN, 0),
)


@dataclass(frozen=True)
class LadderStep:
    strikes: int
    action: str
    seconds: int = 0


def parse_ladder(raw) -> tuple[LadderStep, ...]:
    """Accept [[3, "timeout", 600], ...] or the default when it is unusable."""
    steps: list[LadderStep] = []
    if isinstance(raw, (list, tuple)):
        for entry in raw:
            if not isinstance(entry, (list, tuple)) or len(entry) < 2:
                continue
            try:
                strikes = int(entry[0])
                action = str(entry[1]).strip().lower()
                seconds = int(entry[2]) if len(entry) > 2 else 0
            except (TypeError, ValueError):
                continue
            if strikes < 1 or action not in {ACTION_TIMEOUT, ACTION_KICK, ACTION_BAN}:
                continue
            steps.append(LadderStep(strikes, action, max(0, seconds)))
    if not steps:
        return tuple(LadderStep(*entry) for entry in DEFAULT_LADDER)
    return tuple(sorted(steps, key=lambda step: step.strikes))


def decide(strikes: int, ladder: tuple[LadderStep, ...] | None = None) -> LadderStep | None:
    """The step a member has reached, or None below the first rung.

    The highest rung at or below the strike count wins, so a member who jumps from two
    to eight strikes gets the ban, not the timeout.
    """
    steps = ladder or tuple(LadderStep(*entry) for entry in DEFAULT_LADDER)
    reached = None
    for step in steps:
        if strikes >= step.strikes:
            reached = step
    return reached


def describe(step: LadderStep | None) -> str:
    if step is None:
        return "no escalation"
    if step.action == ACTION_TIMEOUT:
        return f"timeout for {step.seconds // 60} minute(s) at {step.strikes} strike(s)"
    return f"{step.action} at {step.strikes} strike(s)"

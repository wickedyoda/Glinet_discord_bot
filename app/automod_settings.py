"""Reading, validating and normalising the automod settings the web form submits.

The form is text: comma-separated lists, one regex per line, numbers in boxes. This
module turns that into the JSON the bot stores, and refuses anything it cannot honour
rather than storing a value that will silently misbehave later.
"""

from __future__ import annotations

import math

import regex as _regex

from app.automod import (
    DEFAULT_BLOCKED_EXTENSIONS,
    DEFAULT_DUPLICATE_COUNT,
    DEFAULT_MAX_ATTACHMENT_MB,
    DEFAULT_MAX_CAPS_RATIO,
    DEFAULT_MAX_EMOJI,
    DEFAULT_MAX_MENTIONS,
    DEFAULT_MAX_MESSAGES,
    DEFAULT_MAX_ZALGO,
    DEFAULT_WINDOW_SECONDS,
    MAX_PATTERN_LENGTH,
)
from app.escalation import DEFAULT_LADDER, parse_ladder

MAX_REGEX_RULES = 25


def _int(raw, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        return default
    if value < minimum or value > maximum:
        return default
    return value


def _float(raw, default: float, minimum: float, maximum: float) -> float:
    try:
        value = float(str(raw).strip())
    except (TypeError, ValueError):
        return default
    # NaN compares false with everything, so a plain range check lets it through and it
    # explodes later in the multiplier. Only finite numbers are settings.
    if not math.isfinite(value) or value < minimum or value > maximum:
        return default
    return value


def _bool(raw) -> bool:
    return str(raw or "").strip().lower() in {"1", "true", "yes", "on"}


def _csv(raw) -> list[str]:
    return [part.strip() for part in str(raw or "").split(",") if part.strip()]


def _int_csv(raw) -> list[int]:
    values = []
    for token in _csv(raw):
        if token.isascii() and token.isdigit():
            values.append(int(token))
    return values


def parse_regex_lines(raw: str) -> list[dict]:
    """One rule per line: ``pattern`` or ``pattern => reason``.

    An invalid pattern is rejected here, with the line number, rather than being stored
    and quietly skipped at runtime.
    """
    rules: list[dict] = []
    for index, line in enumerate(str(raw or "").splitlines(), start=1):
        text = line.strip()
        if not text or text.startswith("#"):
            continue
        if "=>" in text:
            pattern, _, reason = text.partition("=>")
            pattern, reason = pattern.strip(), reason.strip()
        else:
            pattern, reason = text, "blocked pattern"
        if not pattern:
            continue
        # Validate exactly what will be stored: truncating after validation can turn a
        # valid pattern into an invalid one (a character class losing its bracket).
        if len(pattern) > MAX_PATTERN_LENGTH:
            raise ValueError(
                f"line {index}: the pattern is longer than {MAX_PATTERN_LENGTH} characters"
            )
        try:
            _regex.compile(pattern, _regex.IGNORECASE)
        except _regex.error as exc:
            raise ValueError(f"line {index}: {exc}") from exc
        rules.append({"pattern": pattern, "reason": (reason or "blocked pattern")[:120]})
        if len(rules) > MAX_REGEX_RULES:
            raise ValueError(f"at most {MAX_REGEX_RULES} regex rules")
    return rules


def normalize_automod(form: dict) -> tuple[dict, list[str]]:
    """Return (config, errors). Errors mean nothing is saved."""
    errors: list[str] = []
    try:
        regex_rules = parse_regex_lines(form.get("regex_rules", ""))
    except ValueError as exc:
        errors.append(str(exc))
        regex_rules = []

    extensions = _csv(form.get("blocked_extensions", "")) or list(DEFAULT_BLOCKED_EXTENSIONS)
    allowed = _csv(form.get("allowed_domains", ""))

    quiet_start = str(form.get("quiet_start_hour", "")).strip()
    quiet_end = str(form.get("quiet_end_hour", "")).strip()
    quiet_hours_valid = True
    if quiet_start or quiet_end:
        try:
            start = int(quiet_start)
            end = int(quiet_end)
            if not (0 <= start <= 23 and 0 <= end <= 23):
                raise ValueError
        except (TypeError, ValueError):
            errors.append("Quiet hours must be two whole hours between 0 and 23.")
            quiet_hours_valid = False

    config = {
        "enabled": _bool(form.get("enabled")),
        "window_seconds": _int(form.get("window_seconds"), DEFAULT_WINDOW_SECONDS, 3, 120),
        "max_messages": _int(form.get("max_messages"), DEFAULT_MAX_MESSAGES, 2, 60),
        "duplicate_count": _int(form.get("duplicate_count"), DEFAULT_DUPLICATE_COUNT, 2, 20),
        "max_mentions": _int(form.get("max_mentions"), DEFAULT_MAX_MENTIONS, 1, 50),
        "max_caps_ratio": _float(form.get("max_caps_ratio"), DEFAULT_MAX_CAPS_RATIO, 0.3, 1.0),
        "max_emoji": _int(form.get("max_emoji"), DEFAULT_MAX_EMOJI, 1, 100),
        "max_zalgo": _int(form.get("max_zalgo"), DEFAULT_MAX_ZALGO, 1, 100),
        "block_invites": _bool(form.get("block_invites")),
        "block_links": _bool(form.get("block_links")),
        "allowed_domains": [domain.lower()[:120] for domain in allowed][:50],
        "blocked_extensions": [ext.lower().lstrip(".")[:10] for ext in extensions][:60],
        "max_attachment_mb": _int(
            form.get("max_attachment_mb"), DEFAULT_MAX_ATTACHMENT_MB, 1, 500
        ),
        "regex_rules": regex_rules,
        "exempt_role_ids": _int_csv(form.get("exempt_role_ids", ""))[:50],
        "exempt_channel_ids": _int_csv(form.get("exempt_channel_ids", ""))[:50],
        "delete_on_violation": _bool(form.get("delete_on_violation")),
        "quiet_multiplier": _float(form.get("quiet_multiplier"), 1.0, 1.0, 4.0),
    }
    if quiet_hours_valid:
        config["quiet_start_hour"] = int(quiet_start) if quiet_start else None
        config["quiet_end_hour"] = int(quiet_end) if quiet_end else None
    return config, errors


def normalize_raid(form: dict) -> tuple[dict, list[str]]:
    errors: list[str] = []
    quarantine = str(form.get("quarantine_role_id", "")).strip()
    if quarantine and not (quarantine.isascii() and quarantine.isdigit()):
        errors.append("The quarantine role ID must be numeric.")
    return (
        {
            "enabled": _bool(form.get("raid_enabled")),
            "max_joins_per_minute": _int(form.get("max_joins_per_minute"), 8, 2, 100),
            "min_account_age_hours": _int(form.get("min_account_age_hours"), 0, 0, 8760),
            "quarantine_role_id": int(quarantine) if quarantine.isdigit() else 0,
            "auto_lockdown": _bool(form.get("auto_lockdown")),
            "lockdown_minutes": _int(form.get("lockdown_minutes"), 30, 5, 1440),
        },
        errors,
    )


def normalize_ladder(form: dict) -> tuple[dict, list[str]]:
    """Three thresholds, in strikes: timeout, kick, ban."""
    errors: list[str] = []
    try:
        timeout_at = int(str(form.get("ladder_timeout_strikes", "3")).strip())
        kick_at = int(str(form.get("ladder_kick_strikes", "5")).strip())
        ban_at = int(str(form.get("ladder_ban_strikes", "7")).strip())
    except (TypeError, ValueError):
        return {"steps": [list(entry) for entry in DEFAULT_LADDER]}, ["The ladder thresholds must be whole numbers."]
    if not (1 <= timeout_at < kick_at < ban_at <= 50):
        errors.append("The ladder must increase: timeout < kick < ban, all between 1 and 50.")
    minutes = _int(form.get("ladder_timeout_minutes"), 10, 1, 10080)
    return {"steps": [[timeout_at, "timeout", minutes * 60], [kick_at, "kick", 0], [ban_at, "ban", 0]]}, errors


def ladder_for_form(config: dict) -> dict:
    """The stored ladder as the three fields the form shows."""
    steps = parse_ladder((config or {}).get("steps"))
    by_action = {step.action: step for step in steps}
    timeout = by_action.get("timeout")
    return {
        "timeout_strikes": timeout.strikes if timeout else 3,
        "kick_strikes": by_action["kick"].strikes if "kick" in by_action else 5,
        "ban_strikes": by_action["ban"].strikes if "ban" in by_action else 7,
        "timeout_minutes": (timeout.seconds // 60) if timeout else 10,
    }

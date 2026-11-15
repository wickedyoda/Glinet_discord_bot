"""The member-activity and forum-activity options, in one place.

Both features grew out of the bot's environment settings, so their knobs were spread
across a generic settings page and, in several cases, existed only as constants nobody
could change without editing code. This module is the single description of that
option set: what each key is called, what it accepts, what happens if it is left unset,
and whether a change takes effect immediately or needs a restart.

It is deliberately free of Flask and of the bot: the web layer renders it, the bot
reads its defaults, and both can be tested without either.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

#: How a submitted value is parsed and normalised.
KIND_CSV_INT = "csv_int"
KIND_CSV_STR = "csv_str"
KIND_INT = "int"
KIND_BOOL = "bool"
KIND_TEXT = "text"
KIND_CRON = "cron"


@dataclass(frozen=True)
class ActivityOption:
    key: str
    label: str
    group: str
    kind: str
    default: str
    help_text: str
    minimum: int | None = None
    maximum: int | None = None
    live: bool = False
    """True when the running process picks the change up on its own."""
    secret: bool = False


#: Discord-side tracking first, then the forum ingest it sits beside.
ACTIVITY_OPTIONS: tuple[ActivityOption, ...] = (
    ActivityOption(
        key="MEMBER_ACTIVITY_EXCLUDED_ROLE_IDS",
        label="Excluded role IDs",
        group="Discord activity",
        kind=KIND_CSV_INT,
        default="",
        help_text=(
            "Roles hidden from member-activity rankings, as comma-separated IDs. "
            "Leave blank to rank every eligible member, staff roles included."
        ),
        live=True,
    ),
    ActivityOption(
        key="MEMBER_ACTIVITY_WEB_TOP_LIMIT",
        label="Rows per ranking",
        group="Discord activity",
        kind=KIND_INT,
        default="20",
        help_text="How many members each activity window shows.",
        minimum=1,
        maximum=100,
    ),
    ActivityOption(
        key="MEMBER_ACTIVITY_RECENT_RETENTION_DAYS",
        label="Hourly retention (days)",
        group="Discord activity",
        kind=KIND_INT,
        default="200",
        help_text=(
            "How long per-hour message counts are kept. Pruning runs on write, so a "
            "shorter window frees pages at the next message."
        ),
        minimum=7,
        maximum=3650,
    ),
    ActivityOption(
        key="MEMBER_ACTIVITY_BACKFILL_ENABLED",
        label="Historical backfill",
        group="Discord activity",
        kind=KIND_BOOL,
        default="false",
        help_text=(
            "Scan channel history once at startup to seed activity counts. It reads "
            "message history in bulk, so switch it off once the first run completes."
        ),
    ),
    ActivityOption(
        key="MEMBER_ACTIVITY_BACKFILL_SINCE",
        label="Backfill start date",
        group="Discord activity",
        kind=KIND_TEXT,
        default="",
        help_text="ISO date or timestamp the backfill scans from, for example 2026-01-01.",
    ),
    ActivityOption(
        key="MEMBER_ACTIVITY_BACKFILL_GUILD_ID",
        label="Backfill server ID",
        group="Discord activity",
        kind=KIND_TEXT,
        default="",
        help_text="Server to backfill. Leave blank on a single-server install.",
    ),
    ActivityOption(
        key="ENABLE_MEMBERS_INTENT",
        label="Members intent",
        group="Discord activity",
        kind=KIND_BOOL,
        default="true",
        help_text=(
            "Required for role names and member lookups. Discord must also have the "
            "intent enabled for the bot, or the connection is refused."
        ),
    ),
    ActivityOption(
        key="DISCOURSE_STATS_ENABLED",
        label="Forum statistics",
        group="Forum activity",
        kind=KIND_BOOL,
        default="true",
        help_text="Collect staff post counts and response times from the forum.",
    ),
    ActivityOption(
        key="DISCOURSE_STATS_CATEGORIES",
        label="Categories",
        group="Forum activity",
        kind=KIND_CSV_STR,
        default="glrouter,glkvm,goodcloud,iot,feedback",
        help_text="Forum category slugs to include. Everything else is ignored.",
    ),
    ActivityOption(
        key="DISCOURSE_STATS_GROUP",
        label="Staff group",
        group="Forum activity",
        kind=KIND_TEXT,
        default="GL.iNet_Staff",
        help_text="Forum group whose members are treated as staff.",
    ),
    ActivityOption(
        key="DISCOURSE_STATS_BACKFILL_DAYS",
        label="Initial window (days)",
        group="Forum activity",
        kind=KIND_INT,
        default="90",
        help_text="How far back the first ingest reaches; later runs continue from the watermark.",
        minimum=1,
        maximum=3650,
    ),
    ActivityOption(
        key="DISCOURSE_STATS_SCHEDULE",
        label="Ingest schedule",
        group="Forum activity",
        kind=KIND_CRON,
        default="15 */6 * * *",
        help_text="Cron expression for the ingest. The daily full reconcile runs separately.",
    ),
    ActivityOption(
        key="DISCOURSE_STATS_FULL_RECONCILE_HOURS",
        label="Full reconcile interval (hours)",
        group="Forum activity",
        kind=KIND_INT,
        default="24",
        help_text=(
            "How often to re-walk the whole window instead of stopping at the newest "
            "post, to catch edits and deletions."
        ),
        minimum=1,
        maximum=720,
    ),
)

OPTIONS_BY_KEY = {option.key: option for option in ACTIVITY_OPTIONS}
OPTION_KEYS = tuple(option.key for option in ACTIVITY_OPTIONS)
OPTION_GROUPS = tuple(dict.fromkeys(option.group for option in ACTIVITY_OPTIONS))


class OptionValueError(ValueError):
    """A submitted value that the option cannot accept."""


def _split_tokens(raw: str) -> list[str]:
    return [part.strip() for part in re.split(r"[\s,]+", str(raw or "")) if part.strip()]


def _split_csv(raw: str) -> list[str]:
    return [part.strip() for part in str(raw or "").split(",") if part.strip()]


def parse_int_set(raw: str) -> list[int]:
    """Parse role IDs strictly: commas or spaces, optional ``<@&123>`` mentions.

    Unlike the lenient role normaliser, a malformed token raises. That difference is
    the point: a typo in an exclusion list must be rejected so the running manager
    keeps its last good list, rather than resolving to [] and quietly re-including
    everyone the operator meant to hide. Blank input is valid and means "exclude
    nothing".
    """
    values: list[int] = []
    for token in _split_tokens(raw):
        cleaned = token.strip()
        if cleaned.startswith("<@&") and cleaned.endswith(">"):
            cleaned = cleaned[3:-1]
        # isdigit() is true for characters like "²" that int() refuses, so check the
        # ASCII range and still guard the conversion: a form typo must be a validation
        # error, never an unhandled ValueError.
        if not cleaned.isascii() or not cleaned.isdigit():
            raise OptionValueError(
                f"MEMBER_ACTIVITY_EXCLUDED_ROLE_IDS contains a non-numeric role id: {token!r}"
            )
        try:
            value = int(cleaned)
        except ValueError as exc:  # pragma: no cover - guarded by the check above
            raise OptionValueError(
                f"MEMBER_ACTIVITY_EXCLUDED_ROLE_IDS contains a non-numeric role id: {token!r}"
            ) from exc
        if value <= 0:
            raise OptionValueError(
                f"MEMBER_ACTIVITY_EXCLUDED_ROLE_IDS contains a non-positive role id: {token!r}"
            )
        if value not in values:
            values.append(value)
    return values


def parse_bool(raw: str) -> bool:
    value = str(raw or "").strip().lower()
    if value in {"1", "true", "yes", "on", "enabled"}:
        return True
    if value in {"0", "false", "no", "off", "disabled", ""}:
        return False
    raise OptionValueError(f"{raw!r} is not a yes/no value")


def normalize_option_value(option: ActivityOption, raw: str) -> str:
    """Return the canonical text for a value, or raise OptionValueError."""
    text = str(raw or "").strip()

    if option.kind == KIND_CSV_INT:
        return ",".join(str(value) for value in parse_int_set(text))
    if option.kind == KIND_CSV_STR:
        seen: list[str] = []
        for part in _split_csv(text):
            lowered = part.lower()
            if lowered not in seen:
                seen.append(lowered)
        return ",".join(seen)
    if option.kind == KIND_INT:
        if text == "":
            return option.default
        try:
            value = int(text)
        except ValueError as exc:
            raise OptionValueError(f"{text!r} is not a whole number") from exc
        if option.minimum is not None and value < option.minimum:
            raise OptionValueError(f"{value} is below the minimum of {option.minimum}")
        if option.maximum is not None and value > option.maximum:
            raise OptionValueError(f"{value} is above the maximum of {option.maximum}")
        return str(value)
    if option.kind == KIND_BOOL:
        return "true" if parse_bool(text) else "false"
    if option.kind == KIND_CRON:
        if text == "":
            return option.default
        from croniter import croniter

        if not croniter.is_valid(text):
            raise OptionValueError(f"{text!r} is not a valid cron expression")
        return " ".join(text.split())
    return text


def validate_submission(submitted: dict[str, str]) -> tuple[dict[str, str], list[str]]:
    """Normalise every known key; return (values, errors) without raising."""
    values: dict[str, str] = {}
    errors: list[str] = []
    for option in ACTIVITY_OPTIONS:
        if option.key not in submitted:
            continue
        try:
            values[option.key] = normalize_option_value(option, submitted[option.key])
        except OptionValueError as exc:
            errors.append(f"{option.label}: {exc}")
    return values, errors


def effective_value(option: ActivityOption, env_values: dict[str, str]) -> str:
    """The value in force: the configured one, else the built-in default."""
    if option.key in env_values and str(env_values[option.key]).strip() != "":
        return str(env_values[option.key]).strip()
    return option.default


def resolve_activity_options(env_values: dict[str, str]) -> list[dict]:
    """Every option with its effective value, ready for the template."""
    resolved = []
    for option in ACTIVITY_OPTIONS:
        configured = option.key in env_values and str(env_values[option.key]).strip() != ""
        value = effective_value(option, env_values)
        if option.secret and value:
            value = "•••••• configured"
        resolved.append(
            {
                "key": option.key,
                "label": option.label,
                "group": option.group,
                "kind": option.kind,
                "value": value,
                "default": option.default,
                "configured": configured,
                "help_text": option.help_text,
                "live": option.live,
                "secret": option.secret,
                "minimum": option.minimum,
                "maximum": option.maximum,
            }
        )
    return resolved

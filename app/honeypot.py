from __future__ import annotations

HONEYPOT_ACTION_SOFTBAN = "softban"
HONEYPOT_ACTION_BAN = "ban"
HONEYPOT_ACTION_TIMEOUT = "timeout"
HONEYPOT_ACTION_ROLE = "role"

HONEYPOT_ACTION_CHOICES = (
    HONEYPOT_ACTION_SOFTBAN,
    HONEYPOT_ACTION_BAN,
    HONEYPOT_ACTION_TIMEOUT,
    HONEYPOT_ACTION_ROLE,
)

HONEYPOT_DEFAULT_ACTION = HONEYPOT_ACTION_SOFTBAN
HONEYPOT_DEFAULT_DELETE_MESSAGE_DAYS = 1
HONEYPOT_DEFAULT_TIMEOUT_HOURS = 24
HONEYPOT_DEFAULT_JOIN_ACCOUNT_AGE_HOURS = 72
HONEYPOT_MAX_DELETE_MESSAGE_DAYS = 5
HONEYPOT_MAX_TIMEOUT_HOURS = 24 * 28
HONEYPOT_MAX_JOIN_ACCOUNT_AGE_HOURS = 24 * 365


def normalize_honeypot_action(value: str | None) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in HONEYPOT_ACTION_CHOICES:
        return normalized
    return HONEYPOT_DEFAULT_ACTION


def clamp_honeypot_delete_message_days(value: int | str | None) -> int:
    try:
        parsed = int(value or 0)
    except (TypeError, ValueError):
        parsed = HONEYPOT_DEFAULT_DELETE_MESSAGE_DAYS
    if parsed < 0:
        return 0
    if parsed > HONEYPOT_MAX_DELETE_MESSAGE_DAYS:
        return HONEYPOT_MAX_DELETE_MESSAGE_DAYS
    return parsed


def clamp_honeypot_timeout_hours(value: int | str | None) -> int:
    try:
        parsed = int(value or 0)
    except (TypeError, ValueError):
        parsed = HONEYPOT_DEFAULT_TIMEOUT_HOURS
    if parsed < 1:
        return 1
    if parsed > HONEYPOT_MAX_TIMEOUT_HOURS:
        return HONEYPOT_MAX_TIMEOUT_HOURS
    return parsed


def clamp_honeypot_join_account_age_hours(value: int | str | None) -> int:
    try:
        parsed = int(value or 0)
    except (TypeError, ValueError):
        parsed = HONEYPOT_DEFAULT_JOIN_ACCOUNT_AGE_HOURS
    if parsed < 1:
        return 1
    if parsed > HONEYPOT_MAX_JOIN_ACCOUNT_AGE_HOURS:
        return HONEYPOT_MAX_JOIN_ACCOUNT_AGE_HOURS
    return parsed


def honeypot_action_label(action: str | None) -> str:
    normalized = normalize_honeypot_action(action)
    if normalized == HONEYPOT_ACTION_SOFTBAN:
        return "Soft ban"
    if normalized == HONEYPOT_ACTION_BAN:
        return "Ban"
    if normalized == HONEYPOT_ACTION_TIMEOUT:
        return "Timeout"
    if normalized == HONEYPOT_ACTION_ROLE:
        return "Grant role"
    return "Soft ban"


def format_honeypot_summary(entry: dict) -> str:
    channel_id = int(entry.get("channel_id") or 0)
    action = normalize_honeypot_action(entry.get("action"))
    enabled = 1 if int(entry.get("enabled") or 0) > 0 else 0
    parts = [
        f"Channel: <#{channel_id}>" if channel_id > 0 else "Channel: Unknown",
        f"Action: {honeypot_action_label(action)}",
        f"Enabled: {'Yes' if enabled else 'No'}",
    ]
    if action in {HONEYPOT_ACTION_SOFTBAN, HONEYPOT_ACTION_BAN}:
        parts.append(f"Delete Message Days: {clamp_honeypot_delete_message_days(entry.get('delete_message_days'))}")
    elif action == HONEYPOT_ACTION_TIMEOUT:
        parts.append(f"Timeout Hours: {clamp_honeypot_timeout_hours(entry.get('timeout_hours'))}")
    elif action == HONEYPOT_ACTION_ROLE:
        role_id = int(entry.get("role_id") or 0)
        parts.append(f"Role: <@&{role_id}>" if role_id > 0 else "Role: Not set")
    return " | ".join(parts)


def format_honeypot_join_guard_summary(entry: dict) -> str:
    action = normalize_honeypot_action(entry.get("action"))
    enabled = 1 if int(entry.get("enabled") or 0) > 0 else 0
    parts = [
        f"Enabled: {'Yes' if enabled else 'No'}",
        f"Action: {honeypot_action_label(action)}",
        f"Minimum Account Age Hours: {clamp_honeypot_join_account_age_hours(entry.get('min_account_age_hours'))}",
    ]
    if action in {HONEYPOT_ACTION_SOFTBAN, HONEYPOT_ACTION_BAN}:
        parts.append(f"Delete Message Days: {clamp_honeypot_delete_message_days(entry.get('delete_message_days'))}")
    elif action == HONEYPOT_ACTION_TIMEOUT:
        parts.append(f"Timeout Hours: {clamp_honeypot_timeout_hours(entry.get('timeout_hours'))}")
    elif action == HONEYPOT_ACTION_ROLE:
        role_id = int(entry.get("role_id") or 0)
        parts.append(f"Role: <@&{role_id}>" if role_id > 0 else "Role: Not set")
    return " | ".join(parts)

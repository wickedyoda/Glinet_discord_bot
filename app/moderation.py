from __future__ import annotations

import json

BAD_WORD_ACTION_TIMEOUT = "timeout"
BAD_WORD_ACTION_WARN_ONLY = "warn_only"
BAD_WORD_ACTION_OPTIONS = (
    {"value": BAD_WORD_ACTION_TIMEOUT, "label": "Timeout / mute"},
    {"value": BAD_WORD_ACTION_WARN_ONLY, "label": "Warning only"},
)
BAD_WORD_WINDOW_HOUR_OPTIONS = (1, 6, 12, 24, 48, 72, 96, 168)
BAD_WORD_THRESHOLD_OPTIONS = (1, 2, 3, 4, 5, 6)
BAD_WORD_TIMEOUT_MINUTE_OPTIONS = (5, 10, 15, 30, 60, 180, 720, 1440, 10080)


def normalize_bad_word_action(raw_value) -> str:
    value = str(raw_value or "").strip().lower()
    if value == BAD_WORD_ACTION_WARN_ONLY:
        return BAD_WORD_ACTION_WARN_ONLY
    return BAD_WORD_ACTION_TIMEOUT


def parse_bad_word_list(raw_value) -> list[str]:
    if isinstance(raw_value, list):
        candidates = raw_value
    else:
        text = str(raw_value or "").strip()
        if not text:
            return []
        parsed = None
        if text.startswith("["):
            try:
                parsed = json.loads(text)
            except (TypeError, ValueError, json.JSONDecodeError):
                parsed = None
        if isinstance(parsed, list):
            candidates = parsed
        else:
            candidates = text.replace(",", "\n").splitlines()
    normalized: list[str] = []
    seen: set[str] = set()
    for item in candidates:
        value = " ".join(str(item or "").strip().split())
        folded = value.casefold()
        if not folded or folded in seen:
            continue
        seen.add(folded)
        normalized.append(value)
    return normalized


def serialize_bad_word_list(words) -> str:
    return json.dumps(parse_bad_word_list(words), ensure_ascii=True)


def parse_bad_word_list_text(raw_value) -> str:
    return "\n".join(parse_bad_word_list(raw_value))


def _is_word_boundary_char(char: str) -> bool:
    return not (char.isalnum() or char == "_")


def find_bad_word_match(content: str, configured_words) -> str | None:
    text = str(content or "")
    if not text:
        return None
    folded_text = text.casefold()
    for candidate in parse_bad_word_list(configured_words):
        folded_candidate = candidate.casefold()
        if not folded_candidate:
            continue
        if any(ch.isspace() for ch in folded_candidate):
            if folded_candidate in folded_text:
                return candidate
            continue
        start = folded_text.find(folded_candidate)
        while start >= 0:
            end = start + len(folded_candidate)
            before_ok = start == 0 or _is_word_boundary_char(folded_text[start - 1])
            after_ok = end >= len(folded_text) or _is_word_boundary_char(folded_text[end])
            if before_ok and after_ok:
                return candidate
            start = folded_text.find(folded_candidate, start + 1)
    return None

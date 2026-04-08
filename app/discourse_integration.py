from __future__ import annotations

import json
from urllib.parse import urlparse

DISCOURSE_FEATURE_SEARCH = "search"
DISCOURSE_FEATURE_TOPIC_LOOKUP = "topic_lookup"
DISCOURSE_FEATURE_CATEGORIES = "categories"
DISCOURSE_FEATURE_CREATE_TOPIC = "create_topic"
DISCOURSE_FEATURE_REPLY = "reply"

DISCOURSE_FEATURE_OPTIONS = (
    {"key": DISCOURSE_FEATURE_SEARCH, "label": "Forum Search"},
    {"key": DISCOURSE_FEATURE_TOPIC_LOOKUP, "label": "Topic Lookups"},
    {"key": DISCOURSE_FEATURE_CATEGORIES, "label": "Category Browsing"},
    {"key": DISCOURSE_FEATURE_CREATE_TOPIC, "label": "Create Topics"},
    {"key": DISCOURSE_FEATURE_REPLY, "label": "Reply To Topics"},
)
DISCOURSE_FEATURE_LABELS = {entry["key"]: entry["label"] for entry in DISCOURSE_FEATURE_OPTIONS}
DISCOURSE_DEFAULT_FEATURES = (
    DISCOURSE_FEATURE_SEARCH,
    DISCOURSE_FEATURE_TOPIC_LOOKUP,
    DISCOURSE_FEATURE_CATEGORIES,
)
DISCOURSE_STATE_OPTIONS = (
    {"value": "1", "label": "Enabled"},
    {"value": "0", "label": "Disabled"},
    {"value": "-1", "label": "Use global default"},
)
DISCOURSE_TIMEOUT_SECOND_OPTIONS = (5, 10, 15, 20, 30, 45, 60)
FEATURE_TOGGLE_OPTIONS = (
    {"value": "1", "label": "Enabled"},
    {"value": "0", "label": "Disabled"},
)


def normalize_discourse_override(raw_value) -> int:
    try:
        parsed = int(str(raw_value).strip())
    except (TypeError, ValueError):
        return -1
    if parsed > 0:
        return 1
    if parsed == 0:
        return 0
    return -1


def format_discourse_override_label(raw_value) -> str:
    normalized = normalize_discourse_override(raw_value)
    if normalized > 0:
        return "enabled"
    if normalized == 0:
        return "disabled"
    return "use global"


def normalize_discourse_base_url(raw_value) -> str:
    value = str(raw_value or "").strip()
    if not value:
        return ""
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("Discourse base URL must start with http:// or https:// and include a host.")
    normalized = f"{parsed.scheme}://{parsed.netloc}{parsed.path or ''}"
    return normalized.rstrip("/")


def normalize_discourse_profile_text(raw_value, *, max_length: int = 80) -> str:
    return " ".join(str(raw_value or "").strip().split())[:max_length]


def parse_discourse_features(raw_value) -> list[str]:
    if raw_value is None:
        candidates = []
    elif isinstance(raw_value, (list, tuple, set)):
        candidates = list(raw_value)
    else:
        text = str(raw_value or "").strip()
        if not text:
            candidates = []
        else:
            try:
                parsed = json.loads(text)
            except (TypeError, ValueError, json.JSONDecodeError):
                parsed = None
            if isinstance(parsed, list):
                candidates = parsed
            else:
                candidates = [part.strip() for part in text.split(",")]

    normalized = []
    seen = set()
    for item in candidates:
        value = str(item or "").strip().lower()
        if value not in DISCOURSE_FEATURE_LABELS or value in seen:
            continue
        normalized.append(value)
        seen.add(value)
    return normalized or list(DISCOURSE_DEFAULT_FEATURES)


def serialize_discourse_features(raw_value) -> str:
    return json.dumps(parse_discourse_features(raw_value), ensure_ascii=True, separators=(",", ":"))


def discourse_feature_enabled(raw_value, feature_key: str) -> bool:
    return str(feature_key or "").strip().lower() in set(parse_discourse_features(raw_value))


def discourse_features_summary(raw_value) -> str:
    features = parse_discourse_features(raw_value)
    return ", ".join(DISCOURSE_FEATURE_LABELS.get(item, item) for item in features)

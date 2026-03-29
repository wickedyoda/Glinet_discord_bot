from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from urllib.parse import urlparse

import requests

ALLOWED_HTTP_METHODS = {"GET", "HEAD"}
GLINET_DOMAIN_MONITOR_PRESETS = (
    {"name": "GL.iNet Core: gl-inet.com", "url": "https://gl-inet.com/"},
    {"name": "GL.iNet Core: gl-inet.cn", "url": "https://gl-inet.cn/"},
    {"name": "GL.iNet Core: gl-inet.net", "url": "https://gl-inet.net/"},
    {"name": "Firmware: fw.gl-inet.com", "url": "https://fw.gl-inet.com/"},
    {"name": "Firmware: dl.gl-inet.com", "url": "https://dl.gl-inet.com/"},
    {"name": "Firmware: dev.gl-inet.com", "url": "https://dev.gl-inet.com/"},
    {"name": "Cloud: glinet.io", "url": "https://glinet.io/"},
    {"name": "Cloud: goodcloud.xyz", "url": "https://goodcloud.xyz/"},
    {"name": "Cloud: remotetohome.io", "url": "https://remotetohome.io/"},
    {"name": "Cloud: glddns.com", "url": "https://glddns.com/"},
    {"name": "Docs: docs.gl-inet.com", "url": "https://docs.gl-inet.com/"},
    {"name": "Community: forum.gl-inet.com", "url": "https://forum.gl-inet.com/"},
    {"name": "Ecosystem: astrowarp.net", "url": "https://astrowarp.net/"},
    {"name": "Ecosystem: docs.astrowarp.net", "url": "https://docs.astrowarp.net/"},
    {"name": "Supporting: glinet.biz", "url": "https://glinet.biz/"},
    {"name": "Supporting: glinet.ai", "url": "https://glinet.ai/"},
    {"name": "Supporting: glinet.hk", "url": "https://glinet.hk/"},
)


def normalize_service_monitor_targets(
    raw_value,
    *,
    default_timeout_seconds: int,
    default_channel_id: int = 0,
):
    if raw_value in (None, ""):
        return []
    if isinstance(raw_value, list):
        parsed = raw_value
    else:
        text = str(raw_value or "").strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError("SERVICE_MONITOR_TARGETS_JSON must be valid JSON.") from exc
    if not isinstance(parsed, list):
        raise ValueError("SERVICE_MONITOR_TARGETS_JSON must be a JSON array.")

    normalized = []
    for index, item in enumerate(parsed, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"Service monitor entry #{index} must be an object.")
        name = str(item.get("name") or "").strip()
        url = str(item.get("url") or "").strip()
        method = str(item.get("method") or "GET").strip().upper()
        contains_text = str(item.get("contains_text") or item.get("contains") or "").strip()
        if not name:
            raise ValueError(f"Service monitor entry #{index} is missing name.")
        if not url.startswith(("http://", "https://")):
            raise ValueError(f"Service monitor '{name}' must use http:// or https://.")
        if method not in ALLOWED_HTTP_METHODS:
            raise ValueError(f"Service monitor '{name}' method must be GET or HEAD.")
        try:
            expected_status = int(item.get("expected_status", 200))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Service monitor '{name}' expected_status must be an integer.") from exc
        try:
            timeout_seconds = int(item.get("timeout_seconds", default_timeout_seconds))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Service monitor '{name}' timeout_seconds must be an integer.") from exc
        try:
            channel_id = int(item.get("channel_id", default_channel_id) or 0)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Service monitor '{name}' channel_id must be an integer.") from exc
        try:
            guild_id = int(item.get("guild_id", 0) or 0)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Service monitor '{name}' guild_id must be an integer.") from exc
        if timeout_seconds < 3:
            timeout_seconds = 3
        monitor_id = hashlib.sha256(f"{name}\n{url}\n{channel_id}".encode()).hexdigest()[:24]
        normalized.append(
            {
                "id": monitor_id,
                "guild_id": guild_id,
                "name": name,
                "url": url,
                "method": method,
                "expected_status": expected_status,
                "contains_text": contains_text,
                "timeout_seconds": timeout_seconds,
                "channel_id": channel_id,
            }
        )
    return normalized


def serialize_service_monitor_targets(targets):
    normalized = normalize_service_monitor_targets(
        targets,
        default_timeout_seconds=10,
        default_channel_id=0,
    )
    return json.dumps(normalized, sort_keys=True, separators=(",", ":"))


def build_glinet_domain_monitor_targets(*, guild_id: int, channel_id: int, timeout_seconds: int):
    raw_targets = [
        {
            "guild_id": int(guild_id or 0),
            "name": str(entry["name"]),
            "url": str(entry["url"]),
            "method": "GET",
            "expected_status": 200,
            "contains_text": "",
            "timeout_seconds": int(timeout_seconds or 10),
            "channel_id": int(channel_id or 0),
        }
        for entry in GLINET_DOMAIN_MONITOR_PRESETS
    ]
    return normalize_service_monitor_targets(
        raw_targets,
        default_timeout_seconds=max(3, int(timeout_seconds or 10)),
        default_channel_id=int(channel_id or 0),
    )


def merge_service_monitor_targets(existing_targets, incoming_targets):
    merged = []
    key_to_index: dict[tuple[int, str], int] = {}
    added = 0
    updated = 0
    deduped = 0

    for target in existing_targets or []:
        target_copy = dict(target)
        key = (
            int(target_copy.get("guild_id") or 0),
            str(target_copy.get("url") or "").strip().lower(),
        )
        if key in key_to_index:
            existing_entry = merged[key_to_index[key]]
            preserved_id = existing_entry.get("id")
            existing_entry.update(target_copy)
            if preserved_id:
                existing_entry["id"] = preserved_id
            deduped += 1
            continue
        key_to_index[key] = len(merged)
        merged.append(target_copy)

    for target in incoming_targets or []:
        target_copy = dict(target)
        key = (
            int(target_copy.get("guild_id") or 0),
            str(target_copy.get("url") or "").strip().lower(),
        )
        existing_index = key_to_index.get(key)
        if existing_index is None:
            key_to_index[key] = len(merged)
            merged.append(target_copy)
            added += 1
            continue
        existing_entry = merged[existing_index]
        preserved_id = existing_entry.get("id")
        existing_entry.update(target_copy)
        if preserved_id:
            existing_entry["id"] = preserved_id
        updated += 1

    return {
        "targets": merged,
        "added": added,
        "updated": updated,
        "deduped": deduped,
    }


def is_valid_service_monitor_url(raw_url: str):
    text = str(raw_url or "").strip()
    if not text.startswith(("http://", "https://")):
        return False
    parsed = urlparse(text)
    return bool(parsed.scheme in {"http", "https"} and parsed.netloc)


def run_service_monitor_check(target: dict):
    checked_at = datetime.now(UTC).isoformat()
    response = None
    try:
        response = requests.request(
            str(target.get("method") or "GET"),
            str(target.get("url") or "").strip(),
            timeout=max(3, int(target.get("timeout_seconds") or 10)),
            headers={"User-Agent": "glinet-discord-bot/1.0"},
            allow_redirects=True,
        )
        status_code = int(response.status_code)
        contains_text = str(target.get("contains_text") or "").strip()
        body_text = ""
        if contains_text:
            body_text = response.text or ""
        if status_code != int(target.get("expected_status") or 200):
            return {
                "state": "down",
                "checked_at": checked_at,
                "status_code": status_code,
                "error": f"Expected HTTP {int(target.get('expected_status') or 200)}, got HTTP {status_code}.",
            }
        if contains_text and contains_text not in body_text:
            return {
                "state": "down",
                "checked_at": checked_at,
                "status_code": status_code,
                "error": "Response body did not contain required text.",
            }
        return {
            "state": "up",
            "checked_at": checked_at,
            "status_code": status_code,
            "error": "",
        }
    except requests.RequestException as exc:
        status_code = int(getattr(response, "status_code", 0) or 0)
        return {
            "state": "down",
            "checked_at": checked_at,
            "status_code": status_code,
            "error": str(exc),
        }


def format_service_monitor_transition_message(target: dict, previous_state: str, result: dict):
    name = str(target.get("name") or "Service").strip()
    url = str(target.get("url") or "").strip()
    expected_status = int(target.get("expected_status") or 200)
    checked_at = str(result.get("checked_at") or "").strip()
    status_code = int(result.get("status_code") or 0)
    error = str(result.get("error") or "").strip()
    state = str(result.get("state") or "").strip().lower()

    if previous_state == "down" and state == "up":
        lines = [
            "🟢 **Service recovered**",
            f"Service: **{name}**",
            f"Status: `UP` (HTTP {status_code or expected_status})",
            f"URL: <{url}>",
        ]
    else:
        lines = [
            "🔴 **Service outage detected**",
            f"Service: **{name}**",
            "Status: `DOWN`",
            f"URL: <{url}>",
            f"Expected: HTTP {expected_status}",
        ]
        if status_code > 0:
            lines.append(f"Observed: HTTP {status_code}")
        if error:
            lines.append(f"Reason: {error}")
    if checked_at:
        lines.append(f"Checked: `{checked_at}`")
    return "\n".join(lines)

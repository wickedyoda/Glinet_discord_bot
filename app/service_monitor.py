from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from urllib.parse import urlparse

import requests

ALLOWED_HTTP_METHODS = {"GET", "HEAD"}


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

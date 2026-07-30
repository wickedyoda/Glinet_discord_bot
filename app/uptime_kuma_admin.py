from __future__ import annotations

import os
from typing import Any


FEATURE_FLAG = str(os.getenv("WEB_ADMIN_UPTIME_KUMA_GUI", "false")).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}


def is_uptime_kuma_admin_gui_enabled() -> bool:
    return FEATURE_FLAG


class UptimeKumaAdminError(RuntimeError):
    pass


def _request_json(*, base_url: str, path: str, payload: dict[str, Any], method: str = "POST", api_key: str = "", timeout: int = 30, verify_tls: bool = True):
    import requests

    url = f"{str(base_url).rstrip('/')}{path}"
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    normalized_api_key = str(api_key or "").strip()
    if normalized_api_key:
        headers["Authorization"] = f"Bearer {normalized_api_key}"
    response = requests.request(method, url, json=payload, headers=headers, timeout=timeout, verify=verify_tls)
    return response


def list_monitors(*, instance_url: str, api_key: str, timeout: int = 30, verify_tls: bool = True):
    response = _request_json(
        base_url=instance_url,
        path="/api/monitors",
        payload={},
        api_key=api_key,
        timeout=timeout,
        verify_tls=verify_tls,
    )
    if response.status_code in (401, 403):
        raise UptimeKumaAdminError("Uptime Kuma authentication failed. Check API key or guest API access.")
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise UptimeKumaAdminError("Unexpected Uptime Kuma monitors response.")
    monitors = payload.get("monitors") or []
    if not isinstance(monitors, list):
        raise UptimeKumaAdminError("Unexpected monitor list format.")
    normalized = []
    for monitor in monitors:
        if not isinstance(monitor, dict):
            continue
        normalized.append(
            {
                "id": monitor.get("id"),
                "name": monitor.get("name"),
                "url": monitor.get("url"),
                "method": monitor.get("method", "GET"),
                "hostname": monitor.get("hostname"),
                "port": monitor.get("port"),
                "path": monitor.get("path", "/"),
                "interval": monitor.get("interval"),
                "timeout": monitor.get("timeout"),
                "retry": monitor.get("retry"),
                "resend_interval": monitor.get("resendInterval"),
                "active": monitor.get("active"),
                "type": monitor.get("type"),
                "expected_status": monitor.get("expectedStatusCodes"),
                "keyword": monitor.get("keyword"),
                "notify": monitor.get("notify"),
                "upside_down": monitor.get("upsideDown"),
                "max_redirects": monitor.get("maxredirects"),
                "ignore_ssl": monitor.get("ignoreSsl"),
                "tags": [str(tag.get("name", "")) for tag in (monitor.get("tags") or []) if isinstance(tag, dict)],
                "created_at": monitor.get("createdAt"),
            }
        )
    return normalized


def add_monitor(*, instance_url: str, api_key: str, monitor: dict[str, Any], timeout: int = 30, verify_tls: bool = True):
    response = _request_json(
        base_url=instance_url,
        path="/api/monitors",
        payload={"action": "add", **monitor},
        api_key=api_key,
        timeout=timeout,
        verify_tls=verify_tls,
    )
    if response.status_code in (401, 403):
        raise UptimeKumaAdminError("Uptime Kuma authentication failed. Check API key or guest API access.")
    payload = response.json() if response.status_code == 200 else {}
    if not isinstance(payload, dict) or payload.get("ok") is not True:
        raise UptimeKumaAdminError(payload.get("msg") or "Failed to add monitor.")
    return payload


def update_monitor(*, instance_url: str, api_key: str, monitor_id: str | int, monitor: dict[str, Any], timeout: int = 30, verify_tls: bool = True):
    response = _request_json(
        base_url=instance_url,
        path="/api/monitors",
        payload={"action": "edit", "id": str(monitor_id), **monitor},
        api_key=api_key,
        timeout=timeout,
        verify_tls=verify_tls,
    )
    if response.status_code in (401, 403):
        raise UptimeKumaAdminError("Uptime Kuma authentication failed. Check API key or guest API access.")
    payload = response.json() if response.status_code == 200 else {}
    if not isinstance(payload, dict) or payload.get("ok") is not True:
        raise UptimeKumaAdminError(payload.get("msg") or "Failed to edit monitor.")
    return payload


def remove_monitor(*, instance_url: str, api_key: str, monitor_id: str | int, timeout: int = 30, verify_tls: bool = True):
    response = _request_json(
        base_url=instance_url,
        path="/api/monitors",
        payload={"action": "delete", "id": str(monitor_id)},
        api_key=api_key,
        timeout=timeout,
        verify_tls=verify_tls,
    )
    if response.status_code in (401, 403):
        raise UptimeKumaAdminError("Uptime Kuma authentication failed. Check API key or guest API access.")
    payload = response.json() if response.status_code == 200 else {}
    if not isinstance(payload, dict) or payload.get("ok") is not True:
        raise UptimeKumaAdminError(payload.get("msg") or "Failed to remove monitor.")
    return payload

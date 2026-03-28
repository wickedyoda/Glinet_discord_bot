from __future__ import annotations

from urllib.parse import urlparse

from app.service_monitor import is_valid_service_monitor_url


def _status_label(status_code: int):
    return {0: "down", 1: "up", 2: "pending", 3: "maintenance"}.get(status_code, "unknown")


def build_uptime_api_urls(page_url: str):
    normalized_page_url = str(page_url or "").strip()
    if not normalized_page_url:
        raise ValueError("Uptime Kuma page URL is required.")
    parsed = urlparse(normalized_page_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("Uptime Kuma page URL must start with http:// or https://.")
    path_parts = [part for part in parsed.path.split("/") if part]
    if len(path_parts) != 2 or path_parts[0] != "status":
        raise ValueError("Uptime Kuma page URL must match /status/<slug>.")
    slug = path_parts[1].strip()
    if not slug:
        raise ValueError("Uptime Kuma page URL is missing the status page slug.")
    base_url = f"{parsed.scheme}://{parsed.netloc}"
    return {
        "page_url": normalized_page_url,
        "slug": slug,
        "config_url": f"{base_url}/api/status-page/{slug}",
        "heartbeat_url": f"{base_url}/api/status-page/heartbeat/{slug}",
    }


def fetch_uptime_public_config(*, page_url: str, fetch_json):
    api_urls = build_uptime_api_urls(page_url)
    return fetch_json(api_urls["config_url"])


def extract_service_monitor_targets_from_uptime_config(
    config_payload: dict,
    *,
    guild_id: int = 0,
    channel_id: int = 0,
    timeout_seconds: int = 10,
):
    if not isinstance(config_payload, dict):
        raise ValueError("Uptime Kuma config payload is invalid.")
    public_group_list = config_payload.get("publicGroupList", [])
    if not isinstance(public_group_list, list):
        raise ValueError("Uptime Kuma config payload is missing publicGroupList.")

    targets = []
    skipped = []
    for group in public_group_list:
        if not isinstance(group, dict):
            continue
        group_name = str(group.get("name") or "").strip() or "Status Group"
        monitor_list = group.get("monitorList", [])
        if not isinstance(monitor_list, list):
            continue
        for monitor in monitor_list:
            if not isinstance(monitor, dict):
                continue
            monitor_name = str(monitor.get("name") or "").strip() or "Service"
            raw_url = str(monitor.get("url") or "").strip()
            if not is_valid_service_monitor_url(raw_url):
                skipped.append(
                    {
                        "group_name": group_name,
                        "monitor_name": monitor_name,
                        "reason": "no public http(s) URL",
                    }
                )
                continue
            targets.append(
                {
                    "guild_id": int(guild_id or 0),
                    "name": f"{group_name} - {monitor_name}",
                    "url": raw_url,
                    "method": "GET",
                    "expected_status": 200,
                    "contains_text": "",
                    "timeout_seconds": max(3, int(timeout_seconds or 10)),
                    "channel_id": int(channel_id or 0),
                }
            )
    return {
        "targets": targets,
        "skipped": skipped,
    }


def fetch_uptime_snapshot(
    *,
    config_url: str,
    heartbeat_url: str,
    page_url: str,
    fetch_json,
):
    if not config_url or not heartbeat_url:
        raise RuntimeError("UPTIME_STATUS_PAGE_URL is not configured correctly.")
    config_payload = fetch_json(config_url)
    heartbeat_payload = fetch_json(heartbeat_url)
    group_list = config_payload.get("publicGroupList", [])
    heartbeat_list = heartbeat_payload.get("heartbeatList", {})
    uptime_list = heartbeat_payload.get("uptimeList", {})
    if not isinstance(group_list, list) or not isinstance(heartbeat_list, dict):
        raise RuntimeError("Uptime payload is missing expected fields.")

    monitor_names = {}
    for group in group_list:
        if not isinstance(group, dict):
            continue
        monitors = group.get("monitorList", [])
        if not isinstance(monitors, list):
            continue
        for monitor in monitors:
            if not isinstance(monitor, dict):
                continue
            monitor_id = monitor.get("id")
            monitor_name = monitor.get("name")
            if isinstance(monitor_id, int) and isinstance(monitor_name, str):
                monitor_names[monitor_id] = monitor_name.strip()

    status_counts = {"up": 0, "down": 0, "pending": 0, "maintenance": 0, "unknown": 0}
    down_monitors = []
    monitor_statuses = []
    latest_timestamp = ""
    monitor_ids = sorted(monitor_names.keys())
    if not monitor_ids:
        monitor_ids = sorted(int(key) for key in heartbeat_list.keys() if str(key).isdigit())

    for monitor_id in monitor_ids:
        entries = heartbeat_list.get(str(monitor_id), [])
        latest_entry = entries[-1] if isinstance(entries, list) and entries else None
        if not isinstance(latest_entry, dict):
            status_counts["unknown"] += 1
            monitor_statuses.append(
                {
                    "id": monitor_id,
                    "name": monitor_names.get(monitor_id, f"Monitor {monitor_id}"),
                    "status": "unknown",
                    "uptime_24": None,
                    "time": "",
                }
            )
            continue
        status_code = latest_entry.get("status")
        status_label = _status_label(status_code) if isinstance(status_code, int) else "unknown"
        status_counts[status_label] += 1
        current_time = latest_entry.get("time")
        if isinstance(current_time, str) and current_time > latest_timestamp:
            latest_timestamp = current_time
        if status_label == "down":
            monitor_name = monitor_names.get(monitor_id, f"Monitor {monitor_id}")
            uptime_key = f"{monitor_id}_24"
            uptime_value = uptime_list.get(uptime_key) if isinstance(uptime_list, dict) else None
            if isinstance(uptime_value, (int, float)):
                down_monitors.append(f"{monitor_name} ({uptime_value * 100:.1f}% 24h)")
            else:
                down_monitors.append(monitor_name)
        uptime_key = f"{monitor_id}_24"
        uptime_value = uptime_list.get(uptime_key) if isinstance(uptime_list, dict) else None
        monitor_statuses.append(
            {
                "id": monitor_id,
                "name": monitor_names.get(monitor_id, f"Monitor {monitor_id}"),
                "status": status_label,
                "uptime_24": uptime_value if isinstance(uptime_value, (int, float)) else None,
                "time": current_time if isinstance(current_time, str) else "",
            }
        )

    return {
        "title": config_payload.get("config", {}).get("title", "Uptime Status"),
        "page_url": page_url,
        "total": len(monitor_ids),
        "counts": status_counts,
        "down_monitors": down_monitors,
        "monitors": monitor_statuses,
        "last_sample": latest_timestamp,
    }


def format_uptime_summary(snapshot: dict, *, page_url: str, truncate_text):
    counts = snapshot.get("counts", {})
    total = int(snapshot.get("total", 0))
    up = int(counts.get("up", 0))
    down = int(counts.get("down", 0))
    pending = int(counts.get("pending", 0))
    maintenance = int(counts.get("maintenance", 0))
    unknown = int(counts.get("unknown", 0))
    lines = [
        f"**{snapshot.get('title', 'Uptime Status')}**",
        f"Page: {snapshot.get('page_url', page_url)}",
        f"Monitors: {total} | Up: {up} | Down: {down} | Pending: {pending} | Maintenance: {maintenance} | Unknown: {unknown}",
    ]
    last_sample = str(snapshot.get("last_sample", "")).strip()
    if last_sample:
        lines.append(f"Last sample: {last_sample} UTC")
    down_monitors = snapshot.get("down_monitors", [])
    if isinstance(down_monitors, list) and down_monitors:
        lines.append("Down monitors:")
        for item in down_monitors[:10]:
            lines.append(f"- {truncate_text(str(item), max_length=120)}")
        if len(down_monitors) > 10:
            lines.append(f"- ...and {len(down_monitors) - 10} more")
    else:
        lines.append("No monitors are currently down.")
    return "\n".join(lines)

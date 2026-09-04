"""Freshdesk API client — read-only.

Provides read-only helpers for searching tickets, viewing ticket details,
and listing categories/solutions on GL.iNet's Freshdesk instance.

All write endpoints are intentionally **not** implemented here.

Authentication uses API-key basic auth (`api_key:X`), per the Freshdesk docs.
"""
from __future__ import annotations

import logging
from html import unescape
from typing import Any

import requests

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
#  Errors
# --------------------------------------------------------------------------- #
class FreshdeskApiError(RuntimeError):
    """Base error for Freshdesk API failures."""


class FreshdeskRateLimitError(FreshdeskApiError):
    """Raised when Freshdesk returns HTTP 429 (rate limited)."""


# --------------------------------------------------------------------------- #
#  Text helpers
# --------------------------------------------------------------------------- #
def clean_freshdesk_text(value: Any) -> str:
    """Strip HTML tags and decode entities from Freshdesk text fields."""
    text = str(value or "")
    cleaned: list[str] = []
    in_tag = False
    for char in text:
        if char == "<":
            in_tag = True
            continue
        if char == ">":
            in_tag = False
            cleaned.append(" ")
            continue
        if not in_tag:
            cleaned.append(char)
    # Collapse whitespace
    return " ".join(unescape("".join(cleaned)).split())


# --------------------------------------------------------------------------- #
#  Headers / auth
# --------------------------------------------------------------------------- #
FRESHDESK_USER_AGENT = "GLiNetDiscordBot/1.0 (+https://github.com/wickedyoda/Glinet_discord_bot)"


def _build_headers(api_key: str) -> dict[str, str]:
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": FRESHDESK_USER_AGENT,
    }
    if str(api_key or "").strip():
        # Freshdesk uses API-key basic auth: api_key:X
        import base64
        token = base64.b64encode(f"{api_key}:X".encode()).decode()
        headers["Authorization"] = f"Basic {token}"
    return headers


def _check_rate_limit(response: requests.Response, source_name: str):
    status_code = int(getattr(response, "status_code", 0) or 0)
    if status_code == 429:
        retry_after = response.headers.get("Retry-After", "60")
        raise FreshdeskRateLimitError(
            f"{source_name} is rate-limited. Retry after {retry_after}s."
        )
    if status_code in (401, 403):
        raise FreshdeskApiError(f"{source_name} authentication failed (HTTP {status_code}).")
    if status_code >= 400:
        raise FreshdeskApiError(f"{source_name} request failed with HTTP {status_code}.")


# --------------------------------------------------------------------------- #
#  Search tickets
# --------------------------------------------------------------------------- #
def search_freshdesk_tickets(
    *,
    base_url: str,
    query: str,
    max_results: int = 10,
    timeout_seconds: int = 15,
    api_key: str = "",
) -> list[dict[str, Any]]:
    """Search Freshdesk tickets using the Filter Tickets API.

    Query syntax: ``"status:2"``, ``"priority:4 OR priority:3"``, etc.
    See https://developer.freshdesk.com/api/#filter_tickets
    """
    endpoint = f"{base_url.rstrip('/')}/api/v2/search/tickets"
    params: dict[str, Any] = {}
    if query.strip():
        params["query"] = query.strip()

    tickets: list[dict[str, Any]] = []
    page = 1
    while len(tickets) < max_results:
        params["page"] = page
        response = requests.get(
            endpoint, params=params, timeout=timeout_seconds,
            headers=_build_headers(api_key),
        )
        _check_rate_limit(response, "Freshdesk ticket search")
        try:
            data = response.json()
        except ValueError as exc:
            raise FreshdeskApiError("Freshdesk returned invalid JSON for ticket search.") from exc

        results = data.get("results", []) if isinstance(data, dict) else []
        if not isinstance(results, list) or not results:
            break
        for item in results:
            _append_ticket(tickets, item, base_url, max_results)
        if len(results) < 30:  # last page
            break
        page += 1
        if page > 10:  # safety limit
            break
    return tickets


def _append_ticket(
    tickets: list[dict[str, Any]],
    item: dict[str, Any],
    base_url: str,
    max_results: int,
) -> None:
    if not isinstance(item, dict):
        return
    ticket_id = item.get("id") or item.get("subject_id")
    if not ticket_id or len(tickets) >= max_results:
        return
    subject = clean_freshdesk_text(item.get("subject", ""))[:200]
    ticket_url = f"{base_url.rstrip('/')}/helpdesk/tickets/{ticket_id}"
    tickets.append({
        "id": int(ticket_id),
        "subject": subject,
        "url": ticket_url,
        "status": str(item.get("status", "")),
        "priority": str(item.get("priority", "")),
        "type": str(item.get("ticket_type", "")).strip() or "N/A",
        "created_at": str(item.get("created_at", "")),
        "updated_at": str(item.get("updated_at", "")),
        "requester_name": str(item.get("requester", {}).get("name", "")) if isinstance(item.get("requester"), dict) else "",
        "agent_id": item.get("responder_id") or item.get("agent_id"),
    })


# --------------------------------------------------------------------------- #
#  View a single ticket
# --------------------------------------------------------------------------- #
def fetch_freshdesk_ticket(
    *,
    base_url: str,
    ticket_id: int,
    timeout_seconds: int = 15,
    api_key: str = "",
) -> dict[str, Any]:
    """View a single Freshdesk ticket by ID.

    Uses GET /api/v2/tickets/[id]
    """
    endpoint = f"{base_url.rstrip('/')}/api/v2/tickets/{int(ticket_id)}"
    response = requests.get(endpoint, timeout=timeout_seconds, headers=_build_headers(api_key))
    _check_rate_limit(response, "Freshdesk ticket view")
    try:
        data = response.json()
    except ValueError as exc:
        raise FreshdeskApiError("Freshdesk returned invalid JSON for ticket view.") from exc
    if not isinstance(data, dict):
        return {}
    return _normalize_ticket_detail(data, base_url)


def _normalize_ticket_detail(data: dict, base_url: str) -> dict[str, Any]:
    ticket_id = data.get("id", 0)
    descriptions = data.get("description_text") or data.get("description") or ""
    return {
        "id": int(ticket_id or 0),
        "subject": clean_freshdesk_text(data.get("subject", ""))[:300],
        "description": clean_freshdesk_text(descriptions)[:2000],
        "status": _status_label(int(data.get("status", 2))),
        "priority": _priority_label(int(data.get("priority", 1))),
        "type": str(data.get("type", "")).strip() or "N/A",
        "url": f"{base_url.rstrip('/')}/helpdesk/tickets/{ticket_id}" if ticket_id else "",
        "created_at": str(data.get("created_at", "")),
        "updated_at": str(data.get("updated_at", "")),
        "due_at": str(data.get("due_by", "")),
        "tags": [str(t) for t in (data.get("tags") or []) if str(t).strip()],
        "requester": {
            "name": str(data.get("requester", {}).get("name", "")) if isinstance(data.get("requester"), dict) else "",
            "email": str(data.get("requester", {}).get("email", "")) if isinstance(data.get("requester"), dict) else "",
        },
        "responder_id": data.get("responder_id") or data.get("agent_id"),
    }


def _status_label(status_code: int) -> str:
    labels = {
        2: "Open", 3: "Pending", 4: "Resolved", 5: "Closed", 6: "Waiting on Customer",
        7: "Waiting on External", 8: "Archived", 9: "Locked",
    }
    return labels.get(status_code, str(status_code))


def _priority_label(priority_code: int) -> str:
    labels = {1: "Low", 2: "Medium", 3: "High", 4: "Urgent"}
    return labels.get(priority_code, str(priority_code))


# --------------------------------------------------------------------------- #
#  List ticket fields (categories, etc.)
# --------------------------------------------------------------------------- #
def list_freshdesk_ticket_fields(
    *,
    base_url: str,
    timeout_seconds: int = 15,
    api_key: str = "",
) -> list[dict[str, Any]]:
    """List available ticket fields (for understanding available filterable fields).

    Uses GET /api/v2/ticket_fields
    """
    endpoint = f"{base_url.rstrip('/')}/api/v2/ticket_fields"
    response = requests.get(endpoint, timeout=timeout_seconds, headers=_build_headers(api_key))
    _check_rate_limit(response, "Freshdesk ticket fields")
    try:
        data = response.json()
    except ValueError as exc:
        raise FreshdeskApiError("Freshdesk returned invalid JSON for ticket fields.") from exc
    if not isinstance(data, list):
        return []
    return [
        {
            "name": str(field.get("name", "")),
            "label": str(field.get("label", "")),
            "type": str(field.get("type", "")),
        }
        for field in data
        if isinstance(field, dict)
    ]


# --------------------------------------------------------------------------- #
#  List solution categories (knowledge base)
# --------------------------------------------------------------------------- #
def list_freshdesk_solution_categories(
    *,
    base_url: str,
    timeout_seconds: int = 15,
    api_key: str = "",
) -> list[dict[str, Any]]:
    """List Freshdesk solution (knowledge base) categories.

    Uses GET /api/v2/solutions/categories
    """
    endpoint = f"{base_url.rstrip('/')}/api/v2/solutions/categories"
    response = requests.get(endpoint, timeout=timeout_seconds, headers=_build_headers(api_key))
    _check_rate_limit(response, "Freshdesk solution categories")
    try:
        data = response.json()
    except ValueError as exc:
        raise FreshdeskApiError("Freshdesk returned invalid JSON for solution categories.") from exc
    if not isinstance(data, list):
        return []
    return [
        {
            "id": int(cat.get("id", 0) or 0),
            "name": clean_freshdesk_text(cat.get("name", "")),
            "description": clean_freshdesk_text(cat.get("description", "")),
            "url": f"{base_url.rstrip('/')}/solutions/categories/{cat.get('id', 0)}" if cat.get("id") else "",
        }
        for cat in data
        if isinstance(cat, dict)
    ]


# --------------------------------------------------------------------------- #
#  Configuration helpers
# --------------------------------------------------------------------------- #
FRESHDESK_ENV_KEYS = {
    "FRESHDESK_BASE_URL",
    "FRESHDESK_API_KEY",
    "FRESHDESK_REQUEST_TIMEOUT_SECONDS",
}


def build_freshdesk_config(env_values: dict[str, str]) -> dict[str, Any]:
    """Build effective Freshdesk config from environment values."""
    base_url = str(env_values.get("FRESHDESK_BASE_URL", "")).strip()
    api_key = str(env_values.get("FRESHDESK_API_KEY", "")).strip()
    timeout = 15
    try:
        timeout = int(env_values.get("FRESHDESK_REQUEST_TIMEOUT_SECONDS", "15") or "15")
        if timeout < 3:
            timeout = 15
    except (ValueError, TypeError):
        timeout = 15
    return {
        "base_url": base_url,
        "api_key": api_key,
        "timeout": timeout,
    }


def freshdesk_configured(config: dict[str, Any]) -> bool:
    """Check if Freshdesk is properly configured (base URL + API key)."""
    return bool(str(config.get("base_url", "")).strip()) and bool(str(config.get("api_key", "")).strip())

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
#  List Freshdesk groups (for ticket routing)
# --------------------------------------------------------------------------- #
def list_freshdesk_groups(
    *,
    base_url: str,
    timeout_seconds: int = 15,
    api_key: str = "",
) -> list[dict[str, Any]]:
    """List Freshdesk groups for ticket routing.

    Uses GET /api/v2/groups
    """
    endpoint = f"{base_url.rstrip('/')}/api/v2/groups"
    response = requests.get(endpoint, timeout=timeout_seconds, headers=_build_headers(api_key))
    _check_rate_limit(response, "Freshdesk groups")
    try:
        data = response.json()
    except ValueError as exc:
        raise FreshdeskApiError("Freshdesk returned invalid JSON for groups.") from exc
    if not isinstance(data, list):
        return []
    return [
        {
            "id": int(group.get("id", 0) or 0),
            "name": str(group.get("name", "")) or "",
            "description": str(group.get("description_text", "")) or "",
            "escalate_to": group.get("escalate_to", 0),
            "agent_ids": [int(a) for a in (group.get("agent_ids") or []) if a],
        }
        for group in data
        if isinstance(group, dict) and group.get("id")
    ]


def find_freshdesk_group_by_name(
    *,
    base_url: str,
    group_name: str,
    timeout_seconds: int = 15,
    api_key: str = "",
    groups_cache: list[dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    """Find a Freshdesk group by case-insensitive name match.

    If *groups_cache* is provided, search it instead of hitting the API.
    """
    if groups_cache is None:
        groups_cache = list_freshdesk_groups(
            base_url=base_url, timeout_seconds=timeout_seconds, api_key=api_key
        )
    target = (group_name or "").strip().lower()
    if not target:
        return None
    for g in groups_cache:
        if g.get("name", "").strip().lower() == target:
            return g
    return None


def list_freshdesk_agents(
    *,
    base_url: str,
    timeout_seconds: int = 15,
    api_key: str = "",
) -> list[dict[str, Any]]:
    """List Freshdesk agents (employees).

    Uses GET /api/v2/agents
    """
    endpoint = f"{base_url.rstrip('/')}/api/v2/agents"
    response = requests.get(endpoint, timeout=timeout_seconds, headers=_build_headers(api_key))
    _check_rate_limit(response, "Freshdesk agents")
    try:
        data = response.json()
    except ValueError as exc:
        raise FreshdeskApiError("Freshdesk returned invalid JSON for agents.") from exc
    if not isinstance(data, list):
        return []
    return [
        {
            "id": int(agent.get("id", 0) or 0),
            "contact_id": agent.get("contact_id", 0),
            "email": str(agent.get("contact", {}).get("email", "")) if isinstance(agent.get("contact"), dict) else "",
            "name": str(agent.get("contact", {}).get("name", "")) if isinstance(agent.get("contact"), dict) else str(agent.get("name", "")),
            "role": str(agent.get("role", "")),
            "group_id": agent.get("group_id", 0),
        }
        for agent in data
        if isinstance(agent, dict) and agent.get("id")
    ]


# --------------------------------------------------------------------------- #
#  List solution categories (knowledge base)
# --------------------------------------------------------------------------- #
def create_freshdesk_ticket(
    *,
    base_url: str,
    subject: str,
    description: str,
    timeout_seconds: int = 15,
    api_key: str = "",
    email: str = "",
    name: str = "",
    priority: int = 1,
    status: int = 2,
    ticket_type: str = "Question",
    group_id: int = 0,
    custom_fields: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create a Freshdesk ticket via POST /api/v2/tickets.

    See https://developer.freshdesk.com/api/#create_ticket
    """
    endpoint = f"{base_url.rstrip('/')}/api/v2/tickets"
    payload: dict[str, Any] = {
        "subject": subject[:200],
        "description": description[:4000],
        "status": status,
        "priority": priority,
        "ticket_type": ticket_type,
    }
    if group_id:
        payload["group_id"] = int(group_id)
    if custom_fields:
        payload["custom_fields"] = custom_fields
    if email.strip():
        payload["requester"] = {"email": email.strip(), "name": name.strip() or email.strip().split("@", 1)[0]}
    else:
        payload["requester"] = {"name": name.strip() or "Discord User"}
    response = requests.post(
        endpoint,
        json=payload,
        timeout=timeout_seconds,
        headers=_build_headers(api_key),
    )
    _check_rate_limit(response, "Freshdesk ticket create")
    try:
        data = response.json()
    except ValueError as exc:
        raise FreshdeskApiError("Freshdesk returned invalid JSON for ticket create.") from exc
    try:
        ticket_id = int(data.get("id", 0))
    except (TypeError, ValueError):
        ticket_id = 0
    return {
        "id": ticket_id,
        "subject": clean_freshdesk_text(data.get("subject", subject))[:200],
        "url": f"{base_url.rstrip('/')}/helpdesk/tickets/{ticket_id}" if ticket_id else "",
        "status": str(data.get("status", status)),
        "priority": str(data.get("priority", priority)),
        "type": str(data.get("ticket_type", ticket_type)).strip() or "N/A",
        "created_at": str(data.get("created_at", "")),
        "updated_at": str(data.get("updated_at", "")),
        "tags": [str(t) for t in (data.get("tags") or []) if str(t).strip()],
    }


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
    "FRESHDESK_ENABLED",
    "FRESHDESK_BASE_URL",
    "FRESHDESK_DOMAIN",
    "FRESHDESK_API_KEY",
    "FRESHDESK_POLL_INTERVAL_SECONDS",
    "FRESHDESK_REQUEST_TIMEOUT_SECONDS",
    "FRESHDESK_TICKET_TARGET_CHANNEL_ID",
    "FRESHDESK_TECH_SUPPORT_GROUP_NAME",
    "FRESHDESK_CUSTOMER_SERVICE_GROUP_NAME",
    "FRESHDESK_ADMIN",
    "FRESHDESK_USER",
}


def _normalize_freshdesk_base_url(env_values: dict[str, str]) -> str:
    base_url = str(env_values.get("FRESHDESK_BASE_URL", "")).strip()
    if not base_url:
        domain = str(env_values.get("FRESHDESK_DOMAIN", "")).strip().strip("/")
        if domain:
            base_url = domain
    if base_url and not base_url.startswith(("http://", "https://")):
        base_url = f"https://{base_url}"
    return base_url.rstrip("/")


def build_freshdesk_config(env_values: dict[str, str]) -> dict[str, Any]:
    """Build effective Freshdesk config from environment values."""
    enabled = str(env_values.get("FRESHDESK_ENABLED", "true") or "true").strip().lower() not in {"0", "false", "no", "off"}
    base_url = _normalize_freshdesk_base_url(env_values)
    api_key = str(env_values.get("FRESHDESK_API_KEY", "")).strip()
    timeout = 15
    try:
        timeout = int(env_values.get("FRESHDESK_REQUEST_TIMEOUT_SECONDS", "15") or "15")
        if timeout < 3:
            timeout = 15
    except (ValueError, TypeError):
        timeout = 15
    return {
        "enabled": enabled,
        "base_url": base_url,
        "api_key": api_key,
        "timeout": timeout,
        "ticket_target_channel_id": str(env_values.get("FRESHDESK_TICKET_TARGET_CHANNEL_ID", "")).strip(),
        "tech_support_group_name": str(env_values.get("FRESHDESK_TECH_SUPPORT_GROUP_NAME", "")).strip(),
        "customer_service_group_name": str(env_values.get("FRESHDESK_CUSTOMER_SERVICE_GROUP_NAME", "")).strip(),
    }


def freshdesk_configured(config: dict[str, Any]) -> bool:
    """Check if Freshdesk is enabled and properly configured (base URL + API key)."""
    return (
        bool(config.get("enabled", True))
        and bool(str(config.get("base_url", "")).strip())
        and bool(str(config.get("api_key", "")).strip())
    )

"""Tests for app/freshdesk_api.py — read-only Freshdesk API client."""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from app.freshdesk_api import (
    FreshdeskApiError,
    FreshdeskRateLimitError,
    clean_freshdesk_text,
    fetch_freshdesk_ticket,
    list_freshdesk_solution_categories,
    list_freshdesk_ticket_fields,
    search_freshdesk_tickets,
    build_freshdesk_config,
    freshdesk_configured,
    _build_headers,
    _check_rate_limit,
    FRESHDESK_ENV_KEYS,
)


# --------------------------------------------------------------------------- #
#  clean_freshdesk_text
# --------------------------------------------------------------------------- #
def test_clean_freshdesk_text_strips_html():
    assert clean_freshdesk_text("<p>Hello <b>world</b></p>") == "Hello world"


def test_clean_freshdesk_text_decodes_entities():
    assert clean_freshdesk_text("a &amp; b &lt; c") == "a & b < c"


def test_clean_freshdesk_text_empty():
    assert clean_freshdesk_text(None) == ""
    assert clean_freshdesk_text(12345) == "12345"


# --------------------------------------------------------------------------- #
#  _build_headers
# --------------------------------------------------------------------------- #
def test_build_headers_without_api_key():
    headers = _build_headers("")
    assert "Authorization" not in headers
    assert headers["Accept"] == "application/json"


def test_build_headers_with_api_key():
    headers = _build_headers("secret123")
    assert headers["Authorization"].startswith("Basic ")


# --------------------------------------------------------------------------- #
#  _check_rate_limit
# --------------------------------------------------------------------------- #
def _mock_response(status_code):
    resp = MagicMock()
    resp.status_code = status_code
    resp.headers = {}
    return resp


def test_check_rate_limit_429():
    resp = _mock_response(429)
    with pytest.raises(FreshdeskRateLimitError):
        _check_rate_limit(resp, "test")


def test_check_rate_limit_401():
    resp = _mock_response(401)
    with pytest.raises(FreshdeskApiError):
        _check_rate_limit(resp, "test")


def test_check_rate_limit_500():
    resp = _mock_response(500)
    with pytest.raises(FreshdeskApiError):
        _check_rate_limit(resp, "test")


def test_check_rate_limit_ok():
    resp = _mock_response(200)
    # Should not raise
    _check_rate_limit(resp, "test")


# --------------------------------------------------------------------------- #
#  search_freshdesk_tickets
# --------------------------------------------------------------------------- #
@patch("app.freshdesk_api.requests.get")
def test_search_tickets_success(mock_get):
    mock_get.return_value = MagicMock(
        status_code=200,
        json=lambda: {
            "results": [
                {"id": 1, "subject": "Ticket 1", "status": 2, "priority": 3, "ticket_type": "Question", "created_at": "2025-01-01", "updated_at": "2025-01-02"},
                {"id": 2, "subject": "Ticket 2", "status": 2, "priority": 2, "ticket_type": "Bug", "created_at": "2025-01-03", "updated_at": "2025-01-04"},
            ]
        },
    )
    tickets = search_freshdesk_tickets(base_url="https://test.freshdesk.com", query="status:2", api_key="key")
    assert len(tickets) == 2
    assert tickets[0]["id"] == 1
    assert tickets[0]["subject"] == "Ticket 1"
    assert tickets[0]["url"] == "https://test.freshdesk.com/helpdesk/tickets/1"


@patch("app.freshdesk_api.requests.get")
def test_search_tickets_empty(mock_get):
    mock_get.return_value = MagicMock(status_code=200, json=lambda: {"results": []})
    tickets = search_freshdesk_tickets(base_url="https://test.freshdesk.com", query="nothing", api_key="key")
    assert tickets == []


@patch("app.freshdesk_api.requests.get")
def test_search_tickets_rate_limit(mock_get):
    mock_get.return_value = _mock_response(429)
    with pytest.raises(FreshdeskRateLimitError):
        search_freshdesk_tickets(base_url="https://test.freshdesk.com", query="x", api_key="key")


@patch("app.freshdesk_api.requests.get")
def test_search_tickets_no_api_key(mock_get):
    """Should work without API key (public search endpoint may allow it)."""
    mock_get.return_value = MagicMock(status_code=200, json=lambda: {"results": []})
    tickets = search_freshdesk_tickets(base_url="https://test.freshdesk.com", query="test", api_key="")
    assert tickets == []


# --------------------------------------------------------------------------- #
#  fetch_freshdesk_ticket
# --------------------------------------------------------------------------- #
@patch("app.freshdesk_api.requests.get")
def test_fetch_ticket_success(mock_get):
    mock_get.return_value = MagicMock(
        status_code=200,
        json=lambda: {
            "id": 42,
            "subject": "Login issue",
            "description": "User can't log in",
            "description_text": "User can't log in<p>Details</p>",
            "status": 2,
            "priority": 3,
            "type": "Bug",
            "url": "https://test.freshdesk.com/api/v2/tickets/42",
            "created_at": "2025-01-01",
            "updated_at": "2025-01-02",
            "due_by": "2025-01-10",
            "tags": ["urgent", "login"],
        },
    )
    ticket = fetch_freshdesk_ticket(base_url="https://test.freshdesk.com", ticket_id=42, api_key="key")
    assert ticket["id"] == 42
    assert ticket["subject"] == "Login issue"
    assert "Bug" == ticket["type"]
    assert ticket["status"] == "Open"  # status 2 -> Open
    assert ticket["priority"] == "High"  # priority 3 -> High
    assert ticket["tags"] == ["urgent", "login"]


@patch("app.freshdesk_api.requests.get")
def test_fetch_ticket_not_found(mock_get):
    mock_get.return_value = MagicMock(status_code=404, json=lambda: {"error": "Not found"})
    with pytest.raises(FreshdeskApiError):
        fetch_freshdesk_ticket(base_url="https://test.freshdesk.com", ticket_id=999, api_key="key")


@patch("app.freshdesk_api.requests.get")
def test_fetch_ticket_api_error(mock_get):
    mock_get.return_value = _mock_response(500)
    with pytest.raises(FreshdeskApiError):
        fetch_freshdesk_ticket(base_url="https://test.freshdesk.com", ticket_id=1, api_key="key")


# --------------------------------------------------------------------------- #
#  list_freshdesk_solution_categories
# --------------------------------------------------------------------------- #
@patch("app.freshdesk_api.requests.get")
def test_list_categories_success(mock_get):
    mock_get.return_value = MagicMock(
        status_code=200,
        json=lambda: [
            {"id": 1, "name": "Getting Started", "description": "Intro docs"},
            {"id": 2, "name": "Troubleshooting", "description": "Fix guides"},
        ],
    )
    cats = list_freshdesk_solution_categories(base_url="https://test.freshdesk.com", api_key="key")
    assert len(cats) == 2
    assert cats[0]["name"] == "Getting Started"
    assert cats[0]["id"] == 1
    assert cats[0]["url"] == "https://test.freshdesk.com/solutions/categories/1"


@patch("app.freshdesk_api.requests.get")
def test_list_categories_empty(mock_get):
    mock_get.return_value = MagicMock(status_code=200, json=lambda: [])
    cats = list_freshdesk_solution_categories(base_url="https://test.freshdesk.com", api_key="key")
    assert cats == []


# --------------------------------------------------------------------------- #
#  list_freshdesk_ticket_fields
# --------------------------------------------------------------------------- #
@patch("app.freshdesk_api.requests.get")
def test_list_ticket_fields_success(mock_get):
    mock_get.return_value = MagicMock(
        status_code=200,
        json=lambda: [
            {"name": "subject", "label": "Subject", "type": "default_subject"},
            {"name": "status", "label": "Status", "type": "default_status"},
        ],
    )
    fields = list_freshdesk_ticket_fields(base_url="https://test.freshdesk.com", api_key="key")
    assert len(fields) == 2
    assert fields[0]["name"] == "subject"
    assert fields[0]["label"] == "Subject"


# --------------------------------------------------------------------------- #
#  Config helpers
# --------------------------------------------------------------------------- #
def test_build_freshdesk_config():
    config = build_freshdesk_config({
        "FRESHDESK_BASE_URL": "https://support.example.com",
        "FRESHDESK_API_KEY": "secret_key",
        "FRESHDESK_REQUEST_TIMEOUT_SECONDS": "30",
    })
    assert config["base_url"] == "https://support.example.com"
    assert config["api_key"] == "secret_key"
    assert config["timeout"] == 30


def test_build_freshdesk_config_defaults():
    config = build_freshdesk_config({})
    assert config["base_url"] == ""
    assert config["api_key"] == ""
    assert config["timeout"] == 15


def test_build_freshdesk_config_timeout_minimum():
    config = build_freshdesk_config({"FRESHDESK_REQUEST_TIMEOUT_SECONDS": "1"})
    assert config["timeout"] == 15  # falls back to 15


def test_build_freshdesk_config_bad_timeout():
    config = build_freshdesk_config({"FRESHDESK_REQUEST_TIMEOUT_SECONDS": "notanumber"})
    assert config["timeout"] == 15


def test_freshdesk_configured_with_key():
    config = {"base_url": "https://example.com", "api_key": "key"}
    assert freshdesk_configured(config) is True


def test_freshdesk_configured_no_url():
    assert freshdesk_configured({"base_url": "", "api_key": "key"}) is False


def test_freshdesk_configured_no_key():
    assert freshdesk_configured({"base_url": "https://example.com", "api_key": ""}) is False


def test_freshdesk_env_keys():
    assert "FRESHDESK_BASE_URL" in FRESHDESK_ENV_KEYS
    assert "FRESHDESK_API_KEY" in FRESHDESK_ENV_KEYS
    assert "FRESHDESK_REQUEST_TIMEOUT_SECONDS" in FRESHDESK_ENV_KEYS

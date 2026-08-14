# Freshdesk Ticket Viewer Integration

## Overview

The Freshdesk integration provides **read-only** access to GL.iNet's Freshdesk
support tickets from both the Discord bot and the Web Admin GUI.

No write/delete endpoints are exposed — this is strictly a viewer.

## Configuration

Set these environment variables (via `.env` or the Web Admin settings page):

| Variable | Description | Default |
|---|---|---|
| `FRESHDESK_BASE_URL` | GL.iNet Freshdesk helpdesk root URL (e.g. `https://support.gl-inet.com`) | *(empty)* |
| `FRESHDESK_API_KEY` | API key for the Freshdesk account (read-only scope recommended) | *(empty)* |
| `FRESHDESK_REQUEST_TIMEOUT_SECONDS` | HTTP timeout for Freshdesk API calls | `15` |

## Discord Slash Commands

| Command | Description |
|---|---|
| `/freshdesk-search query:<query>` | Search tickets (e.g. `status:2`, `priority:4`) |
| `/freshdesk-ticket ticket_id:<id>` | View a single ticket by ID |
| `/freshdesk-categories` | List knowledge-base solution categories |

All results are sent as **ephemeral** messages (visible only to the user who
invoked the command).

## Web Admin GUI

Navigate to **Admin → Freshdesk** (or click the Freshdesk card on the dashboard).

The viewer page provides:
- Read-only ticket search
- Single-ticket detail view
- Solution category browser
- Integration settings summary

## Freshdesk API Endpoints Used (read-only)

- `GET /api/v2/search/tickets?query=...` — filter tickets
- `GET /api/v2/tickets/[id]` — view a ticket
- `GET /api/v2/solutions/categories` — list KB categories
- `GET /api/v2/ticket_fields` — list available fields (for understanding the schema)

## Security

- `FRESHDESK_API_KEY` is treated as a **sensitive** environment variable — it
  is never displayed in the Web Admin GUI and is masked in logs.
- The API client only uses `GET` requests (no `POST`, `PUT`, `DELETE`).
- Rate limiting (HTTP 429) is handled gracefully with a retry-after suggestion.
- All API errors are caught and surfaced as user-friendly messages.

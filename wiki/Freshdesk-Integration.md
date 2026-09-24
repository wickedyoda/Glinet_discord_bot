# Freshdesk Ticket Viewer & Creation

## Overview

The Freshdesk integration provides viewing and creation of support tickets from both the Discord bot and the Web Admin GUI.

### Discord Slash Commands

| Command | Default Access | Description |
|---|---|---|
| `/freshdesk-search query:<query>` | Moderator | Search Freshdesk tickets (e.g. `status:2`, `priority:4`) |
| `/freshdesk-ticket ticket_id:<id>` | Moderator | View a ticket by ID |
| `/freshdesk-categories` | Moderator | List knowledge-base solution categories |
| `/freshdesk-create` | Moderator | Create a new ticket via modal (Name, Email, Subject, Message) |

### Role-Based Access Control

Each Freshdesk command can be restricted to specific Discord roles:

1. Navigate to **Admin → Freshdesk → Viewer**
2. Use the "Freshdesk Command Permissions" panel on the page
3. Select or adjust role restrictions for each command:
   - **Default**: Uses built-in moderator-only gate
   - **Custom roles**: Multi-select dropdown of guild roles

Alternatively, go to **Admin → Command Permissions** for consolidated control across all commands.

## Configuration

Set these environment variables (via `.env` or the Web Admin settings page):

| Variable | Description | Default |
|---|---|---|
| `FRESHDESK_ENABLED` | Enable Freshdesk integration | `false` |
| `FRESHDESK_BASE_URL` | Freshdesk API root URL | Auto-derived from `FRESHDESK_DOMAIN` |
| `FRESHDESK_DOMAIN` | Freshdesk domain (e.g. `glinetservice.freshdesk.com`) | *(empty)* |
| `FRESHDESK_API_KEY` | API key (read/write scope) | *(empty)* |
| `FRESHDESK_POLL_INTERVAL_SECONDS` | Sync interval | `300` |
| `FRESHDESK_TICKET_TARGET_CHANNEL_ID` | Channel for webhook messages | *(empty)* |
| `FRESHDESK_REQUEST_TIMEOUT_SECONDS` | HTTP timeout | `15` |

## Creating Tickets

The `/freshdesk-create` command opens a modal where users enter:
- **Name** — Ticket requester name
- **Email** — Requester email address
- **Subject** — Ticket subject line
- **Message** — Ticket description

Upon submission, the bot creates a private ticket thread in the configured `FRESHDESK_TICKET_TARGET_CHANNEL_ID` (or the command's configured per-guild channel if set).

## Web Admin GUI

Navigate to **Admin → Freshdesk** (or click the Freshdesk card on the dashboard).

The viewer page provides:
- Read-only ticket search
- Single-ticket detail view
- Solution category browser
- Integration settings summary
- Command permissions panel with Discord role multi-select

## Freshdesk API Endpoints Used

| Endpoint | Method | Purpose |
|---|---|---|
| `GET /api/v2/search/tickets` | Search tickets |
| `GET /api/v2/tickets/[id]` | View single ticket |
| `GET /api/v2/solutions/categories` | List KB categories |
| `POST /api/v2/tickets` | Create new ticket |

## Security

- `FRESHDESK_API_KEY` is treated as a **sensitive** environment variable — it is never displayed in the Web Admin GUI and is masked in logs.
- Rate limiting (HTTP 429) is handled gracefully with retry-after suggestions.
- All API errors are caught and surfaced as user-friendly messages.

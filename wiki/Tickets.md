# Tickets

The bot has **two independent ticket systems**. They are not interchangeable and use different storage:

| System | Storage | How a ticket is opened |
|---|---|---|
| **Freshdesk tickets** | GL.iNet Freshdesk (external) | [`/create-ticket`](#freshdesk-tickets) |
| **Role-tier tickets** | SQLite (local) | See [Role-Tier Tickets](#role-tier-tickets) — *currently no command registered* |

---

## Freshdesk Tickets

Creates and reads **real GL.iNet Freshdesk support tickets**. These are *not* stored in the bot's SQLite database.

On success a private Discord thread is created in the configured intake channel and linked to the Freshdesk ticket.

### Commands

| Command | Access | Purpose |
|---|---|---|
| `/create-ticket` | Freshdesk Admin | Open a Freshdesk ticket via a category picker and form |
| `/support-ticket-search` | Freshdesk Admin | Search Freshdesk tickets by email and ticket number |
| `/support-ticket-view` | Freshdesk User or Admin | View a single Freshdesk ticket by ID |
| `/support-ticket-categories` | Freshdesk User or Admin | List Freshdesk solution / knowledge-base categories |

All Freshdesk commands are **ephemeral** and rate-limited to one use per 5 seconds per user.

### Flow

1. `/create-ticket` presents a category picker (`SupportTicketCategoryView`).
2. Selecting a category opens the `SupportTicketCreateModal` form (name, email, subject, message).
3. On submit the bot calls the Freshdesk API and creates a private thread in the intake channel.

### Required Configuration

These commands are registered whether or not Freshdesk is enabled, but they refuse to run and reply that the integration is not configured. To enable:

| Variable | Purpose |
|---|---|
| `FRESHDESK_ENABLED` | Must be `true` for `/create-ticket` |
| `FRESHDESK_DOMAIN` or `FRESHDESK_BASE_URL` | Freshdesk tenant URL |
| `FRESHDESK_API_KEY` | API key |
| `FRESHDESK_TICKET_TARGET_CHANNEL_ID` | Channel that receives the created threads |
| `FRESHDESK_ADMIN` | Comma-separated role IDs allowed to create and search tickets |
| `FRESHDESK_USER` | Comma-separated role IDs allowed to view tickets |

Notes:

- Role IDs are comma-separated lists and are read from the environment at authorization time, so Web GUI changes take effect without a bot restart.
- If no intake channel resolves, `/create-ticket` replies that an admin must set `FRESHDESK_TICKET_TARGET_CHANNEL_ID` instead.
- `FRESHDESK_ENABLED` is enforced only by `/create-ticket`; the read-only commands check the base URL and API key directly.

---

## Role-Tier Tickets

A SQLite-backed ticket system. The supporting code is fully implemented in `app/tickets.py` — `TicketStore` (create, close, reopen, reassign, search, stats), the role-tier helpers, and the button view are all present and used by the bot.

> **Note:** no `/ticket` command is currently registered. The store and the `/ticket-search` and `/ticket-stats` commands are available, but opening a new role-tier ticket has no registered command, so the "open a ticket" path described in earlier versions of this page is unavailable.

### Available Commands

| Command | Access | Purpose |
|---|---|---|
| `/ticket-search` | Tier 1+ | Search tickets by number or owner email |
| `/ticket-stats` | Tier 1+ | Show ticket counts by category and status |

Both responses are **ephemeral**; only the command user can see them.

### Role Tiers

Tier permissions for ticket access:

- `search`
- `create`
- `reassign`
- `admin`

Higher tiers inherit lower-tier capabilities. Ticket features are disabled until at least one tier has at least one role assigned.

### Web GUI

- `/admin/ticket-settings` manages role tiers and effective role IDs.
- Saved role maps are persisted in `guild_settings.ticket_role_map_json`.
- Ticket settings are scoped to the selected guild.

### Buttons

`app.tickets.ticket_view()` builds a view with `Claim`, `Close`, and `Reassign` buttons. Tier checks are enforced server-side before role changes are applied.

### Workflow Notes

- Ticket creation uses per-guild categories configured in `app.tickets`.
- Ticket state is stored in SQLite, with schema created automatically on first ticket use.
- Off-guild or missing-member cases are handled with explicit error messaging.
- Legacy web callback hooks remain disabled by default; ticket management is currently slash/button-driven.

---

## Related Pages

- [Command Reference](Command-Reference.md)
- [Web Admin Interface](Web-Admin-Interface.md)
- [Environment Variables](Environment-Variables.md)
- [Freshdesk Integration](Freshdesk-Integration.md)

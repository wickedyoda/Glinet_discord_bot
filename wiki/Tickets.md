# Tickets

The bot includes a role-tier ticket system backed by SQLite and managed through slash commands, button interactions, and the web GUI.

## Commands

| Command | Access | Purpose |
|---|---|---|
| `/ticket` | Tier 2+ | Open a support ticket from configured categories |
| `/ticket-search` | Tier 1+ | Search tickets by number or owner email |
| `/ticket-stats` | Tier 1+ | Show ticket counts by category and status |

All ticket search and stats responses are **ephemeral**; only the command user can see them.

## Role Tiers

Tier permissions for ticket access:

- `search`
- `create`
- `reassign`
- `admin`

Higher tiers inherit lower-tier capabilities. Ticket features are disabled until at least one tier has at least one role assigned.

## Web GUI

- `/admin/ticket-settings` manages role tiers and effective role IDs.
- Saved role maps are persisted in `guild_settings.ticket_role_map_json`.
- Ticket settings are scoped to the selected guild.

## Buttons

Each ticket message supports interactive actions:

- `Claim`
- `Close`
- `Reassign`
- `Reopen`

Tier checks are enforced server-side before role changes are applied.

## Workflow Notes

- Ticket creation uses per-guild categories configured in `app.tickets`.
- The bot creates a dedicated `Tickets` category when possible.
- Ticket state is stored in SQLite, with schema created automatically on first ticket use.
- Off-guild or missing-member cases are handled with explicit error messaging.
- Legacy web callback hooks remain disabled by default; ticket management is currently slash/button-driven.

## Related Pages

- [Command Reference](Command-Reference.md)
- [Web Admin Interface](Web-Admin-Interface.md)
- [Environment Variables](Environment-Variables.md)

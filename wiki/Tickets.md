# Tickets

Role-tiered support tickets with Discord slash commands and web admin role mapping.

## Concepts

- Ticket tiers are **disabled by default**.
- Ticket features remain off until at least one tier has at least one Discord role assigned in `/admin/ticket-settings`.
- Each tier inherits the capabilities of the lower tiers.

## Tier Model

| Tier | Key | Capabilities |
|---|---|---|
| Role 1 | `search` | Search/view tickets only |
| Role 2 | `create` | Create, update, and close tickets |
| Role 3 | `reassign` | Create, update, close, and reassign tickets |
| Role 4 | `admin` | Full ticket administration |

## Discord Commands

| Command | Type | Access | Notes |
|---|---|---|---|
| `/ticket` | Slash | Tier 2+ | Opens a ticket creation modal with category selection |
| `/ticket-search` | Slash | Tier 1+ | Search by ticket number or owner email |
| `/ticket-stats` | Slash | Tier 1+ | Shows open/closed counts |

All search and stats responses are **ephemeral**; only the command user can see results.

### /ticket

Requires ticket categories to be defined. The command posts a category selector, then a question modal. On submit, the bot creates a private text channel under the `Tickets` category and posts the initial ticket embed.

### /ticket-search

Accepts a ticket number or owner email. Numeric-only input searches by ticket number; otherwise it searches by owner email. If multiple matches exist, the first match is returned.

### /ticket-stats

Shows open/closed counts by category.

## Ticket Buttons

Each ticket channel message can include:

- Claim
- Close
- Reassign
- Reopen

Reassign is restricted to tier 3+.

## Web Admin: `/admin/ticket-settings`

Path: `/admin/ticket-settings`

This page provides:

- Tier label table
- Discord role multi-selects per tier
- Effective role IDs display
- Save action that persists `ticket_role_map_json` to `guild_settings`

Safety check:

- If no roles are assigned, ticket command handlers return a closed failure message instead of creating or modifying tickets.

## Database

Tickets persist in SQLite under the primary bot database.

- Ticket store schema is initialized at bot startup by the existing SQLite migrator.
- Role map is stored in `guild_settings.ticket_role_map_json` as JSON.

## Deployment Notes

- The `ticket-bot` branch publishes a ticket-specific Docker image to GitHub Packages on push to `ticket-bot`.
- Image naming pattern: `ghcr.io/<owner>/<repo>-ticket-beta:YYYYMMDD-HHMMSS`
- Floating tag: `latest-ticket-beta`

## Related

- Command Reference: `wiki/Command-Reference.md`
- Web Admin Interface: `wiki/Web-Admin-Interface.md`
- Docker and Portainer Deploy: `wiki/Docker-and-Portainer-Deploy.md`

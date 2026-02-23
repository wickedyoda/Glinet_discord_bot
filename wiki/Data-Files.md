# Data Files

Persistent runtime data is stored under `DATA_DIR` (default `data/`).

## Files

- `bot_data.db` (primary SQLite database)
- `bot.log`
- `container_errors.log`

## Legacy Migration

If these legacy files exist, they are migrated into SQLite at startup:

- `access_role.txt`
- `role_codes.txt`
- `invite_roles.json`
- `tag_responses.json`
- `firmware_seen.json`
- `web_users.json`
- `command_permissions.json`

Migration is merge-only: existing SQLite records remain unchanged.

## Purpose

- Invite/code role mapping state
- Tag response map
- Firmware seen-entry cache
- Web admin user accounts
- Command permission policy overrides
- Runtime logging output
- Container-wide error logging output (used by `/logs`)

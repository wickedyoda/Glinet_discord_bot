# Data Files

Persistent runtime state uses:

- `DATA_DIR` (default `data/`) for database and legacy compatibility files
- `LOG_DIR` (default `/logs`) for runtime log files

## File Inventory

| File | Purpose |
|---|---|
| `${DATA_DIR}/bot_data.db` | Primary SQLite database for runtime and config state |
| `${LOG_DIR}/bot.log` | Application/runtime logs |
| `${LOG_DIR}/container_errors.log` | Error-focused log file used by `/logs` command |

## SQLite Scope

`bot_data.db` stores core persistent entities, including:

- Invite/role mapping state
- Tag responses
- Firmware seen entries
- Web users and metadata
- Command permission overrides
- Additional runtime-managed configuration state

## Legacy Import on Boot

Legacy files are imported at startup if present:

- `access_role.txt`
- `role_codes.txt`
- `invite_roles.json`
- `tag_responses.json`
- `firmware_seen.json`
- `web_users.json`
- `command_permissions.json`

Import strategy:

- Merge-only
- Never overwrites existing SQLite records
- Allows migration continuity while preserving newer DB data

## File and Permission Hardening

When enabled (`WEB_HARDEN_FILE_PERMISSIONS=true`), application attempts:

- `.env` -> `0600`
- `data/` directory -> `0700`
- `bot_data.db` -> `0600`

## Backup Guidance

Minimum backup set:

- `${DATA_DIR}/bot_data.db`
- `${LOG_DIR}/bot.log` (optional for auditing)
- `${LOG_DIR}/container_errors.log` (optional for incident traces)

For reliable restore:

1. Stop container.
2. Restore DB and required files.
3. Start container.
4. Validate key workflows (login, command permissions, tag replies).

## Performance Notes

- SQLite provides low-overhead persistence suitable for single-container deployments.
- WAL mode is used for better concurrency and durability tradeoff.
- Keep data volume on reliable storage to reduce corruption risk.

## Related Pages

- [Environment Variables](Environment-Variables)
- [Docker and Portainer Deploy](Docker-and-Portainer-Deploy)
- [Security Hardening](Security-Hardening)

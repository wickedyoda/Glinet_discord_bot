# Data Files

Persistent runtime state uses:

- `DATA_DIR` (default `data/`) for database and legacy compatibility files
- `LOG_DIR` (default `/logs`) for runtime log files

## File Inventory

| File | Purpose |
|---|---|
| `MySQL database (${DB_NAME})` | Primary runtime and config state store when `DB_BACKEND=mysql` |
| `${DATA_DIR}/bot_data.db` | SQLite fallback database and import source when `DB_BACKEND=sqlite` or `DB_IMPORT_SQLITE_ON_BOOT=true` |
| `${LOG_DIR}/bot.log` | Application/runtime logs |
| `${LOG_DIR}/bot_log.log` | Mirror of payloads sent (or attempted) to bot log channels |
| `${LOG_DIR}/container_errors.log` | Error-focused log file used by `/logs` command |
| `${LOG_DIR}/web_gui_audit.log` | Web GUI interaction audit entries (`WEB_AUDIT ...`) |
| `${LOG_DIR}/web_probe.log` | Anonymous unknown-route `404` web probe entries (`WEB_PROBE ...`) |

## Database Scope

The primary database stores core persistent entities, including:

- Invite/role mapping state
- Tag responses
- Firmware seen entries
- Web users and metadata
- Command permission overrides
- Additional runtime-managed configuration state

## Legacy Import on Boot

Legacy JSON/text files are imported at startup if present:

- `access_role.txt`
- `role_codes.txt`
- `invite_roles.json`
- `tag_responses.json`
- `firmware_seen.json`
- `web_users.json`
- `command_permissions.json`

Import strategy:

- Merge-only
- Never overwrites existing database records
- Allows migration continuity while preserving newer DB data

When `DB_BACKEND=mysql` and `DB_IMPORT_SQLITE_ON_BOOT=true`:

- `${DB_SQLITE_PATH}` is treated as a migration source
- each supported table is imported only if the matching MySQL table is empty
- existing MySQL rows are not overwritten

## File and Permission Hardening

When enabled (`WEB_HARDEN_FILE_PERMISSIONS=true`), application attempts:

- `.env` -> `0600`
- `data/` directory -> `0700`
- `bot_data.db` -> `0600` when SQLite storage is in use

When enabled (`LOG_HARDEN_FILE_PERMISSIONS=true`), application attempts:

- `${LOG_DIR}` directory -> `0700`
- `${LOG_DIR}/bot.log` -> `0600`
- `${LOG_DIR}/bot_log.log` -> `0600`
- `${LOG_DIR}/container_errors.log` -> `0600`
- `${LOG_DIR}/web_gui_audit.log` -> `0600`
- `${LOG_DIR}/web_probe.log` -> `0600`

## Log Rotation and Retention

- Runtime logs rotate on a timed schedule (`LOG_ROTATION_INTERVAL_DAYS`, default `1`).
- Retention is bounded by `LOG_RETENTION_DAYS` (default `90` days).
- Rotation is UTC-based and keeps only the latest retention window.

## Backup Guidance

Minimum backup set:

- MySQL volume backup for `${DB_NAME}` when using MySQL
- `${DATA_DIR}/bot_data.db` when using SQLite or preserving the SQLite import source
- `${LOG_DIR}/bot.log` (optional for auditing)
- `${LOG_DIR}/bot_log.log` (recommended for channel-post audit trails)
- `${LOG_DIR}/container_errors.log` (optional for incident traces)
- `${LOG_DIR}/web_gui_audit.log` (recommended for web admin activity auditing)
- `${LOG_DIR}/web_probe.log` (useful for monitoring internet scan noise separately from operator actions)

For reliable restore:

1. Stop container.
2. Restore DB and required files.
3. Start container.
4. Validate key workflows (login, command permissions, tag replies).

## Performance Notes

- MySQL is the default deployment backend for better concurrent write behavior and service separation.
- SQLite remains available as a fallback backend and migration source.
- Keep database volumes on reliable storage and back them up independently from logs.

## Related Pages

- [Environment Variables](Environment-Variables)
- [Docker and Portainer Deploy](Docker-and-Portainer-Deploy)
- [Security Hardening](Security-Hardening)

# Web Admin Interface

Password-protected admin UI for runtime bot management.

## Access

- Runs inside container on `WEB_PORT` (default `8080`)
- Exposed via mapped host port (`WEB_HOST_PORT`)
- Login is email/password

## User Model

- No public signup
- First-run default admin is created from env values
- Admin can create additional users
- Password policy:
  - At least 6 digits
  - At least 2 uppercase letters
  - At least 1 symbol

## Admin Pages

- `/admin/settings`
  - Environment-backed settings editor
  - Channel dropdowns for channel fields
  - Role dropdowns for role fields
- `/admin/tag-responses`
  - Edit JSON tag map and refresh runtime tag commands
- `/admin/bulk-role-csv`
  - Upload CSV and run bulk role assignment
- `/admin/users`
  - Create/delete users and promote/demote admin status
- `/admin/bot-profile`
  - View bot identity
  - Upload bot avatar image

## Discord Catalog Dropdowns

- Web UI polls guild channels and roles from Discord.
- Cached in-memory with TTL and fetch timeout controls.

## Env Variables

- `WEB_ENABLED`
- `WEB_BIND_HOST`
- `WEB_PORT`
- `WEB_HOST_PORT`
- `WEB_ENV_FILE`
- `WEB_ADMIN_DEFAULT_USERNAME`
- `WEB_ADMIN_DEFAULT_PASSWORD`
- `WEB_ADMIN_SESSION_SECRET`
- `WEB_DISCORD_CATALOG_TTL_SECONDS`
- `WEB_DISCORD_CATALOG_FETCH_TIMEOUT_SECONDS`
- `WEB_BOT_PROFILE_TIMEOUT_SECONDS`
- `WEB_AVATAR_MAX_UPLOAD_BYTES`

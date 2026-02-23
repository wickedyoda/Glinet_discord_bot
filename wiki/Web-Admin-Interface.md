# Web Admin Interface

<p align="center">
  <img src="../assets/images/glinet-bot-full.png" alt="GL.iNet Bot Full Logo" width="320" />
</p>

Password-protected admin UI for runtime bot management.

## Access

- Runs inside container on `WEB_PORT` (default `8080`)
- Exposed via mapped host port (`WEB_HOST_PORT`)
- Login is email/password
- Login is web-only (`/login` route in the web UI)
- Auto-logout timeout is configurable in settings (`WEB_SESSION_TIMEOUT_MINUTES`) with 5-minute steps from 5 to 30 minutes
- Login form includes an optional "Keep me signed in" checkbox that keeps the session for up to 5 days on the current device
- Theme selector supports `Light` and `Black` modes in the web header
- Login endpoint includes basic rate limiting to reduce brute-force attempts
- Security headers are applied (CSP, frame deny, no-sniff, referrer policy)
- Reverse proxy deployment guide: [Reverse Proxy Web GUI](Reverse-Proxy-Web-GUI)

## User Model

- No public signup
- First-run default admin is created from env values
- First-run admin creation requires a valid `WEB_ADMIN_DEFAULT_PASSWORD` (no insecure fallback password)
- Admin can create additional users
- No Discord `/login` or `!login` command exists for web-user creation
- Password policy:
  - Minimum 6 characters, maximum 16 characters
  - At least 2 numbers
  - At least 1 uppercase letter
  - At least 1 symbol
- Create-user and reset-password forms include a show-password toggle

## Admin Pages

- `/admin` (dashboard)
  - Quick-action cards with direct buttons for all available web-admin tools
- `/admin/settings`
  - Environment-backed settings editor
  - Channel dropdowns for channel fields
  - Role dropdowns for role fields
- `/admin/command-permissions`
  - Configure per-command access mode
  - Default/public/custom role permission support
  - Select custom roles with multi-select role dropdowns from live Discord data
  - Optional manual role-ID entry for roles not present in dropdown
- `/admin/tag-responses`
  - Edit tag map and refresh runtime tag commands
- `/admin/bulk-role-csv`
  - Upload CSV and run bulk role assignment
- `/admin/users`
  - Create/delete users and promote/demote admin status
- `/admin/bot-profile`
  - View bot identity
  - Update bot username and server nickname (admin-only; web GUI only)
  - Upload bot avatar image

## Discord Catalog Dropdowns

- Web UI polls guild channels and roles from Discord.
- Cached in-memory with TTL and fetch timeout controls.

## Env Variables

- `WEB_ENABLED`
- `WEB_BIND_HOST`
- `WEB_PORT`
- `WEB_HOST_PORT`
- `WEB_SESSION_TIMEOUT_MINUTES`
- `WEB_PUBLIC_BASE_URL`
- `WEB_ENV_FILE`
- `WEB_ADMIN_DEFAULT_USERNAME`
- `WEB_ADMIN_DEFAULT_PASSWORD`
- `WEB_ADMIN_SESSION_SECRET`
- `WEB_DISCORD_CATALOG_TTL_SECONDS`
- `WEB_DISCORD_CATALOG_FETCH_TIMEOUT_SECONDS`
- `WEB_BOT_PROFILE_TIMEOUT_SECONDS`
- `WEB_AVATAR_MAX_UPLOAD_BYTES`

# Web Admin Interface

<p align="center">
  <img src="../assets/images/glinet-bot-full.png" alt="GL.iNet Bot Full Logo" width="320" />
</p>

Password-protected admin UI for runtime bot and policy management.

## Access and Session Model

- Default bind: `WEB_BIND_HOST=127.0.0.1`, `WEB_PORT=8080`
- Typical container bind: `WEB_BIND_HOST=0.0.0.0` with host/proxy controls
- Login uses email + password (web-only account model)
- Optional "Keep me signed in" extends session to 5 days on device
- Inactivity timeout is configurable: 5 to 30 minutes in 5-minute steps
- Theme options in header: `Light` and `Black`

Security controls include:

- Login rate limiting
- CSRF enforcement
- Same-origin POST checks
- Strict cookie settings and browser hardening headers

## User and Identity Fields

Each web user includes:

- Email (login identifier)
- Password hash
- First name
- Last name
- Display name (shown in GUI)
- Role (`Admin` or `Read-only`)
- Password age metadata (90-day rotation enforcement)

User self-service capabilities:

- Change password
- Change email
- Update first/last/display names

Admin-only user management capabilities:

- Create users
- Delete users
- Promote/demote admin users
- Reset user credentials as needed

Read-only capabilities:

- Can sign in and navigate all admin pages
- Can view all settings/options/data exposed by the web GUI
- Cannot apply management/configuration changes (save/update/delete/restart actions are blocked server-side)

No Discord `/login` or `!login` flow exists for web-user creation.

## Password Policy

All web passwords must satisfy:

- Minimum 6 characters
- Maximum 16 characters
- At least 2 numbers
- At least 1 uppercase letter
- At least 1 symbol

UI forms include show/hide password toggles and validation feedback.

## Navigation and Layout

- Top menu uses dropdown-based section navigation.
- Dedicated dashboard link is shown beside the dropdown.
- Dashboard includes direct action buttons/cards for major admin workflows.
- Mobile layout is responsive for smaller screens and touch interaction.

## Admin Pages and Capabilities

### `/admin`

- Dashboard overview
- Quick links to settings, users, moderation tooling, and logs-related actions

### `/admin/settings`

- Environment-backed settings editor
- Live dropdowns for known channel and role fields
- Bot profile and web-session/security settings
- Auto-logout selection (5 to 30 minutes)

### `/admin/command-permissions`

- Per-command access policy editor
- Modes: `default`, `public`, `custom_roles`
- Multi-select role dropdown by role name
- Manual role-ID entry fallback if catalog is incomplete

### `/admin/tag-responses`

- JSON tag editor
- Save + runtime reload
- Dynamic slash refresh trigger (restart not required)

### `/admin/bulk-role-csv`

- CSV upload and target-role selection
- Assignment execution with timeout protections
- Structured results with unmatched/ambiguous/failure sections

### `/admin/users`

- User and role management (`Admin` / `Read-only`)
- User creation with password policy enforcement
- Password visibility toggle in create/reset forms

### `/admin/bot-profile`

- Read bot identity
- Rename bot username
- Set server nickname/listing label
- Upload avatar image

Rename/profile updates are admin-only and web-GUI-only (read-only users can view this page but cannot apply changes).

## Reverse Proxy Behavior

Recommended for production:

- Put web UI behind HTTPS reverse proxy
- Set `WEB_PUBLIC_BASE_URL` to exact external origin
- Keep `WEB_TRUST_PROXY_HEADERS=true` only for trusted proxy
- Keep CSRF and same-origin checks enabled

If behind proxy, ensure forwarded headers include:

- `Host`
- `X-Forwarded-Host`
- `X-Forwarded-Proto`
- `X-Forwarded-For`

## Common Login Issues

- `Blocked request due to origin policy.`
  - `WEB_PUBLIC_BASE_URL` mismatch with browser origin
  - missing/incorrect forwarded host headers
- Login loops back to login page
  - session secret/cookie settings issue
  - HTTPS mismatch when secure cookies enabled
- Proxy-only login failure
  - check trusted proxy header forwarding and origin alignment

## Browser/Accessibility Notes

- Password field uses `autocomplete="current-password"`
- Labels are explicitly associated with form controls (`for` + `id`)
- Inputs are styled to consistent size/shape for usability

## Environment Variables (Web)

- `WEB_ENABLED`
- `WEB_BIND_HOST`
- `WEB_PORT`
- `WEB_HOST_PORT`
- `LOG_HARDEN_FILE_PERMISSIONS`
- `WEB_SESSION_TIMEOUT_MINUTES`
- `WEB_PUBLIC_BASE_URL`
- `WEB_ENV_FILE`
- `WEB_RESTART_ENABLED`
- `WEB_GITHUB_WIKI_URL`
- `WEB_ADMIN_DEFAULT_USERNAME`
- `WEB_ADMIN_DEFAULT_PASSWORD`
- `WEB_ADMIN_SESSION_SECRET`
- `WEB_SESSION_COOKIE_SECURE`
- `WEB_TRUST_PROXY_HEADERS`
- `WEB_ENFORCE_CSRF`
- `WEB_ENFORCE_SAME_ORIGIN_POSTS`
- `WEB_HARDEN_FILE_PERMISSIONS`
- `WEB_DISCORD_CATALOG_TTL_SECONDS`
- `WEB_DISCORD_CATALOG_FETCH_TIMEOUT_SECONDS`
- `WEB_BULK_ASSIGN_TIMEOUT_SECONDS`
- `WEB_BULK_ASSIGN_MAX_UPLOAD_BYTES`
- `WEB_BULK_ASSIGN_REPORT_LIST_LIMIT`
- `WEB_BOT_PROFILE_TIMEOUT_SECONDS`
- `WEB_AVATAR_MAX_UPLOAD_BYTES`

## Related Pages

- [Reverse Proxy Web GUI](Reverse-Proxy-Web-GUI)
- [Environment Variables](Environment-Variables)
- [Security Hardening](Security-Hardening)

# Web Admin Interface

<p align="center">
  <img src="../assets/images/glinet-bot-full.png" alt="GL.iNet Bot Full Logo" width="320" />
</p>

Password-protected admin UI for runtime bot and policy management.

## Access and Session Model

- Default bind: `WEB_BIND_HOST=127.0.0.1`, `WEB_PORT=8080`
- Built-in HTTPS bind: `WEB_HTTPS_PORT=8081`
- Typical container bind: `WEB_BIND_HOST=0.0.0.0` with host/proxy controls
- If no TLS files exist, the bot generates a self-signed certificate under `${DATA_DIR}/ssl/`.
- Replace `${DATA_DIR}/ssl/tls.crt` and `${DATA_DIR}/ssl/tls.key` with your own files if you want a browser-trusted HTTPS listener.
- Login uses email + password (web-only account model)
- Optional "Keep me signed in" extends session to 5 days on device
- Inactivity timeout is configurable: `5`, `10`, `15`, `20`, `30`, `45`, `60`, `90`, or `120` minutes
- Theme options in header: `Light`, `Black`, `Forest`, `Ember`, and `Ice`

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
- Role (`Admin`, `Read-only`, `Glinet-Read-Only`, or `Glinet-RW`)
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

`Glinet-Read-Only` capabilities:

- Can sign in
- Is automatically pinned to the primary Discord server (intended for the GL.iNet Community Discord)
- Can view the guild-scoped GL.iNet pages for that primary server, including dashboard, command permissions, actions, feeds, bot profile, guild settings, tag responses, bulk role CSV, and member activity
- Can export member-activity ZIP archives from `/admin/member-activity/export`
- Can manage their own `/admin/account` page
- Cannot access global settings, logs, documentation, user management, or other non-guild/global admin pages

`Glinet-RW` capabilities:

- Includes all `Glinet-Read-Only` viewing access
- Is pinned to the same primary Discord server only
- Can save guild-scoped settings/pages for that server:
  - command permissions
  - guild settings
  - Reddit/YouTube/LinkedIn/Beta subscriptions
  - tag responses
  - bot nickname on the selected server
- Cannot access global settings, user management, container restart, or cross-guild selection

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

- Main page lists the Discord servers the bot is currently in.
- Select a server first, then open the server dashboard for guild-scoped admin actions.
- Top menu uses dropdown-based section navigation.
- Direct `Logout` action is available from the top header on desktop and mobile layouts.
- Mobile layout uses a compact quickbar plus collapsible menu drawer for server jump, account access, theme switching, and primary page links.
- Dedicated dashboard link is shown beside the dropdown.
- Dashboard includes direct action buttons/cards for major admin workflows.
- Mobile layout is responsive for smaller screens and touch interaction.

## Command Permissions

- `/admin/command-permissions` manages command access per selected guild.
- Each command has an `Enabled` checkbox.
  - unchecked: command is disabled for that guild
  - checked: command is enabled and follows the selected access mode
- Available modes for enabled commands:
  - `Default rule`: follow the bot's built-in default access policy for that command
  - `Public`: allow any guild member
  - `Custom roles`: restrict the command to one or more selected roles
- Custom-role mode requires at least one role ID or selected role.
- Reddit feed management page lets admins map subreddits to Discord text channels and set the polling interval from a dropdown.
- LinkedIn profile management page lets admins map public LinkedIn profiles to Discord text channels for new-post notifications.
- GL.iNet beta program page lets admins map the public GL.iNet beta-testing page to Discord text channels for added/removed program notifications.
- Role access page lets admins review invite/code/role mappings, pause or disable them individually, and manually restore a mapping with an existing Discord invite.
- Tag responses and guild settings pages now follow the selected server context instead of using one global mapping.
- Member activity page shows top-20 member activity windows for the selected server.
- Member activity exports are generated for the selected server only and match the currently retained 90-day dataset.
- Guild settings also control per-server welcome messages, optional join DMs, and optional uploaded welcome images.

## Admin Pages and Capabilities

### `/admin`

- Server selector / entry page
- Lists every Discord server the bot can currently access
- Sets the active server context used by guild-scoped admin pages
- Admin users can remove the bot from a server directly from this page using the per-server `Remove Bot` action
- `Glinet-Read-Only` and `Glinet-RW` users do not stay here; they are redirected to `/admin/dashboard` using the primary guild

### `/admin/dashboard`

- Server dashboard overview
- Grouped sections for core controls, community tools, notification feeds, and runtime operations
- Quick Notes panel includes:
  - direct links for the most common admin path
  - clickable recently visited page links for the current web session
- Links to the dedicated `Command Status`, `Guild Settings`, feed pages, logs, docs, and account areas for the selected server

### `/admin/command-status`

- Scoped to the selected server
- Dedicated quick-toggle page for command enable/disable state
- Shows each command with:
  - effective access level (`Public`, `Mod Only`, `Named Roles`, `Custom Roles`, or `Disabled`)
  - current enabled/disabled state
- Uses the same guild-scoped command-permissions data the bot enforces at runtime
- Intended for fast on/off control without opening the deeper permissions editor

### `/admin/guild-settings`

- Scoped to the selected server
- Per-guild overrides for:
  - bot log channel
  - moderation log channel
  - firmware notify channel
  - firmware monitor enabled/disabled
  - Reddit feed monitor enabled/disabled
  - YouTube notifications enabled/disabled
  - LinkedIn notifications enabled/disabled
  - beta program notifications enabled/disabled
  - self-assign access role
  - welcome channel
  - welcome channel message
  - welcome DM enable/disable
  - welcome DM message
  - uploaded welcome image
  - image attachment enable/disable for channel and DM
- Bot log, moderation log, and firmware notification channels can be set here per guild
- Monitor feature flags can also be overridden here per guild
- These values override the global defaults configured in `/admin/settings`
- If a guild-level channel is left unset, the bot falls back to the corresponding global channel setting
- If a per-guild feature override is left unset, the bot falls back to the corresponding global feature toggle

Welcome-message placeholders:

- `{member_mention}`
- `{member_name}`
- `{display_name}`
- `{guild_name}`
- `{member_count}`
- `{account_created_at}`

How to configure welcome automation:

1. Open `/admin/guild-settings`
2. Select the target guild
3. Set `Welcome Channel` if you want a public join post
4. Enter `Welcome Channel Message`, or leave it blank to use the default
5. Enable `Send Welcome DM` if you want a DM on join
6. Enter `Welcome DM Message`, or leave it blank to use the default
7. Upload a welcome image if desired
8. Enable image attachment for channel, DM, or both
9. Save the guild settings

Notes:

- If no welcome channel is selected, the bot will not post a public welcome message
- If the member blocks DMs, the DM send is skipped and the join flow continues normally
- Supported image formats: `PNG`, `JPG`, `JPEG`, `WEBP`, `GIF`
- Upload size follows the configured web avatar upload limit (`WEB_AVATAR_MAX_UPLOAD_BYTES`; default `2097152` bytes / `2048 KiB`)
- Welcome images must be between `64x64` and `4096x4096`
- Per-guild monitor overrides use two controls:
  - `Override global setting`
  - `Enabled for this guild`
- If `Override global setting` is unchecked, the selected server follows the global setting from `/admin/settings`
- The page shows current uploaded image metadata:
  - filename
  - media type
  - size in bytes / KiB
  - width x height
- Recommended welcome image layout is landscape artwork around `1200x675` for clearer preview sizing inside Discord

### `/admin/role-access`

- Scoped to the selected server
- Shows each stored role-access mapping with:
  - 6-digit code
  - invite link
  - invite code
  - target role
  - current status
- Quick actions:
  - `Activate`
  - `Pause`
  - `Disable`
- Manual restore/add form lets admins enter:
  - 6-digit code
  - existing Discord invite URL or invite code
  - target role
  - initial status
- Paused and disabled entries stop working for both:
  - join-by-invite role assignment
  - `/enter_role`

### `/admin/settings`

- Global environment-backed settings editor
- Live dropdowns for known channel and role fields load from the currently selected server
- Managed-guild allowlist and utility integration settings
- Global feature toggles for:
  - firmware monitor
  - Reddit feed monitor
  - YouTube monitor
  - LinkedIn monitor
  - GL.iNet beta program monitor
- Disabling a monitor stops polling/posting but keeps its saved subscriptions and web pages available
- Web-session/security settings
- Auto-logout selection (`5`, `10`, `15`, `20`, `30`, `45`, `60`, `90`, `120` minutes)
- Writes to `WEB_ENV_FILE`, which should point to a writable path such as `${DATA_DIR}/web-settings.env`

### Feed and Profile Watchers

- `/admin/reddit-feeds`
  - Add subreddit-to-channel mappings
  - Edit existing subreddit or destination channel without deleting the row
  - Enable/disable or delete existing mappings
  - Editing a subreddit resets that feed's seen-post baseline so old posts are not reposted
- `/admin/youtube`
  - Add YouTube channel subscriptions
  - Edit an existing source URL or destination channel in place
  - Delete existing subscriptions
  - Editing reseeds the subscription from the current latest upload so old uploads are not replayed
- `/admin/linkedin`
  - Add LinkedIn profile subscriptions
  - Edit an existing profile URL or destination channel in place
  - Delete existing subscriptions
  - Editing reseeds the subscription from the current latest public post so old posts are not replayed

### `/status/everything` (Public Read-Only Status)

- Runtime observability view in web GUI
- CPU, memory, I/O, network, and uptime snapshot cards
- 24-hour rolling metrics summary (min/avg/max) retained in-memory
- Manual refresh plus auto-refresh interval dropdown (`1`, `5`, `10`, `30`, `60`, `120` seconds)
- Public and read-only (no login required)
- `/admin/observability` redirects to `/status/everything`

### `/admin/logs` (Login Required)

- Log viewer with dropdown selection (`bot.log`, `bot_log.log`, `container_errors.log`, `web_gui_audit.log`)
- Refresh button plus auto-refresh interval dropdown (`1`, `5`, `10`, `30`, `60`, `120` seconds)
- `Export All Logs` downloads a single ZIP archive containing the available runtime logs plus a manifest file
- Log timestamps in the GUI are rendered in readable UTC format
- Requires web GUI login

### `/admin/actions`

- Scoped to the selected server
- Read-only activity history for moderation actions and server-event log writes
- Timestamps are rendered in readable UTC format
- Useful for reviewing what the bot did without reading raw log files

### `/admin/account`

- Self-service account page for the current web GUI user
- Change password
- Change email
- Update first name, last name, and display name

### `/admin/member-activity`

- Scoped to the selected server
- Read-only top 20 member activity tables for:
  - last 90 days
  - last 30 days
  - last 7 days
  - last 24 hours
- Each table shows:
  - message count
  - active day count
  - last seen timestamp
- Timestamps are rendered in readable UTC format
- Export option at the bottom of the page downloads a compressed ZIP archive for the selected server
- Export respects the selected server context; there is no cross-guild combined export
- Export includes:
  - per-window leaderboard CSV files
  - raw member activity summary CSV
  - raw hourly activity CSV
  - JSON summary manifest

### `/admin/command-permissions`

- Per-command access policy editor for the selected server
- Modes: `default`, `public`, `disabled`, `custom_roles`
- Multi-select role dropdown by role name
- Manual role-ID entry fallback if catalog is incomplete

### `/admin/reddit-feeds`

- Scoped to the selected server
- Add a subreddit feed using a subreddit name or Reddit `/r/` URL
- Pick the target Discord text channel from a live dropdown
- Global Reddit polling interval dropdown (default every 30 minutes)
- Feed list shows enabled state, last checked time, last posted time, and last error
- New subscriptions baseline existing posts first, then only publish newer Reddit submissions

### `/admin/youtube`

- Scoped to the selected server
- Add a YouTube channel URL and target Discord text channel
- Stores last seen video metadata so only newer uploads are posted
- Per-subscription enable/disable and delete controls

### `/admin/linkedin`

- Scoped to the selected server
- Add a public LinkedIn profile URL and target Discord text channel
- Uses the public profile page to detect newer visible posts
- Stores last seen post metadata so only newer posts are announced
- Best-effort public-profile monitoring: private or login-gated activity will not be detected

### `/admin/beta-programs`

- Scoped to the selected server
- Add the public GL.iNet beta-testing page monitor and target Discord text channel
- Detects when beta programs are added to or removed from the page
- Stores the last seen program snapshot per guild/channel so only changes are announced
- Best-effort public-page monitoring: if GL.iNet changes the page structure, the watcher may need adjustment

### `/admin/documentation`

- Built-in documentation page inside the web GUI
- Presents operator guidance and shortcuts for bot administration topics

### `/admin/wiki`

- Embedded wiki/documentation viewer in the web GUI
- Useful when the operator wants docs without leaving the admin interface

### `/admin/tag-responses`

- JSON tag editor scoped to the selected server
- Save + runtime reload
- Dynamic slash refresh trigger (restart not required)

### `/admin/bulk-role-csv`

- Scoped to the selected server
- CSV upload and target-role selection
- Assignment execution with timeout protections
- Structured results with unmatched/ambiguous/failure sections

### `/admin/users`

- User and role management (`Admin` / `Read-only` / `Glinet-Read-Only` / `Glinet-RW`)
- User creation with password policy enforcement
- Admins can edit another web user's:
  - first name
  - last name
  - display name
  - email
- Admins can reset another web user's password from the same page
- Password visibility toggle in create/reset forms

### `/admin/bot-profile`

- Read bot identity
- Update server nickname/listing label for the selected server using a guild-scoped form
- Rename bot username using a separate dedicated global form/action
- Upload avatar image

Rename/profile updates are admin-only and web-GUI-only (read-only users can view this page but cannot apply changes).

Scope notes:

- Guild-scoped:
  - `/admin/dashboard`
  - `/admin/guild-settings`
  - `/admin/actions`
  - `/admin/member-activity`
  - `/admin/command-permissions`
  - `/admin/reddit-feeds`
  - `/admin/youtube`
  - `/admin/linkedin`
  - `/admin/tag-responses`
  - `/admin/bulk-role-csv`
  - server nickname in `/admin/bot-profile`
- Global:
  - `.env` settings in `/admin/settings`
  - bot username/avatar in `/admin/bot-profile`
  - web users
  - logs and observability

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
- `WEB_HTTP_PUBLISH`
- `WEB_HTTPS_PUBLISH`
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
- `MANAGED_GUILD_IDS`
- `ENABLE_MEMBERS_INTENT`
- `COMMAND_RESPONSES_EPHEMERAL`
- `PUPPY_IMAGE_API_URL`
- `PUPPY_IMAGE_TIMEOUT_SECONDS`
- `SHORTENER_ENABLED`
- `SHORTENER_BASE_URL`
- `SHORTENER_TIMEOUT_SECONDS`
- `YOUTUBE_NOTIFY_ENABLED`
- `YOUTUBE_POLL_INTERVAL_SECONDS`
- `YOUTUBE_REQUEST_TIMEOUT_SECONDS`
- `UPTIME_STATUS_ENABLED`
- `UPTIME_STATUS_PAGE_URL`
- `UPTIME_STATUS_TIMEOUT_SECONDS`

## Related Pages

- [Reverse Proxy Web GUI](Reverse-Proxy-Web-GUI.md)
- [Environment Variables](Environment-Variables.md)
- [Security Hardening](Security-Hardening.md)

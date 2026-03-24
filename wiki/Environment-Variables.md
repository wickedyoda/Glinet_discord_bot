# Environment Variables

This page lists all supported environment variables, defaults, and accepted options.

## Value Conventions

- Boolean flags: use `true`/`false` (also accepted in web settings: `1/0`, `yes/no`, `on/off`)
- Channel field `firmware_notification_channel`: numeric channel ID or `<#channel_id>`
- Cron field `firmware_check_schedule`: valid 5-field cron in UTC
- URL fields: include scheme (`http://` or `https://`) where noted

## Required

| Variable | Default | Allowed / Options | Notes |
|---|---|---|---|
| `DISCORD_TOKEN` | none | Discord bot token string | Required to start bot |
| `GUILD_ID` | none | Integer guild ID | Required primary/default guild ID used for legacy migration and fallback behavior |
| `MANAGED_GUILD_IDS` | empty | Comma/space-separated positive guild IDs | Optional allowlist. When set, the bot syncs commands and processes guild events only for listed guilds. |

## Core

| Variable | Default | Allowed / Options | Notes |
|---|---|---|---|
| `BOT_LOG_CHANNEL_ID` | `0` | Integer, `>= 0` | Global fallback bot log/activity channel ID. Selected guild settings can override it per server. |
| `DATA_DIR` | `data` | Path string | Persistent runtime data directory |
| `LOG_DIR` | `/logs` | Path string | Directory for `bot.log`, `bot_log.log`, `container_errors.log`, and `web_gui_audit.log` |
| `LOG_HARDEN_FILE_PERMISSIONS` | `true` | Boolean | Best-effort log storage hardening (`LOG_DIR` -> `0700`, log files -> `0600`) |
| `LOG_RETENTION_DAYS` | `90` | Integer, `>= 1` | Retention window for rotated logs |
| `LOG_ROTATION_INTERVAL_DAYS` | `1` | Integer, `>= 1` | Rotation interval for runtime logs |
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` | Runtime bot/web verbosity |
| `CONTAINER_LOG_LEVEL` | `ERROR` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` | Threshold for `${LOG_DIR}/container_errors.log` |
| `DISCORD_LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` | Discord/werkzeug logger verbosity (keep `INFO` or higher to avoid verbose payload logs) |

## Search and Docs

| Variable | Default | Allowed / Options | Notes |
|---|---|---|---|
| `FORUM_BASE_URL` | `https://forum.gl-inet.com` | URL | Forum search base URL |
| `FORUM_MAX_RESULTS` | `5` | Integer, `>= 1` | Max forum links returned |
| `REDDIT_SUBREDDIT` | `GlInet` | Subreddit name, URL, or `r/<name>` format | Subreddit used by `/search_reddit` and `!searchreddit` |
| `REDDIT_FEED_CHECK_SCHEDULE` | `*/30 * * * *` | Valid 5-field cron (UTC) | Poll interval for web-managed Reddit feed subscriptions |
| `DOCS_MAX_RESULTS_PER_SITE` | `2` | Integer, `>= 1` | Max docs results per docs source |
| `DOCS_INDEX_TTL_SECONDS` | `3600` | Integer, `>= 60` | Docs index cache TTL |
| `SEARCH_RESPONSE_MAX_CHARS` | `1900` | Integer, `>= 200` | Max chars in search response |
| `BOT_HELP_WIKI_URL` | `https://github.com/wickedyoda/Glinet_discord_bot/wiki/Home` | URL with `http://` or `https://` | Link target shown in `/help` for advanced docs |

## Utility Integrations

| Variable | Default | Allowed / Options | Notes |
|---|---|---|---|
| `COMMAND_RESPONSES_EPHEMERAL` | `false` | Boolean | When true, utility slash-command replies default to ephemeral where supported |
| `ENABLE_MEMBERS_INTENT` | `true` | Boolean | Enables Discord members intent for richer moderation/member flows |
| `MEMBER_ACTIVITY_BACKFILL_ENABLED` | `false` | Boolean | Run a one-time startup backfill for member activity history |
| `MEMBER_ACTIVITY_BACKFILL_GUILD_ID` | empty | Integer guild ID | Optional guild to backfill. If blank, falls back to `GUILD_ID` |
| `MEMBER_ACTIVITY_BACKFILL_SINCE` | empty | `YYYY-MM-DD` or ISO timestamp | Inclusive UTC lower bound for the member activity backfill run |
| `MEMBER_ACTIVITY_ENCRYPTION_KEY` | empty | Passphrase or Fernet key | Encrypts member-activity identity fields at rest. If blank, the bot creates `${DATA_DIR}/member_activity.key` automatically |
| `PUPPY_IMAGE_API_URL` | `https://random.dog/woof.json` | URL | Source used by `/happy` |
| `PUPPY_IMAGE_TIMEOUT_SECONDS` | `10` | Integer, `>= 1` | Timeout for `/happy` image fetch |
| `SHORTENER_ENABLED` | `false` | Boolean | Enables `/shorten` and `/expand` integration |
| `SHORTENER_BASE_URL` | empty | URL | Base URL of the shortener service |
| `SHORTENER_TIMEOUT_SECONDS` | `10` | Integer, `>= 1` | Timeout for shortener API requests |
| `YOUTUBE_NOTIFY_ENABLED` | `false` | Boolean | Enables the YouTube subscription monitor and web page |
| `YOUTUBE_POLL_INTERVAL_SECONDS` | `1800` | Integer, `>= 60` | Poll cadence for YouTube subscriptions |
| `YOUTUBE_REQUEST_TIMEOUT_SECONDS` | `20` | Integer, `>= 1` | Timeout for YouTube feed/channel requests |
| `LINKEDIN_NOTIFY_ENABLED` | `true` | Boolean | Enables the LinkedIn profile monitor and web page |
| `LINKEDIN_POLL_INTERVAL_SECONDS` | `900` | Integer, `>= 60` | Poll cadence for LinkedIn profile checks |
| `LINKEDIN_REQUEST_TIMEOUT_SECONDS` | `15` | Integer, `>= 5` | Timeout for LinkedIn public profile requests |
| `BETA_PROGRAM_PAGE_URL` | `https://www.gl-inet.com/beta-testing/#register` | URL | GL.iNet beta testing page monitored for program changes |
| `BETA_PROGRAM_NOTIFY_ENABLED` | `true` | Boolean | Enables the GL.iNet beta program monitor and web page |
| `BETA_PROGRAM_POLL_INTERVAL_SECONDS` | `900` | Integer, `>= 60` | Poll cadence for GL.iNet beta program checks |
| `BETA_PROGRAM_REQUEST_TIMEOUT_SECONDS` | `20` | Integer, `>= 5` | Timeout for GL.iNet beta page requests |
| `UPTIME_STATUS_ENABLED` | `false` | Boolean | Enables `/uptime` command |
| `UPTIME_STATUS_PAGE_URL` | `https://status.example.invalid/status/everything` | URL | Public status page used for uptime summary lookups |
| `UPTIME_STATUS_TIMEOUT_SECONDS` | `10` | Integer, `>= 1` | Timeout for uptime status fetch |

## Moderation

| Variable | Default | Allowed / Options | Notes |
|---|---|---|---|
| `MODERATOR_ROLE_ID` | `1294957416294645771` | Integer role ID | Moderator role gate |
| `ADMIN_ROLE_ID` | `1138302148292116551` | Integer role ID | Additional role gate |
| `MOD_LOG_CHANNEL_ID` | `1311820410269995009` | Integer channel ID | Moderation/server log channel |
| `KICK_PRUNE_HOURS` | `72` | Integer, `>= 1` | Prune window for kick actions |

## CSV Role Assignment

| Variable | Default | Allowed / Options | Notes |
|---|---|---|---|
| `CSV_ROLE_ASSIGN_MAX_NAMES` | `500` | Integer, `>= 1` | Max unique names accepted |
| `WEB_BULK_ASSIGN_TIMEOUT_SECONDS` | `300` | Integer, `>= 30` | Timeout for web CSV assignment execution |
| `WEB_BULK_ASSIGN_MAX_UPLOAD_BYTES` | `2097152` | Integer, `>= 1024` | Max CSV upload size in bytes |
| `WEB_BULK_ASSIGN_REPORT_LIST_LIMIT` | `50` | Integer, `>= 1` | Max items shown per result section |

## Firmware Monitor

| Variable | Default | Allowed / Options | Notes |
|---|---|---|---|
| `firmware_notification_channel` | none | Channel ID or `<#channel_id>` | Required to enable firmware notifications |
| `FIRMWARE_FEED_URL` | `https://gl-fw.remotetohome.io/` | URL | Firmware source URL |
| `firmware_check_schedule` | `*/30 * * * *` | Valid 5-field cron (UTC) | Primary scheduler |
| `FIRMWARE_REQUEST_TIMEOUT_SECONDS` | `30` | Integer, `>= 5` | HTTP timeout for firmware fetch |
| `FIRMWARE_RELEASE_NOTES_MAX_CHARS` | `900` | Integer, `>= 200` | Legacy compatibility value (compact firmware notifications no longer send long release note excerpts) |

## Web Admin

| Variable | Default | Allowed / Options | Notes |
|---|---|---|---|
| `WEB_ENABLED` | `true` | Boolean | Enable/disable web admin interface |
| `WEB_BIND_HOST` | `127.0.0.1` | Host/IP string | Use `0.0.0.0` in container deployments |
| `WEB_PORT` | `8080` | Integer port | Internal web service port |
| `WEB_HTTP_PUBLISH` | `8080` | `HOST_PORT` or `HOST_IP:HOST_PORT` | Optional Docker Compose HTTP publish override. Leave unset to disable explicit host/IP pinning and publish `8080` on all host interfaces. |
| `WEB_HTTPS_ENABLED` | `true` | Boolean | Enable built-in HTTPS listener |
| `WEB_HTTPS_PORT` | `8081` | Integer port | Internal HTTPS web service port |
| `WEB_HTTPS_PUBLISH` | `8081` | `HOST_PORT` or `HOST_IP:HOST_PORT` | Optional Docker Compose HTTPS publish override. Leave unset to disable explicit host/IP pinning and publish `8081` on all host interfaces. |
| `WEB_SESSION_TIMEOUT_MINUTES` | `60` | `5`, `10`, `15`, `20`, `30`, `45`, `60`, `90`, `120` | Inactivity timeout for all users (including remember-login sessions) |
| `WEB_PUBLIC_BASE_URL` | empty | URL with `http://` or `https://` | External URL used for origin checks behind proxy |
| `WEB_SSL_DIR` | `${DATA_DIR}/ssl` | Path string | Directory used for HTTPS certificate and key files |
| `WEB_SSL_CERT_FILE` | `tls.crt` | Filename or absolute path | Certificate file used by built-in HTTPS |
| `WEB_SSL_KEY_FILE` | `tls.key` | Filename or absolute path | Private key file used by built-in HTTPS |
| `WEB_SSL_COMMON_NAME` | `localhost` | Hostname string | Subject/Common Name used when generating a self-signed fallback certificate |
| `WEB_ENV_FILE` | `${DATA_DIR}/web-settings.env` | Path string | Writable env file path used by the web settings editor and loaded again on startup |
| `WEB_RESTART_ENABLED` | `true` | Boolean | Enables admin restart button |
| `WEB_GITHUB_WIKI_URL` | `https://github.com/wickedyoda/Glinet_discord_bot/wiki` | URL with `http://` or `https://` | Header docs link |
| `WEB_ADMIN_DEFAULT_USERNAME` | `admin@example.com` | Valid email | First-boot admin email |
| `WEB_ADMIN_DEFAULT_PASSWORD` | empty | Must satisfy password policy | Required on first boot when no web users exist |
| `WEB_ADMIN_SESSION_SECRET` | generated at runtime if unset | Secret string | Session signing secret |
| `WEB_SESSION_COOKIE_SECURE` | `true` | Boolean | Secure cookie flag (HTTPS recommended) |
| `WEB_SESSION_COOKIE_SAMESITE` | `Lax` | `Lax`, `Strict`, `None` | Session cookie SameSite policy (`None` requires secure HTTPS) |
| `WEB_TRUST_PROXY_HEADERS` | `true` | Boolean | Trust forwarded host/proto/IP headers |
| `WEB_ENFORCE_CSRF` | `true` | Boolean | CSRF checks on state-changing requests |
| `WEB_ENFORCE_SAME_ORIGIN_POSTS` | `true` | Boolean | Same-origin checks for state-changing requests |
| `WEB_HARDEN_FILE_PERMISSIONS` | `true` | Boolean | Best-effort file permission hardening |
| `WEB_DISCORD_CATALOG_TTL_SECONDS` | `120` | Integer, `>= 15` | Cache TTL for Discord channels/roles catalog |
| `WEB_DISCORD_CATALOG_FETCH_TIMEOUT_SECONDS` | `20` | Integer, `>= 5` | Timeout for Discord catalog fetch |
| `WEB_BOT_PROFILE_TIMEOUT_SECONDS` | `20` | Integer, `>= 5` | Timeout for bot profile web actions |
| `WEB_AVATAR_MAX_UPLOAD_BYTES` | `2097152` | Integer, `>= 1024` | Max upload size for bot-profile avatars and guild welcome images |

## Compatibility Aliases

| Variable | Used As | Notes |
|---|---|---|
| `FIRMWARE_NOTIFICATION_CHANNEL` | Fallback for `firmware_notification_channel` | Uppercase alias for Portainer/stack compatibility |
| `FIRMWARE_CHECK_SCHEDULE` | Fallback for `firmware_check_schedule` | Uppercase alias for Portainer/stack compatibility |
| `FIRMWARE_NOTIFY_CHANNEL_ID` | Fallback for `firmware_notification_channel` | Legacy alias |
| `FIRMWARE_CHECK_INTERVAL_SECONDS` | Legacy fallback scheduler | Used only when `firmware_check_schedule` is empty |
| `WEB_ADMIN_DEFAULT_EMAIL` | Preferred over `WEB_ADMIN_DEFAULT_USERNAME` when set | Legacy/admin alias |
| `GENERAL_CHANNEL_ID` | Fallback for `BOT_LOG_CHANNEL_ID` | Legacy alias |

## Password Policy (Web Users)

- Minimum 6 characters
- Maximum 16 characters
- At least 2 numbers
- At least 1 uppercase letter
- At least 1 symbol

## Configuration Profiles

### Local Development (No External Proxy)

```env
WEB_BIND_HOST=0.0.0.0
WEB_PORT=8080
WEB_HTTP_PUBLISH=8080
WEB_HTTPS_ENABLED=true
WEB_HTTPS_PORT=8081
WEB_HTTPS_PUBLISH=8081
WEB_PUBLIC_BASE_URL=http://localhost:8080/
WEB_SESSION_COOKIE_SECURE=false
WEB_TRUST_PROXY_HEADERS=false
WEB_ENFORCE_CSRF=true
WEB_ENFORCE_SAME_ORIGIN_POSTS=true
```

### Reverse Proxy Production (Recommended)

```env
WEB_BIND_HOST=0.0.0.0
WEB_PORT=8080
WEB_HTTP_PUBLISH=127.0.0.1:8080
WEB_HTTPS_ENABLED=true
WEB_HTTPS_PORT=8081
WEB_HTTPS_PUBLISH=127.0.0.1:8081
WEB_PUBLIC_BASE_URL=https://discord-admin.example.com/
WEB_SESSION_COOKIE_SECURE=true
WEB_TRUST_PROXY_HEADERS=true
WEB_ENFORCE_CSRF=true
WEB_ENFORCE_SAME_ORIGIN_POSTS=true
```

### Hardened Logging Profile

```env
LOG_DIR=/logs
LOG_HARDEN_FILE_PERMISSIONS=true
LOG_LEVEL=INFO
CONTAINER_LOG_LEVEL=ERROR
WEB_HARDEN_FILE_PERMISSIONS=true
```

## Reference

- Built-in HTTPS creates a self-signed certificate in `${DATA_DIR}/ssl/` when no cert/key files are present.
- Replace `${DATA_DIR}/ssl/tls.crt` and `${DATA_DIR}/ssl/tls.key` with your own certificate files if you want browsers to trust the HTTPS listener.

- Complete `.env` template: [`.env.example`](../.env.example)
- Deployment defaults/examples: [`README.md`](../README.md)

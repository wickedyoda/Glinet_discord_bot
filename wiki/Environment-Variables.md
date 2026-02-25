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
| `GUILD_ID` | none | Integer guild ID | Required to scope guild operations |

## Core

| Variable | Default | Allowed / Options | Notes |
|---|---|---|---|
| `BOT_LOG_CHANNEL_ID` | `0` | Integer, `>= 0` | Bot log/activity channel ID (used for invite fallback and bot activity routing) |
| `DATA_DIR` | `data` | Path string | Persistent runtime data directory |
| `LOG_DIR` | `/logs` | Path string | Directory for `bot.log`, `bot_log.log`, `container_errors.log`, and `web_gui_audit.log` |
| `LOG_HARDEN_FILE_PERMISSIONS` | `true` | Boolean | Best-effort log storage hardening (`LOG_DIR` -> `0700`, log files -> `0600`) |
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` | Runtime bot/web verbosity |
| `CONTAINER_LOG_LEVEL` | `ERROR` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` | Threshold for `${LOG_DIR}/container_errors.log` |
| `DISCORD_LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` | Discord/werkzeug logger verbosity (keep `INFO` or higher to avoid verbose payload logs) |

## Search and Docs

| Variable | Default | Allowed / Options | Notes |
|---|---|---|---|
| `FORUM_BASE_URL` | `https://forum.gl-inet.com` | URL | Forum search base URL |
| `FORUM_MAX_RESULTS` | `5` | Integer, `>= 1` | Max forum links returned |
| `REDDIT_SUBREDDIT` | `GlInet` | Subreddit name, URL, or `r/<name>` format | Subreddit used by `/search_reddit` and `!searchreddit` |
| `DOCS_MAX_RESULTS_PER_SITE` | `2` | Integer, `>= 1` | Max docs results per docs source |
| `DOCS_INDEX_TTL_SECONDS` | `3600` | Integer, `>= 60` | Docs index cache TTL |
| `SEARCH_RESPONSE_MAX_CHARS` | `1900` | Integer, `>= 200` | Max chars in search response |

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
| `WEB_HOST_PORT` | `8080` | Integer port | Compose host mapping variable |
| `WEB_SESSION_TIMEOUT_MINUTES` | `5` | `5`, `10`, `15`, `20`, `25`, `30` | Inactivity timeout for non-remembered sessions |
| `WEB_PUBLIC_BASE_URL` | empty | URL with `http://` or `https://` | External URL used for origin checks behind proxy |
| `WEB_ENV_FILE` | `.env` | Path string | Env file path used by web settings editor |
| `WEB_RESTART_ENABLED` | `true` | Boolean | Enables admin restart button |
| `WEB_GITHUB_WIKI_URL` | `http://discord.glinet.wickedyoda.com/wiki` | URL with `http://` or `https://` | Header docs link |
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
| `WEB_AVATAR_MAX_UPLOAD_BYTES` | `2097152` | Integer, `>= 1024` | Max avatar upload size |

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
WEB_HOST_PORT=8080
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

- Complete `.env` template: [`.env.example`](../.env.example)
- Deployment defaults/examples: [`README.md`](../README.md)

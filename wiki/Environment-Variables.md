# Environment Variables

This page reflects the current environment behavior in bot.py and .env.example.

## Conventions

- Boolean values: `true` / `false`
- Cron values: 5-field UTC cron
- Channel IDs: integer or `<#channel_id>` text form where noted
- Path values: container path unless otherwise stated

## Required

| Variable | Default | Notes |
|---|---|---|
| `DISCORD_TOKEN` | none | Discord bot token |
| `GUILD_ID` | none | Primary/default guild ID |
| `MANAGED_GUILD_IDS` | empty | Optional allowlist of guild IDs |

## Core

| Variable | Default | Notes |
|---|---|---|
| `BOT_LOG_CHANNEL_ID` | `0` | Global bot log/activity channel |
| `DATA_DIR` | `data` | Runtime data directory |
| `LOG_DIR` | `/logs` | Log file directory |
| `LOG_HARDEN_FILE_PERMISSIONS` | `true` | Restrictive file permissions for logs |
| `LOG_RETENTION_DAYS` | `90` | Rotated log retention |
| `LOG_ROTATION_INTERVAL_DAYS` | `1` | Log rotation interval |
| `LOG_LEVEL` | `INFO` | Bot/web verbosity |
| `CONTAINER_LOG_LEVEL` | `ERROR` | `container_errors.log` threshold |
| `DISCORD_LOG_LEVEL` | `INFO` | Discord/werkzeug verbosity |

## Web Admin

| Variable | Default | Notes |
|---|---|---|
| `WEB_ENABLED` | `true` | Enable web admin |
| `WEB_BIND_HOST` | `0.0.0.0` | Web bind host |
| `WEB_PORT` | `8080` | HTTP port |
| `WEB_HTTP_PUBLISH` | unset | Optional host:port publish override |
| `WEB_HTTPS_ENABLED` | `true` | Built-in HTTPS listener |
| `WEB_HTTPS_PORT` | `8081` | HTTPS port |
| `WEB_HTTPS_PUBLISH` | unset | Optional HTTPS publish override |
| `WEB_SESSION_TIMEOUT_MINUTES` | `60` | Allowed values: 5, 10, 15, 20, 30, 45, 60, 90, 120 |
| `WEB_PUBLIC_BASE_URL` | empty | External URL behind proxy |
| `WEB_SSL_DIR` | `${DATA_DIR}/ssl` | TLS directory |
| `WEB_SSL_CERT_FILE` | `tls.crt` | Certificate filename or path |
| `WEB_SSL_KEY_FILE` | `tls.key` | Key filename or path |
| `WEB_SSL_COMMON_NAME` | `localhost` | Fallback cert common name |
| `WEB_ENV_FILE` | `${DATA_DIR}/web-settings.env` | Writable web settings env file |
| `WEB_RESTART_ENABLED` | `true` | Admin restart button |
| `WEB_GITHUB_WIKI_URL` | wiki URL | Docs link shown in web GUI |
| `WEB_ADMIN_DEFAULT_USERNAME` | `admin@example.com` | First-boot admin email |
| `WEB_ADMIN_DEFAULT_EMAIL` | empty | Alias used before `WEB_ADMIN_DEFAULT_USERNAME` when set |
| `WEB_ADMIN_DEFAULT_PASSWORD` | empty | Required on first boot until a web user exists |
| `WEB_SESSION_SECRET` | generated at runtime | Session signing secret |
| `WEB_SESSION_COOKIE_SECURE` | `true` | Secure cookie flag |
| `WEB_SESSION_COOKIE_SAMESITE` | `Lax` | SameSite policy; `None` requires HTTPS |
| `WEB_TRUST_PROXY_HEADERS` | `true` | Trust forwarded host/proto/IP headers |
| `WEB_ENFORCE_CSRF` | `true` | CSRF enforcement |
| `WEB_ENFORCE_SAME_ORIGIN_POSTS` | `true` | Same-origin POST checks |
| `WEB_HARDEN_FILE_PERMISSIONS` | `true` | Best-effort file permission hardening |
| `WEB_DISCORD_CATALOG_TTL_SECONDS` | `120` | Discord catalog cache TTL |
| `WEB_DISCORD_CATALOG_FETCH_TIMEOUT_SECONDS` | `20` | Discord catalog fetch timeout |
| `WEB_BULK_ASSIGN_TIMEOUT_SECONDS` | `300` | Bulk CSV assignment timeout |
| `WEB_BULK_ASSIGN_MAX_UPLOAD_BYTES` | `2097152` | Max CSV upload size |
| `WEB_BULK_ASSIGN_REPORT_LIST_LIMIT` | `50` | Max report items |
| `WEB_BOT_PROFILE_TIMEOUT_SECONDS` | `20` | Bot profile action timeout |
| `WEB_AVATAR_MAX_UPLOAD_BYTES` | `2097152` | Avatar and welcome image upload cap |

## Access, Moderation, and CSV

| Variable | Default | Notes |
|---|---|---|
| `ENABLE_MEMBERS_INTENT` | `true` | Members intent |
| `MODERATOR_ROLE_ID` | example ID | Moderator role gate |
| `ADMIN_ROLE_ID` | example ID | Admin role gate |
| `MOD_LOG_CHANNEL_ID` | example ID | Default moderation log channel |
| `KICK_PRUNE_HOURS` | `72` | Kick prune window |
| `CSV_ROLE_ASSIGN_MAX_NAMES` | `500` | Max unique names in bulk role CSV |

## Member Activity

| Variable | Default | Notes |
|---|---|---|
| `MEMBER_ACTIVITY_BACKFILL_ENABLED` | `false` | Startup backfill |
| `MEMBER_ACTIVITY_BACKFILL_GUILD_ID` | empty | Backfill target guild; defaults to `GUILD_ID` |
| `MEMBER_ACTIVITY_BACKFILL_SINCE` | empty | UTC lower bound for backfill |
| `MEMBER_ACTIVITY_ENCRYPTION_KEY` | empty | Enables external key management |
| `MEMBER_ACTIVITY_RECENT_RETENTION_DAYS` | `200` | Recent activity retention |

## Monitoring and Feeds

| Variable | Default | Notes |
|---|---|---|
| `FIRMWARE_MONITOR_ENABLED` | `true` | Firmware monitor toggle |
| `firmware_notification_channel` | empty | Default firmware notification channel |
| `FIRMWARE_NOTIFICATION_CHANNEL` | empty | Uppercase alias |
| `FIRMWARE_NOTIFY_CHANNEL_ID` | empty | Legacy alias |
| `FIRMWARE_FEED_URL` | `https://gl-fw.remotetohome.io/` | Firmware source |
| `firmware_check_schedule` | `*/30 * * * *` | Firmware poll cron |
| `FIRMWARE_CHECK_SCHEDULE` | empty | Uppercase alias |
| `FIRMWARE_REQUEST_TIMEOUT_SECONDS` | `30` | Firmware fetch timeout |
| `FIRMWARE_RELEASE_NOTES_MAX_CHARS` | `900` | Release note excerpt cap |
| `REDDIT_FEED_CHECK_SCHEDULE` | `*/30 * * * *` | Reddit feed poll cron |
| `REDDIT_FEED_NOTIFY_ENABLED` | `true` | Reddit feed toggle |
| `REDDIT_SUBREDDIT` | `GlInet` | Subreddit name, URL, or `r/<name>` |
| `YOUTUBE_NOTIFY_ENABLED` | `false` | YouTube monitor toggle |
| `YOUTUBE_POLL_INTERVAL_SECONDS` | `300` | YouTube poll cadence |
| `YOUTUBE_REQUEST_TIMEOUT_SECONDS` | `12` | YouTube fetch timeout |
| `LINKEDIN_NOTIFY_ENABLED` | `true` | LinkedIn monitor toggle |
| `LINKEDIN_POLL_INTERVAL_SECONDS` | `900` | LinkedIn poll cadence |
| `LINKEDIN_REQUEST_TIMEOUT_SECONDS` | `15` | LinkedIn fetch timeout |
| `BETA_PROGRAM_PAGE_URL` | GL.iNet beta page | Beta program monitor URL |
| `BETA_PROGRAM_NOTIFY_ENABLED` | `true` | Beta program monitor toggle |
| `BETA_PROGRAM_POLL_INTERVAL_SECONDS` | `900` | Beta program poll cadence |
| `BETA_PROGRAM_REQUEST_TIMEOUT_SECONDS` | `20` | Beta program fetch timeout |
| `SERVICE_MONITOR_ENABLED` | `false` | Direct service checks toggle |
| `SERVICE_MONITOR_DEFAULT_CHANNEL_ID` | empty | Default service alert channel |
| `SERVICE_MONITOR_CHECK_SCHEDULE` | `*/5 * * * *` | Service check cron |
| `SERVICE_MONITOR_REQUEST_TIMEOUT_SECONDS` | `10` | Service check timeout |
| `SERVICE_MONITOR_TARGETS_JSON` | `[]` | Service targets JSON |
| `UPTIME_STATUS_ENABLED` | `false` | Uptime status toggle |
| `UPTIME_STATUS_NOTIFY_ENABLED` | `false` | Uptime alert toggle |
| `UPTIME_STATUS_NOTIFY_CHANNEL_ID` | empty | Uptime alert channel |
| `UPTIME_STATUS_CHECK_SCHEDULE` | `*/5 * * * *` | Uptime check cron |
| `UPTIME_STATUS_PAGE_URL` | example status page | Public status page |
| `UPTIME_STATUS_INSTANCE_URL` | empty | Authenticated Uptime Kuma URL |
| `UPTIME_STATUS_API_KEY` | empty | Uptime Kuma API key |
| `UPTIME_STATUS_TIMEOUT_SECONDS` | `10` | Uptime fetch timeout |
| `UPTIME_STATUS_VERIFY_TLS` | `true` | Uptime TLS verification |

## Search and Docs

| Variable | Default | Notes |
|---|---|---|
| `FORUM_BASE_URL` | `https://forum.gl-inet.com` | Forum search base URL |
| `FORUM_MAX_RESULTS` | `5` | Max forum results |
| `FORUM_REQUEST_TIMEOUT_SECONDS` | `10` | Forum request timeout |
| `FORUM_API_KEY` | empty | Optional Discourse API key |
| `FORUM_API_USERNAME` | empty | Optional Discourse username |
| `OPENWRT_FORUM_REQUEST_TIMEOUT_SECONDS` | `10` | OpenWrt request timeout |
| `OPENWRT_FORUM_API_KEY` | empty | Optional OpenWrt API key |
| `OPENWRT_FORUM_API_USERNAME` | empty | Optional OpenWrt username |
| `DOCS_MAX_RESULTS_PER_SITE` | `2` | Max docs results per site |
| `DOCS_INDEX_TTL_SECONDS` | `3600` | Docs index cache TTL |
| `SEARCH_RESPONSE_MAX_CHARS` | `1900` | Search response cap |

## Utilities

| Variable | Default | Notes |
|---|---|---|
| `COMMAND_RESPONSES_EPHEMERAL` | `false` | Default ephemeral replies |
| `PUPPY_IMAGE_API_URL` | `https://dog.ceo/api/breeds/image/random` | `/happy` source |
| `PUPPY_IMAGE_TIMEOUT_SECONDS` | `8` | `/happy` timeout |
| `SHORTENER_ENABLED` | `false` | Shortener toggle |
| `SHORTENER_BASE_URL` | empty | Shortener base URL |
| `SHORTENER_TIMEOUT_SECONDS` | `10` | Shortener timeout |

## Web Password Policy

- Minimum 6 characters
- Maximum 16 characters
- At least 2 numbers
- At least 1 uppercase letter
- At least 1 symbol

## Compatibility Aliases

| Variable | Resolved As | Notes |
|---|---|---|
| `FIRMWARE_NOTIFICATION_CHANNEL` | fallback for `firmware_notification_channel` | Uppercase alias |
| `FIRMWARE_CHECK_SCHEDULE` | fallback for `firmware_check_schedule` | Uppercase alias |
| `FIRMWARE_NOTIFY_CHANNEL_ID` | fallback for `firmware_notification_channel` | Legacy alias |
| `FIRMWARE_CHECK_INTERVAL_SECONDS` | legacy scheduler | Used when `firmware_check_schedule` is empty |
| `WEB_ADMIN_DEFAULT_EMAIL` | preferred over `WEB_ADMIN_DEFAULT_USERNAME` when set | Legacy admin alias |
| `GENERAL_CHANNEL_ID` | fallback for `BOT_LOG_CHANNEL_ID` | Legacy alias |

## Reference

- Complete template: [`.env.example`](../.env.example)
- Deployment defaults: [`README.md`](../README.md)

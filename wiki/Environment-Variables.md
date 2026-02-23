# Environment Variables

This page groups runtime variables by feature area.

## Core

- `DISCORD_TOKEN` (required)
- `GUILD_ID` (required)
- `GENERAL_CHANNEL_ID`
- `DATA_DIR`
- `LOG_LEVEL`

## Search / Docs

- `FORUM_BASE_URL`
- `FORUM_MAX_RESULTS`
- `DOCS_MAX_RESULTS_PER_SITE`
- `DOCS_INDEX_TTL_SECONDS`
- `SEARCH_RESPONSE_MAX_CHARS`

## Moderation

- `MODERATOR_ROLE_ID`
- `ADMIN_ROLE_ID`
- `MOD_LOG_CHANNEL_ID`
- `KICK_PRUNE_HOURS`

## CSV Role Assignment

- `CSV_ROLE_ASSIGN_MAX_NAMES`
- `WEB_BULK_ASSIGN_TIMEOUT_SECONDS`
- `WEB_BULK_ASSIGN_MAX_UPLOAD_BYTES`
- `WEB_BULK_ASSIGN_REPORT_LIST_LIMIT`

## Firmware Monitor

- `firmware_notification_channel`
- `FIRMWARE_FEED_URL`
- `firmware_check_schedule`
- `FIRMWARE_REQUEST_TIMEOUT_SECONDS`
- `FIRMWARE_RELEASE_NOTES_MAX_CHARS`

## Web Admin

- `WEB_ENABLED`
- `WEB_BIND_HOST` (default local bind for non-container runs; set to `0.0.0.0` inside container)
- `WEB_PORT`
- `WEB_HOST_PORT`
- `WEB_SESSION_TIMEOUT_MINUTES` (auto-logout timeout in minutes; allowed `5,10,15,20,25,30`)
- `WEB_ENV_FILE`
- `WEB_RESTART_ENABLED`
- `WEB_GITHUB_WIKI_URL`
- `WEB_ADMIN_DEFAULT_USERNAME`
- `WEB_ADMIN_DEFAULT_PASSWORD` (required for first boot when no web users exist; must meet password policy)
- `WEB_ADMIN_SESSION_SECRET`
- `WEB_DISCORD_CATALOG_TTL_SECONDS`
- `WEB_DISCORD_CATALOG_FETCH_TIMEOUT_SECONDS`
- `WEB_BOT_PROFILE_TIMEOUT_SECONDS`
- `WEB_AVATAR_MAX_UPLOAD_BYTES`

## Reference

For defaults and examples, see [`README.md`](../README.md).

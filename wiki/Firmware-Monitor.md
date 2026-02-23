# Firmware Monitor

Scheduled monitor for new firmware mirrored on GL.iNet firmware feed pages.

## Source

- Default feed: `https://gl-fw.remotetohome.io/`

## Schedule

- Uses cron-format env var: `firmware_check_schedule`
- Default: `*/30 * * * *` (UTC)

## Notification Target

- `firmware_notification_channel`
- Accepts numeric channel ID or `<#channel>` format

## Notification Content

- Model
- Track/stage (stable/testing)
- Version
- Published date
- Download links and SHA256 values
- Release notes excerpt

## Persistence

- Seen entries are stored in SQLite (`data/bot_data.db`)
- Legacy `data/firmware_seen.json` is imported on startup (merge-only)
- Prevents repeat alerts after restart

## Env Variables

- `FIRMWARE_FEED_URL`
- `firmware_check_schedule`
- `firmware_notification_channel`
- `FIRMWARE_REQUEST_TIMEOUT_SECONDS`
- `FIRMWARE_RELEASE_NOTES_MAX_CHARS`

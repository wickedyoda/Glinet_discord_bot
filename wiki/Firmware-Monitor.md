# Firmware Monitor

Scheduled monitor for new firmware entries from GL.iNet firmware feed pages.

## Overview

- Polls feed on cron schedule.
- Detects unseen firmware entries.
- Posts structured notifications to configured Discord channel.
- Stores seen-item fingerprints in SQLite to avoid repeat alerts.

## Source and Schedule

| Variable | Default | Purpose |
|---|---|---|
| `FIRMWARE_FEED_URL` | `https://gl-fw.remotetohome.io/` | Source URL to poll |
| `firmware_check_schedule` | `*/30 * * * *` | UTC cron schedule |
| `FIRMWARE_REQUEST_TIMEOUT_SECONDS` | `30` | HTTP timeout |
| `FIRMWARE_RELEASE_NOTES_MAX_CHARS` | `900` | Excerpt length cap |
| `firmware_notification_channel` | none | Target Discord channel |

## Channel Target Formats

Accepted formats:

- Numeric channel ID: `123456789012345678`
- Mention format: `<#123456789012345678>`

If channel cannot be resolved:

- Monitor still records entries as pending/seen state behavior defined by implementation.
- Warnings are logged, and next schedule tick retries channel resolution.

## Notification Payload

Typical fields:

- Device/model
- Track/stage (stable/testing)
- Version
- Publish date
- Download URLs and SHA256 checksums
- Release note excerpt (clipped)

## Persistence and Migration

- Primary storage: SQLite (`data/bot_data.db`)
- Legacy import source: `data/firmware_seen.json` on startup
- Import mode: merge-only, no overwrite of existing DB state

## Schedule Variations

Example cron values (UTC):

- Every 15 minutes: `*/15 * * * *`
- Hourly: `0 * * * *`
- Twice daily: `0 0,12 * * *`

## Troubleshooting

- Warning: notify channel not found:
  - Verify channel ID and guild/channel availability.
  - Verify bot has access to target channel.
- Too many first-run notifications:
  - Expected when feed history is unseen; confirm desired behavior and seen-cache state.
- No notifications:
  - Validate schedule syntax and monitor startup logs.
  - Validate outbound HTTP access to feed URL.

## Related Pages

- [Environment Variables](Environment-Variables)
- [Moderation and Logs](Moderation-and-Logs)
- [Data Files](Data-Files)

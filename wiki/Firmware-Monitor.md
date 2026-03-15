# Firmware Monitor

Scheduled monitor for new firmware entries from GL.iNet firmware feed pages.

## Overview

- Polls feed on cron schedule.
- Builds a baseline snapshot of current feed entries on first run.
- Detects only deltas after baseline:
  - new firmware entries
  - changed firmware entries (same model/track/version, updated metadata)
- Posts a short summary notification to configured Discord channel.
- Stores seen IDs and signature snapshots in SQLite to avoid repeat alerts.

## Source and Schedule

| Variable | Default | Purpose |
|---|---|---|
| `FIRMWARE_FEED_URL` | `https://gl-fw.remotetohome.io/` | Source URL to poll |
| `firmware_check_schedule` | `*/30 * * * *` | UTC cron schedule |
| `FIRMWARE_REQUEST_TIMEOUT_SECONDS` | `30` | HTTP timeout |
| `FIRMWARE_RELEASE_NOTES_MAX_CHARS` | `900` | Legacy compatibility setting (compact summary mode does not include long excerpts) |
| `firmware_notification_channel` | none | Target Discord channel |

## Channel Target Formats

Accepted formats:

- Numeric channel ID: `123456789012345678`
- Mention format: `<#123456789012345678>`

If channel cannot be resolved:

- New/changed update notifications remain pending.
- Warnings are logged, and next schedule tick retries channel resolution.

## Notification Payload (Compact)

Summary includes:

- total new count
- total changed count
- compact list items:
  - model code
  - track/stage
  - version
  - published date
- source URL

## Persistence and Migration

- Primary storage: SQLite (`data/bot_data.db`)
- Legacy import source: `data/firmware_seen.json` on startup
- Import mode: merge-only, no overwrite of existing DB state
- First run after enabling signature snapshots initializes baseline and suppresses historical spam alerts.

## Schedule Variations

Example cron values (UTC):

- Every 15 minutes: `*/15 * * * *`
- Hourly: `0 * * * *`
- Twice daily: `0 0,12 * * *`

## Troubleshooting

- Warning: notify channel not found:
  - Verify channel ID and guild/channel availability.
  - Verify bot has access to target channel.
- First run produced no alerts:
  - Expected. Baseline snapshot is created first, then only future deltas are notified.
- No notifications:
  - Validate schedule syntax and monitor startup logs.
  - Validate outbound HTTP access to feed URL.

## Related Pages

- [Environment Variables](Environment-Variables)
- [Moderation and Logs](Moderation-and-Logs)
- [Data Files](Data-Files)

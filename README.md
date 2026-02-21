# Discord Invite + Utility Bot

Discord bot for GL.iNet community operations:
- Role-bound invite links and 6-digit access codes
- Search across GL.iNet forum/docs
- Country code nickname suffix (` - CC`)
- Moderator actions (ban, kick+prune, timeout)
- Moderation + server event logging to a dedicated logs channel

## Features

1. Role Invite/Code Access
- `/submitrole` generates:
  - Permanent invite link
  - 6-digit code
- Members joining via invite or entering code with `/enter_role` get the mapped role.
- `/getaccess` gives a default configured role.
- `/bulk_assign_role_csv` (moderators) prompts for:
  - A target role mention
  - A CSV upload of Discord names (comma-separated)
  Then bulk-assigns that role and reports unmatched/ambiguous names.

2. Tag Auto-Replies
- Message-based tags from `data/tag_responses.json` (example: `!betatest`).
- Tags are also exposed as slash commands at startup (dynamic registration).
- `!list` shows available tag commands.

3. Search Commands
- Combined:
  - `/search`
  - `!search`
- Source-specific:
  - Forum: `/search_forum`, `!searchforum`
  - KVM docs: `/search_kvm`, `!searchkvm`
  - IoT docs: `/search_iot`, `!searchiot`
  - Router docs v4: `/search_router`, `!searchrouter`

4. Country Nickname
- Set: `/country US` or `!country US`
- Clear: `/clear_country` or `!clearcountry`
- Format is always: `display_name - CC` (uppercase country code).

5. Moderator Commands (ID-restricted)
- `/ban_member`, `!banmember`
- `/kick_member`, `!kickmember` (includes message prune window, default 72h)
- `/timeout_member`, `!timeoutmember` (durations like `30m`, `2h`, `1d`)
- `/modlog_test`, `!modlogtest` to verify logs channel delivery

6. Logging
- Moderation actions are logged to `MOD_LOG_CHANNEL_ID`.
- Additional server events logged to same channel:
  - Message deletions (single + bulk)
  - Username/global name changes
  - Avatar changes
  - Member joins/leaves
  - Invite creation
  - Channel/category create/delete
  - Role creation
  - Role add/remove on members

7. Firmware Mirror Monitor
- Polls `https://gl-fw.remotetohome.io/` (or custom URL) on a schedule.
- Detects newly added firmware rows by model/track/version/files.
- Posts a notification to `FIRMWARE_NOTIFY_CHANNEL_ID` with:
  - Model, track, version, date
  - Download links + SHA256
  - Release notes excerpt
- Uses `data/firmware_seen.json` to persist seen entries across restarts.

## Command Reference

| Slash Command | Prefix Command | Access |
|---|---|---|
| `/submitrole` | N/A | `Employee`, `Admin`, `Gl.iNet Moderator` role names |
| `/bulk_assign_role_csv` | N/A | Moderator role IDs only (see env vars) |
| `/enter_role` | N/A | Any member |
| `/getaccess` | N/A | Any member |
| Dynamic tag commands (from JSON) | Tag text (e.g. `!betatest`) | Any member |
| N/A | `!list` | Any member |
| `/search` | `!search` | Any member |
| `/search_forum` | `!searchforum` | Any member |
| `/search_kvm` | `!searchkvm` | Any member |
| `/search_iot` | `!searchiot` | Any member |
| `/search_router` | `!searchrouter` | Any member |
| `/country` | `!country` | Any member |
| `/clear_country` | `!clearcountry` | Any member |
| `/ban_member` | `!banmember` | Moderator role IDs only (see env vars) |
| `/kick_member` | `!kickmember` | Moderator role IDs only (see env vars) |
| `/timeout_member` | `!timeoutmember` | Moderator role IDs only (see env vars) |
| `/modlog_test` | `!modlogtest` | Moderator role IDs only (see env vars) |

## Environment Variables

Required:
- `DISCORD_TOKEN`
- `GUILD_ID`

Optional:
- `GENERAL_CHANNEL_ID` (used for invite generation; defaults to command channel)
- `DATA_DIR` (default `data`)
- `LOG_LEVEL` (default `INFO`)
- `FORUM_BASE_URL` (default `https://forum.gl-inet.com`)
- `FORUM_MAX_RESULTS` (default `5`)
- `DOCS_MAX_RESULTS_PER_SITE` (default `2`)
- `DOCS_INDEX_TTL_SECONDS` (default `3600`)
- `SEARCH_RESPONSE_MAX_CHARS` (default `1900`)
- `KICK_PRUNE_HOURS` (default `72`)
- `MODERATOR_ROLE_ID` (default `1294957416294645771`)
- `ADMIN_ROLE_ID` (default `1138302148292116551`)
- `MOD_LOG_CHANNEL_ID` (default `1311820410269995009`)
- `CSV_ROLE_ASSIGN_MAX_NAMES` (default `500`)
- `FIRMWARE_NOTIFY_CHANNEL_ID` (required to enable firmware alerts)
- `FIRMWARE_FEED_URL` (default `https://gl-fw.remotetohome.io/`)
- `FIRMWARE_CHECK_INTERVAL_SECONDS` (default `1800`)
- `FIRMWARE_REQUEST_TIMEOUT_SECONDS` (default `30`)
- `FIRMWARE_RELEASE_NOTES_MAX_CHARS` (default `900`)

Example `.env`:
```env
DISCORD_TOKEN=your_bot_token
GUILD_ID=your_guild_id
GENERAL_CHANNEL_ID=your_general_channel_id
LOG_LEVEL=INFO
FORUM_BASE_URL=https://forum.gl-inet.com
FORUM_MAX_RESULTS=5
DOCS_MAX_RESULTS_PER_SITE=2
DOCS_INDEX_TTL_SECONDS=3600
SEARCH_RESPONSE_MAX_CHARS=1900
KICK_PRUNE_HOURS=72
MODERATOR_ROLE_ID=1294957416294645771
ADMIN_ROLE_ID=1138302148292116551
MOD_LOG_CHANNEL_ID=1311820410269995009
CSV_ROLE_ASSIGN_MAX_NAMES=500
FIRMWARE_NOTIFY_CHANNEL_ID=123456789012345678
FIRMWARE_FEED_URL=https://gl-fw.remotetohome.io/
FIRMWARE_CHECK_INTERVAL_SECONDS=1800
FIRMWARE_REQUEST_TIMEOUT_SECONDS=30
FIRMWARE_RELEASE_NOTES_MAX_CHARS=900
```

## Docker

Repository `docker-compose.yml`:
```yaml
version: "3.9"
services:
  discord-bot:
    build:
      context: .
    container_name: discord_role_bot
    env_file: .env
    volumes:
      - ./data:/app/data
    restart: unless-stopped
```

Run:
```bash
docker compose up -d --build
```

View logs:
```bash
docker logs -f discord_role_bot
```

## Discord Requirements

Bot intents:
- Message Content Intent
- Server Members Intent

Bot permissions:
- View Channels
- Send Messages
- Read Message History
- Use Application Commands
- Manage Roles
- Create Instant Invite
- Manage Guild (for invite tracking behavior)
- Manage Messages (for prune + deletion visibility)
- Kick Members
- Ban Members
- Moderate Members

## Data Files

Stored under `data/` (or `DATA_DIR`):
- `access_role.txt`
- `role_codes.txt`
- `invite_roles.json`
- `tag_responses.json`
- `bot.log`
- `firmware_seen.json`

## Security

- Never commit `.env`.
- Rotate credentials immediately if exposed.

## License

This project is licensed under the **GNU General Public License v3.0 (GPLv3)**.
See [`LICENSE`](./LICENSE) for the full text.

## Maintainer

Created and maintained by [WickedYoda](https://wickedyoda.com)  
Support Discord: https://discord.gg/m6UjX6UhKe

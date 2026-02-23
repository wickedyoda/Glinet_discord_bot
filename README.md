# Discord Invite + Utility Bot

<p align="center">
  <img src="./assets/images/glinet-bot-round.png" alt="GL.iNet Bot Logo (Round)" width="200" />
</p>


Discord bot for GL.iNet community operations:
- Role-bound invite links and 6-digit access codes
- Search across GL.iNet forum/docs
- Country code nickname suffix (` - CC`)
- Moderator actions (ban, unban, kick+prune, timeout, role/member management)
- Moderation + server event logging to a dedicated logs channel
- SQLite-backed persistent storage (WAL mode) for runtime data

## Wiki

- Start here: [`wiki/Home.md`](./wiki/Home.md)
- Feature pages and operational docs live under [`wiki/`](./wiki/)

## Features

1. Role Invite/Code Access
- `/submitrole` generates:
  - Permanent invite link
  - 6-digit code
- Members joining via invite or entering code with `/enter_role` get the mapped role.
- `/getaccess` gives a default configured role.
- `/bulk_assign_role_csv` (moderators):
  - Takes a target role parameter
  - Takes a `.csv` attachment of Discord names (comma-separated or one-per-line)
  - Bulk-assigns that role and reports missing/ambiguous members and assignment errors
  - Returns a downloadable detailed report file

2. Tag Auto-Replies
- Message-based tags from persistent storage (example: `!betatest`).
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
- `/create_role`
- `/edit_role`
- `/delete_role`
- `/ban_member`, `!banmember`
- `/unban_member`, `!unbanmember`
- `/kick_member`, `!kickmember` (includes message prune window, default 72h)
- `/timeout_member`, `!timeoutmember` (durations like `30m`, `2h`, `1d`)
- `/untimeout_member`, `!untimeoutmember`
- `/add_role_member`, `!addrolemember`
- `/remove_role_member`, `!removerolemember`
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
- Posts a notification to `firmware_notification_channel` with:
  - Model, track, version, date
  - Download links + SHA256
  - Release notes excerpt
- Uses SQLite-backed firmware state in `data/bot_data.db` to persist seen entries across restarts.
- Legacy `firmware_seen.json` is imported on boot (merge-only; existing DB state is preserved).

8. Web Admin Interface
- Built-in password-protected web panel for bot management.
- Login uses email + password.
- There is no Discord `/login` or `!login` command for creating web users.
- Password policy is enforced for created/updated users:
  - Minimum 6 characters, maximum 16 characters
  - At least 2 numbers
  - At least 1 uppercase letter
  - At least 1 symbol
- No self-signup route; users can only be created by an admin.
- Supports multiple users (admin and non-admin accounts).
- On first boot (when no users exist), `WEB_ADMIN_DEFAULT_PASSWORD` must satisfy password policy; startup does not use insecure fallback passwords.
- Runs in the container on HTTP `WEB_PORT` (default `8080`) and can be host-mapped via `WEB_HOST_PORT`.
- Admin can manage:
  - Dashboard quick-action cards with direct buttons to all web-admin tools
  - Light/Black theme toggle in the web header (persisted in browser local storage)
  - Bot environment settings (channels, firmware schedule, logging/mod settings, etc.)
  - Per-command access rules (default/public/custom roles) in web GUI
  - Multi-role command restrictions using Discord role-name dropdowns (with multi-select)
  - Bot profile identity (username + server nickname) and avatar
  - GitHub wiki docs link from the web header
  - Admin restart button in the web header (with confirmation)
  - Live Discord channel/role dropdowns (polled from guild) for channel/role settings
  - Tag response mappings (saved changes refresh tag slash commands without container reload)
  - Bulk role assignment from uploaded CSV (with missing/error report)
  - Web users (create/delete, admin toggle, password reset, show-password toggle on create/reset forms)

## Command Reference

| Slash Command | Prefix Command | Access |
|---|---|---|
| `/submitrole` | N/A | `Employee`, `Admin`, `Gl.iNet Moderator` role names |
| `/bulk_assign_role_csv` | N/A | Moderator role IDs only (see env vars) |
| `/enter_role` | N/A | Any member |
| `/getaccess` | N/A | Any member |
| Dynamic tag commands (from persistent storage) | Tag text (e.g. `!betatest`) | Any member |
| N/A | `!list` | Any member |
| `/search` | `!search` | Any member |
| `/search_forum` | `!searchforum` | Any member |
| `/search_kvm` | `!searchkvm` | Any member |
| `/search_iot` | `!searchiot` | Any member |
| `/search_router` | `!searchrouter` | Any member |
| `/country` | `!country` | Any member |
| `/clear_country` | `!clearcountry` | Any member |
| `/create_role` | N/A | Moderator role IDs only (see env vars) |
| `/edit_role` | N/A | Moderator role IDs only (see env vars) |
| `/delete_role` | N/A | Moderator role IDs only (see env vars) |
| `/ban_member` | `!banmember` | Moderator role IDs only (see env vars) |
| `/unban_member` | `!unbanmember` | Moderator role IDs only (see env vars) |
| `/kick_member` | `!kickmember` | Moderator role IDs only (see env vars) |
| `/timeout_member` | `!timeoutmember` | Moderator role IDs only (see env vars) |
| `/untimeout_member` | `!untimeoutmember` | Moderator role IDs only (see env vars) |
| `/add_role_member` | `!addrolemember` | Moderator role IDs only (see env vars) |
| `/remove_role_member` | `!removerolemember` | Moderator role IDs only (see env vars) |
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
- `firmware_notification_channel` (required to enable firmware alerts; channel ID or `<#channel>` mention)
- `FIRMWARE_FEED_URL` (default `https://gl-fw.remotetohome.io/`)
- `firmware_check_schedule` (cron, 5-field, UTC; default `*/30 * * * *`)
- `FIRMWARE_REQUEST_TIMEOUT_SECONDS` (default `30`)
- `FIRMWARE_RELEASE_NOTES_MAX_CHARS` (default `900`)
- `WEB_ENABLED` (default `true`)
- `WEB_BIND_HOST` (default `127.0.0.1` for local non-container runs)
- `WEB_PORT` (default `8080`, internal container port)
- `WEB_HOST_PORT` (default `8080`, host mapping used by docker-compose)
- `WEB_RESTART_ENABLED` (default `true`, enables/disables admin restart button in web header)
- `WEB_GITHUB_WIKI_URL` (default `https://github.com/wickedyoda/Glinet_discord_bot/wiki`, external docs link in web header)
- `WEB_DISCORD_CATALOG_TTL_SECONDS` (default `120`, cache TTL for polled channel/role dropdown data)
- `WEB_DISCORD_CATALOG_FETCH_TIMEOUT_SECONDS` (default `20`, timeout for Discord channel/role catalog fetch)
- `WEB_BULK_ASSIGN_TIMEOUT_SECONDS` (default `300`, timeout for web CSV role assignment execution)
- `WEB_BULK_ASSIGN_MAX_UPLOAD_BYTES` (default `2097152`, max CSV upload size in bytes for web bulk assignment)
- `WEB_BULK_ASSIGN_REPORT_LIST_LIMIT` (default `50`, max items shown per section in web bulk-assignment details)
- `WEB_BOT_PROFILE_TIMEOUT_SECONDS` (default `20`, timeout for loading/updating bot profile actions from web UI)
- `WEB_AVATAR_MAX_UPLOAD_BYTES` (default `2097152`, max avatar upload size in bytes for bot profile uploads)
- `WEB_ENV_FILE` (default `.env`)
- `WEB_ADMIN_DEFAULT_USERNAME` (default admin email used on first run)
- `WEB_ADMIN_DEFAULT_PASSWORD` (default admin password used on first run; must satisfy password policy)
- `WEB_ADMIN_SESSION_SECRET` (optional explicit session signing secret)

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
firmware_notification_channel=123456789012345678
FIRMWARE_FEED_URL=https://gl-fw.remotetohome.io/
firmware_check_schedule=*/30 * * * *
FIRMWARE_REQUEST_TIMEOUT_SECONDS=30
FIRMWARE_RELEASE_NOTES_MAX_CHARS=900
WEB_ENABLED=true
WEB_BIND_HOST=0.0.0.0
WEB_PORT=8080
WEB_HOST_PORT=8080
WEB_RESTART_ENABLED=true
WEB_GITHUB_WIKI_URL=https://github.com/wickedyoda/Glinet_discord_bot/wiki
WEB_DISCORD_CATALOG_TTL_SECONDS=120
WEB_DISCORD_CATALOG_FETCH_TIMEOUT_SECONDS=20
WEB_BULK_ASSIGN_TIMEOUT_SECONDS=300
WEB_BULK_ASSIGN_MAX_UPLOAD_BYTES=2097152
WEB_BULK_ASSIGN_REPORT_LIST_LIMIT=50
WEB_BOT_PROFILE_TIMEOUT_SECONDS=20
WEB_AVATAR_MAX_UPLOAD_BYTES=2097152
WEB_ENV_FILE=.env
WEB_ADMIN_DEFAULT_USERNAME=admin@example.com
WEB_ADMIN_DEFAULT_PASSWORD=use_a_strong_unique_password
WEB_ADMIN_SESSION_SECRET=replace_with_random_secret
```

## Docker

Repository `docker-compose.yml`:
```yaml
services:
  discord_invite_bot:
    build:
      context: .
    container_name: discord_role_bot
    env_file:
      - .env
    environment:
      - WEB_BIND_HOST=0.0.0.0
    ports:
      - "127.0.0.1:${WEB_HOST_PORT:-8080}:${WEB_PORT:-8080}"
    volumes:
      - ./data:/app/data
      - ./.env:/app/.env
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

Open web admin:
```bash
http://localhost:${WEB_HOST_PORT:-8080}
```

Security note:
- Default compose mapping binds the web admin UI to localhost only.
- To expose it externally, intentionally change the port mapping and place it behind HTTPS/reverse-proxy auth.

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
- `bot_data.db` (primary SQLite database)
- `bot.log`

Legacy files are auto-migrated into SQLite on startup if present:
- `access_role.txt`
- `role_codes.txt`
- `invite_roles.json`
- `tag_responses.json`
- `firmware_seen.json`
- `web_users.json`
- `command_permissions.json`

Migration is merge-only: existing SQLite rows are not overwritten.

## Security

- Never commit `.env`.
- Rotate credentials immediately if exposed.
- Vulnerability reporting process: [`SECURITY.md`](./SECURITY.md)

## License

This project is licensed under the **GNU General Public License v3.0 (GPLv3)**.
See [`LICENSE`](./LICENSE) for the full text.
Additional rights/policy summary: [`LICENSE.md`](./LICENSE.md)

## Maintainer

Created and maintained by [WickedYoda](https://wickedyoda.com)  
Support Discord: https://discord.gg/m6UjX6UhKe

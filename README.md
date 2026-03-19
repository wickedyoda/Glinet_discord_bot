# WickedYoda'sLittleHelper

<p align="center">
  <img src="./assets/images/glinet-bot-round.png" alt="GL.iNet Bot Logo (Round)" width="170" />
  <img src="./assets/images/glinet-bot-full.png" alt="GL.iNet Bot Logo (Full)" width="240" />
</p>

Discord bot for GL.iNet community operations under the `WickedYoda'sLittleHelper` identity, with invite/code role access, moderation tools, search helpers, firmware and YouTube monitoring, member-activity analytics, utility commands, and a secured multi-guild web admin GUI.

## Documentation

Detailed feature behavior, deployment options, environment variables, proxy variations, and security guidance are maintained in the wiki.

- Wiki home: [`wiki/Home.md`](./wiki/Home.md)
- GitHub wiki page: [https://github.com/wickedyoda/Glinet_discord_bot/wiki](https://github.com/wickedyoda/Glinet_discord_bot/wiki)
- Public repo landing redirect target: [http://discord.glinet.wickedyoda.com/](http://discord.glinet.wickedyoda.com/)
- Public wiki redirect target: [http://discord.glinet.wickedyoda.com/wiki](http://discord.glinet.wickedyoda.com/wiki)

## Quick Start (Docker)

1. Copy env template:

```bash
cp .env.example .env
```

2. Set required values in `.env`:

- `DISCORD_TOKEN`
- `GUILD_ID`
- `WEB_ADMIN_DEFAULT_PASSWORD` (required when no web users exist yet)

3. Start:

```bash
docker compose up -d --build
```

4. Open web admin:

```text
http://localhost:8080
https://localhost:8081
```

If no certificate is present, the bot generates a default self-signed certificate in `${DATA_DIR}/ssl/`. Replace the generated files with your own certificate and key if you want a browser-trusted deployment.

## Architecture Support

- Native local builds (`docker compose up -d --build`) run on the host architecture (Apple Silicon `arm64` or Intel/AMD `amd64`).
- Published GHCR images are built as a multi-arch manifest for:
  - `linux/amd64`
  - `linux/arm64`
- Optional multi-arch publish command:

```bash
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t ghcr.io/<owner>/discord_invite_bot:multiarch-test \
  --push \
  .
```

- Optional host-native local test image command:

```bash
docker compose build
```

## What It Includes

- Role access via invite links and 6-digit access codes
- Bulk CSV role assignment
- Dynamic guild-scoped tag responses (`!tag` + slash variants)
- Source-specific search commands for GL.iNet forums, OpenWrt forums, Reddit, and docs
- Utility commands for `ping`, uptime/status, URL shortening/expansion, and quick image/greeting helpers
- Country nickname suffix commands
- Extended moderation commands and event logging
- Moderator-only random member selection command that excludes staff roles and enforces a 7-day per-guild cooldown before the same member can be picked again
- Firmware monitor (baseline + delta notifications)
- YouTube subscription monitor with channel-to-Discord posting
- LinkedIn public profile monitor with channel-to-Discord posting
- Web-managed Reddit feed posting for new subreddit submissions
- Member activity tracking with private `/stats` output, web top-20 views for rolling 90/30/7/1-day windows, and ZIP export from the web GUI
- Web admin GUI with server selection, guild-scoped management pages, action history, member activity, YouTube subscriptions, LinkedIn profile subscriptions, and per-guild channel/tag/invite settings
- Optional guild allowlist mode for multi-guild deployments and public invites
- Guild-scoped data quarantine on bot removal with 14-day restore window on same-ID rejoin before permanent purge
- SQLite persistence with legacy merge import on startup
- Local verification tooling for lint, tests, security checks, and Docker builds

## Where To Find Details

- Full command list and role restrictions: [`wiki/Command-Reference.md`](./wiki/Command-Reference.md)
- Web admin pages and workflows: [`wiki/Web-Admin-Interface.md`](./wiki/Web-Admin-Interface.md)
- Environment variables (complete): [`wiki/Environment-Variables.md`](./wiki/Environment-Variables.md)
- Docker and Portainer deployment variants: [`wiki/Docker-and-Portainer-Deploy.md`](./wiki/Docker-and-Portainer-Deploy.md)
- Reverse proxy setups (Nginx, Caddy, Traefik, Apache, HAProxy): [`wiki/Reverse-Proxy-Web-GUI.md`](./wiki/Reverse-Proxy-Web-GUI.md)
- Developer verification workflow: [`scripts/verify.sh`](./scripts/verify.sh)
- Security controls and hardening checklist: [`wiki/Security-Hardening.md`](./wiki/Security-Hardening.md)
- Data and log file layout: [`wiki/Data-Files.md`](./wiki/Data-Files.md)

## Runtime Data and Logs

- Primary DB: `${DATA_DIR}/bot_data.db`
- App log: `${LOG_DIR}/bot.log`
- Bot channel mirror log: `${LOG_DIR}/bot_log.log`
- Error log used by `/logs`: `${LOG_DIR}/container_errors.log`
- Web GUI interaction audit log: `${LOG_DIR}/web_gui_audit.log`

Defaults:

- `DATA_DIR=data`
- `LOG_DIR=/logs`
- `LOG_HARDEN_FILE_PERMISSIONS=true` (enforces `0700` on log dir and `0600` on log files when possible)
- `LOG_RETENTION_DAYS=90`
- `LOG_ROTATION_INTERVAL_DAYS=1`

## Member Activity Backfill

To backfill member activity history once at startup, set:

- `MEMBER_ACTIVITY_BACKFILL_ENABLED=true`
- `MEMBER_ACTIVITY_BACKFILL_GUILD_ID=<target guild id>` or leave blank to use `GUILD_ID`
- `MEMBER_ACTIVITY_BACKFILL_SINCE=2026-02-01`

Behavior:

- Scans readable channel history from the selected guild starting at the given UTC date
- Reuses completed backfill coverage so reruns only scan missing time ranges instead of rereading already indexed periods
- Uses a one-time state record so the same completed backfill range does not rerun every restart
- Only keeps the last 90 days of activity data
- Feeds the same `/stats` command and `/admin/member-activity` web views used for live collection

## Security

- No public web signup; web users are admin-created.
- Password policy and 90-day password rotation are enforced.
- CSRF and session hardening are enabled by default.
- Member-activity identity fields are encrypted at rest. Set `MEMBER_ACTIVITY_ENCRYPTION_KEY` for external key management, or let the bot generate `${DATA_DIR}/member_activity.key`.
- Deployment hardening guidance: [`wiki/Security-Hardening.md`](./wiki/Security-Hardening.md)
- Project Terms of Service and Privacy Policy: [https://wickedyoda.com/?p=3460](https://wickedyoda.com/?p=3460)
- Discord developer terms: [https://support-dev.discord.com/hc/en-us/articles/8562894815383-Discord-Developer-Terms-of-Service](https://support-dev.discord.com/hc/en-us/articles/8562894815383-Discord-Developer-Terms-of-Service)

## Contributing

Use complete commit and PR descriptions for all changes.

- Contributor guide: [`CONTRIBUTING.md`](./CONTRIBUTING.md)

## License

- License text: [`LICENSE`](./LICENSE)
- Additional rights/policy summary: [`LICENSE.md`](./LICENSE.md)

## Maintainer

Created and maintained by [WickedYoda](https://wickedyoda.com)

Support Discord: [https://discord.gg/m6UjX6UhKe](https://discord.gg/m6UjX6UhKe)

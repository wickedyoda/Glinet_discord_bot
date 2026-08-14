# GL.iNet UnOfficial Discord Bot

<p align="center">
  <img src="./assets/images/glinet-bot-round.png" alt="GL.iNet Bot Logo (Round)" width="250" />
</p>

Discord bot for GL.iNet community operations under the public `GL.iNet UnOfficial Discord Bot` name, with invite/code role access, moderation tools, search helpers, firmware/YouTube/LinkedIn/beta-program monitoring, member-activity analytics, utility commands, role-tier ticket handling, Discourse forum integration, and a secured multi-guild web admin GUI.

- Install / invite link: [Add GL.iNet UnOfficial Discord Bot to your server](https://discord.com/oauth2/authorize?client_id=1390519966050291734)

## Documentation

- Installation: [`docs-installation/install.md`](./docs-installation/install.md)
- Setup: [`docs-installation/setup.md`](./docs-installation/setup.md)
- Wiki home: [`wiki/Home.md`](./wiki/Home.md)
- GitHub wiki: [https://github.com/wickedyoda/Glinet_discord_bot/wiki](https://github.com/wickedyoda/Glinet_discord_bot/wiki)

## Quick Start

1. Install dependencies and copy env template:
   - `cp .env.example .env`
2. Set required values in `.env`:
   - `DISCORD_TOKEN`
   - `GUILD_ID`
   - `WEB_ADMIN_DEFAULT_PASSWORD` on first boot
3. Start with Docker Compose:
   - `docker compose up -d --build`
4. Open the web GUI:
   - `http://localhost:8080`
   - `https://localhost:8081`

If the bot starts with no TLS material, it generates a self-signed certificate in `${DATA_DIR}/ssl/`. Replace those files with your own certificate and key if you need browser-trusted HTTPS.

## Runtime Data and Logs

- Primary DB: `${DATA_DIR}/bot_data.db`
- App log: `${LOG_DIR}/bot.log`
- Bot channel mirror log: `${LOG_DIR}/bot_log.log`
- Error log used by `/logs`: `${LOG_DIR}/container_errors.log`
- Web GUI interaction audit log: `${LOG_DIR}/web_gui_audit.log`

Defaults:
- `DATA_DIR=data`
- `LOG_DIR=/logs`
- `LOG_HARDEN_FILE_PERMISSIONS=true`
- `LOG_RETENTION_DAYS=90`
- `LOG_ROTATION_INTERVAL_DAYS=1`

## Security

- No public web signup; web users are admin-created.
- Password policy and 90-day password rotation are enforced.
- CSRF and session hardening are enabled by default.
- Member-activity identity fields are encrypted at rest. Set `MEMBER_ACTIVITY_ENCRYPTION_KEY` for external key management, or let the bot generate `${DATA_DIR}/member_activity.key`.
- Security controls and hardening checklist: `wiki/Security-Hardening.md`

## Discourse Forum Integration

The bot integrates with GL.iNet Discourse forums (e.g., `https://forum.gl-inet.com/`). Integration is configurable **per guild** — each Discord server can independently enable or disable Discourse features.

**Web Admin interface:**
- `/admin/discourse` — configure Discourse integration settings (base URL, API key, username, profile, timeout, features, enabled state)
- `/admin/discourse/viewer` — **read-only forum viewer** showing live categories, recent topics, and a search page. Accessible to all web users; only admins can edit settings.
- `/admin/discourse/viewer/search?q=...` — search the Discourse forum
- `/admin/discourse/viewer/api/categories` and `/admin/discourse/viewer/api/topics` — backing AJAX endpoints

**Discord commands:**
- `/search_forum` — search the Discourse forum
- `/forum_categories` — browse forum categories
- `/view_topic` — view a specific topic

**Environment variables:**
- `FORUM_BASE_URL` — Discourse base URL
- `FORUM_API_KEY` — optional Discourse API key
- `FORUM_API_USERNAME` — optional Discourse API username
- `FORUM_MAX_RESULTS` — max results per search
- `FORUM_REQUEST_TIMEOUT_SECONDS` — request timeout

Detailed documentation: [`wiki/Discourse-Integration.md`](./wiki/Discourse-Integration.md)

## Contributing

Use complete commit and PR descriptions for all changes.

- Contributor guide: `CONTRIBUTING.md`

## License

- License text: `LICENSE`
- Additional rights/policy summary: `LICENSE.md`

## Maintainer

Created and maintained by [WickedYoda](https://wickedyoda.com)

Support Discord: [https://discord.gg/m6UjX6UhKe](https://discord.gg/m6UjX6UhKe)

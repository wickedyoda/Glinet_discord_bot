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

## Freshdesk Ticket Viewer Integration (Read-Only)

The bot provides a **read-only** Freshdesk ticket viewer for GL.iNet support tickets, accessible from both the Discord bot and the Web Admin GUI.

**Web Admin interface:**
- `/admin/freshdesk/viewer` — read-only Freshdesk ticket viewer showing live categories and a search page
- `/admin/freshdesk/viewer/search?q=...` — search Freshdesk tickets
- `/admin/freshdesk/viewer/ticket/<id>` — view a single ticket by ID
- `/admin/freshdesk/viewer/api/categories` — backing AJAX endpoint

**Discord commands:**
- `/freshdesk-search query:<query>` — search Freshdesk tickets (e.g. `status:2`)
- `/freshdesk-ticket ticket_id:<id>` — view a ticket by ID
- `/freshdesk-categories` — list knowledge-base solution categories

**Environment variables:**
- `FRESHDESK_BASE_URL` — Freshdesk helpdesk root URL (e.g. `https://support.gl-inet.com`)
- `FRESHDESK_API_KEY` — Freshdesk API key (read-only scope recommended)
- `FRESHDESK_REQUEST_TIMEOUT_SECONDS` — request timeout (default: `15`)

Detailed documentation: [`wiki/Freshdesk-Integration.md`](./wiki/Freshdesk-Integration.md)

## Reddit Auto-Responder

The bot can automatically post replies as comments on Reddit submissions that match configurable keyword patterns. This is a **web-GUI-only** feature — all management is done through the Web Admin interface under `/admin/reddit-auto-responds`.

**Web Admin interface (requires login):**
- `/admin/reddit-auto-responds` — manage auto-respond rules (add, edit, toggle enable/disable, delete)
- Each rule defines a subreddit, a regex keyword pattern, and a response template with variable substitution (`{title}`, `{author}`, `{subreddit}`, `{{post_id}}`, `{{post_link}}`)
- Runtime status shown per rule: last matched post, last reply timestamp, last error
- Status bar shows whether Reddit OAuth is configured and auto-reply is enabled

**Requirements:**
- `REDDIT_AUTO_REPLY_ENABLED=true` — enables the auto-responder feature
- `REDDIT_CLIENT_ID`, `REDDIT_CLIENT_SECRET` — Reddit OAuth2 app credentials (create a "Script" app at https://www.reddit.com/prefs/apps)
- `REDDIT_USERNAME`, `REDDIT_PASSWORD` — Reddit account credentials for the bot account

**How it works:**
1. On each Reddit feed polling cycle, the bot checks all enabled auto-respond rules
2. For each rule, it fetches new posts from the subreddit and matches the keyword pattern against post titles
3. When a match is found, the bot posts a comment using the response template
4. Posted comments prevent duplicate replies via a seen-post tracking table
5. The rule's runtime status (last matched, last replied, last error) is updated in a single DB write per run

**Security notes:**
- OAuth credentials are stored as environment variables only — never committed to git
- Comments are posted from the linked Reddit account
- Rate limiting is handled automatically with exponential backoff
- Auth errors automatically disable the affected rule

Detailed documentation: [`wiki/Reddit-Auto-Responder.md`](./wiki/Reddit-Auto-Responder.md)

## Contributing

Use complete commit and PR descriptions for all changes.

- Contributor guide: `CONTRIBUTING.md`

## License

- License text: `LICENSE`
- Additional rights/policy summary: `LICENSE.md`

## Maintainer

Created and maintained by [WickedYoda](https://wickedyoda.com)

Support Discord: [https://discord.gg/m6UjX6UhKe](https://discord.gg/m6UjX6UhKe)

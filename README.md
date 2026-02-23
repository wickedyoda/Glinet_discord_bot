# Discord Invite + Utility Bot

<p align="center">
  <img src="./assets/images/glinet-bot-round.png" alt="GL.iNet Bot Logo (Round)" width="170" />
  <img src="./assets/images/glinet-bot-full.png" alt="GL.iNet Bot Logo (Full)" width="240" />
</p>

Discord bot for GL.iNet community operations with invite/code role access, moderation tools, search helpers, firmware monitoring, and a secured web admin GUI.

## Documentation

Detailed feature behavior, deployment options, environment variables, proxy variations, and security guidance are maintained in the wiki.

- Wiki home: [`wiki/Home.md`](./wiki/Home.md)
- GitHub wiki page: [https://github.com/wickedyoda/Glinet_discord_bot/blob/main/wiki/Home.md](https://github.com/wickedyoda/Glinet_discord_bot/blob/main/wiki/Home.md)
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
```

## What It Includes

- Role access via invite links and 6-digit access codes
- Bulk CSV role assignment
- Dynamic tag responses (`!tag` + slash variants)
- GL.iNet forum/docs search commands
- Country nickname suffix commands
- Extended moderation commands and event logging
- Firmware monitor (baseline + delta notifications)
- Web admin GUI (user management, command permissions, bot profile, settings)
- SQLite persistence with legacy merge import on startup

## Where To Find Details

- Full command list and role restrictions: [`wiki/Command-Reference.md`](./wiki/Command-Reference.md)
- Web admin pages and workflows: [`wiki/Web-Admin-Interface.md`](./wiki/Web-Admin-Interface.md)
- Environment variables (complete): [`wiki/Environment-Variables.md`](./wiki/Environment-Variables.md)
- Docker and Portainer deployment variants: [`wiki/Docker-and-Portainer-Deploy.md`](./wiki/Docker-and-Portainer-Deploy.md)
- Reverse proxy setups (Nginx, Caddy, Traefik, Apache, HAProxy): [`wiki/Reverse-Proxy-Web-GUI.md`](./wiki/Reverse-Proxy-Web-GUI.md)
- Security controls and hardening checklist: [`wiki/Security-Hardening.md`](./wiki/Security-Hardening.md)
- Data and log file layout: [`wiki/Data-Files.md`](./wiki/Data-Files.md)

## Runtime Data and Logs

- Primary DB: `${DATA_DIR}/bot_data.db`
- App log: `${LOG_DIR}/bot.log`
- Error log used by `/logs`: `${LOG_DIR}/container_errors.log`

Defaults:

- `DATA_DIR=data`
- `LOG_DIR=/logs`

## Security

- No public web signup; web users are admin-created.
- Password policy and 90-day password rotation are enforced.
- CSRF and session hardening are enabled by default.
- Deployment hardening guidance: [`wiki/Security-Hardening.md`](./wiki/Security-Hardening.md)
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

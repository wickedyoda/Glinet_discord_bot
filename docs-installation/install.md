# Installation — GL.iNet UnOfficial Discord Bot

Supported runtime methods:

- Docker Compose (recommended)
- GHCR prebuilt image
- Local Python runtime
- Portainer stack

## Prerequisites

- Discord bot token from the Discord Developer Portal
- Discord application public key / client ID for invite link generation
- Target guild/server IDs
- Optional: a publicly reachable domain or reverse proxy if exposing the web GUI outside localhost

## 1) Clone the repo

```bash
git clone https://github.com/wickedyoda/Glinet_discord_bot.git
cd Glinet_discord_bot
```

## 2) Create the environment file

Copy the example env file:

```bash
cp .env.example .env
```

Minimum required values in `.env`:

- `DISCORD_TOKEN`
- `GUILD_ID`
- `WEB_ADMIN_DEFAULT_PASSWORD` when starting fresh with no existing web users

Review the full variable list in `wiki/Environment-Variables.md`.

## 3) Start the bot

Using Docker Compose:

```bash
docker compose up -d --build
```

Using Portainer:

- Import the stack
- Map:
  - `${WEB_HTTP_PUBLISH:-8080}:${WEB_PORT:-8080}`
  - `${WEB_HTTPS_PUBLISH:-8081}:${WEB_HTTPS_PORT:-8081}`
- Mount:
  - `./data:/app/data`
  - `./logs:/logs`

## 4) Verify health endpoints

- Web alive/process check: `http://<host>:8080/healthz`
- Bot ready check: `http://<host>:8080/readyz`
- Docker’s own container healthcheck uses `/readyz` already.

## 5) Open the web GUI

- HTTP: `http://localhost:8080`
- HTTPS: `https://localhost:8081`

If the bot started with no TLS files, a self-signed cert is generated in `${DATA_DIR}/ssl/`. Replace `tls.crt` and `tls.key` with your own certificate files for browser-trusted HTTPS.

## 6) Create your first web admin user

Use the web GUI signup flow. If no web users exist, the first-boot default is governed by `WEB_ADMIN_DEFAULT_USERNAME`, `WEB_ADMIN_DEFAULT_EMAIL`, and `WEB_ADMIN_DEFAULT_PASSWORD`.

## 7) Next steps

- Set up guild settings per server in `/admin/guild-settings`
- Review command permissions in `/admin/command-permissions`
- Enable monitors and feeds in `/admin/settings` or `/admin/service-monitors`

# Docker and Portainer Deploy

Current deployment states are based on `docker-compose.yml`, `Dockerfile`, `.env.example`, and the published GHCR workflows.

## Local Docker Compose

```bash
cp .env.example .env
docker compose up -d --build
```

Mounts in the default compose file:

- `./data:/app/data`
- `./logs:/logs`
- `./.env:/app/.env:ro`

Ports:

- `${WEB_HTTP_PUBLISH:-8080}:${WEB_PORT:-8080}`
- `${WEB_HTTPS_PUBLISH:-8081}:${WEB_HTTPS_PORT:-8081}`

## Portainer Stack

When Portainer cannot read a local `.env` path:

- Remove `env_file:`
- Move values into `environment:`
- Example image options:
  - main: `ghcr.io/wickedyoda/discord_invite_bot:latest`
  - ticket-beta: `ghcr.io/wickedyoda/Glinet_discord_bot-ticket-beta:latest-ticket-beta`

Recommended Portainer persistence:

- keep source `.env` read-only
- set `WEB_ENV_FILE=/app/data/web-settings.env`
- let the web GUI write only the writable env file inside `/app/data`

## Prebuilt Image

Use a prebuilt image when:

- build context is unavailable
- Dockerfile is not present in the stack path
- you want predictable immutable deployments

## Multi-Architecture

GHCR images are published as a multi-arch manifest for:

- `linux/amd64`
- `linux/arm64`

Local multi-arch build example:

```bash
docker buildx create --use --name glinet-multiarch-builder
docker buildx inspect --bootstrap
docker buildx build   --platform linux/amd64,linux/arm64   -t ghcr.io/<owner>/discord_invite_bot:local-multiarch   --push   .
```

Notes:

- Use `--push` for true multi-arch output; `--load` only loads one architecture into the local engine.
- Standard `docker compose build` remains the fastest local test path.

## Proxy Production

Recommended adjustments:

- Keep container ports private or internal-network only.
- Set `WEB_PUBLIC_BASE_URL=https://discord-admin.example.com/`.
- Keep `WEB_SESSION_COOKIE_SECURE=true`.
- Keep CSRF and same-origin checks enabled.
- The bot can also listen on built-in HTTPS `8081` and generates a self-signed cert in `${DATA_DIR}/ssl/` if none exists.

Example bind override:

```yaml
ports:
  - "127.0.0.1:8080:8080"
  - "127.0.0.1:8081:8081"
```

Equivalent `.env` values:

```env
WEB_HTTP_PUBLISH=127.0.0.1:8080
WEB_HTTPS_PUBLISH=127.0.0.1:8081
```

If your reverse proxy is on another machine, either disable explicit host/IP pinning or use the Docker host's private LAN IP.

## Healthchecks and Diagnostics

Persistent log files:

- `${LOG_DIR}/bot.log`
- `${LOG_DIR}/bot_log.log`
- `${LOG_DIR}/container_errors.log`
- `${LOG_DIR}/web_gui_audit.log`

Use `/readyz` for health and readiness probes. It returns HTTP `200` only when the Discord bot runtime is ready, avoiding false positives from a healthy Flask listener with an unready bot loop.

Recommended healthcheck shape:

```yaml
healthcheck:
  test:
    [
      "CMD",
      "python",
      "-c",
      "import urllib.request,sys; r=urllib.request.urlopen('http://127.0.0.1:8080/readyz', timeout=8); sys.exit(0 if r.status == 200 else 1)",
    ]
  interval: 30s
  timeout: 10s
  retries: 3
  start_period: 45s
```

## Upgrade and Restart

1. Pull latest image or code.
2. Review `.env` and compose changes.
3. Recreate container with `docker compose up -d --build`.
4. Inspect logs with `docker compose logs -f discord_invite_bot`.

## Common Failures

- `env file ... not found`: replace `env_file` with explicit `environment` in Portainer.
- `failed to read dockerfile`: use image-based deploy or correct stack path.
- Web UI unavailable: check host bind, host port mapping, and proxy upstream target.

## Related Pages

- [Environment Variables](Environment-Variables.md)
- [Health Checks and Readiness](Health-Checks-and-Readiness.md)
- [Reverse Proxy Web GUI](Reverse-Proxy-Web-GUI.md)
- [Security Hardening](Security-Hardening.md)

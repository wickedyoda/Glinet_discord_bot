# Docker and Portainer Deploy

Deployment guide for local Docker Compose, Portainer stacks, and proxy-fronted production setups.

## Deployment Variations

- Variation A: Local development on localhost bind.
- Variation B: Container behind reverse proxy (recommended production).
- Variation C: Portainer stack with direct `environment:` variables.
- Variation D: Prebuilt image deployment (no local build context).

## Variation A: Local Compose (Development)

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
      - WEB_ENABLED=${WEB_ENABLED:-true}
      - WEB_PORT=${WEB_PORT:-8080}
      - WEB_HTTP_PUBLISH=${WEB_HTTP_PUBLISH:-8080}
      - WEB_HTTPS_ENABLED=${WEB_HTTPS_ENABLED:-true}
      - WEB_HTTPS_PORT=${WEB_HTTPS_PORT:-8081}
      - WEB_HTTPS_PUBLISH=${WEB_HTTPS_PUBLISH:-8081}
      - LOG_DIR=${LOG_DIR:-/logs}
      - LOG_HARDEN_FILE_PERMISSIONS=${LOG_HARDEN_FILE_PERMISSIONS:-true}
      - LOG_RETENTION_DAYS=${LOG_RETENTION_DAYS:-90}
      - LOG_ROTATION_INTERVAL_DAYS=${LOG_ROTATION_INTERVAL_DAYS:-1}
      - LOG_LEVEL=${LOG_LEVEL:-INFO}
      - CONTAINER_LOG_LEVEL=${CONTAINER_LOG_LEVEL:-ERROR}
      - WEB_PUBLIC_BASE_URL=${WEB_PUBLIC_BASE_URL:-}
      - WEB_SSL_DIR=${WEB_SSL_DIR:-/app/data/ssl}
      - WEB_SSL_CERT_FILE=${WEB_SSL_CERT_FILE:-tls.crt}
      - WEB_SSL_KEY_FILE=${WEB_SSL_KEY_FILE:-tls.key}
      - WEB_SSL_COMMON_NAME=${WEB_SSL_COMMON_NAME:-localhost}
      - WEB_TRUST_PROXY_HEADERS=${WEB_TRUST_PROXY_HEADERS:-true}
      - WEB_SESSION_COOKIE_SECURE=${WEB_SESSION_COOKIE_SECURE:-true}
      - WEB_ENFORCE_CSRF=${WEB_ENFORCE_CSRF:-true}
      - WEB_ENFORCE_SAME_ORIGIN_POSTS=${WEB_ENFORCE_SAME_ORIGIN_POSTS:-true}
    ports:
      - "${WEB_HTTP_PUBLISH:-8080}:${WEB_PORT:-8080}"
      - "${WEB_HTTPS_PUBLISH:-8081}:${WEB_HTTPS_PORT:-8081}"
    volumes:
      - ./data:/app/data
      - ./logs:/logs
      - ./.env:/app/.env
    restart: unless-stopped
```

Run:

```bash
docker compose up -d --build
```

## Variation B: Reverse Proxy Fronted (Production)

Recommended adjustments:

- Keep container port private (localhost bind or internal network only).
- Set `WEB_PUBLIC_BASE_URL=https://discord-admin.example.com/`.
- Keep `WEB_SESSION_COOKIE_SECURE=true`.
- Keep CSRF and same-origin checks enabled.
- The bot can also listen on built-in HTTPS `8081`; it generates a self-signed cert in `${DATA_DIR}/ssl/` if none exists.

Example host mapping:

```yaml
ports:
  - "127.0.0.1:8080:8080"
  - "127.0.0.1:8081:8081"
```

Equivalent `.env` values for same-host reverse proxy only:

```env
WEB_HTTP_PUBLISH=127.0.0.1:8080
WEB_HTTPS_PUBLISH=127.0.0.1:8081
```

If your reverse proxy is on another machine, leave the publish override disabled or set it to the Docker host's private LAN IP:

```env
# Disabled explicit host/IP pinning:
# WEB_HTTP_PUBLISH=8080
# WEB_HTTPS_PUBLISH=8081

# Explicit private LAN bind:
# WEB_HTTP_PUBLISH=192.168.1.50:8080
# WEB_HTTPS_PUBLISH=192.168.1.50:8081
```

Use your proxy to publish HTTPS domain externally and restrict those ports with a firewall.

## Variation C: Portainer Stack

When Portainer cannot access local `.env` path:

- Remove `env_file:` reference
- Provide variables under `environment:` directly

Example image:

- `ghcr.io/wickedyoda/discord_invite_bot:latest`

Recommended web settings persistence:

- keep the source `.env` mounted read-only
- set `WEB_ENV_FILE=/app/data/web-settings.env`
- let the web GUI write only that writable file inside `/app/data`

Recommended persistent volume:

- `/root/docker/linkbot/data:/app/data`

## Variation D: Image-Only Deploy

Use prebuilt image when:

- build context is unavailable
- Dockerfile is not present in stack path
- you want predictable immutable deployments

## Variation E: Multi-Architecture (amd64 + arm64)

Supported deployment model:

- GHCR published images are pushed as a single multi-arch manifest list:
  - `linux/amd64`
  - `linux/arm64`
- Docker automatically pulls the correct architecture image for the host.

Local multi-arch build (Buildx):

```bash
docker buildx create --use --name glinet-multiarch-builder
docker buildx inspect --bootstrap
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t ghcr.io/<owner>/discord_invite_bot:local-multiarch \
  --push \
  .
```

Notes:

- Use `--push` for true multi-arch output; `--load` only loads a single architecture into the local Docker engine.
- Standard `docker compose build` stays host-native and is still the fastest local test path.

## Port and Network Model

- App listens on `WEB_PORT` inside container.
- Host publish override controlled by `WEB_HTTP_PUBLISH` / `WEB_HTTPS_PUBLISH`.
- Public exposure should happen via reverse proxy, not direct open port.

## Logs and Diagnostics

Persistent log files:

- `${LOG_DIR}/bot.log` (application logs, default `/logs/bot.log`)
- `${LOG_DIR}/bot_log.log` (bot channel payload mirror, default `/logs/bot_log.log`)
- `${LOG_DIR}/container_errors.log` (error stream used by `/logs`, default `/logs/container_errors.log`)
- `${LOG_DIR}/web_gui_audit.log` (web admin interaction audit stream, default `/logs/web_gui_audit.log`)

Tune with:

- `LOG_LEVEL`
- `CONTAINER_LOG_LEVEL`
- `LOG_DIR`
- `LOG_HARDEN_FILE_PERMISSIONS` (recommended `true`, enforces `0700` log directory and `0600` log files where supported)
- `LOG_RETENTION_DAYS` (default `90`)
- `LOG_ROTATION_INTERVAL_DAYS` (default `1`)

## Container Healthcheck

Recommended Docker healthcheck:

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

Use `/readyz`, not a generic port probe.

Why:

- `/readyz` returns `200` only when the Discord bot is actually ready
- it avoids self-signed HTTPS issues on `8081`
- it checks the internal container port directly

If you override healthchecks in Portainer or Compose, make sure the override matches the bot's health endpoint instead of a generic example from another app.

## Upgrade and Restart Workflow

1. Pull latest image or code.
2. Review `.env`/compose changes.
3. Recreate container:
   - `docker compose up -d --build`
4. Check logs:
   - `docker compose logs -f discord_invite_bot`

## Common Failures

- `env file ... not found`:
  - Replace `env_file` with explicit `environment` values in Portainer.
- `failed to read dockerfile`:
  - Use image-based deploy or correct stack path.
- Web UI unavailable:
  - Check host bind address, host port mapping, and proxy upstream target.

## Security Guidance

- Avoid exposing container port directly to internet.
- Use HTTPS proxy + HSTS + strict forwarding headers.
- Keep secrets only in trusted env/secret management tooling.

## Related Pages

- [Environment Variables](Environment-Variables.md)
- [Health Checks and Readiness](Health-Checks-and-Readiness.md)
- [Reverse Proxy Web GUI](Reverse-Proxy-Web-GUI.md)
- [Security Hardening](Security-Hardening.md)

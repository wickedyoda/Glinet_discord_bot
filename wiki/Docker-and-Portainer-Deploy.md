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
  mysql:
    image: mysql:8.4
    environment:
      - MYSQL_DATABASE=${DB_NAME:-discord_bot}
      - MYSQL_USER=${DB_USER:-discord_bot}
      - MYSQL_PASSWORD=${DB_PASSWORD:-change_me}
      - MYSQL_ROOT_PASSWORD=${DB_ROOT_PASSWORD:-change_me_root}
    volumes:
      - mysql_data:/var/lib/mysql
    restart: unless-stopped

  discord_invite_bot:
    image: ${BOT_IMAGE:-ghcr.io/wickedyoda/discord_invite_bot}:${BOT_IMAGE_TAG:-latest}
    container_name: discord_role_bot
    pull_policy: always
    depends_on:
      mysql:
        condition: service_healthy
    env_file:
      - .env
    environment:
      - DB_BACKEND=${DB_BACKEND:-mysql}
      - DB_HOST=${DB_HOST:-mysql}
      - DB_PORT=${DB_PORT:-3306}
      - DB_NAME=${DB_NAME:-discord_bot}
      - DB_USER=${DB_USER:-discord_bot}
      - DB_PASSWORD=${DB_PASSWORD:-change_me}
      - DB_IMPORT_SQLITE_ON_BOOT=${DB_IMPORT_SQLITE_ON_BOOT:-true}
      - DB_SQLITE_PATH=${DB_SQLITE_PATH:-/app/data/bot_data.db}
      - WEB_BIND_HOST=0.0.0.0
      - WEB_ENABLED=${WEB_ENABLED:-true}
      - WEB_PORT=${WEB_PORT:-8080}
      - WEB_HOST_PORT=${WEB_HOST_PORT:-8080}
      - LOG_DIR=${LOG_DIR:-/logs}
      - LOG_HARDEN_FILE_PERMISSIONS=${LOG_HARDEN_FILE_PERMISSIONS:-true}
      - LOG_RETENTION_DAYS=${LOG_RETENTION_DAYS:-90}
      - LOG_ROTATION_INTERVAL_DAYS=${LOG_ROTATION_INTERVAL_DAYS:-1}
      - LOG_LEVEL=${LOG_LEVEL:-INFO}
      - CONTAINER_LOG_LEVEL=${CONTAINER_LOG_LEVEL:-ERROR}
      - WEB_PUBLIC_BASE_URL=${WEB_PUBLIC_BASE_URL:-}
      - WEB_TRUST_PROXY_HEADERS=${WEB_TRUST_PROXY_HEADERS:-true}
      - WEB_SESSION_COOKIE_SECURE=${WEB_SESSION_COOKIE_SECURE:-true}
      - WEB_ENFORCE_CSRF=${WEB_ENFORCE_CSRF:-true}
      - WEB_ENFORCE_SAME_ORIGIN_POSTS=${WEB_ENFORCE_SAME_ORIGIN_POSTS:-true}
    ports:
      - "127.0.0.1:${WEB_HOST_PORT:-8080}:${WEB_PORT:-8080}"
    volumes:
      - ./data:/app/data
      - ./logs:/logs
      - ./.env:/app/.env
    restart: unless-stopped

volumes:
  mysql_data:
```

Run:

```bash
docker compose pull
docker compose up -d
```

## Variation B: Reverse Proxy Fronted (Production)

Recommended adjustments:

- Keep container port private (localhost bind or internal network only).
- Set `WEB_PUBLIC_BASE_URL=https://discord-admin.example.com/`.
- Keep `WEB_SESSION_COOKIE_SECURE=true`.
- Keep CSRF and same-origin checks enabled.
- Keep `DB_BACKEND=mysql` and do not expose the MySQL port publicly.

Example host mapping:

```yaml
ports:
  - "127.0.0.1:8080:8080"
```

Use your proxy to publish HTTPS domain externally.

## Variation C: Portainer Stack

When Portainer cannot access local `.env` path:

- Remove `env_file:` reference
- Provide variables under `environment:` directly

Example image:

- `ghcr.io/wickedyoda/discord_invite_bot:latest`

Optional image override env vars:

- `BOT_IMAGE`
- `BOT_IMAGE_TAG`

Recommended persistent volume:

- `/root/docker/linkbot/data:/app/data`
- MySQL named volume or bind mount for `/var/lib/mysql`

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
- Host published port controlled by `WEB_HOST_PORT` in compose mapping.
- Public exposure should happen via reverse proxy, not direct open port.
- MySQL should remain on the internal Docker network only.

## Logs and Diagnostics

Persistent log files:

- `${LOG_DIR}/bot.log` (application logs, default `/logs/bot.log`)
- `${LOG_DIR}/bot_log.log` (bot channel payload mirror, default `/logs/bot_log.log`)
- `${LOG_DIR}/container_errors.log` (error stream used by `/logs`, default `/logs/container_errors.log`)
- `${LOG_DIR}/web_gui_audit.log` (web admin interaction audit stream, default `/logs/web_gui_audit.log`)
- `${LOG_DIR}/web_probe.log` (anonymous unknown-route `404` scan/probe traffic, default `/logs/web_probe.log`)

Tune with:

- `LOG_LEVEL`
- `CONTAINER_LOG_LEVEL`
- `LOG_DIR`
- `LOG_HARDEN_FILE_PERMISSIONS` (recommended `true`, enforces `0700` log directory and `0600` log files where supported)
- `LOG_RETENTION_DAYS` (default `90`)
- `LOG_ROTATION_INTERVAL_DAYS` (default `1`)

## Upgrade and Restart Workflow

1. Pull latest image or code.
2. Review `.env`/compose changes.
   - If moving from SQLite, leave `${DB_SQLITE_PATH}` mounted and keep `DB_IMPORT_SQLITE_ON_BOOT=true` for the first MySQL boot.
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
  - Check bind host/port mapping and proxy upstream target.

## Security Guidance

- Avoid exposing container port directly to internet.
- Use HTTPS proxy + HSTS + strict forwarding headers.
- Keep secrets only in trusted env/secret management tooling.

## Related Pages

- [Environment Variables](Environment-Variables)
- [Reverse Proxy Web GUI](Reverse-Proxy-Web-GUI)
- [Security Hardening](Security-Hardening)

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
      - WEB_HOST_PORT=${WEB_HOST_PORT:-8080}
      - LOG_DIR=${LOG_DIR:-/logs}
      - LOG_HARDEN_FILE_PERMISSIONS=${LOG_HARDEN_FILE_PERMISSIONS:-true}
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
- Host published port controlled by `WEB_HOST_PORT` in compose mapping.
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
  - Check bind host/port mapping and proxy upstream target.

## Security Guidance

- Avoid exposing container port directly to internet.
- Use HTTPS proxy + HSTS + strict forwarding headers.
- Keep secrets only in trusted env/secret management tooling.

## Related Pages

- [Environment Variables](Environment-Variables)
- [Reverse Proxy Web GUI](Reverse-Proxy-Web-GUI)
- [Security Hardening](Security-Hardening)

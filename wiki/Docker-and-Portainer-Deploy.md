# Docker and Portainer Deploy

This bot can run as a local Docker Compose project or as a Portainer stack.

## Local Compose

Use repository compose and Dockerfile:

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
      - ./.env:/app/.env
    restart: unless-stopped
```

Run:

```bash
docker compose up -d --build
```

## Portainer Stack (No `.env` File Mount)

Use `environment:` entries directly in the stack spec instead of `env_file` references that may not exist in Portainer-managed paths.

Example service image:

- `ghcr.io/wickedyoda/discord_invite_bot:latest`

Recommended volume:

- `/root/docker/linkbot/data:/app/data`

## Web Port

- Container listens on `WEB_PORT` (default `8080`)
- Default secure mapping is localhost-only: `127.0.0.1:${WEB_HOST_PORT:-8080}:${WEB_PORT:-8080}`
- Expose externally only behind HTTPS + additional access controls

## Logging

- Runtime log file: `data/bot.log`
- Container-wide error log file: `data/container_errors.log`
- Moderator command `/logs` returns recent lines from `container_errors.log`

## Troubleshooting

- `env file ... not found` in Portainer:
  - Remove `env_file:` dependency and use `environment:` block.
- `failed to read dockerfile: open Dockerfile`:
  - Happens when using `build:` in a directory without Dockerfile.
  - Use prebuilt image (`ghcr.io/...`) in Portainer unless build context is present.

# Docker and Portainer Deploy

This bot can run as a local Docker Compose project or as a Portainer stack.

## Local Compose

Use repository compose and Dockerfile:

```yaml
services:
  discord_invite_bot:
    build:
      context: .
    env_file:
      - .env
    environment:
      - WEB_BIND_HOST=0.0.0.0
    ports:
      - "127.0.0.1:${WEB_HOST_PORT:-8080}:${WEB_PORT:-8080}"
    volumes:
      - ./data:/app/data
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

## Troubleshooting

- `env file ... not found` in Portainer:
  - Remove `env_file:` dependency and use `environment:` block.
- `failed to read dockerfile: open Dockerfile`:
  - Happens when using `build:` in a directory without Dockerfile.
  - Use prebuilt image (`ghcr.io/...`) in Portainer unless build context is present.

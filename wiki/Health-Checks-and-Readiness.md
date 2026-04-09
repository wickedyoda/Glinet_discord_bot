# Health Checks and Readiness

This page documents the bot's health endpoints and the recommended Docker healthcheck configuration.

## Endpoints

The web admin service exposes two unauthenticated health endpoints:

- `/healthz`
  - Liveness endpoint
  - Confirms the web process is running
  - Returns structured JSON
- `/readyz`
  - Readiness endpoint
  - Confirms the Discord bot runtime is actually ready
  - Returns HTTP `200` only when the bot loop is running, the client is logged in, and Discord is ready
  - Returns HTTP `503` when the web process is up but the bot is not yet ready

## Recommended Docker Healthcheck

Use `/readyz` for container health.

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

Why this is the recommended check:

- it uses the bot's actual readiness endpoint
- it avoids HTTPS/self-signed certificate failures on port `8081`
- it avoids depending on `curl` or `wget` in the container image
- it checks the internal container port directly instead of the published host port

## What The Health Payload Means

The health endpoints return JSON including fields like:

- `ok`
- `ready`
- `service`
- `timestamp`
- `discord_logged_in`
- `discord_ready`
- `discord_closed`
- `loop_running`
- `managed_guild_count`
- `latency_ms`

Typical interpretation:

- `ok=true, ready=true`
  - web service is up and the bot is ready
- `ok=true, ready=false`
  - web service is up but Discord is not ready yet
- `ok=false`
  - health callback itself failed

## Uptime Kuma Guidance

Recommended monitor targets:

- use `/readyz` if you want alerts only when the bot is not usable
- use `/healthz` if you only care that the web service is alive

Recommended URL from another system:

- `http://<docker-host>:<published-http-port>/readyz`

Example if your compose publishes `8090:8080`:

- `http://<host>:8090/readyz`

## Common Mistakes

Avoid these healthcheck patterns:

- `http://localhost:3000/health`
  - wrong app and wrong port
- `https://127.0.0.1:8081/...`
  - often fails because the built-in HTTPS listener uses a self-signed certificate unless you replace it
- checking published host ports from inside the container
  - use the internal container port instead

## Deployment Notes

The repo Dockerfile now includes a built-in readiness healthcheck using `/readyz`.

If Portainer or Compose defines its own `healthcheck`, that override will be used instead of the Dockerfile default. In that case, make sure the override matches the recommended snippet above.

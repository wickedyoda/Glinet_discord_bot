# Security Hardening

This page captures the current security-relevant controls in the bot and its deployment.

## Web GUI

- No public signup; web users are created by admins only.
- Password policy enforced at create and reset time.
- 90-day password rotation is enforced.
- CSRF and same-origin POST checks are enabled by default.
- Strict cookie settings and browser hardening headers are applied.
- Session cookie `SameSite` defaults to `Lax` and is configurable.
- Login rate limiting is enforced.
- Trusted proxy headers are optional; set `WEB_TRUST_PROXY_HEADERS=true` when behind a reverse proxy that sets standard forwarding headers.

## Secrets Handling

- Store secrets outside the image where possible.
- Prefer `.env` and runtime-mounted files over compiled-in values.
- Do not log `DISCORD_TOKEN` or other credentials.

## Deployment

- Do not expose container ports directly to the internet.
- Prefer reverse proxy HTTPS with HSTS and trusted forwarding headers.
- Restrict published ports with firewall rules or bind controls.
- Mount `.env` read-only inside the container when the web GUI uses `WEB_ENV_FILE` for mutable settings.

## Logs

- `LOG_HARDEN_FILE_PERMISSIONS=true` enforces restrictive file permissions when supported.
- Sensitive logs are mirrored under `/logs`; do not publish them publicly.

## Supply Chain and CI

- Pin GitHub Actions to full commit SHAs when feasible.
- Avoid script injection patterns by passing untrusted workflow inputs through `env:` before `run:` steps.
- Review dependency updates before merging. Dependabot and Renovate are supported dependency-update tools.

## Disable Unused Features

- Turn off monitors, feeds, and integrations you do not use.
- Disabling a monitor stops polling/posting while keeping saved subscriptions and web pages available.

## Security Checklist

Current implementation status based on live code review:

- [x] HTTPS reverse proxy recommended; app supports `WEB_PUBLIC_BASE_URL` and proxy headers
- [x] Public origin configured via `WEB_PUBLIC_BASE_URL`
- [x] Strong secrets supported; `WEB_ADMIN_SESSION_SECRET` auto-generates at runtime if unset, but set it explicitly for production restart stability
- [x] CSRF enabled by default (`WEB_ENFORCE_CSRF=true`)
- [x] Same-origin POST checks enabled by default (`WEB_ENFORCE_SAME_ORIGIN_POSTS=true`)
- [x] Session secure cookies enabled by default (`WEB_SESSION_COOKIE_SECURE=true`; auto-stripped on plain HTTP)
- [x] Admin roster managed via web users; no public signup or Discord command user creation
- [x] Backups recommended; platform/host encryption is a compensating control for DB-at-rest

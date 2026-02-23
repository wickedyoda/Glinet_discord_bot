# Security Hardening

This page documents concrete security actions implemented in the bot and web admin interface.

## External Reference

- Discord Developer Terms of Service:
  - https://support-dev.discord.com/hc/en-us/articles/8562894815383-Discord-Developer-Terms-of-Service

## Authentication and Account Controls

- Web-only authentication model for admin UI (no Discord command for creating web users).
- Password hashing at rest (`werkzeug` hash format) with stronger configured method and automatic rehash upgrade on successful login.
- Password complexity policy:
  - Minimum 6 characters, maximum 16 characters
  - At least 2 numbers
  - At least 1 uppercase letter
  - At least 1 symbol
- Forced password rotation every 90 days.
- User self-service account page:
  - Password change
  - Email change (requires current password)
  - First name, last name, and display name updates
- Login attempt throttling window to reduce brute-force attempts.
- Session hardening:
  - HttpOnly cookie
  - SameSite=Strict
  - Optional Secure cookie for HTTPS deployments (`WEB_SESSION_COOKIE_SECURE=true`)
  - Session lifetime limit

## Request and Browser Protections

- CSRF protection for state-changing requests (POST/PUT/PATCH/DELETE).
- Same-origin checks for state-changing requests using Origin/Referer host validation.
- Content Security Policy (CSP) and restrictive browser security headers.
- HSTS header when requests are HTTPS.
- Frame embedding blocked (`X-Frame-Options: DENY`).
- MIME sniffing disabled (`X-Content-Type-Options: nosniff`).
- Referrer policy set to `no-referrer`.
- Cross-origin policy headers configured.
- Cache-control no-store headers applied to reduce sensitive page caching.

## Access and Authorization Controls

- Admin-only route guards for sensitive web actions.
- Non-admin users are redirected away from admin-only actions.
- Command access controls are configurable per command (default/public/custom role rules).

## Data and File Hardening

- SQLite persistent storage with WAL mode and foreign key enforcement.
- Legacy data migration is merge-only to avoid overwriting existing SQLite records.
- Best-effort restrictive file permissions:
  - `.env` permission tightening to `0600`
  - data directory permission tightening to `0700`
  - SQLite DB file permission tightening to `0600`
- Upload request-size limits enforced to reduce abuse.

## Deployment Security Guidance

- Run web UI behind a trusted HTTPS reverse proxy.
- Keep `WEB_TRUST_PROXY_HEADERS=true` only when proxy headers come from trusted infrastructure.
- Keep CSRF and same-origin enforcement enabled in production:
  - `WEB_ENFORCE_CSRF=true`
  - `WEB_ENFORCE_SAME_ORIGIN_POSTS=true`
- Use a strong random `WEB_ADMIN_SESSION_SECRET`.
- Restrict external exposure of web admin and protect with network policy/firewall.

## Security Limits / Gaps

- Application-level database encryption at rest is not currently built into the bot.
- For encryption at rest, use host/platform disk encryption and encrypted backups.


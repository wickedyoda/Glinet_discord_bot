# Security Hardening

This page documents implemented controls and operational practices aligned with Discord bot security expectations.

## External Reference

- Discord Developer Terms of Service:
  - https://support-dev.discord.com/hc/en-us/articles/8562894815383-Discord-Developer-Terms-of-Service

## Security Principles Applied

- Least privilege
- Defense in depth
- Secure defaults
- Explicit admin controls
- Auditable operations

## Identity, Authentication, and Account Security

Implemented:

- Web-only account model for GUI administration.
- No Discord command path for web-user creation.
- Password hashing at rest with secure hash method and opportunistic rehash upgrades.
- Password policy enforcement:
  - 6 to 16 characters
  - 2+ numbers
  - 1+ uppercase letter
  - 1+ symbol
- Forced password rotation every 90 days.
- Self-service account management for existing users:
  - Change password
  - Change email
  - Update first/last/display name
- Login throttling to reduce brute-force effectiveness.

## Session and Cookie Controls

Implemented:

- Signed server-side session protection
- `HttpOnly` cookie
- `SameSite=Strict`
- Optional `Secure` cookie enforcement for HTTPS
- Configurable inactivity timeout (5-30 minutes)
- Optional remember-me duration for 5 days on trusted device

## Request and Browser Protections

Implemented:

- CSRF protection for state-changing actions
- Same-origin POST policy checks
- Content Security Policy
- Frame deny (`X-Frame-Options: DENY`)
- MIME sniffing disable (`X-Content-Type-Options: nosniff`)
- Referrer policy (`no-referrer`)
- HSTS on HTTPS responses
- Additional cross-origin policy headers where appropriate
- Cache-control `no-store` on sensitive pages

## Authorization and Access Segmentation

- Admin-only guards on sensitive web routes/actions
- Per-command access modes (`default`, `public`, `custom_roles`)
- Moderator/admin role gates for moderation commands
- Multi-role restriction support via role-name multi-select UI

## Data Security and Storage Controls

- SQLite persistence with WAL and foreign-key enforcement
- Legacy data imports are merge-only and non-destructive
- File permission hardening for `.env`, data dir, and DB file
- Upload request size limits to reduce abuse surface
- `/logs` command returns controlled error log excerpts only

## Deployment Hardening Requirements

Recommended production baseline:

- Deploy behind trusted HTTPS reverse proxy
- Restrict direct app-port exposure
- Set `WEB_PUBLIC_BASE_URL` to exact public origin
- Keep `WEB_ENFORCE_CSRF=true`
- Keep `WEB_ENFORCE_SAME_ORIGIN_POSTS=true`
- Keep `WEB_SESSION_COOKIE_SECURE=true` when HTTPS is used
- Use strong random `WEB_ADMIN_SESSION_SECRET`

## Large Guild and Scale Considerations

For multi-thousand-member guilds:

- Keep log levels conservative in production (`INFO`/`ERROR`).
- Enforce strict command permissions for risky moderation actions.
- Monitor permission drift after role hierarchy changes.
- Run periodic credential hygiene checks for web users.

## Incident Response Basics

1. Restrict access (proxy/firewall) if compromise suspected.
2. Rotate Discord token and web session secret.
3. Reset affected web user credentials.
4. Review `bot.log` and `container_errors.log`.
5. Validate command permission rules and admin roster.

## Known Limits and Compensating Controls

Limit:

- No built-in application-layer database encryption at rest.

Compensating controls:

- Host/platform disk encryption
- Encrypted offsite backups
- Restricted filesystem and container runtime access

## Security Checklist

- [ ] HTTPS reverse proxy configured
- [ ] Public origin configured via `WEB_PUBLIC_BASE_URL`
- [ ] Strong secrets configured and rotated
- [ ] CSRF and same-origin checks enabled
- [ ] Session secure cookies enabled in HTTPS deployments
- [ ] Admin roster minimized and reviewed
- [ ] Backups tested and encrypted

## Related Pages

- [Reverse Proxy Web GUI](Reverse-Proxy-Web-GUI)
- [Environment Variables](Environment-Variables)
- [Web Admin Interface](Web-Admin-Interface)

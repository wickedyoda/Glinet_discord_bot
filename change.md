# Changelog

All notable changes to this project are documented in this file.

## [2026-02-23] - Web Admin, Security, and Storage Overhaul

### Added
- Full web-admin account model with admin-created users only (no Discord `/login` flow).
- User profile fields for web accounts:
  - first name
  - last name
  - display name
  - email management with current-password verification
- Self-service password change flow for existing users.
- Password visibility toggles in user-create/reset/account forms.
- Optional "keep me signed in" login mode for 5 days.
- Admin web controls for bot profile:
  - bot username
  - server nickname
  - avatar upload
- Admin web controls for command permissions with per-command modes:
  - default policy
  - public
  - custom roles (multi-role selection)
- Moderator slash command `/logs` for recent container error log retrieval (ephemeral).
- Container-wide error log file `data/container_errors.log`.
- Runtime log-level separation:
  - `LOG_LEVEL` for general runtime logging
  - `CONTAINER_LOG_LEVEL` for container error capture
- Reverse-proxy documentation page with common proxy examples:
  - Nginx
  - Caddy
  - Traefik
  - Apache
  - HAProxy

### Changed
- Migrated persistent runtime data to SQLite (`data/bot_data.db`) with WAL mode and tuned pragmas.
- Implemented merge-only legacy import on startup from old `/app/data` files (no overwrite of existing DB rows).
- Expanded moderation capability coverage for member/role operations and web-driven controls.
- Enforced stronger password policy globally:
  - minimum 6 characters
  - maximum 16 characters
  - at least 2 numbers
  - at least 1 uppercase character
  - at least 1 symbol
- Enforced password rotation every 90 days.
- Made session timeout configurable in the web GUI (5-minute steps, 5-30 minutes).
- Updated session handling to support inactivity timeout and remember-login mode together.
- Hardened web request validation for reverse proxies using:
  - `WEB_PUBLIC_BASE_URL`
  - forwarded host handling (`X-Forwarded-Host`, `X-Original-Host`, `Forwarded`)
- Improved local (non-HTTPS localhost) login behavior when secure cookies are enabled.
- Added explicit web login/security decision logging for troubleshooting:
  - origin-policy blocks
  - CSRF validation blocks
  - session-loss warnings after recent successful login (proxy/cookie troubleshooting)
- Improved login page form semantics and field consistency:
  - associated labels (`for`/`id`)
  - password/email autocomplete attributes
  - consistent field sizing and styling
- Firmware monitor update behavior:
  - first-run baseline now captures the current firmware list without sending historical alerts
  - notifications now trigger only for true deltas (new entries or changed existing entries)
  - firmware notifications are now compact summaries instead of long per-entry posts

### Security
- CSRF protection enabled for state-changing requests.
- Same-origin enforcement for state-changing requests with proxy-aware host checks.
- Secure cookie support and strict cookie settings.
- Browser security headers hardened; COOP applied only for trustworthy origins (HTTPS/loopback).
- Added configurable session cookie `SameSite` policy (`WEB_SESSION_COOKIE_SAMESITE`) for reverse-proxy compatibility tuning.
- File permission hardening for sensitive files/directories where supported.
- Removed/blocked clear-text password logging patterns flagged by scanning.

### Ops and Deployment
- Updated `docker-compose.yml` to reflect current runtime/security variables.
- Added/updated environment examples for new and compatibility variables:
  - `CONTAINER_LOG_LEVEL`
  - `DISCORD_LOG_LEVEL`
  - `WEB_PUBLIC_BASE_URL`
  - `WEB_TRUST_PROXY_HEADERS`
  - `WEB_SESSION_COOKIE_SECURE`
  - `WEB_SESSION_COOKIE_SAMESITE`
  - `WEB_ENFORCE_CSRF`
  - `WEB_ENFORCE_SAME_ORIGIN_POSTS`
  - compatibility aliases documented in `.env.example`
- Updated docs for proxy deployment, security posture, command access, and logging paths.

## [2025-07-24] - Invite Tracking Enhancements

### Added
- `/enter_role` modal flow for 6-digit access code entry.
- Docker publish workflow for the `beta` branch.
- Persistent invite data via mounted `data/` volume.
- Automatic role assignment on invite-based joins.

### Changed
- Improved runtime logging and container output handling.
- Fixed syntax/runtime issues in `bot.py`.

## [2025-07-06] - Role Invite Bot Restructure

### Changed
- Reworked invite + code flow to multi-step interaction:
  1. `/submitrole`
  2. role capture
  3. invite/code generation
  4. join-time role assignment via invite mapping
  5. `/enter_role` for existing members

### Added
- Persistent role-code pairing.
- Code generation constraints to avoid long repeated-digit patterns.
- `/getaccess` for default access role assignment.

## [2025-07-05] - Core Functional Bot Build

### Added
- Dockerized deployment model.
- Initial slash-command access tooling.
- Persistent invite/role tracking files.
- Guild slash-command sync on startup.

## [2025-07-04] - Initial Commit

### Added
- Base Discord bot scaffold.
- `.env` token/guild configuration.
- Initial Dockerfile and CI pipeline.

# Changelog

All notable changes to this project are documented in this file.

## [2026-02-25] - Bot Channel Logging and Env Rename

### Added
- New runtime log file `${LOG_DIR}/bot_log.log` for bot-channel payload auditing.
- `bot_log.log` now records payloads that moderation/server-event handlers send (or attempt to send) to the log channel.
- Web GUI `/admin/logs` dropdown now includes `bot_log.log`.
- Auto-refresh interval dropdowns added to `/staus` and `/admin/logs` with `1`, `5`, `10`, `30`, `60`, and `120` second options.
- New GitHub Actions workflows for integrity and security:
  - `CI Integrity` (critical Ruff checks, Python compile checks, optional pytest discovery)
  - `Dependency Review` (PR dependency risk gate)
  - `Python Vulnerability Scan` (`pip-audit` on requirements)
  - `Secret Scan` (`gitleaks`)
  - `Container Security Scan` (Trivy image scan + SARIF upload + critical gate)
  - `SBOM Generate` (CycloneDX artifact)
  - `OSSF Scorecards` (scheduled security posture reporting)

### Changed
- Renamed settings key from `GENERAL_CHANNEL_ID` to `BOT_LOG_CHANNEL_ID` in bot runtime config and web settings UI.
- Kept backward compatibility by accepting `GENERAL_CHANNEL_ID` as a legacy fallback alias.
- Updated compose/example/wiki/docs references to include `BOT_LOG_CHANNEL_ID` and `bot_log.log`.
- Strengthened `/admin/account` password-change validation:
  - current password explicitly required and verified
  - new password must be entered twice and match
  - added client-side mismatch validation before submit
- Normalized `/staus` metric card table alignment:
  - consistent heading spacing
  - fixed label/value column widths
  - right-aligned numeric value column for consistent cross-card formatting
- Centered top header menu controls in the web GUI for consistent navigation alignment.
- Observability metrics now maintain a rolling 24-hour history in memory and display min/avg/max summary on `/staus`.
- Added background observability sampling every 60 seconds with retention pruning at 24 hours.
- Runtime log handling switched to timed rotation with retention controls:
  - default retention `90` days
  - default rotation interval `1` day
  - configurable via `LOG_RETENTION_DAYS` and `LOG_ROTATION_INTERVAL_DAYS`
- Web admin runtime supervision added:
  - auto-restarts web admin when it stops unexpectedly
  - allows up to 5 restarts within 10 minutes
  - when limit is exceeded, halts restarts and posts a critical alert to the bot log channel
  - if Discord loop is not ready yet, alert is queued and delivered on bot `on_ready`
  - after critical alert is posted, container shutdown is scheduled after 10 minutes
- Hardened Docker publish workflows:
  - upgraded action versions (`checkout`, `buildx`, `login`, `build-push`)
  - pull-request builds now validate image build without pushing to registry
- Stabilized `Container Security Scan` workflow:
  - SARIF generation step is non-blocking and uploads when present
  - policy failure now comes only from explicit critical-vulnerability gate
  - Trivy scanning scope limited to vulnerability scanning (`scanners: vuln`)
  - switched Trivy execution to direct CLI (`setup-trivy` + `trivy image`) for deterministic exit-code behavior
- Removed repo-managed `CodeQL` workflow to avoid conflict with GitHub CodeQL default setup.

## [2026-02-23] - Web Admin, Security, and Storage Overhaul

### Added
- Full web-admin account model with admin-created users only (no Discord `/login` flow).
- Reddit search commands:
  - `/search_reddit`
  - `!searchreddit`
  - returns top 5 matching posts from configured subreddit (`REDDIT_SUBREDDIT`, default `r/GlInet`)
- General channel prune commands for moderators:
  - `/prune_messages` (amount 1-500)
  - `!prune` (amount 1-500)
  - skips pinned messages and writes moderation logs
- User profile fields for web accounts:
  - first name
  - last name
  - display name
  - email management with current-password verification
- Self-service password change flow for existing users.
- Web GUI user-role model with two account types:
  - `Admin` (full management/write access)
  - `Read-only` (view-only across admin pages)
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
- Web observability page:
  - runtime snapshot cards for CPU, memory, I/O, network, and uptime
  - public read-only status URL at `/staus` (`/admin/observability` redirects)
  - log viewer moved to `/admin/logs` (login required)
  - log viewer supports dropdown selection and latest 500-line refresh
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
- Logging behavior updated for stronger operations auditing:
  - log directory resolution now prefers `/logs` when available
  - added `${LOG_DIR}/web_gui_audit.log` for web GUI interaction audit entries
  - web admin now writes `WEB_AUDIT` request records (method, path, endpoint, status, ip, user, latency)
  - added `LOG_HARDEN_FILE_PERMISSIONS` to enforce restrictive log permissions (`/logs` -> `0700`, log files -> `0600`) where supported
- Improved web-login reliability behind mixed direct/proxy access:
  - CSRF handling now rehydrates login token when missing server-side token and submitted token is present.
  - Session cookie `Secure` flag is now only enforced on effectively HTTPS requests (`request.is_secure` or `X-Forwarded-Proto=https`), preventing HTTP local/proxy lockouts.
- Explicitly pinned the following commands to moderator/admin default access policy (`MODERATOR_ROLE_ID` + `ADMIN_ROLE_ID`), while still allowing override in web GUI command permissions:
  - `add_role_member`
  - `bulk_assign_role_csv`
  - `ban_member`
  - `create_role`
  - `delete_role`
  - `edit_role`
  - `kick_member`
  - `remove_role_member`
  - `timeout_member`
  - `unban_member`
  - `untimeout_member`
- Hardened Reddit search command handling:
  - command now catches runtime/send exceptions and returns a user-safe failure message
  - Reddit result text is sanitized for Discord-safe output encoding
  - search output trimming now enforces a hard Discord-safe max length
- Hardened global slash-command error response behavior:
  - avoids secondary 40060 "Interaction has already been acknowledged" failures
  - returns explicit "command is still syncing" feedback for `CommandNotFound`
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
- Web GUI access model update:
  - read-only users can open all admin pages and navigation options
  - all non-exempt write actions are blocked server-side for read-only users
  - users page now assigns explicit roles (`Admin` / `Read-only`) instead of admin-only toggle language
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
- Updated Docker publish workflows to build and push multi-arch images for both `linux/amd64` and `linux/arm64`.
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
- Added architecture support guidance in:
  - `README.md`
  - `wiki/Docker-and-Portainer-Deploy.md`

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

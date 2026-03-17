# Changelog

All notable changes to this project are documented in this file.

## [2026-03-16] - Member Activity Analytics

### Added
- Member activity tracking for guild messages with rolling hourly retention for the last 90 days.
- New private `/stats` slash command for personal activity summaries covering:
  - last 90 days
  - last 30 days
  - last 7 days
  - last 24 hours
- New guild-scoped web admin page at `/admin/member-activity` showing top-20 member activity tables for the same time windows.

### Changed
- Help and wiki docs now include the member activity analytics feature and `/stats` command.
- Member activity retention is now capped at 90 days and the lifetime "Since Joining" view has been removed.
- Member activity views now show exact period totals instead of derived average-rate fields.

## [2026-03-17] - Web User Admin Editing

### Added
- Admins can now edit another web GUI user's:
  - first name
  - last name
  - display name
  - email
- Admins can now reset another web GUI user's password directly from that user's edit section.

## [2026-03-17] - Native Role Picker for `/submitrole`

### Changed
- `/submitrole` now uses a native Discord slash-command role parameter instead of waiting for the user to mention a role in a follow-up message.
- The command now validates the selected role before generating the invite link and 6-digit access code.

## [2026-03-17] - Member Activity Backfill Job

### Added
- One-time startup backfill job for member activity history using:
  - `MEMBER_ACTIVITY_BACKFILL_ENABLED`
  - `MEMBER_ACTIVITY_BACKFILL_GUILD_ID`
  - `MEMBER_ACTIVITY_BACKFILL_SINCE`
- Backfill run-state storage so the same completed guild/date range does not replay on every restart.
- Message-level dedupe table for member activity so live collection and backfill do not double-count the same Discord messages.

## [2026-03-17] - Moderator `/random_choice` Command

### Added
- New moderator-only `/random_choice` slash command.
- Random selection excludes members with:
  - configured moderator/admin role IDs
  - named staff roles `Employee`, `Admin`, and `Gl.iNet Moderator`

### Changed
- `/random_choice` now enforces a per-guild 7-day cooldown so the same member cannot be selected twice within that window.

## [2026-03-17] - Member Activity Export

### Added
- ZIP export option at the bottom of `/admin/member-activity`.
- Export archive now includes:
  - one CSV per activity window
  - raw member activity summary CSV
  - raw hourly activity CSV
  - JSON summary manifest

## [2026-03-16] - Web GUI Host Bind Configuration

### Added
- New Compose/env controls for web GUI host-side port publishing:
  - `WEB_HTTP_HOST_BIND`
  - `WEB_HTTPS_HOST_BIND`

### Changed
- Docker Compose web port publishing is no longer hardcoded to `127.0.0.1`.
- Documentation now distinguishes same-host proxy-only localhost binding from direct host exposure using `0.0.0.0`.

## [2026-03-15] - Dependency Security Update

### Changed
- Upgraded `cryptography` from `46.0.1` to `46.0.5` to address `CVE-2026-26007` / `GHSA-r6ph-v2qm-q3c2`.
- Updated Docker dependency bootstrap to require `cryptography>=46.0.5` during image build.
- Updated the Docker image build to refresh Debian base packages and apply security upgrades for `libc-bin` and `libc6`, addressing `CVE-2026-0861`.

## [2026-03-15] - Feature Parity Merge from WickedYodaDiscordBot

### Added
- Utility slash commands:
  - `/ping`
  - `/sayhi`
  - `/happy`
  - `/shorten`
  - `/expand`
  - `/uptime`
- Guild-scoped web admin page at `/admin/actions` for recent moderation and server-event history.
- Guild-scoped web admin page at `/admin/youtube` for YouTube-channel-to-Discord subscriptions.
- Managed-guild allowlist support via `MANAGED_GUILD_IDS`.
- Verification tooling:
  - `pyproject.toml`
  - `requirements-dev.txt`
  - `scripts/verify.sh`
  - `tests/test_web_admin.py`
- Route aliases:
  - `/status/everything`
  - `/admin/wiki`

### Changed
- The current repository now includes the user-visible feature set that previously existed only in `WickedYodaDiscordBot`.
- Discord members intent can now be toggled through `ENABLE_MEMBERS_INTENT`.
- Utility command response visibility can be controlled through `COMMAND_RESPONSES_EPHEMERAL`.
- Wiki and README references were updated to reflect the merged feature surface and verification workflow.

## [2026-03-15] - Dual Web GUI HTTP/HTTPS Listeners

### Added
- Built-in HTTPS listener for the web GUI on port `8081` alongside HTTP on `8080`.
- Automatic self-signed certificate generation in `${DATA_DIR}/ssl/` when no user-provided TLS files exist.
- Configurable TLS file locations and HTTPS port environment variables.

### Changed
- Docker packaging now exposes both `8080` and `8081`.
- Compose and environment examples now include HTTPS listener variables and port mappings.
- Wiki documentation now describes the generated default certificate and how to replace it with a trusted certificate.

## [2026-03-15] - Multi-Guild Runtime Expansion

### Added
- New `/admin/guild-settings` page in the web GUI for selected-server overrides.
- Per-guild web-managed settings for:
  - bot log channel
  - moderation log channel
  - firmware notification channel
  - self-assign access role

### Changed
- Slash-command sync now runs per joined guild instead of only the primary configured guild.
- Dynamic tag responses are now stored per guild and refreshed per guild.
- Invite-role mappings and 6-digit access codes are now stored per guild.
- Server-event and moderation log routing now resolves per guild before falling back to global environment values.
- Firmware notifications now resolve configured channels across joined guilds instead of assuming a single primary guild.
- Web GUI tag-response editing now uses the selected guild context instead of one global JSON mapping.
- Global environment settings remain shared, but guild-owned settings are now split out from the global settings page.

## [2026-03-15] - Bot Identity Naming

### Changed
- Updated the primary project/bot name shown in the README to `WickedYoda'sLittleHelper`.
- Updated the web GUI window title suffix to `WickedYoda'sLittleHelper Dashboard`.
- Updated the web GUI bot-profile username placeholder to `WickedYoda'sLittleHelper`.

## [2026-03-14] - Web GUI Guild Selection

### Added
- New web GUI server-selection landing page at `/admin`.
- Guild-scoped server dashboard at `/admin/dashboard`.
- Web GUI now lists the Discord servers the bot is in and lets the user select which server to manage.

### Changed
- Web GUI guild-scoped pages now use the selected Discord server context:
  - bot profile server nickname
  - command permissions
  - Reddit feed mappings
  - bulk role CSV assignment
- Command permissions and Reddit feed subscriptions are now stored in SQLite per guild instead of globally.
- Global `.env` settings remain global, but Discord role/channel dropdowns now load from the selected server.

## [2026-03-12] - Search Command Cleanup

### Added
- New OpenWrt forum search commands:
  - `/search_openwrt_forum`
  - `!searchopenwrtforum`
  - returns the top 10 links from `https://forum.openwrt.org/`

### Changed
- Removed the combined all-resources search commands:
  - `/search`
  - `!search`
- Updated command help and wiki docs to reflect the source-specific search model.

## [2026-02-25] - Bot Channel Logging and Env Rename

### Added
- New runtime log file `${LOG_DIR}/bot_log.log` for bot-channel payload auditing.
- `bot_log.log` now records payloads that moderation/server-event handlers send (or attempt to send) to the log channel.
- Web GUI `/admin/logs` dropdown now includes `bot_log.log`.
- Auto-refresh interval dropdowns added to `/staus` and `/admin/logs` with `1`, `5`, `10`, `30`, `60`, and `120` second options.
- Public slash command `/help` with a short capabilities summary and a direct link to the GitHub wiki for advanced options.
- New `BOT_HELP_WIKI_URL` setting support (with GitHub wiki default) for the `/help` command link target.
- New web-admin Reddit feed management page:
  - add subreddit-to-channel mappings
  - choose target Discord text channels from a dropdown
  - enable/disable/delete feed subscriptions
  - set Reddit polling interval from a dropdown (default every 30 minutes)
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
- Added `REDDIT_FEED_CHECK_SCHEDULE` to runtime config/docs and wired env changes to restart the Reddit feed monitor loop.
- Updated legal-policy documentation links to the new WickedYoda Terms of Service + Privacy Policy page (`https://wickedyoda.com/?p=3460`).
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
- Updated packaging dependencies to address vulnerability findings:
  - pinned `wheel` to `0.46.2`
  - pinned `jaraco.context` to `6.1.0`
  - Docker build now upgrades `pip`/`setuptools`/`wheel`/`jaraco.context` before installing app requirements
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

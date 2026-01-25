# Changelog

All notable changes to this project will be documented in this file.


# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]
- Implement admin-only invite cleanup feature (planned)
- Add web dashboard for role/code management (planned)

### Added
- Tag-based auto-replies configurable via `data/tag_responses.json`.
- `!list` command to list configured tag commands (one per line).
- Tag commands are case-insensitive.
- Tag responses are also available as slash commands (e.g., `/betatest`).

### Changed
- Tag slash command registration is guarded against duplicate registrations and registration errors.


## [2025-07-24] - Invite Tracking Enhancements
### Added
- `/enter_role` now opens a private modal for the 6-digit code
- Docker publish workflow for the `beta` branch
- Invite data persists via the `data/` Docker volume
- Automatic role assignment when members join via tracked invites

### Changed
- Improved logging and Docker output
- Fixed syntax errors in `bot.py`
---

## [2025-07-06] - Role Invite Bot Restructure
### Changed
- Rewrote logic to operate in a multi-step interaction flow:
  1. Slash command `/submitrole` starts interaction
  2. Bot prompts for role
  3. Generates invite link and 6-digit code
  4. Assigns role to new members using the invite
  5. Existing members use `/enter_role` + code to gain access

### Added
- Role-code pairing stored in `role_codes.txt`
- Code generation ensures no more than 2 consecutive digits


## [Unreleased]
- Implement admin-only invite cleanup feature (planned)
- Add web dashboard for role/code management (planned)


## [2025-07-24] - Invite Tracking Enhancements
### Added
- `/enter_role` now opens a private modal for the 6-digit code
- Docker publish workflow for the `beta` branch
- Invite data persists via the `data/` Docker volume
- Automatic role assignment when members join via tracked invites

### Changed
- Improved logging and Docker output
- Fixed syntax errors in `bot.py`
---

## [2025-07-06] - Role Invite Bot Restructure
### Changed
- Rewrote logic to operate in a multi-step interaction flow:
  1. Slash command `/submitrole` starts interaction
  2. Bot prompts for role
  3. Generates invite link and 6-digit code
  4. Assigns role to new members using the invite
  5. Existing members use `/enter_role` + code to gain access

### Added
- Role-code pairing stored in `role_codes.txt`
- Code generation ensures no more than 2 consecutive digits
- Restricted `/submitrole` and `/enter_role` to `Employee` role
- Added `/getaccess` command for general members using `access_role.txt`

---

## [2025-07-05] - Core Functional Bot Build
### Added
- Dockerized deployment using `docker-compose.yml` and GitHub Container Registry
- Initial slash commands: `/setaccessrole`, `/generateinvite`, `/getaccess`
- Role and invite tracking via `access_role.txt` and `permanent_invite.txt`
- Auto-role assignment to new members using invite join tracking
- Slash command synchronization per guild on startup

---

## [2025-07-04] - Initial Commit
### Added
- Basic Discord bot skeleton using `discord.py`
- `.env` configuration for `DISCORD_TOKEN` and `GUILD_ID`
- Dockerfile and GitHub Actions workflow to build and push image
- Invite generation and persistent invite code storage



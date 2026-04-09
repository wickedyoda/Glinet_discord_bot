# Discord Invite + Utility Bot Wiki

<p align="center">
  <img src="../assets/images/glinet-bot-round.png" alt="GL.iNet Bot Logo (Round)" width="170" />
</p>

This wiki is the complete operations and feature reference for the `GL.iNet UnOfficial Discord Bot`.

## Platform Summary

Core capabilities:

- Role-bound invite links and 6-digit access code flows
- Bulk CSV role assignment with rich result reporting
- Spreadsheet-safe member-activity CSV exports and tolerant CSV input parsing
- Tag auto-replies and dynamic slash command generation
- Source-specific search helpers for GL.iNet forums, OpenWrt forums, Reddit, and docs
- Utility commands for ping, uptime/status, URL shortening/expansion, and quick greeting/image helpers
- Country suffix nickname utilities
- Moderation tooling for members, roles, event logs, and moderator-only random member selection with a 7-day cooldown
- Firmware feed monitor with scheduled notification delivery
- Reddit feed monitor with channel-to-Discord posting for configured subreddits
- YouTube feed monitor with channel-to-Discord posting
- Generic service monitor for website/API online-offline alerts
- Dedicated web GUI page for service monitors and Uptime Kuma public-page or authenticated-instance imports/alerts
- LinkedIn public profile monitor with channel-to-Discord posting
- Member activity analytics with private `/stats`, rolling 90-day retention, optional startup backfill, and ZIP export from the web GUI
- Guild-scoped welcome automation with optional channel message, optional DM, and optional uploaded image attachment with enforced size and dimension validation
- Secure web admin interface with per-command permissions, action history, observability, Reddit/YouTube/LinkedIn subscriptions, member activity views, and user management
- Dashboard quick-notes view with direct common-path links and clickable recent page links for the active web session
- Dedicated command-status page showing the selected server's commands, effective access level, and enabled/disabled state
- Log export ZIP download from the web GUI logs page
- Multi-guild admin model with optional managed-guild allowlist filtering
- Guild-group scoped web admin model with `Guild Admin` users limited to assigned Discord server groups
- Guild data archival for 14 days after the bot leaves a server, with automatic restore on same-ID rejoin during that window
- Four web-user roles:
  - `Admin`
  - `Read-only`
  - `Guild Admin` (group-scoped access limited to assigned Discord server groups)
  - `Glinet-Read-Only` (primary-guild-only read access to GL.iNet community management pages)
  - `Glinet-RW` (primary-guild-only guild-scoped write access for the GL.iNet community)
- SQLite-backed persistence with legacy merge imports on startup

## Read by Goal

- I need full command list and access restrictions:
  - [Command Reference](Command-Reference.md)
- I need onboarding/access role setup:
  - [Role Access and Invites](Role-Access-and-Invites.md)
  - [Join With an Invite Code](Join-With-Invite-Code.md)
- I need moderation/logging operations:
  - [Moderation and Logs](Moderation-and-Logs.md)
- I need web GUI administration details:
  - [Web Admin Interface](Web-Admin-Interface.md)
- I need deployment and proxy guidance:
  - [Docker and Portainer Deploy](Docker-and-Portainer-Deploy.md)
  - [Health Checks and Readiness](Health-Checks-and-Readiness.md)
  - [Reverse Proxy Web GUI](Reverse-Proxy-Web-GUI.md)
- I need security baseline and controls:
  - [Security Hardening](Security-Hardening.md)
- I need variable documentation:
  - [Environment Variables](Environment-Variables.md)

## Feature Pages

- [Role Access and Invites](Role-Access-and-Invites.md)
- [Join With an Invite Code](Join-With-Invite-Code.md)
- [Bulk CSV Role Assignment](Bulk-CSV-Role-Assignment.md)
- [Tag Responses](Tag-Responses.md)
- [Search and Docs](Search-and-Docs.md)
- [Country Code Commands](Country-Code-Commands.md)
- [Moderation and Logs](Moderation-and-Logs.md)
- [Firmware Monitor](Firmware-Monitor.md)
- [Web Admin Interface](Web-Admin-Interface.md)

## Operations and Security Pages

- [Environment Variables](Environment-Variables.md)
- [Docker and Portainer Deploy](Docker-and-Portainer-Deploy.md)
- [Health Checks and Readiness](Health-Checks-and-Readiness.md)
- [Reverse Proxy Web GUI](Reverse-Proxy-Web-GUI.md)
- [Data Files](Data-Files.md)
- [Security Hardening](Security-Hardening.md)
- [Command Reference](Command-Reference.md)

## Source of Truth

- Main README: [`README.md`](../README.md)
- Bot implementation: [`bot.py`](../bot.py)
- Web admin implementation: [`web_admin.py`](../web_admin.py)

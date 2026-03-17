# Discord Invite + Utility Bot Wiki

<p align="center">
  <img src="../assets/images/glinet-bot-round.png" alt="GL.iNet Bot Logo (Round)" width="170" />
</p>

This wiki is the complete operations and feature reference for `WickedYoda'sLittleHelper`.

## Platform Summary

Core capabilities:

- Role-bound invite links and 6-digit access code flows
- Bulk CSV role assignment with rich result reporting
- Tag auto-replies and dynamic slash command generation
- Source-specific search helpers for GL.iNet forums, OpenWrt forums, Reddit, and docs
- Utility commands for ping, uptime/status, URL shortening/expansion, and quick greeting/image helpers
- Country suffix nickname utilities
- Moderation tooling for members, roles, event logs, and moderator-only random member selection with a 7-day cooldown
- Firmware feed monitor with scheduled notification delivery
- YouTube feed monitor with channel-to-Discord posting
- Member activity analytics with private `/stats`, rolling 90-day retention, optional startup backfill, and ZIP export from the web GUI
- Secure web admin interface with per-command permissions, action history, YouTube subscriptions, member activity views, and user management
- Multi-guild admin model with optional managed-guild allowlist filtering
- SQLite-backed persistence with legacy merge imports on startup

## Read by Goal

- I need full command list and access restrictions:
  - [Command Reference](Command-Reference.md)
- I need onboarding/access role setup:
  - [Role Access and Invites](Role-Access-and-Invites.md)
- I need moderation/logging operations:
  - [Moderation and Logs](Moderation-and-Logs.md)
- I need web GUI administration details:
  - [Web Admin Interface](Web-Admin-Interface.md)
- I need deployment and proxy guidance:
  - [Docker and Portainer Deploy](Docker-and-Portainer-Deploy.md)
  - [Reverse Proxy Web GUI](Reverse-Proxy-Web-GUI.md)
- I need security baseline and controls:
  - [Security Hardening](Security-Hardening.md)
- I need variable documentation:
  - [Environment Variables](Environment-Variables.md)

## Feature Pages

- [Role Access and Invites](Role-Access-and-Invites.md)
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
- [Reverse Proxy Web GUI](Reverse-Proxy-Web-GUI.md)
- [Data Files](Data-Files.md)
- [Security Hardening](Security-Hardening.md)
- [Command Reference](Command-Reference.md)

## Source of Truth

- Main README: [`README.md`](../README.md)
- Bot implementation: [`bot.py`](../bot.py)
- Web admin implementation: [`web_admin.py`](../web_admin.py)

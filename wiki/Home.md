# Discord Invite + Utility Bot Wiki

<p align="center">
  <img src="../assets/images/glinet-bot-round.png" alt="GL.iNet Bot Logo (Round)" width="170" />
</p>

This wiki is the complete operations and feature reference for the Discord Invite + Utility Bot.

## Platform Summary

Core capabilities:

- Role-bound invite links and 6-digit access code flows
- Bulk CSV role assignment with rich result reporting
- Tag auto-replies and dynamic slash command generation
- GL.iNet forum/docs search helpers (combined and source-specific)
- Country suffix nickname utilities
- Moderation tooling for members, roles, and event logs
- Firmware feed monitor with scheduled notification delivery
- Secure web admin interface with per-command permissions and user management
- SQLite-backed persistence with legacy merge imports on startup

## Read by Goal

- I need full command list and access restrictions:
  - [Command Reference](Command-Reference)
- I need onboarding/access role setup:
  - [Role Access and Invites](Role-Access-and-Invites)
- I need moderation/logging operations:
  - [Moderation and Logs](Moderation-and-Logs)
- I need web GUI administration details:
  - [Web Admin Interface](Web-Admin-Interface)
- I need deployment and proxy guidance:
  - [Docker and Portainer Deploy](Docker-and-Portainer-Deploy)
  - [Reverse Proxy Web GUI](Reverse-Proxy-Web-GUI)
- I need security baseline and controls:
  - [Security Hardening](Security-Hardening)
- I need variable documentation:
  - [Environment Variables](Environment-Variables)

## Feature Pages

- [Role Access and Invites](Role-Access-and-Invites)
- [Bulk CSV Role Assignment](Bulk-CSV-Role-Assignment)
- [Tag Responses](Tag-Responses)
- [Search and Docs](Search-and-Docs)
- [Country Code Commands](Country-Code-Commands)
- [Moderation and Logs](Moderation-and-Logs)
- [Firmware Monitor](Firmware-Monitor)
- [Web Admin Interface](Web-Admin-Interface)

## Operations and Security Pages

- [Environment Variables](Environment-Variables)
- [Docker and Portainer Deploy](Docker-and-Portainer-Deploy)
- [Reverse Proxy Web GUI](Reverse-Proxy-Web-GUI)
- [Data Files](Data-Files)
- [Security Hardening](Security-Hardening)
- [Command Reference](Command-Reference)

## Source of Truth

- Main README: [`README.md`](../README.md)
- Bot implementation: [`bot.py`](../bot.py)
- Web admin implementation: [`web_admin.py`](../web_admin.py)

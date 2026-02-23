# Discord Invite + Utility Bot Wiki

<p align="center">
  <img src="../assets/images/glinet-bot-round.png" alt="GL.iNet Bot Logo (Round)" width="170" />
</p>

This wiki is the documentation hub for the Discord Invite + Utility Bot.
The content mirrors the core `README.md` and splits features into focused pages.

## What This Bot Does

- Role-bound invite links and 6-digit access codes
- CSV-based bulk role assignment with missing/error reporting
- Tag auto-replies and dynamic tag slash commands
- GL.iNet forum/docs search commands
- Country code nickname management (`- CC`)
- Moderation actions (ban/unban, kick + prune, timeout/untimeout, member role add/remove) and event logging
- Firmware mirror monitoring with scheduled notifications
- Password-protected web admin panel (bot rename/profile, per-command permissions, user management)
- SQLite-backed persistent storage with boot-time merge import from legacy data files

## Main Feature Pages

- [Role Access and Invites](Role-Access-and-Invites)
- [Bulk CSV Role Assignment](Bulk-CSV-Role-Assignment)
- [Tag Responses](Tag-Responses)
- [Search and Docs](Search-and-Docs)
- [Country Code Commands](Country-Code-Commands)
- [Moderation and Logs](Moderation-and-Logs)
- [Firmware Monitor](Firmware-Monitor)
- [Web Admin Interface](Web-Admin-Interface)

## Operations Pages

- [Environment Variables](Environment-Variables)
- [Docker and Portainer Deploy](Docker-and-Portainer-Deploy)
- [Reverse Proxy Web GUI](Reverse-Proxy-Web-GUI)
- [Data Files](Data-Files)
- [Security Hardening](Security-Hardening)
- [Command Reference](Command-Reference)

## Command Reference

See [Command Reference](Command-Reference) for a complete command list, and use the feature pages for behavior details.

## Source of Truth

- Main README: [`README.md`](../README.md)
- Bot implementation: [`bot.py`](../bot.py)
- Web admin implementation: [`web_admin.py`](../web_admin.py)

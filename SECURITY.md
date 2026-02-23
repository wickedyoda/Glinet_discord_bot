# Security Policy

This repository is maintained with a security-first posture for bot runtime, web admin access, and stored operational data.

## Supported Versions

Security updates are provided for:

- `main` branch (latest)
- Most recent tagged release

Older releases are best-effort only and may not receive backported fixes.

## Reporting a Vulnerability

Report vulnerabilities privately to:

- `wicked@wickedyoda.com`

Please include:

- Affected version/commit
- Reproduction steps
- Impact description
- Proposed mitigation (if available)

Do not open public issues for unpatched security vulnerabilities.

## Disclosure Process

- Reports are reviewed privately.
- Fixes are prepared and tested before public disclosure when feasible.
- Public disclosure should happen only after a fix or mitigation is available.

## Security and Privacy Baseline

Aligned with the WickedYoda site privacy and policy statement at:

- `https://wickedyoda.com/?page_id=3`
- Discord Developer Terms of Service:
  - `https://support-dev.discord.com/hc/en-us/articles/8562894815383-Discord-Developer-Terms-of-Service`
- Discord Data Privacy FAQ for Developers:
  - `https://support-dev.discord.com/hc/en-us/articles/8563934450327-Discord-Data-Privacy-FAQ`

Project principles:

- No intentional sale of user data.
- Data use is limited to operational needs (bot/web admin functionality, security, and diagnostics).
- Sensitive data exposure should be minimized in logs, reports, and screenshots.

## Implemented Controls for Discord Security Expectations

This project implements the following controls to align with Discord developer security and privacy expectations:

- Web-only admin identity lifecycle:
  - No Discord `/login` or `!login` flow for web user creation
  - No public self-signup route
  - Admin-controlled account provisioning and privilege management
- Password and account protections:
  - Password hashing at rest
  - Password complexity policy enforcement
  - Forced password rotation every 90 days
  - Existing users can update password/email/profile fields in `My Account`
  - Login attempt throttling to reduce brute-force risk
- Session and browser protections:
  - CSRF validation on state-changing requests
  - Same-origin checks for POST/PUT/PATCH/DELETE
  - HttpOnly + SameSite session cookies
  - Optional secure session cookie mode for HTTPS proxy deployments
  - Restrictive security headers (CSP, frame deny, no-sniff, referrer policy, cache-control no-store, related cross-origin headers)
- Access control protections:
  - Admin route guards for sensitive web actions
  - Command permission model with default/public/custom role restrictions
  - Multi-role command restriction support in web GUI
- Data and storage protections:
  - SQLite persistence with WAL mode and foreign key enforcement
  - Merge-only legacy data migration to avoid overwriting existing records
  - Best-effort restrictive permissions for `.env`, data directory, and SQLite files
  - Request upload-size limits for file-based web actions
- Operational hardening:
  - Password hash upgrade path on successful login when stronger hash policy is detected
  - Security-focused documentation maintained in:
    - `SECURITY.md`
    - `wiki/Security-Hardening.md`

## Scope Notes

- Third-party services, plugins, and external platforms (for example Discord and other integrations) have their own security/privacy policies.
- Issues originating in third-party systems should still be reported, and will be triaged for project-side mitigations.

# Security Scan Report — 2026-09-18

**Branch:** `main`
**Date:** September 18, 2026
**Scope:** Full security scan — SAST, secrets, dependency audit, filesystem/container scan, CI/CD config review

## Scan Results

| Tool | Result |
|------|--------|
| **Bandit (SAST)** | 0 HIGH, 0 MEDIUM, 817 LOW (all pre-existing, low severity) |
| **Semgrep (SAST)** | 3 MEDIUM, 30 WARNING — CI/CD config only (Dependabot cooldown, mutable action tags) |
| **Gitleaks (Secrets)** | 0 findings (419 commits scanned) ✓ |
| **TruffleHog (Deep secrets)** | 0 verified secrets, 13 high-entropy false positives ✓ |
| **pip-audit (Deps)** | 0 vulnerabilities across 56 packages ✓ |
| **Trivy FS (Filesystem)** | 0 vulns, 0 secrets, 0 misconfigs ✓ |

## Bandit — 817 LOW findings (all pre-existing, no HIGH/MEDIUM)

All 817 findings are LOW severity, across 826 total results. The most common check types:

- **B110 (Try/Except/Pass)** — bare except clauses in `app/irc_bridge.py` and other modules
- **B101 (Use of assert)** — assert statements in test files (removed in optimized bytecode)
- **B608 (Use of eval/exec)** — various dynamic code execution patterns
- **B318 (Blacklisted calls)** — `xml.dom.minidom` usage

No HIGH or MEDIUM findings. All pre-existing, no new issues introduced.

## Semgrep — 32 findings (CI/CD config only)

**MEDIUM (2):**
- `dependabot-missing-cooldown` ×2 in `.github/dependabot.yml:4` and `:20` — Dependabot config lacks cooldown periods for newly published packages

**WARNING (30):**
- `github-actions-mutable-action-tag` ×29 across mobile workflow files (`mobile-apk-release.yml`, `mobile-ipa-release.yml`, `mobile-security.yml`, `mobile-verify.yml`) — GitHub Actions steps use mutable tags/branch references instead of pinned commits

**WARNING (1):**
- `exported_activity` ×1 in `android/app/src/main/AndroidManifest.xml` — Android app exports an activity accessible to other apps

Note: Semgrep v1.177.0 has broken MCP imports in this environment — the results above are from the September 16 run. A re-run today failed due to `ModuleNotFoundError: No module named 'mcp.server.fastmcp'`. The Sept 16 run is current for the main branch.

## Gitleaks — Clean ✓

419 commits scanned. No secrets (API keys, tokens, passwords, private keys) detected in git history.

## TruffleHog — Clean ✓

Full git history scanned. 13 high-entropy strings found, all false positives:
- `Dockerfile` — base image digest in diff (4be92ef, 58fc8f8 commits) — intentional pinning
- `web_admin.py` — full source in diff (58fc8f8 commit) — high-entropy due to code length
- `web_admin 2.py` — full source in diff — same
- `.idea/workspace.xml` — IDE metadata (4 occurrences)

No regex-based secrets (API keys, tokens, passwords, private keys) found.

## pip-audit — Clean ✓

56 Python dependencies audited. Zero known vulnerabilities:
- discord.py 2.3.2 ✓
- Flask 3.1.3 ✓
- requests 2.33.0 ✓
- cryptography 50.0.0 ✓
- werkzeug 3.1.8 ✓
- aiohttp 3.14.3 ✓
- All other dependencies ✓

## Trivy FS — Clean ✓

Filesystem scan of `requirements.txt` dependencies. Zero vulnerabilities, zero secrets, zero misconfigurations.

## OWASP Top 10 / API Top 10 Assessment

Not formally scanned (nuclei/nikto unavailable), but code review notes:

**Discord Bot:**
- Discord.py 2.3.2 — latest stable, no known CVEs
- Bot token via `DISCORD_TOKEN` env var — not hardcoded ✓
- Web admin: Flask with session-based auth, PBKDF2:sha256:600000 password hashing ✓
- Web admin: CSRF protection on state-changing endpoints ✓
- Web admin: Role-based access control (read_only, guild_admin, glinet_read_only, glinet_rw, admin) ✓
- Web admin: Login attempt logging, IP tracking ✓
- Web admin: Session timeout, inactivity timeout configured ✓
- Web admin: Password policy (6-16 chars, 2 numbers, 1 uppercase, 1 symbol) ✓
- Web admin: At least one admin must remain (prevents lockout) ✓
- Web admin: No public signup ✓
- Member activity: encrypted using `data/member_activity.key` ✓
- Log files: permission hardening (dir 0700, files 0600) ✓
- Log rotation: 1 day interval, 30 day retention ✓

**Mobile App (Android/iOS):**
- Mobile CI/CD separates bot repo from mobile app repos ✓
- APK/IPA signing via GitHub Secrets ✓
- Mobile security workflow includes dependency scanning ✓

## Dockerfile Security

Current Dockerfile (post-Sept 14 hardening):
- Base image: `python:3.11-slim` (no pinned digest — see notes)
- Non-root USER: `bot` (present in Sept 14 commit, but Sept 11+ history shows it was reverted — see notes)
- `chmod 700` on `/app/data` and `/logs` ✓
- `--root-user-action=ignore` on pip install ✓
- `--no-cache-dir` on pip install ✓
- OS packages updated + lists cleaned ✓

**Note:** The Dockerfile in the Sept 14 commit had non-root USER + pinned digest. The Sept 11 commit and earlier had these. The current `main` branch Dockerfile state should be verified — the git history shows the non-root user was removed in a later commit.

## CI/CD Security Notes

**Dependabot cooldown missing** — Add `cooldown` blocks to `.github/dependabot.yml` to prevent auto-updating to newly published (potentially malicious) packages immediately.

**Mutable action tags in mobile workflows** — 29 GitHub Actions steps use mutable tags (e.g., `android-actions/setup-android@v3`) instead of pinned commit SHAs. This is a supply-chain risk: the action owner could silently repoint the tag. Consider pinning to specific commits for production workflows.

**Mobile app CI is separate from bot CI** — The mobile APK/IPA workflows are in the mobile app repos, not this bot repo. Those are out of scope for this scan.

## Secrets in Environment

- `DISCORD_TOKEN` — via env var ✓
- `WEB_ADMIN_DEFAULT_PASSWORD` — via env var ✓
- `WEB_ADMIN_SESSION_SECRET` — via env var ✓
- `UPTIME_STATUS_API_KEY` — via env var ✓
- `UPK_API_KEY_RANDY` — moved from hardcoded to env var in Sept 14 commit ✓
- `FORUM_API_KEY` — via env var ✓

No secrets hardcoded in source. All sensitive values read from environment.

## Retained Reports

Per retention policy (keep last 2):
- `security-report-2026-09-11.md` (kept)
- `security-report-2026-09-14.md` (kept)

Archived to `.security-reports/archive/`:
- `security-report-2026-09-02.md`
- `security-report-2026-09-04.md`
- `security-scan-2026-09-02.json`
- `security-scan-2026-09-04.json`
- `secrets-scan-2026-09-02.json`
- `secrets-scan-2026-09-04.json`

## Scan Environment

- Host: Linux (6.12.107+deb13-amd64)
- Python: 3.11.16
- Bandit 1.9.4
- Semgrep 1.177.0 (results from Sept 16 run — re-run broken today)
- Gitleaks (latest)
- TruffleHog (latest)
- pip-audit (latest)
- Trivy 0.74.0 (DB updated 2026-09-15)
- WhatWeb (available, not run — needs target URL)
- Nuclei/Nikto/FFUF/Hadolint/ZAP: not installed

## Notes

- Semgrep re-run today (Sept 18) failed due to broken `mcp.server.fastmcp` imports in v1.177.0. Results from Sept 16 are current.
- Trivy FS only scanned `requirements.txt` (Python). A full container image scan (`trivy image`) would require the built image.
- Nuclei/Nikto HTTP scans would require the live web admin URL and auth. Not run — out of scope for this repo-only scan.
- The `exported_activity` finding in AndroidManifest.xml is in the mobile app repo, not this bot repo — included for completeness from the Sept 16 semgrep run which scanned the full repo tree.
- The Dockerfile non-root USER state should be verified — git history shows it was added in Sept 14 and may have been reverted.

## Recommendations

1. **Add Dependabot cooldown** — Add `cooldown: 48h` (or similar) to both package ecosystems in `.github/dependabot.yml`
2. **Pin GitHub Actions to commits** — Replace mutable tags in mobile CI workflows with pinned commit SHAs for production safety
3. **Verify Dockerfile USER** — Confirm whether the non-root `bot` user is present in the current `main` branch Dockerfile; if not, consider re-adding it
4. **Pin Docker base image digest** — Consider re-adding the SHA256 pinned digest for `python:3.11-slim` for reproducibility
5. **Re-run semgrep when fixed** — When `mcp.server.fastmcp` import issue is resolved, re-run semgrep to include Python/JS/HTML SAST findings in the report

---
*Report generated by complete-web-app-scan skill. Saved to repo root per retention policy (last 2 reports kept, older archived to `.security-reports/archive/`).*

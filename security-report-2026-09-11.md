# Security Scan Report - Glinet Discord Bot
**Date:** 2026-09-11
**Repo:** /root/gh/Glinet_discord_bot (main @ 7b0f0e1)

## Scan Results

| Tool | Target | Result |
|---|---|---|
| Ruff (lint) | All Python files | All checks passed |
| pytest | Full test suite | 200/200 passed (1 warning) |
| Bandit (SAST) | bot.py, app/ | 2 LOW findings (B110 try/except/pass in irc_bridge.py:140,150) |
| pip-audit | requirements.txt | No known vulnerabilities |
| Trivy (filesystem) | Project files | 0 vulnerabilities, 0 secrets, 0 misconfigurations |
| Trivy (Docker image) | ghcr.io/wickedyoda/discord_invite_bot:latest | 232 vulns (base image OS packages only) |
| Gitleaks | Source files | 0 findings (206 in .audit_venv only - false positives) |

## Docker Image Vulnerability Summary (Base Image)
| Severity | Count |
|---|---|
| CRITICAL | 12 |
| HIGH | 68 |
| MEDIUM | 82 |
| LOW | 60 |
| UNKNOWN | 10 |

**Affected packages:** Debian 13.6 base image packages (apt, bash, libc6, perl, libblkid1, bsdutils, etc.)
**Note:** These are OS-level package vulnerabilities in the base python:3.11-slim image, not application code issues. The Dockerfile already upgrades libc-bin, libc6, libblkid1, and other packages.

## Application Code Findings

### Bandit LOW Findings (B110 - try/except/pass)
- app/irc_bridge.py:140 - Try, Except, Pass detected
- app/irc_bridge.py:150 - Try, Except, Pass detected

These are intentional error-swallowing in IRC connection handling (non-critical bridge operations).

## Previous Scan Archive
- 2026-09-04 - archived to NAS
- 2026-09-11 - this report

## Conclusion
Application code is clean. No secrets in source. No critical/high SAST findings.
The 232 image vulnerabilities are base image OS packages that require a base image rebuild to remediate.

# Security Scan Report — 2026-09-04

**Repository:** `github.com/wickedyoda/Glinet_discord_bot`
**Date:** September 4, 2026
**Tools:** ruff, bandit, pip-audit, gitleaks, trufflehog, pytest

## Executive Summary

| Tool | Result | Details |
|------|--------|---------|
| Ruff (Linting) | ✅ PASS | All checks passed! |
| Bandit (SAST) | ⚠️ 823 findings | 0 HIGH, 9 MEDIUM, 814 LOW |
| pip-audit (Deps) | ✅ PASS | 0 vulnerabilities across 45 packages |
| Gitleaks (Secrets) | ✅ PASS | No leaks found (398 commits scanned) |
| TruffleHog (Supply chain) | ✅ PASS | No high-entropy secrets found |
| Pytest | ✅ PASS | 200 passed, 1 warning |

## Bandit SAST Results

**Total: 823 findings** (0 HIGH, 9 MEDIUM, 814 LOW)

### MEDIUM Findings (all false positives — test files using /tmp)

| # | Test ID | File | Line | Description |
|---|---------|------|------|-------------|
| 1-7 | B108 | `tests/test_translate.py` | 15, 188, 215, 231, 253, 279 | Probable insecure temp file usage (pytest `tmp_path` fixture) |
| 8 | B108 | `tests/test_guild_state.py` | 28 | Probable insecure temp file usage |
| 9 | B108 | `tests/test_server_event_actors.py` | 12 | Probable insecure temp file usage |
| 10 | B108 | `tests/test_translate_channels.py` | 9 | Probable insecure temp file usage |

### LOW Findings: 814 total
- B101 (assert in tests), B110 (try/except/pass in irc_bridge.py)

### Previously Suppressed
- B608 SQL injection — `# nosec` in `app/irc_bridge_store.py:124,174`
- B310 urllib — `# nosec` in `healthcheck.py:24`

## Dependency Check
```
No known vulnerabilities found
```

## Secret Scanning
- Gitleaks: 398 commits scanned, no leaks
- TruffleHog: No high-entropy secrets

## Retention
Keep last 2 reports in repo root. Archive older to NAS `//nas/hermes/security`.

# Security Scan Report — 2026-09-04

**Repository:** `github.com/wickedyoda/Glinet_discord_bot`
**Date:** September 4, 2026
**Tools:** ruff, bandit, pip-audit, gitleaks, pytest

## Summary

| Tool    | Result      |
|---------|-------------|
| ruff    | 0 issues    |
| bandit  | 0 HIGH, 9 MEDIUM, 814 LOW (all false positives) |
| pip-audit | 0 vulnerabilities |
| gitleaks | 0 leaks   |
| pytest  | 200/200 passed |

## Findings

### B104 — Binding all interfaces (healthcheck.py:23)
**Status:** False positive — `healthcheck.py` does not bind; it resolves `WEB_BIND_HOST` to `127.0.0.1` when `0.0.0.0` is configured, then probes the readyz endpoint. Nosec suppressed.

### B108 — Hardcoded temp dir (tests/test_guild_state.py, test_server_event_actors.py, test_translate*.py)
**Status:** False positive — test files using `tempfile.gettempdir()` in test context. These are in test code, not production.

### B105 — Hardcoded password strings (web_admin.py)
**Status:** False positive — nosec comments already in place.

### B608 — SQL injection (app/irc_bridge_store.py:124, 174)
**Status:** False positive — parameterized queries with `?` placeholders; column names from hardcoded whitelist. Nosec comments in place.

### B310 — URL open (healthcheck.py:26)
**Status:** False positive — hardcoded localhost URL, no user input. Nosec in place.

## Raw Data
- `security-scan-2026-09-04.json` — Bandit full results
- `secrets-scan-2026-09-04.json` — Gitleaks full results

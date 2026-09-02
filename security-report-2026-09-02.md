# Security Scan Report — Glinet Discord Bot
**Date:** 2026-09-02 13:30 UTC  
**Scope:** repository root (main branch, HEAD `54a3216`)

---

## Summary

| Scan Type       | Tool        | Findings | Status |
|-----------------|-------------|----------|--------|
| SAST            | Bandit      | 825 (0 HIGH, 11 MEDIUM, 814 LOW) | ✅ |
| Secrets         | Gitleaks    | 2 (Discord bot token in git history) | ⚠️ |
| Dependencies    | pip-audit   | 0 vulnerabilities (45 deps scanned) | ✅ |
| Credential scan | Trufflehog  | 14 high-entropy hits (all false positives) | ✅ |

---

## 1. SAST — Bandit (825 findings)

### Medium Severity (11)

| # | File | Line | Issue | Assessment |
|---|------|------|-------|------------|
| 1 | `app/irc_bridge_store.py` | 124 | B608 — SQL injection vector (string-based query) | **False positive** |
| 2 | `app/irc_bridge_store.py` | 174 | B608 — SQL injection vector (string-based query) | **False positive** |

**Remainder (9):** B108 insecure temp file usage in test files:
- `tests/test_guild_state.py`
- `tests/test_server_event_actors.py`
- `tests/test_translate.py`
- `tests/test_translate_channels.py`
- `tests/test_irc_bridge_web_manage.py`

> ⚠️ **Note:** 9 of 11 MEDIUM findings are in test fixtures (low risk). The SQL injection findings in `irc_bridge_store.py` are false positives — queries use `?` placeholders with a column whitelist (`allowed` dict).

### Low Severity (814)

- 770× B101 `assert` in test files — expected in pytest, false positives
- B110 `try/except/pass` in `app/irc_bridge.py:140,150` — legitimate exception swallowing, low impact
- B107 hardcoded password `''` in `webui/app.py:590` — empty string default, no real credential

### High/Critical
**None found.**

---

## 2. Secret Scanning — Gitleaks (2 findings)

| # | Rule | File | Commit | Assessment |
|---|------|------|--------|------------|
| 1 | Generic API Key | `.env` | `8cb2b77d` | Discord bot token in git history |
| 2 | Generic API Key | `.env` | `f8b4b74e` | Same token, second historical commit |

**Status:** Token was **revoked earlier in this session** (prior scan at 13:24 UTC). The `.env` file is gitignored in current working tree — token survives only in dangling git objects from earlier `git filter-repo` runs that didn't fully purge.

**Action taken:**
- PR #275 open from branch `security/purge-discord-token-history` (filtered history removes both commits)
- `.env` committed to `.gitignore` — no longer tracked

---

## 3. Dependency Scanning — pip-audit (0 findings)

**Scanned:**
- `requirements.txt` — 18 runtime dependencies (discord.py, Flask, cryptography, requests, openpyxl, etc.)
- `requirements-dev.txt` — 4 dev dependencies (bandit, pytest, ruff, pip-audit)

**Result:** No known vulnerabilities across any package.

---

## 4. Credential Detection — Trufflehog (14 findings, all false positives)

| File | Count | Type | Assessment |
|------|-------|------|------------|
| `web_admin 2.py` | 5 | High entropy | Alphabet sequences in code examples |
| `.idea/workspace.xml` | 4 | High entropy | PyCharm project ID metadata |
| `web_admin.py` | 3 | High entropy | Alphabet sequences in code examples |
| `Dockerfile` | 2 | High entropy | Docker image digest hash (not a secret) |

**No verified credentials** detected. No detector-classified secrets (AWS, Stripe, GitHub tokens, etc.).

---

## Action Items

1. **Merge PR #275** — purge Discord token from GitHub history (blocked by branch protection, requires PR review/approval)
2. **Delete local scan artifacts** — `secrets-report.json` contains the raw token value:
   ```bash
   rm ./secrets-report.json
   rm ./secrets-fresh.json
   rm ./secrets-report-v2.json
   ```
3. **Verify token rotation** — confirm the revoked Discord token is replaced in your deployment/hosting environment
4. **Address test-file MEDIUM findings** — 9 temp-file B108 issues in test fixtures (optional, test-only scope)

---

## Files Generated

- `security-report.json` — Bandit SAST results (825 findings)
- `secrets-report.json` — Gitleaks findings (2 findings, contains raw token — **delete after triage**)
- `security-scan-latest.md` — this report

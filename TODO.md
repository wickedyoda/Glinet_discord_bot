# GL.iNet Discord Bot — Project To-Do List

This document tracks active development branches, open pull requests, pending features, security hardening tasks, and completed milestones for `wickedyoda/Glinet_discord_bot`.

---

## 🚀 Active Branches & Open Pull Requests

- [x] **PR #369 (Merged)**: `docs(wiki): detail member onboarding via invite links and /enter_role command`
  - **Branch**: `docs/wiki-invite-role-onboarding` → `main`
  - **Status**: Merged into `main` (commit `61f1030`).
  - **Details**: Updated in-tree wiki docs (`Join-With-Invite-Code.md`, `Role-Access-and-Invites.md`, `Command-Reference.md`).
- [x] **PR #370 (Merged)**: `feat(web_admin): reaction role message composer, alphabetical dropdowns, and dashboard reorganization`
  - **Branch**: `feat/reaction-role-post-builder` → `main`
  - **Status**: Merged into `main` (commit `f410151`).
  - **Details**: Reaction-role message composer, A–Z dropdowns, 6-category dashboard, 439 tests passing.
- [ ] **Branch `feat/webgui-refresh-discord-catalog`**
  - **Status**: Local commit `1d4dc31` (452 tests passing, Ruff clean, Bandit clean apart from pre-existing B608).
  - **Feature**: "Refresh Channels & Roles" button on `/admin/reaction-roles` that bypasses the 120s `WEB_DISCORD_CATALOG_TTL_SECONDS` cache so newly created Discord channels and roles appear immediately.
  - **Action**: Ready to push and open PR.
- [ ] **Branch `feat/freshdesk-discord-thread-sync`**
  - **Status**: Local commit `75d16ed` (463 tests passing, S1 authorization gates, S2 private notes, S3 HTML escaping).
  - **Action**: Pending review / PR push.

---

## 🔄 Pending Enhancements & Backlog

### 1. Web GUI & Reaction Roles
- [ ] Push and merge `feat/webgui-refresh-discord-catalog` to `main`.
- [ ] Build new Docker image tag (`glinet_discord_bot_v1.10.XX`) and deploy to `serv2`.
- [ ] Verify live Reaction Role Message Composer and the catalog refresh button on `https://glinet.wickedyoda.com/admin/reaction-roles`.

### 2. Freshdesk Integration Hardening (Backlog S4–S7)
- [ ] **S4 — Rate Limiting & 429 Handling**: Add per-(thread, user) cooldown for Discord-to-Freshdesk message relay; catch `FreshdeskRateLimitError` gracefully without stack traces, and honor `Retry-After`.
- [ ] **S5 — Database Transaction Safety**: Route all `FreshdeskThreadStore` operations through `with_db(commit=True)` to prevent read-modify-write race conditions under multi-threaded Flask/bot execution.
- [ ] **S6 — URL Validation & Anti-SSRF**: Validate `FRESHDESK_BASE_URL` in web routes to enforce `https://` and `*.freshdesk.com` domain allowlisting.
- [ ] **S7 — Bidirectional Sync**: Implement Freshdesk webhook receiver endpoint on Web GUI (`/webhook/freshdesk`) with HMAC verification to mirror Freshdesk agent replies into Discord threads.

### 3. Repository & CI Governance
- [ ] Add required status checks to GitHub ruleset `22146433` (`Main Protection`) so CI gates (`secret-scan`, `security-scan`, `python-vulnerability-scan`) enforce passing status before merging.
- [ ] Remove `|| true` from `security-scan.yml` and `continue-on-error: true` from `container-security-scan.yml` once baseline clean.

---

## ✅ Completed Milestones

- [x] **Web GUI Alphabetical Sorting & Reorganization**:
  - Sorted all navigation page select dropdowns (`#desktop-nav-page-select` and `#mobile-nav-page-select`) in alphabetical (A-Z) order.
  - Sorted all Discord channel dropdowns (`_load_discord_catalog_options`) alphabetically (A-Z).
  - Reorganized `/admin/dashboard` main GUI cards into 6 logical categories: *Moderation & Security*, *Roles & Onboarding*, *Channels & Chat Automation*, *Notification Feeds & Monitors*, *Support & Forum Integrations*, and *Bot Configuration & System Administration*.
  - 436 tests passing (`test_web_admin_dashboard_organization.py`).
- [x] **Discord Catalog Refresh Button**:
  - Added a "Refresh Channels & Roles" control to `/admin/reaction-roles` that forces a fresh Discord fetch, bypassing the 120s `WEB_DISCORD_CATALOG_TTL_SECONDS` cache.
  - `bot.py`: extracted `_fetch_discord_catalog_for_web()` and added `run_web_refresh_discord_catalog()`, which pops the cache entry before fetching so a failed refresh cannot resurrect a stale snapshot. Scheduling failures from `run_coroutine_threadsafe()` are now covered by the "Unexpected error" path.
  - `web_admin.py`: `on_refresh_discord_catalog` callback, count-reporting refresh helper, catalog-freshness note, and `refresh_catalog` route handling that reloads the dropdown options.
  - Covered by `tests/test_discord_catalog_refresh.py` (8 tests) and 5 new web-admin tests.
- [x] **Reaction Role Message Builder & Auto-Poster**:
  - Interactive composer card in `/admin/reaction-roles` supporting up to 8 emoji-to-role pairs (Unicode & custom server emojis).
  - Async bot dispatcher (`post_reaction_role_message_for_guild_async`) verifying permissions, posting to Discord, and seeding reactions.
  - Real-time role assignment on `on_raw_reaction_add` and role removal on `on_raw_reaction_remove`.
  - Full test suite coverage in `tests/test_reaction_role_post_builder.py` and `tests/test_web_admin.py` (433 passed).
- [x] **Member Onboarding Documentation**:
  - Detailed GitHub Wiki and in-tree docs for dedicated role invite links (`on_member_join`) and 6-digit access code redemption via `/enter_role`.
  - Pushed to live GitHub Wiki (`Glinet_discord_bot.wiki.git` @ `b45b35b`).
- [x] **Standard Role Permission Gates**:
  - Replaced legacy separate Freshdesk role gate with unified `COMMAND_PERMISSION_DEFAULTS` model and Web GUI per-command overrides (`/admin/command-permissions`).
  - Standardized `/create-ticket` and `/create-ticket-from-message` to public default.
- [x] **Privacy & Actor Audit Logging**:
  - Standardized logging to record only Discord user IDs (`interaction.user.id`) without storing requester email, names, or message bodies.
- [x] **Honeypot & Moderation Tools**:
  - Multi-action honeypot channel protection (softban, ban, timeout, role) with join age limits and per-guild logging.
  - Bad-words filter and moderation warning system.
- [x] **Multi-Feed Monitors**:
  - Firmware notification monitor (`FIRMWARE_FEED_URL`).
  - Reddit subreddit feed monitor and auto-responder rules.
  - YouTube, LinkedIn, and GL.iNet beta program notifications.

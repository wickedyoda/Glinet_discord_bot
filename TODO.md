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
- [x] **PR #371 (Merged & Deployed)**: `feat(web_admin): add Discord channel/role catalog refresh button` + reaction-role fixes
  - **Branch**: `feat/webgui-refresh-discord-catalog` → `main` (squash-merged as `3d56375`)
  - **Status**: Merged 2026-11-08, 12/12 checks green, 0 unresolved review threads, 469 tests passing. Image rebuilt and deployed to `serv2`; container healthy.
  - **Feature 1**: "Refresh Channels & Roles" button on `/admin/reaction-roles` bypassing the 120s catalog TTL, with strict-mode fetching so a failed live fetch is never reported as a successful refresh.
  - **Feature 2**: Reaction-role shortcode support (`:thumbsup:` → 👍) with pre-post emoji validation, and per-row forms so Save/Activate/Pause/Delete actually submit.
  - **Feature 3**: Catalog timestamp lookup skipped when the catalog fails, removing a redundant 20s fetch from failed page loads.
- [ ] **Branch `feat/freshdesk-discord-thread-sync`**
  - **Status**: Local commit `75d16ed` (463 tests passing, S1 authorization gates, S2 private notes, S3 HTML escaping).
  - **Action**: Pending review / PR push.

---

## 🔄 Pending Enhancements & Backlog

### 1. Web GUI & Reaction Roles
- [x] Merged `feat/webgui-refresh-discord-catalog` to `main` (`3d56375`).
- [x] Rebuilt the image and deployed to `serv2` — container healthy, all new code markers verified in the running image.
- [x] Live GitHub Wiki updated (`0b4a192`).
- [ ] Verify the Reaction Role Message Composer and the catalog refresh button against the live site at `https://glinet.wickedyoda.com/admin/reaction-roles`.

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
- [x] **Reaction Role Shortcode Support & Pre-Post Emoji Validation**:
  - Discord shortcodes typed into the web form (`:thumbsup:`, `:computer:`, `:tools:`) are resolved to real Unicode emoji via the new `emoji==2.16.0` dependency plus a supplementary alias table for names the package lacks.
  - `resolve_reaction_emoji_for_discord()` returns `None` for values Discord would refuse, and `post_reaction_role_message_for_guild_async()` validates every emoji **before** sending — a bad emoji aborts the post instead of publishing a prompt nobody can react to.
- [x] **Reaction Role Mappings Table Row Forms**:
  - The mappings table rendered four `type=submit` buttons with no `<form>` element (a form cannot be a child of `<tbody>`/`<tr>`), so Save/Activate/Pause/Delete all did nothing.
  - Each row now renders its own form in the action cell, with the row's inputs joined via the HTML5 `form` attribute. Verified with a DOM pass: 0 orphaned submit buttons.
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

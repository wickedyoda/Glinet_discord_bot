# GL.iNet Discord Bot — Project To-Do List

This document tracks active development branches, open pull requests, pending features, security hardening tasks, and completed milestones for `wickedyoda/Glinet_discord_bot`.

---

## 🚀 Active Branches & Open Pull Requests

- [ ] **PR #369**: `docs(wiki): detail member onboarding via invite links and /enter_role command`
  - **Branch**: `docs/wiki-invite-role-onboarding` → `main`
  - **Status**: Open, mergeable, CI running.
  - **Details**: Updated in-tree wiki docs (`Join-With-Invite-Code.md`, `Role-Access-and-Invites.md`, `Command-Reference.md`).
- [ ] **Branch `feat/reaction-role-post-builder`**
  - **Status**: Local commit `12c8d9f` (433 tests passing, Ruff clean, Bandit clean).
  - **Feature**: Web GUI interactive message composer at `/admin/reaction-roles` with multi-slot emoji-to-role connections, automatic Discord message posting, and auto-reaction seeding.
  - **Action**: Ready to push and open PR.
- [ ] **Branch `feat/freshdesk-discord-thread-sync`**
  - **Status**: Local commit `75d16ed` (463 tests passing, S1 authorization gates, S2 private notes, S3 HTML escaping).
  - **Action**: Pending review / PR push.

---

## 🔄 Pending Enhancements & Backlog

### 1. Web GUI & Reaction Roles
- [ ] Push and merge `feat/reaction-role-post-builder` to `main`.
- [ ] Build new Docker image tag (`glinet_discord_bot_v1.10.XX`) and deploy to `serv2`.
- [ ] Verify live Reaction Role Message Composer on `https://glinet.wickedyoda.com/admin/reaction-roles`.

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

# Role Access and Invites

This feature set manages automated and self-service Discord role assignment through invite link tracking, 6-digit access codes, and default access grants.

---

## Member Onboarding Pathways

The bot provides two synchronized mechanisms for members to receive roles:

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                                   Role Access Flow                                     │
├───────────────────────────────────────────┬────────────────────────────────────────────┤
│   Method 1: Join via Role Invite Link     │    Method 2: Redeem Code via /enter_role   │
├───────────────────────────────────────────┼────────────────────────────────────────────┤
│ 1. Moderator generates invite + code pair │ 1. Member receives 6-digit code            │
│ 2. Member joins server with invite URL    │ 2. Member runs /enter_role in Discord      │
│ 3. Bot tracks invite on member join       │ 3. Member enters 6-digit code in modal UI  │
│ 4. Role assigned automatically on connect │ 4. Bot validates code and assigns role     │
└───────────────────────────────────────────┴────────────────────────────────────────────┘
```

For the member-facing walkthrough, see **[Join With an Invite Code](Join-With-Invite-Code.md)**.

---

## Commands

| Command | Type | Default Access | Parameters | Description |
|---|---|---|---|---|
| `/submitrole` | Slash | Member/Public | `role` (Role Select) | Prompts for a target role, then generates a dedicated Discord invite link and paired 6-digit access code |
| `/enter_role` | Slash | Member/Public | none (Interactive Modal) | Opens a Discord popup modal allowing members to enter their 6-digit code and receive their role |
| `/getaccess` | Slash | Member/Public | none | Self-assigns the server's configured default access role (`access_role_id`) |
| `/restore_code` | Slash | Moderator | `role`, `code`, optional `invite` | Recreates or restores a specific 6-digit code mapping using an existing invite URL/code or generating a fresh one |

---

## Web GUI Management (`/admin/role-access`)

Administrators can manage all active and legacy role mappings via the Web Admin interface under **Role Access**:

- **Mapping Table Columns:**
  - **6-Digit Code**: The unique numeric code redeemed via `/enter_role`.
  - **Invite Link / Code**: The Discord invite URL and vanity code tied to the role.
  - **Target Role**: The server role granted upon redemption or join.
  - **Status**: The current state of the mapping.
  - **Actions**: Status controls to manage availability.

### Mapping States

- `Active`: Both join-by-invite auto-assignment and `/enter_role` code redemption are enabled and processed.
- `Paused`: The mapping is temporarily halted. Members joining with the invite or submitting the code are rejected with a paused notice.
- `Disabled`: The mapping is permanently deactivated until explicitly reactivated by an administrator.

### Manual Restore & Add Form

When migrating from another bot or restoring an existing Discord invite:
1. Navigate to `/admin/role-access` for the target server.
2. Enter the 6-digit numeric access code.
3. Select the target Discord role from the dropdown catalog.
4. Supply an existing Discord invite URL (must belong to the active server) or choose to generate a new invite.
5. Click **Add Mapping**.

---

## How It Works (Technical Implementation)

### 1. Join-by-Invite Tracking (`on_member_join`)
- When a mapping is created via `/submitrole` or Web Admin, the bot registers `(guild_id, invite_code, role_id, status)` in the `invite_roles` SQLite table.
- At runtime, the bot maintains a cached map of invite usage counts per guild (`invite_uses_by_guild`).
- When a new member joins:
  1. The bot inspects active server invites via `guild.invites()`.
  2. Compares current `inv.uses` against cached usage counts.
  3. Identifies which specific invite code incremented in uses.
  4. If that invite code matches an active entry in `invite_roles`, the bot calls `member.add_roles(role)`.
  5. The action is logged to the bot's audit logs.

### 2. Code Redemption Modal (`/enter_role`)
- When a member runs `/enter_role`, the bot renders a `CodeEntryModal` dialog.
- The user enters their 6-digit code and clicks Submit.
- On modal submission (`on_submit`):
  1. The code is normalized (trimmed of whitespace).
  2. The bot queries SQLite (`get_role_id_by_code()`) for an `active` mapping in that guild.
  3. If invalid, expired, or paused: replies with an ephemeral message `❌ Invalid code.`.
  4. If the mapped Discord role no longer exists in the server: replies with `❌ Role not found.`.
  5. If valid: calls `interaction.user.add_roles(role)` and returns `✅ You've been given the **{role.name}** role!`.
- Both the modal interaction and response messages are **ephemeral** (strictly private to the calling user).

### 3. Default Access Shortcut (`/getaccess`)
- Allows members to self-assign a baseline community role (configured via `access_role_id` in guild settings).
- If no access role is set, the command informs the user that self-assign access is not configured.

---

## Inputs, Validation & Security

- **Code Format**: Access codes are strictly 6 numeric digits (e.g., `102938`).
- **Normalization**: User input is trimmed and sanitized before SQLite queries.
- **Role Hierarchy**: The bot's highest role must sit above the target roles in Server Settings > Roles; otherwise, Discord denies role assignment.
- **Bot Permissions**:
  - `Manage Roles` (required to add roles to members)
  - `Create Instant Invite` (required to generate invites in `/submitrole` and `/restore_code`)
- **Idempotency**: Running `/enter_role` when a member already holds the target role is safe and will not throw errors or disrupt existing permissions.

---

## Storage & Migration

- **Primary Database**: `data/bot_data.db` (SQLite)
  - Table `role_codes`: stores `(code, role_id, created_at, guild_id, invite_code, status)`
  - Table `invite_roles`: stores `(guild_id, invite_code, role_id, created_at)`
- **Startup Migration**: Automatically imports legacy flat files if present (`data/access_role.txt`, `data/role_codes.txt`, `data/invite_roles.json`) into SQLite without overwriting existing entries.

---

## Administrator Troubleshooting

### Role is not assigned after invite join
- **Invite Tracking Limitation**: If two users join simultaneously in the exact same millisecond, Discord's API may return identical use counts. Members can use `/enter_role` with the paired 6-digit code as an immediate fallback.
- **Status Check**: Verify in `/admin/role-access` that the mapping status is `Active` (not `Paused` or `Disabled`).
- **Bot Permissions**: Check that the bot has `Manage Roles` and its top role is higher in the role list than the role being assigned.

### User receives "Role not found" on `/enter_role`
- The Discord role was deleted or recreated in Discord server settings, invalidating the stored `role_id`.
- **Fix**: Open `/admin/role-access` or use `/restore_code` to update the mapping to the newly created role.

### `/submitrole` fails to generate invite
- Ensure the bot has `Create Instant Invite` permission in the channel where the command was executed.

---

## Related Documentation

- [Join With an Invite Code](Join-With-Invite-Code.md) — Member step-by-step instructions
- [Bulk CSV Role Assignment](Bulk-CSV-Role-Assignment.md) — Batch role assignment via CSV file upload
- [Command Reference](Command-Reference.md) — Full slash command registry
- [Web Admin Interface](Web-Admin-Interface.md) — Web GUI configuration guide
- [Data Files](Data-Files.md) — SQLite schema and storage architecture

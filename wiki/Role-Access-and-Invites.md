# Role Access and Invites

This feature set handles role assignment through invite tracking, access codes, and default access grants.

## Commands

| Command | Access | Purpose |
|---|---|---|
| `/submitrole` | Member/Public | Pick a Discord role in the slash UI, then generate an invite + 6-digit code for that role |
| `/enter_role` | Member/Public | Redeem a 6-digit code and receive the mapped role |
| `/getaccess` | Member/Public | Receive the configured default access role |
| `/restore_code` | Moderator | Restore a specific 6-digit code for a role, using an existing invite or a fresh one |

## Web GUI Management

- Open `/admin/role-access` for the selected guild.
- The page shows each mapping with:
  - 6-digit code
  - invite link
  - invite code
  - target role
  - current status
- Available status actions:
  - `Activate`
  - `Pause`
  - `Disable`
- Use the manual restore/add form when a mapping row needs to be recreated with an existing Discord invite.
- The supplied invite must belong to the selected Discord server.

Status behavior:

- `Active`: join-by-invite and `/enter_role` both work
- `Paused`: mapping is temporarily blocked
- `Disabled`: mapping is fully blocked until reactivated

## Workflow Variations

### Variation 1: Invite + Code Pair

1. User runs `/submitrole`, picks the target role.
2. Bot generates a persistent invite and a 6-digit code.
3. User shares invite or code with the target member.
4. Target joins, then runs `/enter_role` and enters the code.
5. Bot resolves the mapped role and assigns it.

### Variation 2: Default Access Shortcut

1. Member runs `/getaccess`.
2. Bot assigns the configured default access role.
3. Useful for baseline permissions before role-specific onboarding.

### Variation 3: Restore a Missing Code

1. Moderator opens `/admin/role-access` or uses `/restore_code`.
2. Enters the target role and either an existing invite URL/code or requests a fresh invite.
3. Bot recreates the mapping or reuses the existing row so original invite behavior can be resumed.

## Inputs and Validation

- Role codes are normalized before lookup.
- Access codes are numeric and expected to be 6 digits.
- Invalid, expired, or unmapped codes are rejected with user feedback.
- Repeated redemption attempts avoid duplicate role assignment.

## Assignment Rules

- If member already has the target role, the command is idempotent and reports already-assigned status.
- If bot lacks role hierarchy permissions, assignment fails with error detail.
- If the mapped role no longer exists, the mapping must be corrected before success.

## Required Discord Permissions

Bot requires:

- `Create Instant Invite`
- `Manage Roles`
- Role hierarchy above the roles the bot will grant

## Storage and Migration

Primary storage:

- `data/bot_data.db` (SQLite)

Startup merge import from legacy files:

- `data/access_role.txt`
- `data/role_codes.txt`
- `data/invite_roles.json`

Import behavior:

- Existing SQLite rows are preserved.
- Legacy entries are inserted only when missing.

## Operational Notes

- Role assignment is safe for repeated calls.
- If mappings are frequently updated, validate role IDs after role deletions or renames.
- Use moderation logs to verify assignment events where applicable.

## Troubleshooting

- No role after code entry:
  - Verify mapping exists and target role still exists.
  - Verify bot has `Manage Roles` and role hierarchy is correct.
- `/submitrole` fails:
  - Verify a valid Discord role was selected.
  - Verify bot has invite permission in the target channel.
- Wrong role assigned:
  - Check mapping data in SQLite or admin tooling.

## Related Pages

- [Join With an Invite Code](Join-With-Invite-Code.md)
- [Bulk CSV Role Assignment](Bulk-CSV-Role-Assignment.md)
- [Command Reference](Command-Reference.md)
- [Data Files](Data-Files.md)

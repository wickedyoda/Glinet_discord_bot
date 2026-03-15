# Role Access and Invites

This feature set handles role assignment through invite tracking, access codes, and default access grants.

## Commands

| Command | Access | Purpose |
|---|---|---|
| `/submitrole` | Member/Public (unless overridden) | Generate invite + 6-digit code for mapped role flow |
| `/enter_role` | Member/Public (unless overridden) | Redeem code and receive mapped role |
| `/getaccess` | Member/Public (unless overridden) | Receive default access role |

## Workflow Variations

### Variation 1: Invite + Code Pair

1. User runs `/submitrole`.
2. Bot generates a persistent invite and a 6-digit code.
3. User shares invite/code with target member.
4. Target joins and/or redeems code via `/enter_role`.
5. Bot resolves mapped role and assigns it.

### Variation 2: Default Access Shortcut

1. Member runs `/getaccess`.
2. Bot assigns the configured default access role.
3. Useful for baseline permissions before role-specific onboarding.

## Inputs and Validation

- Role codes are normalized before lookup.
- Access codes are numeric and expected to be 6 digits.
- Expired/invalid/unmapped codes are rejected with user feedback.
- Repeated redemption attempts avoid duplicate role assignment.

## Assignment Rules

- If member already has target role, command is idempotent and reports already-assigned status.
- If bot lacks role hierarchy permissions, assignment fails with error detail.
- If mapped role no longer exists, mapping must be corrected before success.

## Required Discord Permissions

Bot requires:

- `Create Instant Invite` (for invite generation)
- `Manage Roles` (for role assignment)
- Role hierarchy above roles the bot will grant

## Storage and Migration

Primary storage:

- `data/bot_data.db` (SQLite)

Startup merge import (legacy, non-overwriting):

- `data/access_role.txt`
- `data/role_codes.txt`
- `data/invite_roles.json`

Import behavior:

- Existing SQLite rows are preserved.
- Legacy entries are inserted only when missing.

## Operational Notes

- For large guilds, role assignment is designed to be safe for repeated calls.
- If mappings are frequently updated, validate role IDs after role deletions/renames.
- Use moderation logs to verify assignment events where applicable.

## Troubleshooting

- User gets no role after code entry:
  - Verify mapping exists and target role still exists.
  - Verify bot has `Manage Roles` and role hierarchy is correct.
- `/submitrole` fails:
  - Verify bot has invite permission in target channel.
- Wrong role assigned:
  - Check mapping data in SQLite or admin tooling.

## Related Pages

- [Bulk CSV Role Assignment](Bulk-CSV-Role-Assignment)
- [Command Reference](Command-Reference)
- [Data Files](Data-Files)

# Moderation and Logs

Moderator tooling for members, roles, and operational incident visibility.

## Moderation Command Matrix

| Category | Command | Prefix Equivalent | Default Access |
|---|---|---|---|
| Role | `/create_role` | none | Moderator |
| Role | `/edit_role` | none | Moderator |
| Role | `/delete_role` | none | Moderator |
| Role membership | `/add_role_member` | `!addrolemember` | Moderator |
| Role membership | `/remove_role_member` | `!removerolemember` | Moderator |
| Member discipline | `/ban_member` | `!banmember` | Moderator |
| Member discipline | `/unban_member` | `!unbanmember` | Moderator |
| Member discipline | `/kick_member` | `!kickmember` | Moderator |
| Member discipline | `/timeout_member` | `!timeoutmember` | Moderator |
| Member discipline | `/untimeout_member` | `!untimeoutmember` | Moderator |
| Channel hygiene | `/prune_messages` | `!prune` | Moderator |
| Logging test | `/modlog_test` | `!modlogtest` | Moderator |
| Runtime error logs | `/logs` | none | Moderator |

## `/prune_messages` and `!prune` Behavior

- Removes recent messages in the current channel/thread.
- Requires amount between `1` and `500`.
- Skips pinned messages.
- Logs the moderation action to `MOD_LOG_CHANNEL_ID`.

## `/logs` Command Behavior

- Reads recent lines from `${LOG_DIR}/container_errors.log` (default `/logs/container_errors.log`).
- Ephemeral response to reduce accidental exposure.
- Intended for production incident triage without shell access.

Tuning variable:

- `CONTAINER_LOG_LEVEL` controls what gets written to error log file.
- Web GUI request auditing is written separately to `${LOG_DIR}/web_gui_audit.log`.

## Access Control Layers

Layer 1: baseline role gates

- `MODERATOR_ROLE_ID`
- `ADMIN_ROLE_ID`

Layer 2: per-command overrides in web admin

- `/admin/command-permissions`
- Modes: `default`, `public`, `custom_roles`

## Logged Events (Mod Log Channel)

Configured target:

- `MOD_LOG_CHANNEL_ID`

Event coverage includes:

- Ban/unban/kick/timeout actions
- Role add/remove and role object changes
- Message deletions (single and bulk)
- Invite creation events
- User profile changes (name/avatar)
- Join/leave and select channel/category changes

## Operational Variations

- Strict moderation profile:
  - Keep defaults, no public overrides.
  - Route all moderation actions to dedicated private log channel.
- Delegated moderation profile:
  - Use `custom_roles` for specific commands (for example role maintenance but no ban access).

## Tuning and Safety

- `KICK_PRUNE_HOURS` controls message prune window on kick.
- Keep `LOG_LEVEL` at `INFO` or `WARNING` in production unless active debugging is required.
- Keep `CONTAINER_LOG_LEVEL` at `ERROR` to reduce noisy `/logs` output.

## Troubleshooting

- Moderator can run command in one channel but not another:
  - Check Discord channel permission overrides.
- Mod logs missing:
  - Validate `MOD_LOG_CHANNEL_ID` exists and bot can send messages there.
- `/logs` empty during incident:
  - Lower `CONTAINER_LOG_LEVEL` temporarily to capture more detail.

## Related Pages

- [Command Reference](Command-Reference)
- [Environment Variables](Environment-Variables)
- [Security Hardening](Security-Hardening)

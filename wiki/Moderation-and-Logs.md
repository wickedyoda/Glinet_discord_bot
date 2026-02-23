# Moderation and Logs

Moderator-only actions with structured mod-log delivery.

## Commands

- Role management:
  - `/create_role`
  - `/edit_role`
  - `/delete_role`
  - `/add_role_member`, `!addrolemember`
  - `/remove_role_member`, `!removerolemember`
- Member moderation:
  - `/ban_member`, `!banmember`
  - `/unban_member`, `!unbanmember`
  - `/kick_member`, `!kickmember`
  - `/timeout_member`, `!timeoutmember`
  - `/untimeout_member`, `!untimeoutmember`
- Log channel test:
  - `/modlog_test`, `!modlogtest`

## Access Control

- Moderator access is role-ID based.
- Configured with:
  - `MODERATOR_ROLE_ID`
  - `ADMIN_ROLE_ID`
- Per-command overrides can be configured by admins in `/admin/command-permissions`.

## Logging

- All moderation actions are sent to `MOD_LOG_CHANNEL_ID`.
- Additional server-event logs include:
  - Message deletions (single and bulk)
  - Username/global name changes
  - Avatar changes
  - Join/leave
  - Invite creation
  - Channel/category create/delete
  - Role creation and role add/remove

## Env Variables

- `MODERATOR_ROLE_ID`
- `ADMIN_ROLE_ID`
- `MOD_LOG_CHANNEL_ID`
- `KICK_PRUNE_HOURS`

# Command Reference

## Role / Access

- `/submitrole`
- `/enter_role`
- `/getaccess`
- `/bulk_assign_role_csv` (moderator-only)

## Tag Commands

- Dynamic slash commands from persistent storage (SQLite-backed)
- `!list`

## Search Commands

- Combined: `/search`, `!search`
- Forum: `/search_forum`, `!searchforum`
- KVM: `/search_kvm`, `!searchkvm`
- IoT: `/search_iot`, `!searchiot`
- Router v4: `/search_router`, `!searchrouter`

## Country Commands

- `/country`
- `/clear_country`
- `!country`
- `!clearcountry`

## Moderation Commands

- `/create_role`
- `/edit_role`
- `/delete_role`
- `/ban_member`, `!banmember`
- `/unban_member`, `!unbanmember`
- `/kick_member`, `!kickmember`
- `/timeout_member`, `!timeoutmember`
- `/untimeout_member`, `!untimeoutmember`
- `/add_role_member`, `!addrolemember`
- `/remove_role_member`, `!removerolemember`
- `/modlog_test`, `!modlogtest`
- `/logs` (moderator-only, ephemeral)

## Access Rules

- Moderator/admin command access is controlled by env role IDs.
- Admins can override each command's access rule in `/admin/command-permissions`.
- Custom command restrictions support multi-role selection from Discord role dropdowns.
- Web-user creation is web-admin only (`/admin/users`); no Discord `/login` command exists.
- See [Environment Variables](Environment-Variables) and [Moderation and Logs](Moderation-and-Logs).

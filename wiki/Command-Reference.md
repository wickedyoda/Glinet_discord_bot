# Command Reference

## Role / Access

- `/submitrole`
- `/enter_role`
- `/getaccess`
- `/bulk_assign_role_csv` (moderator-only)

## Tag Commands

- Dynamic slash commands from `data/tag_responses.json`
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

## Access Rules

- Moderator/admin command access is controlled by env role IDs.
- See [Environment Variables](Environment-Variables) and [Moderation and Logs](Moderation-and-Logs).

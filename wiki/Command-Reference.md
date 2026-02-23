# Command Reference

This page lists every supported command style, access model, and common usage pattern.

## Access Model Summary

- Public/member commands: usable by normal guild members unless overridden in web admin command permissions.
- Moderator commands: restricted by moderator/admin role gates and command-permissions overrides.
- Web-admin-only actions: not exposed as Discord commands; managed only from web GUI by admin web users.

Default role gates are configured with:

- `MODERATOR_ROLE_ID`
- `ADMIN_ROLE_ID`

Per-command overrides are configured in:

- `/admin/command-permissions`

## Role Access and Invite Commands

| Command | Type | Default Access | Parameters | Notes |
|---|---|---|---|---|
| `/submitrole` | Slash | Member/Public | role code / mapped role flow | Generates invite + 6-digit access code for mapped role flow |
| `/enter_role` | Slash | Member/Public | `code` | Redeems a 6-digit code and assigns mapped role |
| `/getaccess` | Slash | Member/Public | none | Assigns default access role |

## Bulk CSV Role Assignment

| Command | Type | Default Access | Parameters | Notes |
|---|---|---|---|---|
| `/bulk_assign_role_csv` | Slash | Moderator | file + target role | Bulk role assignment from CSV; full result summary |

Web variation:

- `/admin/bulk-role-csv` provides a UI for upload + role select + report output.

## Tag and Auto-Reply Commands

| Command | Type | Default Access | Parameters | Notes |
|---|---|---|---|---|
| `!list` | Prefix | Member/Public | none | Lists configured tags |
| `!<tag>` | Prefix | Member/Public | tag key | Sends configured tag response |
| `/tagname` (dynamic) | Slash | Member/Public | none | Auto-generated for each stored tag |

## Search Commands

| Command | Type | Default Access | Parameters | Notes |
|---|---|---|---|---|
| `/search` | Slash | Member/Public | query text | Combined forum + docs results |
| `!search` | Prefix | Member/Public | query text | Prefix equivalent of combined search |
| `/search_forum` | Slash | Member/Public | query text | Forum-only results |
| `!searchforum` | Prefix | Member/Public | query text | Prefix forum-only search |
| `/search_kvm` | Slash | Member/Public | query text | KVM docs source |
| `!searchkvm` | Prefix | Member/Public | query text | Prefix KVM docs search |
| `/search_iot` | Slash | Member/Public | query text | IoT docs source |
| `!searchiot` | Prefix | Member/Public | query text | Prefix IoT docs search |
| `/search_router` | Slash | Member/Public | query text | Router docs v4 source |
| `!searchrouter` | Prefix | Member/Public | query text | Prefix router docs search |

## Country Nickname Commands

| Command | Type | Default Access | Parameters | Notes |
|---|---|---|---|---|
| `/country` | Slash | Member/Public | `code` (2 letters) | Applies or replaces nickname country suffix |
| `!country` | Prefix | Member/Public | code | Prefix equivalent |
| `/clear_country` | Slash | Member/Public | none | Removes country suffix |
| `!clearcountry` | Prefix | Member/Public | none | Prefix equivalent |

## Moderation and Role Management Commands

| Command | Type | Default Access | Parameters | Notes |
|---|---|---|---|---|
| `/create_role` | Slash | Moderator | role name + options | Creates role |
| `/edit_role` | Slash | Moderator | role + editable fields | Updates role properties |
| `/delete_role` | Slash | Moderator | role | Deletes role |
| `/add_role_member` | Slash | Moderator | member + role | Adds role to member |
| `!addrolemember` | Prefix | Moderator | member + role | Prefix equivalent |
| `/remove_role_member` | Slash | Moderator | member + role | Removes role from member |
| `!removerolemember` | Prefix | Moderator | member + role | Prefix equivalent |
| `/ban_member` | Slash | Moderator | member + optional reason | Bans member |
| `!banmember` | Prefix | Moderator | member + optional reason | Prefix equivalent |
| `/unban_member` | Slash | Moderator | user + optional reason | Unbans user |
| `!unbanmember` | Prefix | Moderator | user + optional reason | Prefix equivalent |
| `/kick_member` | Slash | Moderator | member + optional reason | Kicks member, uses prune setting |
| `!kickmember` | Prefix | Moderator | member + optional reason | Prefix equivalent |
| `/timeout_member` | Slash | Moderator | member + duration + reason | Applies timeout |
| `!timeoutmember` | Prefix | Moderator | member + duration + reason | Prefix equivalent |
| `/untimeout_member` | Slash | Moderator | member + optional reason | Removes timeout |
| `!untimeoutmember` | Prefix | Moderator | member + optional reason | Prefix equivalent |
| `/modlog_test` | Slash | Moderator | none | Sends test log to mod log channel |
| `!modlogtest` | Prefix | Moderator | none | Prefix equivalent |
| `/logs` | Slash | Moderator | optional line count | Returns recent container error lines (ephemeral) |

## Web-Admin-Only Actions (No Discord Command)

These are intentionally restricted to admin web users:

- Create/delete/promote/demote web users
- Reset web user passwords
- Update bot username and server nickname
- Upload bot avatar
- Change per-command role restrictions
- Change theme/session timeout/security-related web settings
- Edit tag JSON and apply runtime refresh

No `/login` or `!login` Discord command exists for creating web GUI users.

## Common Command Permission Variations

In `/admin/command-permissions`, each command can be set to:

- `default`: uses built-in command access (public vs moderator).
- `public`: opens command to all users.
- `custom_roles`: restricts command to one or more selected Discord roles.

Custom role restriction options:

- Multi-select dropdown with live guild role names.
- Manual role-ID input fallback for roles not returned in catalog.

## Troubleshooting

- Command missing from slash list:
  - Confirm bot startup completed command sync.
  - Confirm command is not disabled by Discord application command settings.
- Prefix command not responding:
  - Confirm message content intent and prefix handler configuration are active.
- Unexpected permission denial:
  - Check `/admin/command-permissions` override for that command.
  - Check `MODERATOR_ROLE_ID` and `ADMIN_ROLE_ID` values.

## Related Pages

- [Role Access and Invites](Role-Access-and-Invites)
- [Moderation and Logs](Moderation-and-Logs)
- [Web Admin Interface](Web-Admin-Interface)
- [Environment Variables](Environment-Variables)

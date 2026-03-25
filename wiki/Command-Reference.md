# Command Reference

This page lists every supported command style, access model, and common usage pattern.

## Access Model Summary

- Public/member commands: usable by normal guild members unless overridden in web admin command permissions.
- Moderator commands: restricted by moderator/admin role gates and command-permissions overrides.
- Web-admin-only actions: not exposed as Discord commands; managed only from web GUI by admin web users.
- Web GUI command overrides can also fully disable a command per guild.

Default role gates are configured with:

- `MODERATOR_ROLE_ID`
- `ADMIN_ROLE_ID`

Per-command overrides are configured in:

- `/admin/command-permissions`

## Role Access and Invite Commands

| Command | Type | Default Access | Parameters | Notes |
|---|---|---|---|---|
| `/submitrole` | Slash | Member/Public | `role` | Discord prompts for the target role, then the bot generates the invite + 6-digit access code |
| `/restore_code` | Slash | Moderator | `role`, `code`, optional `invite` | Restores a specific 6-digit code for a role and either reuses the supplied invite URL/code or generates a fresh invite |
| `/enter_role` | Slash | Member/Public | none | Opens a modal to redeem a 6-digit code and assigns mapped role |
| `/getaccess` | Slash | Member/Public | none | Assigns default access role |

Web variation:

- `/admin/role-access` shows stored invite/code mappings for the selected guild and allows pause, disable, activate, and manual restore/add actions.

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
| `/tag` | Slash | Member/Public | `tag` | Selects a stored tag response with autocomplete and posts it |

## Search Commands

| Command | Type | Default Access | Parameters | Notes |
|---|---|---|---|---|
| `/help` | Slash | Member/Public | optional `command` name | Shows overview help or command-specific help plus relevant wiki links |
| `/search_reddit` | Slash | Member/Public | query text | Top 5 matches from configured subreddit (default `r/GlInet`) |
| `!searchreddit` | Prefix | Member/Public | query text | Prefix Reddit search |
| `/search_forum` | Slash | Member/Public | query text | Forum-only results |
| `!searchforum` | Prefix | Member/Public | query text | Prefix forum-only search |
| `/search_openwrt_forum` | Slash | Member/Public | query text | Top 10 matches from [forum.openwrt.org](https://forum.openwrt.org/) |
| `!searchopenwrtforum` | Prefix | Member/Public | query text | Prefix OpenWrt forum search |
| `/search_kvm` | Slash | Member/Public | query text | KVM docs source |
| `!searchkvm` | Prefix | Member/Public | query text | Prefix KVM docs search |
| `/search_iot` | Slash | Member/Public | query text | IoT docs source |
| `!searchiot` | Prefix | Member/Public | query text | Prefix IoT docs search |
| `/search_router` | Slash | Member/Public | query text | Router docs v4 source |
| `!searchrouter` | Prefix | Member/Public | query text | Prefix router docs search |
| `/search_astrowarp` | Slash | Member/Public | query text | AstroWarp docs source (`docs.astrowarp.net`) |
| `!searchastrowarp` | Prefix | Member/Public | query text | Prefix AstroWarp docs search |

## Utility Commands

| Command | Type | Default Access | Parameters | Notes |
|---|---|---|---|---|
| `/ping` | Slash | Member/Public | none | Basic bot responsiveness check |
| `/sayhi` | Slash | Member/Public | none | Sends a short greeting and points users to `/help` |
| `/happy` | Slash | Member/Public | none | Sends a random puppy image when enabled |
| `/coin_flip` | Slash | Member/Public | none | Flips a coin |
| `/eight_ball` | Slash | Member/Public | question | Returns a magic 8-ball answer |
| `/meme` | Slash | Member/Public | none | Sends a random meme when the external API responds |
| `/dad_joke` | Slash | Member/Public | none | Sends a dad joke when the external API responds |
| `/shorten` | Slash | Member/Public | `url` | Creates a shortened URL using the configured shortener |
| `/expand` | Slash | Member/Public | shortened URL or code | Expands a shortened URL |
| `/uptime` | Slash | Member/Public | none | Reads the configured uptime/status summary when enabled |
| `/stats` | Slash | Member/Public | none | Sends your private member-activity summary for last 90 days, last 30 days, last 7 days, and last 24 hours |

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
| `/set_member_nickname` | Slash | Moderator | member + nickname + optional reason | Sets another member's server nickname |
| `/clear_member_nickname` | Slash | Moderator | member + optional reason | Clears another member's server nickname |
| `/voice_mute_member` | Slash | Moderator | member + `mute` bool + optional reason | Server-mutes or unmutes a member in voice |
| `/voice_deafen_member` | Slash | Moderator | member + `deafen` bool + optional reason | Server-deafens or undeafens a member in voice |
| `/voice_disconnect_member` | Slash | Moderator | member + optional reason | Disconnects a member from voice |
| `/voice_move_member` | Slash | Moderator | member + target voice channel + optional reason | Moves a member between voice channels |
| `/prune_messages` | Slash | Moderator | amount (1-500) | Removes recent messages in current channel (skips pinned) |
| `!prune` | Prefix | Moderator | amount (1-500) | Prefix channel prune |
| `/modlog_test` | Slash | Moderator | none | Sends test log to mod log channel |
| `!modlogtest` | Prefix | Moderator | none | Prefix equivalent |
| `/logs` | Slash | Moderator | optional line count | Returns recent container error lines (ephemeral) |
| `/random_choice` | Slash | Moderator | none | Randomly selects one eligible member, excluding moderator/admin role IDs and named staff roles (`Employee`, `Admin`, `Gl.iNet Moderator`); selected members are ineligible again for 7 days |

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
- `disabled`: turns the command off for that guild.
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

- [Role Access and Invites](Role-Access-and-Invites.md)
- [Moderation and Logs](Moderation-and-Logs.md)
- [Web Admin Interface](Web-Admin-Interface.md)
- [Environment Variables](Environment-Variables.md)

# Country Code Commands

Country commands append or remove a normalized country code suffix on member nicknames.

## Commands

| Action | Slash | Prefix | Access |
|---|---|---|---|
| Set country suffix | `/country` | `!country` | Member/Public |
| Clear country suffix | `/clear_country` | `!clearcountry` | Member/Public |

## Format Rules

- Input country code is normalized to uppercase two-letter format.
- Nickname suffix format: ` - CC`.
- Existing country-like suffixes are stripped before writing new suffix.

Examples:

- `Alex` + `us` -> `Alex - US`
- `Alex - CA` + `/country de` -> `Alex - DE`
- `/clear_country` -> `Alex`

## Permission and Hierarchy Requirements

Bot requires:

- `Manage Nicknames`
- Role hierarchy high enough to modify target member nickname

## Edge Cases

- Member has no nickname: command updates effective display nickname.
- Member already has same suffix: no-op behavior with confirmation.
- Unsupported code length/format: validation error returned.

## Troubleshooting

- Command says success but name did not change:
  - Check bot nickname permissions and role hierarchy.
- Command fails on specific members only:
  - Those members may have higher roles than bot.

## Related Pages

- [Command Reference](Command-Reference)
- [Moderation and Logs](Moderation-and-Logs)

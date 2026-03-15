# Tag Responses

Tag responses provide quick reusable replies via prefix and dynamic slash commands.

## Behavior

- Prefix pattern: `!<tag>` returns stored response text.
- Dynamic slash: each tag key is registered as a slash command.
- Discovery command: `!list` shows available tags.

## Data Model

- Tags are key/value entries persisted in SQLite.
- Keys become both lookup keys and slash command names.
- Values are plain response text.

## Key Naming Guidance

Recommended:

- Lowercase keys
- No spaces (use `_` if needed)
- Keep names short and descriptive

Avoid:

- Names colliding with built-in slash commands
- Very long keys that reduce usability

## Web Admin Management

Path:

- `/admin/tag-responses`

Capabilities:

- Edit JSON mapping directly
- Save and apply runtime reload
- Trigger dynamic command refresh without container restart

## Variation Examples

Example tags:

- `!betatest` -> beta access instructions
- `!support` -> support links and escalation steps
- `!warranty` -> warranty policy summary

Equivalent slash examples:

- `/betatest`
- `/support`
- `/warranty`

## Operational Limits

- Response content is still bounded by Discord message limits.
- Large tag sets may increase sync time for dynamic slash command refresh.
- Invalid JSON edits are rejected; fix syntax and re-save.

## Troubleshooting

- Tag not responding:
  - Confirm exact key spelling.
  - Confirm save operation succeeded in web UI.
- Slash command missing:
  - Wait for command sync cycle after tag update.
- JSON save fails:
  - Validate commas/quotes/braces in tag map.

## Related Pages

- [Command Reference](Command-Reference)
- [Web Admin Interface](Web-Admin-Interface)
- [Data Files](Data-Files)

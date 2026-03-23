# Tag Responses

Tag responses provide quick reusable replies via prefix commands and a single slash selector command.

## Behavior

- Prefix pattern: `!<tag>` returns stored response text.
- Slash selector: `/tag` lets the user choose a stored tag via autocomplete.
- Discovery command: `!list` shows available tags.

## Data Model

- Tags are key/value entries persisted in SQLite.
- Keys remain lookup keys used by both prefix and slash access.
- Values are plain response text.

## Key Naming Guidance

Recommended:

- Lowercase keys
- No spaces (use `_` if needed)
- Keep names short and descriptive

Avoid:

- Very long keys that reduce autocomplete usability

## Web Admin Management

Path:

- `/admin/tag-responses`

Capabilities:

- Edit JSON mapping directly
- Save and apply runtime reload
- Apply updated tag choices to `/tag` without container restart

## Variation Examples

Example tags:

- `!betatest` -> beta access instructions
- `!support` -> support links and escalation steps
- `!warranty` -> warranty policy summary

Equivalent slash examples:

- `/tag` -> `!betatest`
- `/tag` -> `!support`
- `/tag` -> `!warranty`

## Operational Limits

- Response content is still bounded by Discord message limits.
- Large tag sets are limited by Discord autocomplete result size.
- Invalid JSON edits are rejected; fix syntax and re-save.

## Troubleshooting

- Tag not responding:
  - Confirm exact key spelling.
  - Confirm save operation succeeded in web UI.
- Slash tag missing:
  - Open `/tag` and start typing the tag name to trigger autocomplete results.
- JSON save fails:
  - Validate commas/quotes/braces in tag map.

## Related Pages

- [Command Reference](Command-Reference.md)
- [Web Admin Interface](Web-Admin-Interface.md)
- [Data Files](Data-Files.md)

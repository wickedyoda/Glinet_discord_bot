# Tag Responses

Tag responses are key/value message snippets loaded from JSON.

## Behavior

- Message tags (example: `!betatest`) respond from `data/tag_responses.json`.
- Tag keys are also registered as dynamic slash commands.
- `!list` shows available tags.

## Web UI

- Page: `/admin/tag-responses`
- Admin edits JSON directly.
- On save, runtime reloads tag responses and schedules slash command refresh.
- Container restart is not required for tag-response edits.

## Data File

- `data/tag_responses.json`

## Related Pages

- [Web Admin Interface](Web-Admin-Interface)

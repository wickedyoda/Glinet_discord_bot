# Tag Responses

Tag responses are key/value message snippets loaded from persistent storage.

## Behavior

- Message tags (example: `!betatest`) respond from stored tag mappings.
- Tag keys are also registered as dynamic slash commands.
- `!list` shows available tags.

## Web UI

- Page: `/admin/tag-responses`
- Admin edits JSON mapping directly.
- On save, runtime reloads tag responses and schedules slash command refresh.
- Container restart is not required for tag-response edits.

## Related Pages

- [Web Admin Interface](Web-Admin-Interface)

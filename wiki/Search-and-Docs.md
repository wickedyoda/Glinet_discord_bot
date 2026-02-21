# Search and Docs

Search GL.iNet forum and documentation sources from Discord.

## Combined Search

- `/search`
- `!search`

## Source-Specific Search

- Forum: `/search_forum`, `!searchforum`
- KVM docs: `/search_kvm`, `!searchkvm`
- IoT docs: `/search_iot`, `!searchiot`
- Router docs v4: `/search_router`, `!searchrouter`

## Env Variables

- `FORUM_BASE_URL`
- `FORUM_MAX_RESULTS`
- `DOCS_MAX_RESULTS_PER_SITE`
- `DOCS_INDEX_TTL_SECONDS`
- `SEARCH_RESPONSE_MAX_CHARS`

## Notes

- Docs indexing is cached by TTL.
- Results are formatted and clipped for Discord message limits.

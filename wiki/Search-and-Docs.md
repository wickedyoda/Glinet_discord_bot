# Search and Docs

Search GL.iNet forum and documentation sources directly from Discord.

## Command Matrix

| Scope | Slash | Prefix | Notes |
|---|---|---|---|
| Combined sources | `/search` | `!search` | Forum + docs blend |
| Forum only | `/search_forum` | `!searchforum` | Uses forum base URL |
| KVM docs | `/search_kvm` | `!searchkvm` | KVM-specific docs index |
| IoT docs | `/search_iot` | `!searchiot` | IoT-specific docs index |
| Router docs v4 | `/search_router` | `!searchrouter` | Router docs source |

## Query Behavior

- Trims and normalizes user query text.
- Rejects empty or malformed search text.
- Formats compact result blocks for Discord readability.
- Clips output when approaching Discord content limits.

## Source Variations

- Combined search prioritizes mixed relevance across sources.
- Source-specific commands reduce noise for focused technical searches.
- Forum URL is configurable for alternate mirror/base paths.

## Caching and Performance

| Variable | Purpose | Behavior |
|---|---|---|
| `FORUM_BASE_URL` | Forum root URL | Source endpoint for forum queries |
| `FORUM_MAX_RESULTS` | Max forum hits | Higher value increases output volume |
| `DOCS_MAX_RESULTS_PER_SITE` | Max docs hits per site | Balances breadth and message size |
| `DOCS_INDEX_TTL_SECONDS` | Docs index cache lifetime | Higher TTL reduces fetch overhead |
| `SEARCH_RESPONSE_MAX_CHARS` | Response clipping threshold | Prevents oversize Discord messages |

## Tuning Guidance

- Increase `DOCS_INDEX_TTL_SECONDS` for lower bandwidth and faster repeated queries.
- Lower `FORUM_MAX_RESULTS` to keep concise responses in busy channels.
- Raise `SEARCH_RESPONSE_MAX_CHARS` only if your result formatting remains readable.

## Example Queries

- `/search gl-mt6000 wireguard`
- `/search_forum mwan issue`
- `!searchrouter dns over tls`
- `!searchkvm vlan trunk`

## Troubleshooting

- No results returned:
  - Verify source URLs are reachable from container network.
  - Try source-specific command to isolate problem source.
- Results truncated too aggressively:
  - Increase `SEARCH_RESPONSE_MAX_CHARS`.
- Slow first docs lookup:
  - Expected when index cache is cold; subsequent queries are faster until TTL expires.

## Related Pages

- [Environment Variables](Environment-Variables)
- [Command Reference](Command-Reference)

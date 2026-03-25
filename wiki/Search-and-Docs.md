# Search and Docs

Search GL.iNet forum and documentation sources directly from Discord.

## Command Matrix

| Scope | Slash | Prefix | Notes |
|---|---|---|---|
| Reddit only | `/search_reddit` | `!searchreddit` | Top 5 posts from configured subreddit (default `r/GlInet`) |
| Forum only | `/search_forum` | `!searchforum` | Uses forum base URL |
| OpenWrt forum only | `/search_openwrt_forum` | `!searchopenwrtforum` | Returns top 10 OpenWrt forum links |
| KVM docs | `/search_kvm` | `!searchkvm` | KVM-specific docs index |
| IoT docs | `/search_iot` | `!searchiot` | IoT-specific docs index |
| Router docs v4 | `/search_router` | `!searchrouter` | Router docs source |
| AstroWarp docs | `/search_astrowarp` | `!searchastrowarp` | AstroWarp how-to docs from `docs.astrowarp.net` |

## Query Behavior

- Trims and normalizes user query text.
- Rejects empty or malformed search text.
- Formats compact result blocks for Discord readability.
- Clips output when approaching Discord content limits.

## Source Variations

- Source-specific commands reduce noise for focused technical searches.
- GL.iNet forum search uses the configurable forum base URL.
- OpenWrt forum search is fixed to [forum.openwrt.org](https://forum.openwrt.org/).
- AstroWarp docs search is backed by the public search index exposed by [docs.astrowarp.net](https://docs.astrowarp.net/en/).

## Caching and Performance

| Variable | Purpose | Behavior |
|---|---|---|
| `FORUM_BASE_URL` | Forum root URL | Source endpoint for forum queries |
| `FORUM_MAX_RESULTS` | Max forum hits | Higher value increases output volume |
| `REDDIT_SUBREDDIT` | Reddit source scope | Restricts Reddit search to a single subreddit |
| `DOCS_MAX_RESULTS_PER_SITE` | Max docs hits per site | Balances breadth and message size |
| `DOCS_INDEX_TTL_SECONDS` | Docs index cache lifetime | Higher TTL reduces fetch overhead |
| `SEARCH_RESPONSE_MAX_CHARS` | Response clipping threshold | Prevents oversize Discord messages |

## Tuning Guidance

- Increase `DOCS_INDEX_TTL_SECONDS` for lower bandwidth and faster repeated queries.
- Lower `FORUM_MAX_RESULTS` to keep concise responses in busy channels.
- Raise `SEARCH_RESPONSE_MAX_CHARS` only if your result formatting remains readable.

## Example Queries

- `/search_reddit wireguard setup`
- `/search_forum mwan issue`
- `/search_openwrt_forum mwan3 policy routing`
- `!searchrouter dns over tls`
- `!searchkvm vlan trunk`
- `/search_astrowarp remote desktop`

## Troubleshooting

- No results returned:
  - Verify source URLs are reachable from container network.
  - Try source-specific command to isolate problem source.
- Results truncated too aggressively:
  - Increase `SEARCH_RESPONSE_MAX_CHARS`.
- Slow first docs lookup:
  - Expected when index cache is cold; subsequent queries are faster until TTL expires.

## Related Pages

- [Environment Variables](Environment-Variables.md)
- [Command Reference](Command-Reference.md)

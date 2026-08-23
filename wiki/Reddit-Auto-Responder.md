# Reddit Auto-Responder

The Reddit auto-responder feature automatically posts comments on Reddit submissions that match configurable keyword patterns. This is a **web-GUI-only** feature — all management is done through the Web Admin interface under `/admin/reddit-auto-responds`. Discord slash commands are not used for this feature.

## Prerequisites

### Reddit OAuth2 App

Create a "Script" application at https://www.reddit.com/prefs/apps:

1. Navigate to https://www.reddit.com/prefs/apps
2. Click "Create App" or "Create Another App"
3. Select "Script" as the app type
4. Note the generated **Client ID** (the app name text at the top of the page) and **Client Secret**
5. The bot account must be the same account that created the app

### Environment Variables

Set the following variables in `.env` or your container environment:

| Variable | Required | Description |
|---|---|---|
| `REDDIT_AUTO_REPLY_ENABLED` | Yes | Set to `true` to enable the auto-responder |
| `REDDIT_CLIENT_ID` | Yes (if enabled) | Reddit OAuth2 app client ID |
| `REDDIT_CLIENT_SECRET` | Yes (if enabled) | Reddit OAuth2 app client secret |
| `REDDIT_USERNAME` | Yes (if enabled) | Reddit account username for the bot account |
| `REDDIT_PASSWORD` | Yes (if enabled) | Reddit account password for the bot account |

**Security note**: `REDDIT_CLIENT_SECRET` and `REDDIT_PASSWORD` are in the `SENSITIVE_KEYS` set — they are masked in the web GUI and never logged in plaintext.

## How It Works

1. **Polling**: On each Reddit feed polling cycle (controlled by `REDDIT_FEED_CHECK_SCHEDULE`), the bot checks all enabled auto-respond rules
2. **New post detection**: For each rule, the bot fetches new posts from the subreddit's `/new` endpoint and filters out posts already seen (tracked in `reddit_auto_respond_seen_replies` table)
3. **Keyword matching**: The bot matches the rule's regex pattern against post titles (case-insensitive)
4. **Comment posting**: When a match is found, the bot posts a comment using the response template via the Reddit `/api/comment` endpoint
5. **Deduplication**: Posted post IDs are recorded to prevent duplicate comments
6. **Status updates**: The rule's runtime status (last matched post, last reply timestamp, last error) is updated in a single batched DB write per rule run

## Managing Rules via Web Admin

Navigate to `/admin/reddit-auto-responds` (requires login + admin):

### Add a Rule
- **Subreddit**: The subreddit name (without `r/` prefix)
- **Keyword Pattern (regex)**: A Python regex pattern matched against post titles (case-insensitive)
- **Response Template**: A text template with variable substitution. Available variables:
  - `{title}` — the post title
  - `{author}` — the post author
  - `{subreddit}` — the subreddit name
  - `{post_id}` — the Reddit post ID
  - `{post_link}` — the Reddit post URL

### Edit a Rule
Click the **Edit** button on any rule row. The edit form pre-populates with the current values.

### Toggle Enable/Disable
Use the **Enable**/**Disable** button to toggle a rule without deleting it.

### Delete a Rule
Click **Delete** and confirm. This action is irreversible.

### Status Bar
At the top of the page, a status bar indicates:
- Whether Reddit OAuth credentials are configured
- Whether `REDDIT_AUTO_REPLY_ENABLED` is set to `true`

## Rate Limiting and Error Handling

- **HTTP 429 (Rate Limited)**: The bot backs off for `REDDIT_AUTO_REPLY_RATE_LIMIT_BACKOFF_SECONDS` (default: 30s) and stops processing the current rule
- **HTTP 401 (Auth Error)**: The rule is automatically disabled to prevent repeated failures
- **Other API errors**: Logged and recorded in the rule's `last_error` field
- **Max posts per run**: Limited to `REDDIT_AUTO_REPLY_MAX_REPLIES_PER_RUN` (default: 5) per rule per cycle

## Configuration Constants

| Constant | Default | Description |
|---|---|---|
| `REDDIT_AUTO_REPLY_MAX_REPLIES_PER_RUN` | 5 | Maximum comments posted per rule per cycle |
| `REDDIT_AUTO_REPLY_RATE_LIMIT_BACKOFF_SECONDS` | 30 | Backoff duration on rate limit |
| `REDDIT_AUTO_REPLY_SEEN_REPLY_RETENTION_LIMIT` | 500 | Max seen-post IDs retained per rule |
| `REDDIT_API_TIMEOUT_SECONDS` | 15 | HTTP timeout for Reddit API calls |
| `REDDIT_TOKEN_TTL_SECONDS` | 3300 (55 min) | OAuth2 token cache TTL |

## Database Schema

### `reddit_auto_respond_rules`
| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Primary key |
| `guild_id` | INTEGER | Discord guild ID |
| `subreddit` | TEXT | Subreddit name (normalized) |
| `keyword_pattern` | TEXT | Regex pattern (case-insensitive) |
| `response_template` | TEXT | Template with variable substitution |
| `enabled` | INTEGER (boolean) | Whether the rule is active |
| `created_at` | TEXT | ISO timestamp |
| `updated_at` | TEXT | ISO timestamp |
| `created_by_email` | TEXT | Web admin email |
| `updated_by_email` | TEXT | Web admin email |
| `last_matched_post_id` | TEXT | Last matched Reddit post ID |
| `last_reply_posted_at` | TEXT | ISO timestamp of last comment posted |
| `last_error` | TEXT | Last error message |

### `reddit_auto_respond_seen_replies`
| Column | Type | Description |
|---|---|---|
| `rule_id` | INTEGER FK | References `reddit_auto_respond_rules.id` |
| `post_id` | TEXT | Reddit post ID |
| `created_at` | TEXT | ISO timestamp when seen |

**Unique constraint**: `(rule_id, post_id)` — prevents duplicate seen entries per rule

## Related

- [Reddit Feeds](Reddit-Feeds.md) — subreddit post monitoring and forwarding to Discord
- [Environment Variables](Environment-Variables.md)
- [Web Admin Interface](Web-Admin-Interface.md)

# Discourse Forum Integration

The bot integrates with GL.iNet Discourse forums (e.g., `https://forum.gl-inet.com/`) for forum search, category browsing, topic lookups, and announcement/webhook posting.

Integration is configurable **per guild** — each Discord server can independently enable or disable Discourse features.

## Web Admin Pages

### `/admin/discourse` (Admin-only)

Full edit page for Discourse integration settings. Admins configure:

- **Integration State** — `Enabled` or `Disabled` (per-guild; no global default fallback)
- **Forum Base URL** — e.g., `https://forum.gl-inet.com`
- **API Key** — optional Discourse API key for authenticated requests (stored encrypted)
- **API Username** — optional Discourse username for authenticated requests
- **Profile Name** — displayed in Discourse topic/post signatures
- **Request Timeout** — dropdown: 5, 10, 15, 20, 30, 45, 60 seconds
- **Enabled Features** — multi-select:
  - Forum Search
  - Topic Lookups
  - Category Browsing
  - Create Topics
  - Reply To Topics

### `/admin/discourse/viewer` (Read-only for all users)

Read-only forum viewer showing live data from the configured Discourse instance.

**Sections:**
1. **Integration Settings** — displays the current effective configuration (base URL, API username status, profile name, timeout, integration state, enabled features)
2. **Bot Commands** — lists the Discord slash commands available (`/search_forum`, `/forum_categories`, `/view_topic`)
3. **Categories** — live list of forum categories fetched from Discourse
4. **Recent Topics** — live list of recent forum topics
5. **Search** — search box that queries the Discourse forum

> **Access control:** Read-only for all authenticated web users. Only admins can edit settings via `/admin/discourse`.

### `/admin/discourse/viewer/api/categories`

AJAX endpoint returning categories from the configured Discourse instance.

### `/admin/discourse/viewer/api/topics`

AJAX endpoint returning recent topics from the configured Discourse instance.

### `/admin/discourse/viewer/search`

Search page — enter a query to search the Discourse forum. Returns matching topics with titles, excerpts, and direct links.

## Discord Commands

| Command | Description |
|---|---|
| `/search_forum` | Search the Discourse forum |
| `/forum_categories` | Browse forum categories |
| `/view_topic` | View a specific topic |

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `FORUM_BASE_URL` | (empty) | Discourse base URL |
| `FORUM_API_KEY` | (empty) | Optional Discourse API key |
| `FORUM_API_USERNAME` | (empty) | Optional Discourse API username |
| `FORUM_MAX_RESULTS` | 10 | Max results per search |
| `FORUM_REQUEST_TIMEOUT_SECONDS` | 15 | HTTP request timeout in seconds |

## Per-Guild Configuration

Discourse integration settings are stored per-guild in the `guild_settings` database table. Each guild can:

- Enable or disable Discourse integration independently
- Configure its own forum base URL and API credentials
- Select which features are enabled (search, topics, categories, create, reply)
- Set its own request timeout

Guild-level settings override global environment variable defaults. See `/admin/guild-settings` for the per-guild override UI.

## API Credentials

Set `FORUM_API_KEY` and `FORUM_API_USERNAME` to enable authenticated Discourse requests. Authenticated requests:

- Bypass anonymous rate limits
- Enable create-topic and reply features
- Return more reliable structured responses

Without API credentials, the bot uses anonymous search, which has stricter rate limits and may miss content from private categories.

## Troubleshooting

- **No categories/topics shown:** Verify the forum base URL is correct and the Discourse instance is publicly accessible
- **Search returns no results:** Check that API credentials are set if the forum requires authentication
- **Request timeout errors:** Increase `FORUM_REQUEST_TIMEOUT_SECONDS` or verify network connectivity to the Discourse instance

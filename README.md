
# 🔐 Discord Invite Role Bot

This bot allows select Discord users to create **role-bound invite links** and **6-digit access codes** that assign roles automatically to new or existing users. The system supports slash commands and permission control via role checks.

---

## 💡 Features

- Slash command `/submitrole`:
  - Asks the user for a role.
  - Generates a 6-digit code (with no more than 2 consecutive digits).
  - Creates a permanent invite link.
  - Assigns the submitted role to:
    - Users joining through the invite.
    - Existing users who submit the generated code via `/enter_role`.

- Slash command `/enter_role`:
  - Prompts the user for a 6-digit code and assigns the matching role.

- `/submitrole`: Submit a role, get an invite and access code.
- `/enter_role`: Enter the 6-digit code to receive the corresponding role.
- `/getaccess`: Public command to assign a preconfigured access role.
- Only users with the **Employee** role can run role-submitting commands.
- **Admins** and **Gl.iNet Moderators** get full access to manage all bot commands.
- Invite-based role assignment works for new members who join via generated links.
- Tag-based auto-replies for messages like `!betatest`, configurable via `data/tag_responses.json`.
- Tag-based auto-replies are also available as slash commands (e.g., `/betatest`).
- `!list` message to display available tag commands.
- Search commands:
  - Combined: `/search`, `!search`
  - Forum only: `/search_forum`, `!searchforum`
  - KVM docs only: `/search_kvm`, `!searchkvm`
  - IoT docs only: `/search_iot`, `!searchiot`
  - Router v4 docs only: `/search_router`, `!searchrouter`
- Country nickname commands:
  - Set: `/country`, `!country` (example: `US`)
  - Clear: `/clear_country`, `!clearcountry`

---

## 🧠 How It Works

### ➤ 1. Role Submission Flow

An authorized user types `/submitrole`.  
The bot will:
- Ask the user to mention a role.
- Generate:
  - A **permanent invite link**
  - A **6-digit access code** (no more than 2 identical digits in a row)
- Log the invite/code → role association.
- Return the invite and code to the user privately.

### ➤ 2. Role Access by Code

Any member can type `/enter_role`. The bot will prompt them privately for the
6-digit code. After they submit it, the bot assigns the associated role if the
code is valid.

### ➤ 3. Automatic Role Assignment via Invite

If someone joins the server via a generated invite, the bot will automatically assign the associated role.

### ➤ 4. Manual Role Access

All users can run `/getaccess` to receive the default access role defined in `access_role.txt`.

### ➤ 5. Tag-Based Auto Replies

You can configure short tag responses (like `!betatest`) in `data/tag_responses.json`. When a user sends a message that starts with a configured tag, the bot replies with the preset response.
Each tag is also registered as a slash command (for example, `!betatest` becomes `/betatest`) on startup.

Example file contents:
```json
{
  "!betatest": "✅ Thanks for your interest in the beta! We'll share more details soon.",
  "!support": "🛠️ Need help? Please open a ticket or message a moderator."
}
```

To add more later, edit the JSON file and add new keys for each tag. Changes are picked up automatically without restarting the bot.

### ➤ 6. Command Listing

Send `!list` in a channel to get a list of configured tag commands from `data/tag_responses.json`.

### ➤ 7. Forum Search

Use these commands:
- `/search` or `!search` for all sources combined.
- `/search_forum` or `!searchforum` for forum-only search.
- `/search_kvm` or `!searchkvm` for KVM docs only.
- `/search_iot` or `!searchiot` for IoT docs only.
- `/search_router` or `!searchrouter` for Router v4 docs only.

Search sources:
- [forum.gl-inet.com](https://forum.gl-inet.com/)
- [docs.gl-inet.com/kvm/en](https://docs.gl-inet.com/kvm/en/)
- [docs.gl-inet.com/iot/en](https://docs.gl-inet.com/iot/en/)
- [docs.gl-inet.com/router/en/4](https://docs.gl-inet.com/router/en/4/)

The bot returns top matching links for the selected source(s).

### ➤ 8. Country of Origin Nickname

Users can add a country flag suffix to their nickname with:
- `/country US`
- `!country US`

This updates their server nickname to end with `-🇺🇸` (or another flag from a 2-letter code).
To remove it, use:
- `/clear_country`
- `!clearcountry`

---

## 🛠 Project Structure

```
.
├── bot.py               # Main bot script
├── Dockerfile           # Image build setup
├── docker-compose.yml   # Deployment stack
├── .gitignore           # Ignore secrets and DS_Store
├── access_role.txt      # (Generated) Default role ID for /getaccess
├── role_codes.txt       # (Generated) Stores role-code pairs
├── permanent_invite.txt # (Generated) Optional saved invite link
├── data
│   └── tag_responses.json # (Config) Tag-based auto reply mapping
└── README.md            # You're reading it!
```

---

## 🚀 Running the Bot via Docker Compose

**Docker Compose (Pulls from GitHub Container Registry):**
```yaml
version: "3.9"

services:
  discord_role_bot:
    image: ghcr.io/wickedyoda/discord_invite_bot:latest
    container_name: discord_role_bot
    restart: unless-stopped
    environment:
      - DISCORD_TOKEN=your_discord_token
      - GUILD_ID=your_guild_id
      - GENERAL_CHANNEL_ID=general_channel_id
      - LOG_LEVEL=INFO
      - FORUM_BASE_URL=https://forum.gl-inet.com
      - FORUM_MAX_RESULTS=5
      - DOCS_MAX_RESULTS_PER_SITE=2
      - DOCS_INDEX_TTL_SECONDS=3600
      - SEARCH_RESPONSE_MAX_CHARS=1900
    volumes:
      - ./data:/app/data
```

> 📁 Create a `data/` folder to persist files like `access_role.txt`, `role_codes.txt`, `invite_roles.json`, and `tag_responses.json`.
> A log file `bot.log` will also be written to this folder.

To start:
```bash
docker compose pull
docker compose up -d
```

To rebuild:
```bash
docker compose build --no-cache
docker compose up -d
```

---

## 🔐 Role Permissions

| Command        | Who Can Use                     |
|----------------|----------------------------------|
| `/submitrole`  | Employee, Admins, Gl.iNet Mods  |
| `/generateinvite` | Admins and Gl.iNet Mods     |
| `/getaccess`   | Any member                      |
| `/enter_role`  | Any member                      |
| `/search`      | Any member                      |
| `/search_forum`| Any member                      |
| `/search_kvm`  | Any member                      |
| `/search_iot`  | Any member                      |
| `/search_router`| Any member                     |
| `/country`     | Any member                      |
| `/clear_country`| Any member                     |

---

## 🌐 Discord OAuth2 Invite Link

Use this link to add the bot to your server:

[👉 Add to Server](https://discord.com/oauth2/authorize?client_id=1390519966050291734&integration_type=0&scope=applications.commands+bot)

Make sure to:
- Enable **Message Content** and **Server Members Intent**.
- Grant it permission to **manage roles**, **create invites**,
  and **manage server** so invite tracking works.

---

## 🔒 Security Notes

- The `.env` file containing `DISCORD_TOKEN` should **never be committed**.
- It is excluded via `.gitignore`.
- Use secrets when deploying via GitHub Actions.

---

## 🧾 Change History

See [`CHANGELOG.md`](./CHANGELOG.md) for a history of updates and improvements.

---

## 🧑‍💻 Maintainers

Created and maintained by [WickedYoda](https://wickedyoda.com)

For advanced support, join WickedYoda's Discord https://discord.gg/m6UjX6UhKe
- Slash command `/getaccess`:
  - Assigns a single configured role to the user.
  - Usable by all members.

## 🛡️ Role Permissions

- Only members with the `Employee` role can use `/submitrole`.
- All members can use `/getaccess`, `/enter_role`, `/search`, `/search_forum`, `/search_kvm`, `/search_iot`, `/search_router`, `/country`, and `/clear_country`.
- You can expand access to more roles like `Admin` or `GL.iNet Moderators` by adjusting role checks in `bot.py`.

---

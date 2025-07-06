# 🔐 Discord Invite Role Bot

This bot allows select Discord users to create **role-bound invite links** and **6-digit access codes** that assign roles automatically to new or existing users. The system supports slash commands and permission control via role checks.

---

## 📦 Features

- `/submitrole`: Submit a role, get an invite and access code.
- `/enter_role <code>`: Enter a 6-digit code to receive the corresponding role.
- `/getaccess`: Public command to assign a preconfigured access role.
- Only users with the **Employee** role can run role-submitting commands.
- **Admins** and **Gl.iNet Moderators** get full access to manage all bot commands.
- Invite-based role assignment works for new members who join via generated links.

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

Any member can type:
```
/enter_role <6-digit-code>
```
The bot checks if the code is valid and assigns the associated role.

### ➤ 3. Automatic Role Assignment via Invite

If someone joins the server via a generated invite, the bot will automatically assign the associated role.

### ➤ 4. Manual Role Access

All users can run `/getaccess` to receive the default access role defined in `access_role.txt`.

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
    volumes:
      - ./data:/app
```

> 📁 Create a `data/` folder to persist files like `access_role.txt` and `role_codes.txt`.

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

---

## 🌐 Discord OAuth2 Invite Link

Use this link to add the bot to your server:

[👉 Add to Server](https://discord.com/oauth2/authorize?client_id=1390519966050291734&integration_type=0&scope=applications.commands+bot)

Make sure to:
- Enable **Message Content** and **Server Members Intent**.
- Grant it permission to **manage roles** and **create invites**.

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

For advanced support, join the [GL.iNet Discord](https://discord.gg/5tH6dGZQEp)
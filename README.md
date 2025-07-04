# 🔐 Discord Role Assignment Bot

This is a lightweight Discord bot built with `discord.py` that:

- ✅ Automatically assigns a role to **new members** when they join the server.
- ✅ Allows **existing members** to request access with `!grantaccess`.
- ✅ Runs in a Docker container.
- ✅ Uses `.env` for configuration.

---

## 📦 Features

- **Auto-role for new users** via `on_member_join`
- **Manual command** for current members (`!grantaccess`)
- **Secure and portable** deployment with Docker

---

## 📁 Setup Instructions

### 1. Clone and configure

```bash
git clone https://github.com/youruser/discord-role-bot.git
cd discord-role-bot
---

## 🧪 Deployment Instructions

### 1. Setup

Create a `.env` file in the root directory:

```env
DISCORD_TOKEN=your_discord_token_here
GUILD_ID=your_discord_guild_id
```

### 2. Build and Run with Docker

```bash
docker build -t discord-role-bot .
docker run --env-file .env -v $(pwd)/data:/app --name rolebot discord-role-bot
```

### 3. Or Use Docker Compose

```bash
docker compose up -d
```

### 4. Usage

- `!setaccessrole @RoleName` — Sets the role to assign
- `!generateinvite` — Generates a permanent invite link

Users (new or existing) who use the invite will receive the set role.

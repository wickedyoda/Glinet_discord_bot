# 🔗 Discord Invite Role Bot

## 🎯 Overview
This is a Discord bot that allows server admins to generate invite links that automatically assign a role to users when they join the server. The bot is designed to work both for new members and existing members who need access to protected channels.

## 💡 Key Features
- Slash command `/setrole` to assign the role to use.
- Slash command `/createinvite` to generate a permanent invite link.
- Slash command `/getaccess` for existing users to claim the access role.
- Automatically assigns role to users who join using the permanent invite.
- Persistent storage of the selected role and invite link.

## 🧱 Project Structure
```
.
├── bot.py                  # Main bot logic
├── Dockerfile              # Docker image setup
├── docker-compose.yml      # Container orchestration
├── requirements.txt        # Python dependencies
├── .env                    # Token and guild config
├── access_role.txt         # Saved role ID (auto-generated)
└── permanent_invite.txt    # Saved invite code (auto-generated)
```

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

## 🛠️ Slash Commands
| Command        | Description                                     |
|----------------|-------------------------------------------------|
| `/setrole`     | Admins use to define the role to assign         |
| `/createinvite`| Generates a no-expire invite for access         |
| `/getaccess`   | Assigns role to user if they already joined     |

## 🐳 Running via Docker
### 1. Create a `.env` file:
```env
DISCORD_TOKEN=your_bot_token
GUILD_ID=your_server_id
```

### 2. Build and run:
```bash
docker compose build
docker compose up
```

## 🔐 Required Bot Permissions
Make sure your bot has the following permissions in your Discord server:
- Manage Roles
- Manage Channels
- Create Invite
- Read Messages / View Channels
- Send Messages
- Use Slash Commands

## ⚙️ Intents Configuration
In the [Discord Developer Portal](https://discord.com/developers/applications/), enable:
- **Server Members Intent**
- **Message Content Intent** (optional warning may appear)

## 🔗 Bot Invite URL
Use this URL to add the bot to your server:
```
https://discord.com/oauth2/authorize?client_id=1390519966050291734&permissions=268512257&integration_type=0&scope=bot+applications.commands
```

## 🧪 Testing Behavior
- New members who join using the invite get the role automatically.
- Existing members use `/getaccess` to get the role.

---

=======
Built with ❤️ for Discord role automation and access control.

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


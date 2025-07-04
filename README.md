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
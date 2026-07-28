# Setup Guide

This guide covers the initial setup workflow from fresh clone to first admin actions.

## Step 1 — Create the bot application

1. Open the Discord Developer Portal
2. Create a new application
3. Add a bot user
4. Enable required intents, including the members intent if you use member activity and moderation features
5. Copy the bot token
6. Copy the application/client ID for the OAuth2 invite URL

## Step 2 — Invite the bot

Use an invite URL with the required scopes and permissions for your deployment.

## Step 3 — Configure `.env`

After copying `.env.example` to `.env`, set at least:

- `DISCORD_TOKEN`
- `GUILD_ID`
- `MANAGED_GUILD_IDS` when using multi-guild mode
- `WEB_ADMIN_DEFAULT_PASSWORD` for initial web admin bootstrap

Start with the smallest set of toggles enabled and expand once the core behavior is confirmed.

## Step 4 — Run checks locally

```bash
python3 -m ruff check --select E9,F63,F82 .
python3 -m py_compile bot.py web_admin.py
python3 -m pytest -q
```

## Step 5 — First web admin login

1. Open `http://localhost:8080` or `https://localhost:8081`
2. Sign in with the default admin email/password
3. Choose the target guild from the server selector
4. Confirm command permissions match your intended access model

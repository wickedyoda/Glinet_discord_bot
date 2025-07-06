# Discord Invite Role Bot

This bot manages invite-based and code-based role assignments within a Discord server. Designed to run in Docker and pull its image from GitHub Container Registry.

## 💡 Features

- Slash command `/submitrole`:
  - Asks the user for a role.
  - Generates a 6-digit code (with no more than 2 consecutive digits).
  - Creates a permanent invite link.
  - Assigns the submitted role to:
    - Users joining through the invite.
    - Existing users who submit the generated code via `/enter_role`.

- Slash command `/enter_role <code>`:
  - Allows members to claim a role by entering the 6-digit code.

- Slash command `/getaccess`:
  - Assigns a single configured role to the user.
  - Usable by all members.

## 🛡️ Role Permissions

- Only members with the `Employee` role can use `/submitrole`.
- All members can use `/getaccess` and `/enter_role`.
- You can expand access to more roles like `Admin` or `GL.iNet Moderators` by adjusting role checks in `bot.py`.

---

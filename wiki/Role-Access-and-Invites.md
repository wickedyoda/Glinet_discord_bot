# Role Access and Invites

This feature set handles role mapping using invite links and 6-digit codes.

## Commands

- `/submitrole`
- `/enter_role`
- `/getaccess`

## Behavior

- `/submitrole` generates:
  - A permanent invite link
  - A 6-digit code
- Joining via that invite or entering the code with `/enter_role` assigns the mapped role.
- `/getaccess` assigns a default access role.

## Data Used

- `data/role_codes.txt`
- `data/invite_roles.json`
- `data/access_role.txt`

## Required Permissions

- Create Instant Invite
- Manage Roles

## Related Pages

- [Bulk CSV Role Assignment](Bulk-CSV-Role-Assignment)
- [Moderation and Logs](Moderation-and-Logs)

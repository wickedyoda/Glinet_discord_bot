# Bulk CSV Role Assignment

Bulk-assign an existing role to many members from uploaded CSV input.

## Command and Web Path

| Interface | Access | Path/Command |
|---|---|---|
| Discord slash | Moderator | `/bulk_assign_role_csv` |
| Web GUI | Admin web users | `/admin/bulk-role-csv` |

## Input Formats

Accepted input variations:

- Comma-separated names in one or more rows
- One name per line
- Mixed whitespace and separators

Normalized matching attempts:

- Display name
- Username
- Member name variants normalized for case/spacing

## Role Selection Variations

- Preferred: select role from live Discord role dropdown.
- Fallback: manual role ID entry when role catalog is unavailable.

## Output Report Fields

- `assigned`: members successfully given role
- `already_had_role`: members that already had target role
- `unmatched`: names not resolved to a guild member
- `ambiguous`: names mapping to multiple candidates
- `failures`: permission/API/hierarchy errors

Web UI includes full text report and summary counts.

## Limits and Tuning

| Variable | Purpose | Typical Tuning |
|---|---|---|
| `CSV_ROLE_ASSIGN_MAX_NAMES` | Max unique names per request | Raise for bigger batches |
| `WEB_BULK_ASSIGN_TIMEOUT_SECONDS` | Execution timeout window | Increase for large guilds |
| `WEB_BULK_ASSIGN_MAX_UPLOAD_BYTES` | Upload size cap | Raise only as needed |
| `WEB_BULK_ASSIGN_REPORT_LIST_LIMIT` | Per-section result display cap | Raise for deeper diagnostics |

## Large Guild Guidance

For ~4000-member guilds:

- Use smaller batch files first to verify matching quality.
- Keep timeout high enough for worst-case API latency.
- Prefer exact display names for lower ambiguity.
- Run off-peak when making very large role changes.

## Failure Modes

- Role hierarchy mismatch: bot role must be above target role.
- Missing permission: `Manage Roles` required.
- Member not found: unresolved name appears in `unmatched`.
- Duplicate or ambiguous names: entries reported under `ambiguous`.

## Related Pages

- [Moderation and Logs](Moderation-and-Logs)
- [Web Admin Interface](Web-Admin-Interface)
- [Environment Variables](Environment-Variables)

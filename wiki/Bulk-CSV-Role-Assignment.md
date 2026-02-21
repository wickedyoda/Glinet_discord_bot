# Bulk CSV Role Assignment

Bulk-assign an existing role to many members using uploaded CSV input.

## Command

- `/bulk_assign_role_csv` (moderator-only)

## Web UI

- Page: `/admin/bulk-role-csv`
- Supports CSV upload and role selection from Discord role dropdown.
- Shows summary, missing members, ambiguous members, and assignment failures.

## Input Rules

- CSV may be comma-separated or one name per line.
- Name matching uses normalized Discord display/member names.
- Role can be selected from current guild roles (dropdown), with manual fallback when catalog is unavailable.

## Output

- Assigned count
- Already-had-role count
- Unmatched names
- Ambiguous names
- Assignment failures
- Full downloadable-style report text in UI/response

## Env Variables

- `CSV_ROLE_ASSIGN_MAX_NAMES`
- `WEB_BULK_ASSIGN_TIMEOUT_SECONDS`
- `WEB_BULK_ASSIGN_MAX_UPLOAD_BYTES`
- `WEB_BULK_ASSIGN_REPORT_LIST_LIMIT`

## Related Pages

- [Web Admin Interface](Web-Admin-Interface)
- [Environment Variables](Environment-Variables)

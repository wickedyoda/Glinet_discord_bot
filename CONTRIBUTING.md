# Contributing

This project requires complete, explicit change descriptions in both commits and pull requests.

## Commit Message Standard

Every commit message must include:

- A concise subject line
- Why the change was needed
- What changed (concrete details)
- Impact (behavior, security, performance, data)
- Validation performed (tests/checks/manual)

Recommended setup:

```bash
git config commit.template .gitmessage
```

Template file:

- `.gitmessage`

## Pull Request Standard

All pull requests must include:

- What changed
- Why this change is needed
- Behavior impact
- Security impact
- Testing performed
- Deployment/migration notes

Template file:

- `.github/pull_request_template.md`

## Review Expectation

PRs missing complete change descriptions should be updated before merge.


# Git hooks

## Install

```bash
./scripts/git-hooks/install.sh
```

Or manually:

```bash
cp scripts/git-hooks/pre-commit .git/hooks/pre-commit
chmod +x .git/hooks/pre-commit
```

## Why

A real `DISCORD_TOKEN` was committed to `.env` on 2025-07-05 (commits
`8cb2b77d21`, `f8b4b74e37`) and remained fetchable from GitHub by commit SHA
for roughly 14 months. The token has since been rotated, so it is no longer
valid, but the commit history was not rewritten.

Two gaps allowed it:

1. The gitleaks CI job only runs *after* a commit exists.
2. The `Main Protection` ruleset (id 22146433) has an **empty**
   `required_status_checks` list, so no CI job can actually block a merge.

This `pre-commit` hook closes gap 1 at the point of commit. It blocks
credential filenames (`.env`, `*.pem`, `id_rsa`, ...) and scans staged lines
for high-confidence secret assignments, while allowing the obvious
placeholders in `.env.example` (`your_...`, `replace_with_...`, `changeme`).

Bypass for a known false positive:

```bash
git commit --no-verify
```

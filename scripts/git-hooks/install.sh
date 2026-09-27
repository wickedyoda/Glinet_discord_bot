#!/bin/sh
# Install the repo's pre-commit secret guard into .git/hooks.
set -eu
cd "$(git rev-parse --show-toplevel)"
mkdir -p .git/hooks
cp scripts/git-hooks/pre-commit .git/hooks/pre-commit
chmod +x .git/hooks/pre-commit
echo "installed .git/hooks/pre-commit"
echo "run '.git/hooks/pre-commit' or make a test commit to verify"

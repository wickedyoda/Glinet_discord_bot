#!/usr/bin/env bash
# Regenerate the hash-pinned lock files after editing requirements*.txt.
#
# Both installs in CI and the Dockerfile use `pip install --require-hashes`, so a
# dependency added to requirements.txt without regenerating the lock simply will not
# install — which is the point, but it means this script is part of the workflow.
#
# Requires: python3 -m venv, network access to PyPI.
set -euo pipefail

cd "$(dirname "$0")/.."

VENV="${TMPDIR:-/tmp}/glinet-lockenv"
if [ ! -x "$VENV/bin/pip-compile" ]; then
  python3 -m venv "$VENV"
  "$VENV/bin/pip" install --quiet --upgrade pip pip-tools
fi

"$VENV/bin/pip-compile" --quiet --generate-hashes --strip-extras \
  --output-file requirements.lock.txt requirements.txt  # CI interpreter (3.11)
"$VENV/bin/pip-compile" --quiet --generate-hashes --strip-extras --allow-unsafe \
  --output-file requirements-dev.lock.txt requirements-dev.txt

# Prove both locks install on a clean interpreter before anyone relies on them.
check="${TMPDIR:-/tmp}/glinet-lockcheck"
rm -rf "$check"
python3 -m venv "$check"
"$check/bin/pip" install --quiet --require-hashes -r requirements.lock.txt
"$check/bin/pip" install --quiet --require-hashes -r requirements-dev.lock.txt
rm -rf "$check"

# The image is Python 3.15 while CI is 3.11, and the dependency graph differs: discord.py
# pulls audioop-lts only on >=3.13. One lock cannot serve both, so the image gets its own,
# compiled inside the same base image it will be installed into.
docker run --rm -v "$PWD:/w" -w /w python:3.15-slim sh -c '
  pip install -q --upgrade pip pip-tools
  pip-compile --quiet --generate-hashes --strip-extras --output-file requirements-py315.lock.txt requirements.txt
  python -m venv /v && /v/bin/pip install -q --require-hashes -r requirements-py315.lock.txt
'

echo "locks regenerated and verified: requirements.lock.txt, requirements-dev.lock.txt, requirements-py315.lock.txt"

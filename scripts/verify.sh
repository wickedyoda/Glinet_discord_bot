#!/usr/bin/env bash
set -euo pipefail

python3 -m py_compile bot.py web_admin.py
ruff check .
ruff format --check .
PYTHONPATH=. pytest -q
python_minor_version="$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")"
if python3 -c "import sys; raise SystemExit(0 if sys.version_info < (3, 14) else 1)"; then
  bandit -q -c pyproject.toml -r .
else
  echo "Skipping bandit on Python ${python_minor_version} (known incompatibility); CI runs bandit on Python 3.12."
fi
pip-audit -r requirements.txt
docker build -t wickedyoda-little-helper:verify .

"""Guard the startup import footprint.

bs4, openpyxl (and through it numpy and Pillow) are heavy and were previously
imported at module scope, so every container start paid for them even though all
their consumers are scheduled scrapes or rare xlsx parsing. They are now imported
lazily at the call sites.

This test runs in a subprocess so it observes a genuinely fresh interpreter -- by
the time the rest of the suite has run, those modules are in sys.modules and an
in-process assertion would pass vacuously.

A failure here means someone re-introduced a module-scope import. Move it back
inside the function that needs it.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

# Heavy packages that must not be paid for at startup.
DEFERRED = ("bs4", "openpyxl", "numpy", "PIL")


def _run_probe() -> str:
    env = dict(os.environ)
    env.setdefault("DISCORD_TOKEN", "test_token")
    env.setdefault("GUILD_ID", "0")
    env.setdefault("LOG_DIR", "/tmp/bot_test_logs")
    env["PYTHONPATH"] = str(REPO_ROOT)
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys, bot; "
            f"print('LOADED:' + ','.join(m for m in {DEFERRED!r} if m in sys.modules))",
        ],
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert completed.returncode == 0, completed.stderr[-2000:]
    for line in reversed(completed.stdout.splitlines()):
        if line.startswith("LOADED:"):
            return line[len("LOADED:"):]
    raise AssertionError(f"probe produced no marker; stdout={completed.stdout[-1500:]!r}")


def test_heavy_imports_stay_off_the_startup_path():
    loaded = [m for m in _run_probe().split(",") if m]
    assert loaded == [], (
        "these are imported at startup and should be deferred to their call sites: "
        + ", ".join(loaded)
    )


def test_deferred_call_sites_still_resolve():
    """A lazy import that never resolves would be invisible until used."""
    env = dict(os.environ)
    env.setdefault("DISCORD_TOKEN", "test_token")
    env.setdefault("GUILD_ID", "0")
    env.setdefault("LOG_DIR", "/tmp/bot_test_logs")
    env["PYTHONPATH"] = str(REPO_ROOT)
    completed = subprocess.run(
        [sys.executable, str(REPO_ROOT / "smoke_deferred_imports.py")],
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert completed.returncode == 0, completed.stdout[-3000:]
    assert "FAIL" not in completed.stdout, completed.stdout[-3000:]

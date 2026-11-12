"""The DAST bootstrap account must satisfy the web admin password policy.

`create_web_app` rejects a bootstrap password that fails `_password_policy_errors`,
which leaves the container with no admin account at all and login disabled. The
generated password therefore has to satisfy the policy on every run, not just on
average -- the previous `secrets.token_urlsafe(24)` was 32 characters against a
16-character cap.
"""
from __future__ import annotations

import importlib.util
import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent


def _load_ci_web_app():
    spec = importlib.util.spec_from_file_location(
        "ci_web_app_under_test", REPO_ROOT / "scripts" / "ci_web_app.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def ci_web_app():
    return _load_ci_web_app()


def test_generated_password_always_meets_the_policy(ci_web_app, monkeypatch):
    """Randomness must not occasionally produce an unusable password."""
    monkeypatch.delenv("DAST_ADMIN_PASSWORD", raising=False)
    from web_admin import _password_policy_errors

    # Repeated: the failure mode is probabilistic, so one sample proves little.
    for _ in range(50):
        password = ci_web_app._bootstrap_password()
        assert _password_policy_errors(password) == [], (
            f"generated password fails policy: {password!r}"
        )
        assert 6 <= len(password) <= 16


def test_generated_password_is_unique_per_run(ci_web_app, monkeypatch):
    monkeypatch.delenv("DAST_ADMIN_PASSWORD", raising=False)
    assert ci_web_app._bootstrap_password() != ci_web_app._bootstrap_password()


def test_env_override_wins(ci_web_app, monkeypatch):
    monkeypatch.setenv("DAST_ADMIN_PASSWORD", "Supplied!12x")
    assert ci_web_app._bootstrap_password() == "Supplied!12x"


def test_generated_password_can_actually_create_the_admin(tmp_path, ci_web_app, monkeypatch):
    """End to end: the app accepts it and login works with it."""
    monkeypatch.delenv("DAST_ADMIN_PASSWORD", raising=False)
    from web_admin import create_web_app

    env_file = tmp_path / "env.env"
    env_file.write_text("WEB_ENFORCE_CSRF=false\n")
    password = ci_web_app._bootstrap_password()

    app = create_web_app(
        data_dir=str(tmp_path),
        env_file_path=str(env_file),
        tag_responses_file=str(tmp_path / "tags.json"),
        default_admin_email="dast@example.invalid",
        default_admin_password=password,
    )
    client = app.test_client()
    base = "https://docker.example:8443"

    # The app enforces CSRF by default (the env file does not reach os.environ for
    # that flag), so log in the way a browser does: take the token from the page.
    page = client.get("/login", base_url=base)
    token = re.search(
        r'<meta name="csrf-token" content="([^"]+)"', page.get_data(as_text=True)
    )
    assert token is not None

    response = client.post(
        "/login",
        data={"email": "dast@example.invalid", "password": password},
        base_url=base,
        headers={"X-CSRF-Token": token.group(1)},
        follow_redirects=True,
    )
    assert response.status_code == 200
    assert b">Logout<" in response.data, "the generated password did not log in"

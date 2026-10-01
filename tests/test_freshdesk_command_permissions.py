"""Freshdesk commands use standard per-command permissions, not Freshdesk roles.

The Freshdesk Admin/User role integration (FRESHDESK_ADMIN / FRESHDESK_USER env
vars) has been removed. Every Freshdesk command now goes through the same
``ensure_interaction_command_access`` gate as every other command, so access is
managed in the Web GUI under Command Permissions.

Default policy:
  /create-ticket                  -> everyone (public)
  /create-ticket-from-message     -> everyone (public)
  /support-ticket-categories      -> moderators
  /support-ticket-view            -> moderators
  /support-ticket-search          -> moderators
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

BOT_PY = Path(__file__).resolve().parents[1] / "bot.py"
PERMS_PY = Path(__file__).resolve().parents[1] / "app" / "command_permissions.py"

PUBLIC_BY_DEFAULT = {"create_ticket", "create_ticket_from_message"}
MODERATOR_BY_DEFAULT = {
    "support_ticket_categories",
    "support_ticket_view",
    "support_ticket_search",
}


def _defaults_map(path: Path) -> dict[str, str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "COMMAND_PERMISSION_DEFAULTS":
                    return {
                        ast.literal_eval(k): ast.unparse(v)
                        for k, v in zip(node.value.keys, node.value.values, strict=True)
                    }
    raise AssertionError(f"COMMAND_PERMISSION_DEFAULTS not found in {path}")


@pytest.mark.parametrize("path", [BOT_PY, PERMS_PY])
class TestDefaultPolicies:
    def test_create_ticket_defaults_to_public(self, path):
        assert _defaults_map(path)["create_ticket"] == "COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC"

    def test_create_ticket_from_message_defaults_to_public(self, path):
        defaults = _defaults_map(path)
        assert defaults["create_ticket_from_message"] == "COMMAND_PERMISSION_DEFAULT_POLICY_PUBLIC"

    @pytest.mark.parametrize("key", sorted(MODERATOR_BY_DEFAULT))
    def test_viewer_commands_default_to_moderator(self, path, key):
        assert _defaults_map(path)[key] == "COMMAND_PERMISSION_DEFAULT_POLICY_MODERATOR_IDS"


class TestFreshdeskRoleIntegrationRemoved:
    """The FRESHDESK_ADMIN / FRESHDESK_USER role layer must not come back."""

    @pytest.mark.parametrize(
        "path",
        [BOT_PY, PERMS_PY, Path(__file__).resolve().parents[1] / "web_admin.py",
         Path(__file__).resolve().parents[1] / "app" / "freshdesk_api.py",
         Path(__file__).resolve().parents[1] / "app" / "web_freshdesk_routes.py",
         Path(__file__).resolve().parents[1] / "app" / "freshdesk_web_helpers.py"],
    )
    def test_no_freshdesk_role_env_vars(self, path):
        source = path.read_text(encoding="utf-8")
        # FRESHDESK_USER_AGENT is an HTTP header, not a role setting.
        scrubbed = source.replace("FRESHDESK_USER_AGENT", "")
        assert "FRESHDESK_ADMIN" not in scrubbed
        assert '"FRESHDESK_USER"' not in scrubbed
        assert "'FRESHDESK_USER'" not in scrubbed

    def test_no_freshdesk_role_gate_functions(self):
        source = BOT_PY.read_text(encoding="utf-8")
        assert "can_use_freshdesk_admin" not in source
        assert "can_use_freshdesk_user" not in source


class TestCommandsUseStandardGate:
    """Each Freshdesk command must call the shared permission helper."""

    @staticmethod
    def _function_source(name: str) -> str:
        source = BOT_PY.read_text(encoding="utf-8")
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)) and node.name == name:
                return ast.get_source_segment(source, node) or ""
        raise AssertionError(f"function {name} not found")

    @pytest.mark.parametrize(
        ("func", "key"),
        [
            ("create_ticket", "create_ticket"),
            ("create_ticket_from_message", "create_ticket_from_message"),
            ("support_ticket_categories", "support_ticket_categories"),
            ("support_ticket_view", "support_ticket_view"),
            ("support_ticket_search", "support_ticket_search"),
        ],
    )
    def test_command_uses_shared_permission_gate(self, func, key):
        body = self._function_source(func)
        assert f'ensure_interaction_command_access(interaction, "{key}")' in body
        assert "can_use_freshdesk" not in body


class TestCommandsConfigurableInWebGui:
    """A default no admin can override is a dead end.

    build_command_permissions_web_payload enumerates COMMAND_PERMISSION_METADATA,
    so a command missing from that map cannot be changed in the Web GUI even if
    it has a default entry.
    """

    @pytest.mark.parametrize("path", [BOT_PY, PERMS_PY])
    @pytest.mark.parametrize("key", sorted(PUBLIC_BY_DEFAULT | MODERATOR_BY_DEFAULT))
    def test_command_present_in_web_gui_metadata(self, path, key):
        source = path.read_text(encoding="utf-8")
        start = source.find("COMMAND_PERMISSION_METADATA = {")
        assert start > 0
        metadata = source[start : source.find("\n}\n", start)]
        assert f'"{key}"' in metadata, f"{key} missing from COMMAND_PERMISSION_METADATA"


class TestTicketSearchPrivacyGuardPreserved:
    """Widening a command must never expose another member's requester email."""

    def test_self_only_guard_still_present(self):
        body = TestCommandsUseStandardGate._function_source("support_ticket_search")
        assert "You can only view your own tickets." in body

    def test_guard_not_bypassed_by_admin_permission(self):
        body = TestCommandsUseStandardGate._function_source("support_ticket_search")
        # The guard must consult Discord moderator/admin rank, never the
        # Freshdesk role helpers that used to drive it.
        assert "can_use_freshdesk" not in body
        assert "has_moderator_access" in body
        assert "has_admin_access" in body


class TestTicketCreationLoggingIsDiscordIdOnly:
    """/create-ticket audit lines must record the numeric ID and nothing else."""

    @pytest.mark.parametrize(
        "func", ["create_ticket", "create_ticket_from_message", "SupportTicketCategoryView"]
    )
    def test_log_line_does_not_include_username(self, func):
        source = BOT_PY.read_text(encoding="utf-8")
        tree = ast.parse(source)
        nodes = [
            n
            for n in ast.walk(tree)
            if isinstance(n, (ast.AsyncFunctionDef, ast.FunctionDef, ast.ClassDef))
            and n.name == func
        ]
        assert nodes, f"{func} not found"
        body = ast.get_source_segment(source, nodes[0]) or ""
        # Match only up to the statement's own closing paren. A greedy
        # ``(.*?)\)\n`` still runs past the call into the next statement,
        # which would pull unrelated log lines into this function's body.
        log_calls = re.findall(r'logger\.(?:info|warning|error)\((.*?)\)(?=\s*(?:\n|$))', body, re.S)
        for call in log_calls:
            assert "interaction.user.id" in call or "user_id=" in call
            # The username is a str(User) -> "name#1234"; it must never be logged.
            assert "{interaction.user}" not in call
            assert "user.name" not in call

    def test_no_ticket_body_or_email_in_creation_logs(self):
        source = BOT_PY.read_text(encoding="utf-8")
        for forbidden in ("ticket_body", "subject", "requester_email", "submitted_email"):
            for line in source.splitlines():
                if "logger." in line and forbidden in line:
                    pytest.fail(f"creation log may leak {forbidden}: {line.strip()}")
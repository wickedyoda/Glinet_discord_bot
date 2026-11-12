"""Web GUI permission matrix.

These tests exist so an access decision is a readable table rather than a literal
buried in a request hook. The first one is the important one: the derived endpoint
set must equal the set the old hand-written allow-lists produced, so adopting the
model grants and revokes nothing.
"""
from __future__ import annotations

import pytest

from app.web_permissions import (
    ADMIN_ONLY_PENDING_MAPPING,
    AREAS,
    ENDPOINT_AREAS,
    ROLE_PERMISSIONS,
    WRITE_EXEMPT_ENDPOINTS,
    endpoint_permission,
    permissions_for_role,
    user_has_permission,
    write_endpoints_for_role,
)

# The literal allow-list that lived in web_admin.enforce_read_only_write_restrictions
# before this module existed. `account` is included there but is exempted before the
# role check, so the derived set omits it.
_LEGACY_ALLOWLIST = {
    "account",
    "command_status",
    "guild_settings",
    "moderation_page",
    "members_page",
    "discourse_page",
    "command_permissions",
    "reddit_feeds",
    "service_monitors_page",
    "uptime_kuma_page",
    "youtube_subscriptions",
    "linkedin_subscriptions",
    "beta_program_subscriptions",
    "role_access_page",
    "tag_responses",
    "bot_profile",
}

OPERATOR_ROLES = ("guild_admin", "glinet_rw")
READ_ROLES = ("glinet_read_only", "read_only")


def test_endpoint_set_matches_the_previous_allowlist():
    """Adopting the model must not change who can write what."""
    expected = _LEGACY_ALLOWLIST - WRITE_EXEMPT_ENDPOINTS
    for role in OPERATOR_ROLES:
        assert write_endpoints_for_role(role) == expected, role


def test_operator_roles_are_capability_identical():
    """guild_admin and glinet_rw differ in scope only, by decision."""
    assert permissions_for_role("guild_admin") == permissions_for_role("glinet_rw")
    assert write_endpoints_for_role("guild_admin") == write_endpoints_for_role("glinet_rw")


@pytest.mark.parametrize("role", OPERATOR_ROLES)
@pytest.mark.parametrize("area", ["activity", "system"])
def test_operators_cannot_write_activity_or_system(role, area):
    """Statistics and system administration stay admin-only."""
    assert f"{area}.write" not in permissions_for_role(role)


@pytest.mark.parametrize("role", READ_ROLES)
def test_read_only_roles_can_write_nothing(role):
    writes = {p for p in permissions_for_role(role) if p.endswith(".write")}
    assert writes == set()
    assert write_endpoints_for_role(role) == frozenset()


@pytest.mark.parametrize("role", ["admin", "guild_admin", "glinet_rw", "glinet_read_only", "read_only"])
def test_every_role_can_read_every_area(role):
    perms = permissions_for_role(role)
    missing = [a for a in AREAS if f"{a}.read" not in perms]
    assert missing == [], f"{role} cannot read {missing}"


def test_admin_can_write_every_area():
    for area in AREAS:
        assert f"{area}.write" in permissions_for_role("admin")
    assert write_endpoints_for_role("admin") == frozenset(ENDPOINT_AREAS)


def test_pending_endpoints_are_not_granted_to_operators():
    """ADMIN_ONLY_PENDING_MAPPING is documentation; mapping one is a grant."""
    granted = set(ENDPOINT_AREAS)
    overlap = set(ADMIN_ONLY_PENDING_MAPPING) & granted
    assert overlap == set(), f"these were granted by accident: {sorted(overlap)}"

    for role in OPERATOR_ROLES + READ_ROLES:
        endpoints = write_endpoints_for_role(role)
        leaked = endpoints & set(ADMIN_ONLY_PENDING_MAPPING)
        assert leaked == frozenset(), f"{role} unexpectedly granted {sorted(leaked)}"


@pytest.mark.parametrize(
    ("role", "endpoint", "allowed"),
    [
        ("admin", "settings", True),
        ("guild_admin", "settings", False),
        ("glinet_rw", "member_activity_page", False),
        ("guild_admin", "guild_settings", True),
        ("glinet_rw", "tag_responses", True),
        ("glinet_read_only", "guild_settings", False),
        ("read_only", "tag_responses", False),
        ("admin", "not_a_real_endpoint", True),  # unmapped -> not gated here
    ],
)
def test_matrix_row(role, endpoint, allowed):
    perms = permissions_for_role(role)
    area = ENDPOINT_AREAS.get(endpoint)
    if area is None:
        # unmapped endpoints are governed elsewhere; the model makes no claim
        assert endpoint_permission(endpoint) is None
        return
    assert (f"{area}.write" in perms) is allowed


def test_legacy_boolean_still_resolves_a_role():
    """A row with no role column must keep its old behaviour."""
    assert permissions_for_role("", is_admin=True) == permissions_for_role("admin")
    assert permissions_for_role("", is_admin=False) == permissions_for_role("read_only")
    assert permissions_for_role("glinet") == permissions_for_role("glinet_read_only")


@pytest.mark.parametrize("missing", [None, {}])
@pytest.mark.parametrize("area", AREAS)
def test_missing_user_is_denied_every_read(missing, area):
    """An absent user is not a read-only user.

    ``user or {}`` normalised a missing record to the read_only role, which holds
    every ``.read`` permission, so an anonymous caller passed read checks. The
    helper must reject before resolving a role.
    """
    assert user_has_permission(missing, f"{area}.read") is False


@pytest.mark.parametrize("missing", [None, {}])
def test_missing_user_is_denied_every_write(missing):
    for area in AREAS:
        assert user_has_permission(missing, f"{area}.write") is False


def test_empty_role_with_is_admin_still_resolves():
    """The guard must not break legacy rows that only carry the boolean."""
    assert user_has_permission({"is_admin": True}, "system.write") is True
    assert user_has_permission({"is_admin": False}, "system.write") is False


def test_user_has_permission_reads_role_and_boolean():
    assert user_has_permission({"role": "admin"}, "system.write") is True
    assert user_has_permission({"role": "glinet_rw"}, "system.write") is False
    assert user_has_permission({"role": "glinet_rw"}, "feeds.write") is True
    assert user_has_permission({}, "feeds.write") is False
    assert user_has_permission({"is_admin": True}, "system.write") is True


def test_every_declared_role_has_a_matrix_row():
    """Adding a role to the normaliser without a row would fail open or closed
    unpredictably."""
    for role in ("admin", "guild_admin", "glinet_rw", "glinet_read_only", "read_only"):
        assert role in ROLE_PERMISSIONS

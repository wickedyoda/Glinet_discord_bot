"""Web GUI permission model.

Capability and scope are separate concerns:

  capability -- what an account may do, expressed as ``<area>.<read|write>``
  scope      -- which managed guilds it may act on, decided by the role
                (there is no per-user scope override)

**This revision is behaviour-preserving.** ``ENDPOINT_AREAS`` lists only the
endpoints the operator roles can write today, so adopting the module changes no
access decision -- the role gate derives exactly the endpoint set that used to be
hand-written twice inside ``web_admin.enforce_read_only_write_restrictions``.

Endpoints that are not listed are admin-only by omission, which is how they behave
now. ``ADMIN_ONLY_PENDING_MAPPING`` names those explicitly so the next pass can map
them deliberately rather than by accident -- adding one to ``ENDPOINT_AREAS`` is a
privilege grant, not a refactor.

`guild_admin` and `glinet_rw` are capability-identical by decision: they differ in
scope only. Activity and system writes stay admin-only.
"""

from __future__ import annotations

# --------------------------------------------------------------------------- #
#  Areas
# --------------------------------------------------------------------------- #

AREAS = (
    "moderation",
    "members",
    "activity",
    "chat_automation",
    "feeds",
    "monitoring",
    "support",
    "bot_config",
    "system",
)

# Endpoint -> area for state-changing requests the operator roles may currently
# perform. Derived from the two allow-lists that lived in web_admin; `account` is
# absent because it is a self-service endpoint exempted before the role check.
ENDPOINT_AREAS = {
    # moderation
    "moderation_page": "moderation",
    # members
    "members_page": "members",
    "role_access_page": "members",
    # chat automation
    "tag_responses": "chat_automation",
    # feeds
    "reddit_feeds": "feeds",
    "youtube_subscriptions": "feeds",
    "linkedin_subscriptions": "feeds",
    "beta_program_subscriptions": "feeds",
    # monitoring
    "service_monitors_page": "monitoring",
    "uptime_kuma_page": "monitoring",
    # support
    "discourse_page": "support",
    # bot configuration
    "guild_settings": "bot_config",
    "command_permissions": "bot_config",
    "command_status": "bot_config",
    "bot_profile": "bot_config",
}

# Endpoints that are admin-only today and are *not* mapped to an area yet.
# Listing them here is documentation, not a grant.
ADMIN_ONLY_PENDING_MAPPING = (
    # activity
    "member_activity_page",
    "member_activity_export",
    # moderation / members extensions
    "honeypot_page",
    "reaction_roles_page",
    "bulk_role_csv",
    # chat automation extensions
    "translate_channels_page",
    "reddit_auto_responds",
    # feeds extensions
    "forum_tags_page",
    # support extensions
    "ticket_settings",
    "discourse_activity_page",
    # system
    "settings",
    "users",
    "restart_service",
    "logs_page",
    "logs_export",
    "observability_page",
)

# Self-service and infrastructure endpoints any authenticated account may use.
WRITE_EXEMPT_ENDPOINTS = frozenset(
    {"login", "logout", "account", "healthz", "readyz", "select_guild"}
)

# --------------------------------------------------------------------------- #
#  Roles
# --------------------------------------------------------------------------- #

# The matrix governs both halves now: <area>.read decides whether the page is
# offered and reachable, <area>.write whether it can change anything. A hidden
# link is not a control, so READ_ENDPOINT_AREAS is enforced by a request gate.
_OPERATOR_WRITE_AREAS = frozenset(
    {
        "moderation",
        "members",
        "chat_automation",
        "feeds",
        "monitoring",
        "support",
        "bot_config",
    }
)


def build_role_permissions() -> dict[str, frozenset[str]]:
    """role -> permission set. Reads for every area, writes per the matrix."""
    reads = {f"{area}.read" for area in AREAS}
    table: dict[str, frozenset[str]] = {}
    for role in ("admin", "guild_admin", "glinet_rw", "glinet_read_only", "read_only"):
        if role == "admin":
            writes = {f"{area}.write" for area in AREAS}
        elif role in ("guild_admin", "glinet_rw"):
            writes = {f"{area}.write" for area in _OPERATOR_WRITE_AREAS}
        else:
            writes = set()
        table[role] = frozenset(reads | writes)
    return table


ROLE_PERMISSIONS = build_role_permissions()


def normalize_role(role: str | None, *, is_admin: bool = False) -> str:
    """Fall back to the legacy boolean when the role column is empty.

    Kept identical to ``web_admin._normalize_web_user_role`` so the two cannot
    disagree while both exist.
    """
    raw = str(role or "").strip().lower()
    if raw == "glinet":
        return "glinet_read_only"
    if raw in ROLE_PERMISSIONS:
        return raw
    return "admin" if bool(is_admin) else "read_only"


def permissions_for_role(role: str | None, *, is_admin: bool = False) -> frozenset[str]:
    return ROLE_PERMISSIONS.get(normalize_role(role, is_admin=is_admin), frozenset())


def user_has_permission(user: dict | None, permission: str) -> bool:
    """Permission check for a resolved user record.

    A missing or empty user is rejected outright. Without this guard the empty dict
    normalises to the read_only role -- which grants every ``.read`` -- so
    ``user_has_permission(None, "system.read")`` returned True. Reads belong to
    authenticated roles, not to anonymous callers. Legacy records that still carry
    only ``is_admin`` keep working through the ``permissions_for_role`` fallback.
    """
    user = user or {}
    if not user:
        return False
    return permission in permissions_for_role(
        user.get("role", ""), is_admin=bool(user.get("is_admin"))
    )


def endpoint_permission(endpoint: str | None) -> str | None:
    """The write permission an endpoint requires, or None if it is not mapped."""
    if not endpoint or endpoint in WRITE_EXEMPT_ENDPOINTS:
        return None
    area = ENDPOINT_AREAS.get(str(endpoint))
    return f"{area}.write" if area else None


def write_endpoints_for_role(role: str | None, *, is_admin: bool = False) -> frozenset[str]:
    """Endpoints a role may write to, derived from its permissions.

    Replaces the hand-written allow-lists. Anything not in ``ENDPOINT_AREAS`` is
    admin-only, which is the deny-by-default rule.
    """
    return write_endpoints_for_permissions(permissions_for_role(role, is_admin=is_admin))


def write_endpoints_for_permissions(permissions) -> frozenset[str]:
    """The same derivation, for a caller holding a permission set rather than a role."""
    perms = frozenset(permissions or ())
    return frozenset(
        endpoint for endpoint, area in ENDPOINT_AREAS.items() if f"{area}.write" in perms
    )


# --------------------------------------------------------------------------- #
#  Read visibility
# --------------------------------------------------------------------------- #
# Until now every authenticated account could open every page and only writes were
# gated. Visibility now derives from the same matrix: a page is reachable when the
# account's group holds ``<area>.read`` for the area it belongs to.
#
# This never widens access. Every seeded group holds all ``*.read`` permissions, so
# only a deliberately narrowed custom group sees a difference. Endpoints absent
# from this map keep their previous behaviour (reachable), and anything in
# ``ADMIN_ONLY_READ_ENDPOINTS`` stays admin-only whatever the matrix says.
READ_ENDPOINT_AREAS = {
    **ENDPOINT_AREAS,
    # activity
    "member_activity_page": "activity",
    "member_activity_export": "activity",
    "discourse_activity_page": "activity",
    # moderation and members
    "honeypot_page": "moderation",
    "actions_page": "moderation",
    "bulk_role_csv": "members",
    # chat automation
    "translate_channels_page": "chat_automation",
    "reddit_auto_responds": "chat_automation",
    "reaction_roles_page": "chat_automation",
    "invite_links_page": "chat_automation",
    "post_message_page": "chat_automation",
    # feeds
    "forum_tags": "feeds",
    "forum_tags_page": "feeds",
    # monitoring
    "public_observability": "monitoring",
    "public_observability_alias": "monitoring",
    "public_observability_everything": "monitoring",
    # support
    "discourse_viewer.discourse_viewer_page": "support",
    "freshdesk_viewer.freshdesk_viewer_page": "support",
    # bot configuration
    "settings": "bot_config",
    "guilds_page": "bot_config",
    # system
    "users": "system",
    "permissions_page": "system",
    "admin_logs": "system",
    "admin_logs_export": "system",
}

# Pages that are admin-only regardless of the matrix: they change the permission
# model itself, or the runtime the model runs in.
ADMIN_ONLY_READ_ENDPOINTS = frozenset(
    {
        "users",
        "permissions_page",
        "admin_logs",
        "admin_logs_export",
        "settings",
    }
)


def read_area_for_endpoint(endpoint: str | None) -> str | None:
    """The area a page belongs to, for visibility. None means 'not mapped'."""
    if not endpoint:
        return None
    return READ_ENDPOINT_AREAS.get(str(endpoint))


def read_endpoints_for_permissions(permissions) -> frozenset[str]:
    """Endpoints an account may open, derived from its ``<area>.read`` set."""
    perms = frozenset(permissions or ())
    return frozenset(
        endpoint
        for endpoint, area in READ_ENDPOINT_AREAS.items()
        if f"{area}.read" in perms
    )

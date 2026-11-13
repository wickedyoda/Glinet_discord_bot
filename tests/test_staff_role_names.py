"""Staff role matching must survive the Employee -> GL.iNet Staff rename.

`DEFAULT_ALLOWED_ROLE_NAMES` is matched by role *name*, in two files, so renaming
the Discord role without updating both would silently revoke staff access to
allowed-role commands until the code caught up. Both names are accepted, and the
moderator role was being matched with the wrong number as well -- the guild role is
"Gl.iNet Moderators", the code looked for "Gl.iNet Moderator".
"""
from __future__ import annotations

from types import SimpleNamespace

import bot
from app import command_permissions


def _member(*role_names):
    return SimpleNamespace(roles=[SimpleNamespace(name=n, id=0) for n in role_names])


def test_both_staff_role_names_are_accepted():
    for name in ("GL.iNet Staff", "Employee"):
        assert name in bot.DEFAULT_ALLOWED_ROLE_NAMES, name
        assert bot.has_allowed_role(_member(name)) is True, name


def test_moderator_role_matches_the_real_guild_name():
    """The guild role is plural; the code asked for the singular."""
    assert "Gl.iNet Moderators" in bot.DEFAULT_ALLOWED_ROLE_NAMES
    assert bot.has_allowed_role(_member("Gl.iNet Moderators")) is True
    # the legacy singular still resolves, so nothing regresses mid-rename
    assert bot.has_allowed_role(_member("Gl.iNet Moderator")) is True


def test_unrelated_roles_are_still_rejected():
    assert bot.has_allowed_role(_member("Random Role")) is False
    assert bot.has_allowed_role(_member()) is False


def test_both_copies_of_the_constant_agree():
    """The permission tables live in two files and are not synced automatically."""
    assert set(bot.DEFAULT_ALLOWED_ROLE_NAMES) == set(command_permissions.DEFAULT_ALLOWED_ROLE_NAMES)

"""Channel scoping for the Freshdesk ticket commands.

The ticket commands split into two groups with deliberately different channel
rules:

- **Channel-scoped** -- /create-ticket, /support-ticket-search,
  /support-ticket-view, /support-ticket-categories only do anything meaningful
  in the configured intake channel. Used elsewhere, they return the friendly
  wrong-channel reply pointing at the right one.

- **Anywhere** -- /create-ticket-from-message may be run in any channel. Its
  whole purpose is to take the command from wherever a message is being
  discussed and carry that message into a ticket; restricting it to the intake
  channel would defeat that, since the message is usually somewhere else.

The distinction matters because the two routes look similar but take different
paths. Both call _resolve_freshdesk_ticket_target_channel_id, but for the
scoped commands that resolution is a *permission* check on where the command was
run, while for create-ticket-from-message it only decides where the resulting
ticket thread is created. A refactor that adds the wrong-channel check to the
wrong command would break the feature the user relies on.

These tests pin the split by inspecting the command sources, which is robust to
how each body is factored internally.
"""

from __future__ import annotations

import re
from pathlib import Path

import bot as bot_module

BOT_SOURCE = Path(bot_module.__file__).read_text()

CHANNEL_SCOPED_COMMANDS = (
    "create_ticket",
    "support_ticket_search",
    "support_ticket_view",
    "support_ticket_categories",
)
ANY_CHANNEL_COMMANDS = ("create_ticket_from_message",)


def _function_source(name: str) -> str:
    """Return the source of a top-level async def, bounded to its own body."""
    start = BOT_SOURCE.index(f"async def {name}(")
    rest = BOT_SOURCE[start + 10 :]
    match = re.search(r"\n(?=(?:async )?def )", rest)
    return BOT_SOURCE[start : start + 10 + match.start()] if match else BOT_SOURCE[start:]


# --------------------------------------------------------------------------- #
#  Channel-scoped commands keep the check
# --------------------------------------------------------------------------- #


def test_channel_scoped_commands_enforce_the_intake_channel():
    for name in CHANNEL_SCOPED_COMMANDS:
        source = _function_source(name)
        assert "_freshdesk_wrong_channel_reply" in source, (
            f"/{name} lost its wrong-channel check; it must only run in the "
            f"configured intake channel"
        )


def test_wrong_channel_check_runs_before_configuration_rejection():
    """A user in the wrong channel gets channel guidance, not a config error.

    This is the bug that started all of it: the config guard used to fire first,
    so a wrong-channel user was told Freshdesk was unconfigured.
    """
    for name in CHANNEL_SCOPED_COMMANDS:
        source = _function_source(name)
        check = source.find("_freshdesk_wrong_channel_reply")
        config_guard = source.find('not config.get("enabled")')
        assert check != -1, f"/{name} has no wrong-channel check"
        if config_guard != -1:
            assert check < config_guard, (
                f"/{name} checks configuration before the channel; a wrong-channel "
                f"user would see 'not configured' instead of channel guidance"
            )


# --------------------------------------------------------------------------- #
#  create-ticket-from-message is deliberately unrestricted
# --------------------------------------------------------------------------- #


def test_create_ticket_from_message_has_no_wrong_channel_check():
    """It must run in any channel -- that is the point of the command."""
    source = _function_source("create_ticket_from_message")
    assert "_freshdesk_wrong_channel_reply" not in source, (
        "/create-ticket-from-message must not enforce the intake channel; it is "
        "meant to be run wherever the message being escalated is"
    )


def test_create_ticket_from_message_still_resolves_a_target_channel():
    """No channel *restriction*, but the thread destination is still configured.

    The resolved channel decides where the ticket thread is created, so an
    unconfigured runtime must still fail with a clear message rather than
    silently dropping the ticket.
    """
    source = _function_source("create_ticket_from_message")
    assert "_resolve_freshdesk_ticket_target_channel_id" in source
    assert "No Freshdesk intake channel configured" in source


def test_create_ticket_from_message_keeps_permission_and_config_guards():
    """Unrestricted by channel, but not unrestricted full stop."""
    source = _function_source("create_ticket_from_message")
    assert (
        'ensure_interaction_command_access(interaction, "create_ticket_from_message")' in source
    ), "standard per-command permission check must remain"
    assert "_freshdesk_not_configured_reply" in source, "config check must remain"
    assert "interaction.guild is None" in source, "must still require a server"


# --------------------------------------------------------------------------- #
#  The command exists and is registered
# --------------------------------------------------------------------------- #


def test_create_ticket_from_message_is_defined():
    """The tree.command decorator rebinds the name to a Command object."""
    assert bot_module.create_ticket_from_message is not None
    command = bot_module.create_ticket_from_message
    assert hasattr(command, "callback") or callable(command)


def test_command_is_registered_on_the_tree():
    """Registered so /create-ticket-from-message is actually invokable."""
    tree = getattr(bot_module, "tree", None)
    assert tree is not None, "command tree missing"
    names = {
        command.name
        for command in tree.get_commands()
        if getattr(command, "name", None)
    }
    assert "create-ticket-from-message" in names, sorted(names)[:20]

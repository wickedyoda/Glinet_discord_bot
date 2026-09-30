"""The Discord message is fetched and used to pre-populate the ticket body.

/create-ticket-from-message accepts a message_id and promises the user that the
message body "will be pre-populated from Discord message". On main the whole
plumbing is present -- the modal accepts message_body, the category view carries
message_content, and on_submit prefers the pre-populated value -- but the command
assigns the empty string:

    view.message_content = ""

so message_id is accepted, validated for nothing, and discarded. The user is
promised a pre-populated body and receives a blank one.

These tests cover the fetch step and the guards around it.
"""

from __future__ import annotations

import re
from pathlib import Path

import bot as bot_module

BOT_SOURCE = Path(bot_module.__file__).read_text()


def _command_source(name: str = "create_ticket_from_message") -> str:
    start = BOT_SOURCE.index(f"async def {name}(")
    rest = BOT_SOURCE[start + 10 :]
    match = re.search(r"\n(?=(?:async )?def )", rest)
    return BOT_SOURCE[start : start + 10 + match.start()] if match else BOT_SOURCE[start:]


# --------------------------------------------------------------------------- #
#  The message is actually fetched
# --------------------------------------------------------------------------- #


def test_command_fetches_the_message_by_id():
    """The whole feature depends on this; it was missing on main.

    Resolved through guild.get_message rather than channel.fetch_message: the
    message may be in any channel, and only the guild-wide lookup can find it.
    """
    source = _command_source()
    assert "get_message" in source, (
        "/create-ticket-from-message never fetches the Discord message; "
        "message_id was accepted and then discarded"
    )
    assert "fetch_message" not in source, (
        "the lookup must not be scoped to one channel; the message can be "
        "anywhere in the guild"
    )


def test_message_id_is_parsed_as_an_integer():
    """Discord IDs are numeric; parsing must be guarded with a clear error."""
    source = _command_source()
    assert re.search(r"int\(\s*str\(message_id[^)]*\)[^)]*\)", source), (
        "message_id should be parsed with int() and validated"
    )
    # The conversion must be guarded, not bare.
    assert "except (TypeError, ValueError)" in source, (
        "the int() conversion must be guarded so a bad ID gives a clear message"
    )


def test_assigns_the_fetched_content_to_the_view():
    """The fetched text must reach view.message_content, not a literal ''."""
    source = _command_source()
    assert re.search(r"view\.message_content\s*=\s*['\"]['\"]", source) is None, (
        "view.message_content is still hardcoded to an empty string, so the "
        "modal is never pre-populated"
    )
    assert "view.message_content =" in source


def test_a_fetch_failure_tells_the_user_instead_of_silently_proceeding():
    """A bad or deleted ID must produce a clear message, not a blank ticket."""
    source = _command_source()
    assert re.search(r"except\s+discord\.", source), (
        "the fetch should handle discord.NotFound/HTTPException and report it"
    )
    assert "could not be read" in source.lower() or "not found" in source.lower(), (
        "there should be a user-facing message for an unreadable message"
    )


# --------------------------------------------------------------------------- #
#  The plumbing it feeds is present and connected
# --------------------------------------------------------------------------- #


def test_modal_accepts_and_prefers_the_prepopulated_body():
    assert "message_body" in BOT_SOURCE.split("class SupportTicketCreateModal")[1][:4000]
    assert "_pre_populated_message_body" in BOT_SOURCE


def test_category_view_forwards_the_content_to_the_modal():
    view_src = BOT_SOURCE.split("class SupportTicketCategoryView")[1][:3000]
    assert "message_content" in view_src
    assert "message_body=message_body" in view_src


def test_message_body_is_truncated_to_the_textinput_limit():
    """A message longer than the modal limit must not crash the modal."""
    view_src = BOT_SOURCE.split("class SupportTicketCategoryView")[1][:3000]
    # Either the fetch truncates, or the modal input caps it.
    assert (
        "4000" in view_src
        or "[:4000]" in view_src
        or "max_length=4000" in BOT_SOURCE.split("class SupportTicketCreateModal")[1][:4000]
    )

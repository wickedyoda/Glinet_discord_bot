"""Audit logging for the Freshdesk ticket commands.

The ticket commands are an audit surface: they open support requests on behalf of
a named requester, and when something goes wrong the logs are the only record of
who asked for what. Most command callbacks attribute an action with::

    logger.info("/thing invoked by %s", f"{interaction.user} (id: {interaction.user.id})")

The Freshdesk ticket path deliberately departs from that: because
``str(discord.User)`` is ``"name#1234"``, it logs the numeric ID alone so no
username is written to the audit log.

Two properties matter and are asserted here:

1. The initiating Discord ID must appear on the paths where a ticket is actually
   created. The /create-ticket callback only logs the *invocation*; the ticket
   itself is created later, after the category select and the modal, in
   _freshdesk_create_on_submit. That function previously logged nothing on
   success, so a completed ticket left only the bare invocation line.

2. Requester PII must NOT be logged. The Discord ID answers "who ran this",
   which is the audit question. The name, email, subject, and message body are
   the requester's own words and address; they belong in Freshdesk and the
   Discord thread, not in the bot log. The searched email is PII too, and
   /support-ticket-search was logging it.
"""

from __future__ import annotations

import re
from pathlib import Path

import bot as bot_module
from bot import _freshdesk_create_on_submit

BOT_SOURCE = Path(bot_module.__file__).read_text()

# Variables holding requester-supplied or requester-identifying values in the
# Freshdesk ticket flow. None of these may appear as a logging argument.
PII_VARIABLES = ("message_body", "description", "subject", "email", "name")

# A logging call that attributes an action, per the project convention.
# Ticket-path audit lines record the numeric Discord ID and nothing else.
# The project-wide convention elsewhere is
#     f"{interaction.user} (id: {interaction.user.id})"
# but str(User) is "name#1234", so that form also records the username. The
# Freshdesk ticket path was tightened to ID-only, and these assertions guard it.
ACTOR_PATTERN = re.compile(r"user_id=%s|\buser_id=|interaction\.user\.id")
PII_AS_LOG_ARG = re.compile(
    r"(?<![\w.])(?:" + "|".join(PII_VARIABLES) + r")(?![\w])"
)


def _log_call_args(source: str, start_offset: int) -> str:
    """Return the argument text of the logger call beginning at start_offset."""
    depth = 0
    started = False
    out = []
    for char in source[start_offset:]:
        if char == "(":
            depth += 1
            if not started:
                started = True
                continue
        elif char == ")":
            depth -= 1
            if depth == 0:
                return "".join(out)
        if started:
            out.append(char)
    return "".join(out)


def _freshdesk_log_calls():
    """Every logger call in the Freshdesk ticket region, with its arguments."""
    calls = []
    for match in re.finditer(
        r"^[ \t]*logger\.(info|warning|error|exception|debug)\(", BOT_SOURCE, re.M
    ):
        line_no = BOT_SOURCE[: match.start()].count("\n") + 1
        args = _log_call_args(BOT_SOURCE, match.end() - 1)
        calls.append((line_no, match.group(1), args))
    return calls


# --------------------------------------------------------------------------- #
#  No requester PII may be logged
# --------------------------------------------------------------------------- #


def _freshdesk_flow_source() -> str:
    """The source of the Freshdesk ticket flow, for PII scanning.

    Covers the four command callbacks, the category view, the modal, and
    _freshdesk_create_on_submit where the ticket is actually created.
    """
    start = BOT_SOURCE.index("async def support_ticket_search")
    return BOT_SOURCE[start : start + 9000]


def test_no_freshdesk_log_passes_requester_pii():
    """No name, email, subject, or message body may be a logging argument.

    Scoped to the Freshdesk ticket flow. A repo-wide scan would flag unrelated
    logs that legitimately print a Discord channel name or a role name -- those
    are server configuration, not requester PII.
    """
    flow = _freshdesk_flow_source()
    offenders = []
    for match in re.finditer(
        r"logger\.(?:info|warning|error|exception|debug)\(", flow
    ):
        line_no = BOT_SOURCE[: BOT_SOURCE.index(flow) + match.start()].count("\n") + 1
        args = _log_call_args(flow, match.end() - 1)
        for variable in PII_VARIABLES:
            if re.search(rf"(?<![\w.]){variable}(?![\w])", args):
                offenders.append((line_no, variable, args.strip()[:90]))
    assert not offenders, f"requester PII passed to a logger: {offenders}"


def test_search_command_does_not_log_the_queried_email():
    """The searched email is requester PII and must stay out of the log."""
    calls = [
        (line_no, args)
        for line_no, _level, args in _freshdesk_log_calls()
        if "support-ticket-search" in args
    ]
    assert calls, "expected a /support-ticket-search log line"
    for line_no, args in calls:
        assert "for email" not in args, f"line {line_no} still logs the searched email"
        assert not re.search(r",\s*email\b", args), (
            f"line {line_no} passes the email variable to the logger"
        )


def test_freshdesk_creation_log_does_not_include_ticket_contents():
    """The creation log may carry the ticket number, never the requester text."""
    creation = [
        args
        for _line, _level, args in _freshdesk_log_calls()
        if "created by %s" in args
    ]
    assert creation, "expected a Freshdesk ticket creation log line"
    for args in creation:
        assert not re.search(r"(?<![\w.])subject(?![\w])", args)
        assert not re.search(r"(?<![\w.])message_body(?![\w])", args)
        assert not re.search(r"(?<![\w.])description(?![\w])", args)


# --------------------------------------------------------------------------- #
#  The initiating Discord ID must be present
# --------------------------------------------------------------------------- #


def test_every_freshdesk_command_logs_the_actor_with_id():
    """Each ticket command attributes the action to a Discord ID."""
    commands = [
        "/create-ticket",
        "/support-ticket-search",
        "/support-ticket-view",
        "/support-ticket-categories",
    ]
    for command in commands:
        matching = [
            args
            for _line, _level, args in _freshdesk_log_calls()
            if command in args
        ]
        assert matching, f"{command} has no invocation log line"
        assert any(ACTOR_PATTERN.search(args) for args in matching), (
            f"{command} does not log the Discord user id"
        )


def _creation_function_source() -> str:
    """Exactly the body of _freshdesk_create_on_submit, and nothing after it.

    Bounded by the next top-level def/async def so an unrelated logger in a
    neighbouring function is never attributed to the ticket flow.
    """
    start = BOT_SOURCE.index("async def _freshdesk_create_on_submit")
    rest = BOT_SOURCE[start + 10 :]
    match = re.search(r"\n(?=(?:async )?def )", rest)
    return BOT_SOURCE[start : start + 10 + match.start()] if match else BOT_SOURCE[start:]


def test_ticket_creation_paths_log_the_actor_with_id():
    """Success and every failure path in _freshdesk_create_on_submit must
    identify who initiated the ticket.

    This is the gap: the command callback logs the invocation, but the ticket is
    created here after the category select and modal. Without these lines a
    completed ticket left no attributable record.
    """
    body = _creation_function_source()

    calls = [
        (m.group(1), _log_call_args(body, m.end() - 1))
        for m in re.finditer(r"logger\.(info|warning|error|exception)\(", body)
    ]
    assert calls, "no logging in _freshdesk_create_on_submit"

    for level, args in calls:
        assert ACTOR_PATTERN.search(args), (
            f"_freshdesk_create_on_submit logs at {level} without the Discord id: "
            f"{args.strip()[:100]}"
        )


def test_creation_success_is_logged_with_ticket_number():
    """A created ticket must leave a positive record naming the ticket."""
    body = _creation_function_source()
    success = [
        args
        for m in re.finditer(r"logger\.info\(", body)
        for args in [_log_call_args(body, m.end() - 1)]
        if "created by %s" in args
    ]
    assert success, "no success log for a created Freshdesk ticket"
    assert "ticket #%s" in success[0], "creation log should carry the ticket number"
    assert ACTOR_PATTERN.search(success[0])


def test_modal_error_log_identifies_the_submitter():
    """A modal failure must say whose submission failed."""
    modal_start = BOT_SOURCE.index("class SupportTicketCreateModal")
    modal = BOT_SOURCE[modal_start:modal_start + 4000]
    errors = [
        args
        for m in re.finditer(r"logger\.exception\(", modal)
        for args in [_log_call_args(modal, m.end() - 1)]
    ]
    assert errors, "modal error path has no logging"
    for args in errors:
        assert ACTOR_PATTERN.search(args), (
            f"modal error log omits the Discord id: {args.strip()[:90]}"
        )


def test_thread_creation_reason_includes_the_discord_id():
    """The private thread's audit reason must carry the ID, not just the name."""
    body = _creation_function_source()
    idx = body.index("reason=")
    reason_block = body[idx:idx + 320]
    assert "interaction.user.id" in reason_block, (
        "the thread audit reason should include the Discord user id"
    )


# --------------------------------------------------------------------------- #
#  The helper exists and is importable
# --------------------------------------------------------------------------- #


def test_creation_helper_is_callable():
    assert callable(_freshdesk_create_on_submit)

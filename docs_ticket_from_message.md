# Ticket-from-message notes

`/create-ticket-from-message` now resolves the message via `guild.get_message`
and carries its text into the Freshdesk ticket body.

Deliberately does **not** carry the wrong-channel check that the other four
Freshdesk commands have: this command exists to be run wherever the message
being escalated is discussed. It still resolves a target channel, but only to
decide where the resulting ticket thread is created.

See PR #361 for details.

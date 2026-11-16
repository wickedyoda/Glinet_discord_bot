"""The web GUI's binder: six sections, their pages, and the copy shown on each.

The navigation dropdown is deliberately short — the twelve pages the operator asked
for. Everything else lives one level down, on that section's landing page, so the menu
stays scannable without any page becoming unreachable.

Announcements are user-facing documentation, not decoration: what the page does, what
it can change, and what to be careful of. They are looked up by endpoint so a page
cannot ship without one (a test pins the twelve menu leaves).
"""

from __future__ import annotations

SECTIONS = (
    {
        "slug": "account",
        "label": "My Account",
        "icon": "👤",
        "blurb": "Your own login, the bot's identity, global settings, and who may sign in.",
        "warning": "Global Settings changes apply to the whole bot, not one server.",
        "pages": (
            ("account", "My Account", "Your name, email and password."),
            ("bot_profile", "Bot Profile", "The bot's display name and avatar."),
            ("settings", "Global Settings", "Instance-wide settings and credentials."),
            ("users", "Users", "Accounts that may sign in, and the role picker."),
            ("documentation", "Documentation", "How the admin is put together."),
        ),
    },
    {
        "slug": "servers",
        "label": "Servers",
        "icon": "🖥️",
        "blurb": "Which Discord servers the bot serves, and the per-server defaults it uses.",
        "warning": "Guild Settings override the global defaults for one server only.",
        "pages": (
            ("guilds_page", "Servers", "Pick the server you are working on."),
            ("guild_settings", "Guild Settings", "Channels, welcome behaviour and overrides."),
            ("command_status", "Command Status", "Turn slash commands on or off per server."),
            ("public_observability", "Observability", "Public status and uptime view."),
        ),
    },
    {
        "slug": "members",
        "label": "Member Related",
        "icon": "👥",
        "blurb": "Who is in the server, what they have been doing, and how roles are handed out.",
        "warning": "Bulk Role CSV changes roles for many members at once — check the file first.",
        "pages": (
            ("members_page", "Members", "Search members and manage their roles."),
            ("bulk_role_csv", "Bulk Role CSV", "Assign one role from a spreadsheet."),
            ("role_access_page", "Role Access", "Which roles may use which commands."),
        ),
    },
    {
        "slug": "activity",
        "label": "Activity",
        "icon": "📈",
        "blurb": "Who is talking in Discord, who is answering on the forum, and every option that governs both.",
        "warning": "Changing the excluded roles reshapes the rankings; shortening retention deletes hourly history at the next message.",
        "pages": (
            ("activity_page", "Overview", "Discord and forum activity side by side."),
            ("member_activity_page", "Discord Activity", "Message volume and active days per member."),
            ("discourse_activity_page", "Forum Activity", "Who answers, and how quickly."),
            ("discourse_query_page", "Forum Query", "Search the archive: who posted, where and when."),
            ("activity_options_page", "Activity Options", "Every tracking and ingest setting in one form."),
        ),
    },
    {
        "slug": "moderation",
        "label": "Mod & Perms",
        "icon": "🛡️",
        "blurb": "Moderation rules, the actions they took, and the groups that may use them.",
        "warning": "A timeout, ban or role change is applied to Discord immediately and is visible to members.",
        "pages": (
            ("moderation_page", "Moderation", "Warnings, timeouts and bad-word rules."),
            ("permissions_page", "Permissions", "Groups, capabilities and guild scope."),
            ("actions_page", "Action History", "What moderation has already done."),
            ("cases_page", "Cases", "Every warning, filterable and exportable."),
            ("automod_page", "Automod", "Spam rules, raid guard and the escalation ladder."),
        ),
    },
    {
        "slug": "discord",
        "label": "Discord Server Related",
        "icon": "💬",
        "blurb": "Everything that changes how the server itself behaves: translation, forums, invites and reactions.",
        "warning": "These pages post messages and hand out roles as the bot — a mistake is visible in the server.",
        "pages": (
            ("translate_channels_page", "Auto-Translate Channels", "Bridge messages between languages."),
            ("discourse_page", "Forums", "Connect the GL.iNet forum and choose what syncs."),
            ("discourse_viewer.discourse_viewer_page", "Forum Viewer", "Search and read the connected forum."),
            ("invite_links_page", "Invite Link Controls", "Invite codes and the roles they grant."),
            ("reaction_roles_page", "Reaction Roles", "Post a message and hand out roles by reaction."),
            ("post_message_page", "Post as Bot", "Send a message to a channel as the bot."),
            ("tag_responses", "Tag Responses", "Canned replies the bot posts on a tag."),
            ("reddit_auto_responds", "Reddit Auto-Reply", "Replies posted when a subreddit match appears."),
        ),
    },
    {
        "slug": "security",
        "label": "Security & Information",
        "icon": "🔐",
        "blurb": "Defences, logs, monitors and the feeds the bot watches.",
        "warning": "Honeypot actions punish members automatically; check the rule before enabling it.",
        "pages": (
            ("honeypot_page", "Honeypot", "Decoy channels and what happens to whoever posts."),
            ("admin_logs", "Logs", "The bot's own logs."),
            ("service_monitors_page", "Service Monitors", "Checks that services are up."),
            ("uptime_kuma_page", "Uptime Kuma", "Import and sync Uptime Kuma monitors."),
            ("forum_tags", "Forum Tag Cross-Posts", "Push new forum topics into Discord."),
            ("reddit_feeds", "Reddit Feeds", "Subreddits and users the bot watches."),
            ("youtube_subscriptions", "YouTube Subscriptions", "Channels that announce new videos."),
            ("linkedin_subscriptions", "LinkedIn Profiles", "Profiles that announce new posts."),
            ("beta_program_subscriptions", "GL.iNet Beta Programs", "Beta announcements."),
            ("freshdesk_viewer.freshdesk_viewer_page", "Freshdesk", "Read-only customer tickets."),
        ),
    },
)

SECTION_BY_SLUG = {section["slug"]: section for section in SECTIONS}
SECTION_FOR_ENDPOINT = {
    endpoint: section
    for section in SECTIONS
    for endpoint, _label, _description in section["pages"]
}

# Copy for the pages themselves. Keyed by endpoint so the layout can render it without
# every route carrying its own string.
ANNOUNCEMENTS = {
    "dashboard": (
        "This server at a glance",
        "The home page for the selected server. The server's name is at the top; the six "
        "sections below are the binder's tabs, and each one lists the pages that belong to it.",
        "",
    ),
    "account": (
        "Your account",
        "Your display name, email and password. The email must stay valid because it is "
        "the login.",
        "",
    ),
    "bot_profile": (
        "The bot's face",
        "Its display name and avatar. The name is what members see in the member list and on "
        "every message it posts.",
        "Renaming the bot is visible to the whole server immediately.",
    ),
    "settings": (
        "Global settings",
        "Instance-wide configuration and credentials, applied to every server unless a guild "
        "overrides them. Values here are stored in the running container's environment file.",
        "A wrong credential here disables the integration it belongs to — keep a copy before editing.",
    ),
    "users": (
        "Accounts and roles",
        "Who may sign in to this admin, and which group each account belongs to. A group is "
        "the account's capability and server scope; the group picker is under Permissions.",
        "Removing the last Owner locks everyone out of this page.",
    ),
    "documentation": (
        "Documentation",
        "How the admin, the bot and the integrations fit together. Read-only.",
        "",
    ),
    "guilds_page": (
        "Servers",
        "The Discord servers the bot is in. Selecting one sets the context for every other "
        "page; a pinned group may see only one.",
        "",
    ),
    "guild_settings": (
        "This server's settings",
        "Channels, welcome messages and per-server overrides. Anything not set here falls back "
        "to Global Settings.",
        "Channel IDs must belong to this server; a stale ID silently stops a feature from posting.",
    ),
    "command_status": (
        "Slash commands",
        "Turn each command on or off for this server. A disabled command disappears from "
        "Discord's picker for members.",
        "",
    ),
    "public_observability": (
        "Observability",
        "The public status view: which services are up and how long they have been running.",
        "",
    ),
    "members_page": (
        "Members",
        "Search the member list and change roles. Role changes are applied to Discord as soon "
        "as the form is submitted.",
        "The bot can only manage roles below its own highest role.",
    ),
    "activity_page": (
        "Activity",
        "Two features in one place: what members do in Discord, and how staff answer on the "
        "forum. Each card links to the full page, and Activity Options holds every setting "
        "that governs both.",
        "Rankings only count messages the bot can see; a channel it cannot read contributes nothing.",
    ),
    "activity_options_page": (
        "Activity options",
        "The complete option set for Discord tracking and forum ingest: which roles are "
        "excluded, how many rows a ranking shows, how long hourly counts are kept, and which "
        "forum categories, group and schedule feed the statistics.",
        "Retention pruning runs on the next message, so a shorter window takes history with it. "
        "Only the excluded-role list applies without a restart.",
    ),
    "member_activity_page": (
        "Discord activity",
        "Message counts and active days per member over rolling windows. Roles listed as "
        "excluded are hidden from the ranking and from this page's role picker.",
        "Counts are recorded per message; a member who talks only in threads may look quiet here.",
    ),
    "bulk_role_csv": (
        "Bulk role assignment",
        "Upload a spreadsheet of names and give all of them one role in a single run.",
        "This changes many members at once. Check the preview and the role before running it.",
    ),
    "role_access_page": (
        "Role access",
        "Which Discord roles may use which commands. This is the server-side mirror of the "
        "web permission groups.",
        "",
    ),
    "moderation_page": (
        "Moderation",
        "Warnings, timeouts and the bad-word rules that trigger them. A warning can escalate "
        "to a timeout automatically.",
        "Warnings are visible to the member; the rule that fired is recorded in Action History.",
    ),
    "permissions_page": (
        "Permission groups",
        "What an account may do, and on which servers. Capability is per area — read or write — "
        "and scope is set by the group.",
        "You cannot grant more than you hold, and the Owner group cannot be edited.",
    ),
    "automod_page": (
        "Automod",
        "The rules that act on their own: floods, repeats, mention spam, invites and links, "
        "regex patterns, executable attachments, overnight tightening, the raid guard and "
        "the escalation ladder that turns warnings into timeouts, kicks and bans.",
        "Automod is off until it is enabled here, and every action it takes is recorded as "
        "a warning with the reason attached. Check the thresholds before switching it on.",
    ),
    "cases_page": (
        "Cases",
        "Every warning recorded for this server, with the case number a member can quote, "
        "who issued it, and whether it is still active. Filter by member, status or date, "
        "and download the result as CSV.",
        "Clearing a warning is a decision with a name attached, so it is done from the "
        "member page or the /clearwarnings command, not from this table.",
    ),
    "actions_page": (
        "Action history",
        "Everything moderation has already done, newest first, with the actor and the reason.",
        "",
    ),
    "translate_channels_page": (
        "Auto-translate",
        "Bridges a channel so messages are mirrored in another language or channel.",
        "A misconfigured bridge posts twice, in both directions.",
    ),
    "discourse_page": (
        "Forums",
        "The GL.iNet forum connection: base URL, API credentials, and which parts sync to Discord.",
        "The API key is a credential. A wrong username with a valid key is rejected as an invalid key.",
    ),
    "discourse_viewer.discourse_viewer_page": (
        "Forum viewer",
        "Read the connected forum without leaving the admin: search topics and open a "
        "thread to see its posts. Read-only.",
        "",
    ),
    "discourse_query_page": (
        "Forum query",
        "Ask the stored forum archive a question: who posted, in which category, between "
        "two dates, and how fast one person answers. Choose a username for a card with their "
        "threads, replies and median reply delay; leave it blank for everyone.",
        "The archive holds usernames, topics and timestamps only -- no post text and no titles. "
        "Topic links open the real thread on the forum.",
    ),
    "discourse_activity_page": (
        "Forum activity",
        "Staff reply volume, first-response times and hourly coverage from the stored forum "
        "statistics. Categories and the staff group come from Activity Options.",
        "Handled means first staff reply, not resolution; the forum has no resolution signal.",
    ),
    "invite_links_page": (
        "Invite links",
        "Invite codes the bot knows about and the role each one grants on join.",
        "Deleting an invite in Discord leaves its mapping here until it is removed.",
    ),
    "reaction_roles_page": (
        "Reaction roles",
        "Post a message with reactions and hand out roles to whoever reacts. The bot needs "
        "Manage Roles above the roles it grants.",
        "A role above the bot's own highest role can never be granted.",
    ),
    "post_message_page": (
        "Post as the bot",
        "Write a message and pick one channel; it is posted immediately under the bot's "
        "name and avatar. Discord markdown works and typed mentions will ping.",
        "The message is public to everyone who can read that channel, and the bot can only "
        "post where it has Send Messages.",
    ),
    "tag_responses": (
        "Tag responses",
        "Canned replies posted when a tag or keyword appears in a channel.",
        "",
    ),
    "reddit_auto_responds": (
        "Reddit auto-reply",
        "Replies posted when a matching subreddit post appears.",
        "",
    ),
    "honeypot_page": (
        "Honeypot",
        "Decoy channels that only spammers would post in, and the action taken when they do.",
        "The action is automatic and can ban. Test the rule before enabling it.",
    ),
    "admin_logs": (
        "Logs",
        "The bot's own log files, for diagnosing what it did and why.",
        "",
    ),
    "service_monitors_page": (
        "Service monitors",
        "HTTP checks that alert when a service stops answering.",
        "",
    ),
    "uptime_kuma_page": (
        "Uptime Kuma",
        "Import monitors from, or sync them to, an Uptime Kuma instance.",
        "A sync can remove monitors that only exist on one side — review the diff first.",
    ),
    "forum_tags": (
        "Forum tag cross-posts",
        "Push new forum topics carrying a tag into a Discord channel.",
        "",
    ),
    "reddit_feeds": (
        "Reddit feeds",
        "Subreddits and users whose new posts are announced.",
        "",
    ),
    "youtube_subscriptions": (
        "YouTube subscriptions",
        "Channels that announce new videos.",
        "",
    ),
    "linkedin_subscriptions": (
        "LinkedIn profiles",
        "Profiles that announce new posts.",
        "",
    ),
    "beta_program_subscriptions": (
        "GL.iNet beta programs",
        "Beta announcements and the channels they go to.",
        "",
    ),
}


def section_for_endpoint(endpoint: str) -> dict | None:
    return SECTION_FOR_ENDPOINT.get(str(endpoint or ""))


def announcement_for(endpoint: str) -> tuple[str, str, str] | None:
    return ANNOUNCEMENTS.get(str(endpoint or ""))


__all__ = [
    "ANNOUNCEMENTS",
    "SECTIONS",
    "SECTION_BY_SLUG",
    "SECTION_FOR_ENDPOINT",
    "announcement_for",
    "section_for_endpoint",
]

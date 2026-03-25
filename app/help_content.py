from __future__ import annotations

HELP_COMMAND_ALIASES = {
    "list": "list",
    "help": "help",
    "ping": "ping",
    "sayhi": "sayhi",
    "happy": "happy",
    "coin_flip": "coin_flip",
    "coinflip": "coin_flip",
    "eight_ball": "eight_ball",
    "8ball": "eight_ball",
    "eightball": "eight_ball",
    "meme": "meme",
    "dad_joke": "dad_joke",
    "dadjoke": "dad_joke",
    "shorten": "shorten",
    "expand": "expand",
    "uptime": "uptime",
    "stats": "stats",
    "tag": "tag_commands",
    "tags": "tag_commands",
    "submitrole": "submitrole",
    "restore_code": "restore_code",
    "enter_role": "enter_role",
    "enterrole": "enter_role",
    "getaccess": "getaccess",
    "bulk_assign_role_csv": "bulk_assign_role_csv",
    "bulkassignrolecsv": "bulk_assign_role_csv",
    "country": "country",
    "clear_country": "clear_country",
    "clearcountry": "clear_country",
    "search_reddit": "search_reddit",
    "searchreddit": "search_reddit",
    "search_forum": "search_forum",
    "searchforum": "search_forum",
    "search_openwrt_forum": "search_openwrt_forum",
    "searchopenwrtforum": "search_openwrt_forum",
    "search_kvm": "search_kvm",
    "searchkvm": "search_kvm",
    "search_iot": "search_iot",
    "searchiot": "search_iot",
    "search_router": "search_router",
    "searchrouter": "search_router",
    "search_astrowarp": "search_astrowarp",
    "searchastrowarp": "search_astrowarp",
    "create_role": "create_role",
    "createrole": "create_role",
    "edit_role": "edit_role",
    "editrole": "edit_role",
    "delete_role": "delete_role",
    "deleterole": "delete_role",
    "add_role_member": "add_role_member",
    "addrolemember": "add_role_member",
    "remove_role_member": "remove_role_member",
    "removerolemember": "remove_role_member",
    "ban_member": "ban_member",
    "banmember": "ban_member",
    "unban_member": "unban_member",
    "unbanmember": "unban_member",
    "kick_member": "kick_member",
    "kickmember": "kick_member",
    "timeout_member": "timeout_member",
    "timeoutmember": "timeout_member",
    "untimeout_member": "untimeout_member",
    "untimeoutmember": "untimeout_member",
    "prune_messages": "prune_messages",
    "prune": "prune_messages",
    "modlog_test": "modlog_test",
    "modlogtest": "modlog_test",
    "logs": "logs",
    "random_choice": "random_choice",
    "randomchoice": "random_choice",
}

HELP_WIKI_PAGE_BY_COMMAND = {
    "help": ["Command-Reference.md", "Search-and-Docs.md"],
    "list": ["Tag-Responses.md", "Command-Reference.md"],
    "tag_commands": ["Tag-Responses.md", "Command-Reference.md"],
    "submitrole": ["Role-Access-and-Invites.md", "Command-Reference.md"],
    "restore_code": ["Role-Access-and-Invites.md", "Command-Reference.md"],
    "enter_role": ["Role-Access-and-Invites.md", "Command-Reference.md"],
    "getaccess": ["Role-Access-and-Invites.md", "Command-Reference.md"],
    "bulk_assign_role_csv": ["Bulk-CSV-Role-Assignment.md", "Command-Reference.md"],
    "search_reddit": ["Search-and-Docs.md", "Command-Reference.md"],
    "search_forum": ["Search-and-Docs.md", "Command-Reference.md"],
    "search_openwrt_forum": ["Search-and-Docs.md", "Command-Reference.md"],
    "search_kvm": ["Search-and-Docs.md", "Command-Reference.md"],
    "search_iot": ["Search-and-Docs.md", "Command-Reference.md"],
    "search_router": ["Search-and-Docs.md", "Command-Reference.md"],
    "search_astrowarp": ["Search-and-Docs.md", "Command-Reference.md"],
    "country": ["Country-Code-Commands.md", "Command-Reference.md"],
    "clear_country": ["Country-Code-Commands.md", "Command-Reference.md"],
    "create_role": ["Moderation-and-Logs.md", "Command-Reference.md"],
    "edit_role": ["Moderation-and-Logs.md", "Command-Reference.md"],
    "delete_role": ["Moderation-and-Logs.md", "Command-Reference.md"],
    "add_role_member": ["Moderation-and-Logs.md", "Command-Reference.md"],
    "remove_role_member": ["Moderation-and-Logs.md", "Command-Reference.md"],
    "ban_member": ["Moderation-and-Logs.md", "Command-Reference.md"],
    "unban_member": ["Moderation-and-Logs.md", "Command-Reference.md"],
    "kick_member": ["Moderation-and-Logs.md", "Command-Reference.md"],
    "timeout_member": ["Moderation-and-Logs.md", "Command-Reference.md"],
    "untimeout_member": ["Moderation-and-Logs.md", "Command-Reference.md"],
    "prune_messages": ["Moderation-and-Logs.md", "Command-Reference.md"],
    "modlog_test": ["Moderation-and-Logs.md", "Command-Reference.md"],
    "logs": ["Moderation-and-Logs.md", "Command-Reference.md"],
    "random_choice": ["Moderation-and-Logs.md", "Command-Reference.md"],
}


def build_wiki_page_url(page_name: str, *, bot_help_wiki_url: str, bot_help_wiki_root_url: str):
    cleaned_page_name = str(page_name or "").strip().lstrip("/")
    if not cleaned_page_name:
        return bot_help_wiki_url
    if "/wiki" in bot_help_wiki_root_url and "/blob/" not in bot_help_wiki_root_url:
        cleaned_page_name = cleaned_page_name.removesuffix(".md")
    return f"{bot_help_wiki_root_url}/{cleaned_page_name}"


def normalize_help_command_name(raw_value: str | None):
    cleaned = str(raw_value or "").strip().lower()
    if not cleaned:
        return ""
    cleaned = cleaned.lstrip("/!")
    return HELP_COMMAND_ALIASES.get(cleaned, cleaned)


def command_default_access_label(
    command_key: str,
    *,
    command_permission_defaults: dict[str, str],
    moderator_policy_value: str,
):
    default_policy = command_permission_defaults.get(command_key)
    if default_policy == moderator_policy_value:
        return "Moderator"
    return "Member/Public"


def build_help_wiki_links(command_key: str, *, bot_help_wiki_url: str, bot_help_wiki_root_url: str):
    page_names = HELP_WIKI_PAGE_BY_COMMAND.get(command_key, ["Command-Reference.md"])
    seen_pages = set()
    links = []
    for page_name in page_names:
        if page_name in seen_pages:
            continue
        seen_pages.add(page_name)
        links.append(
            (
                page_name.replace(".md", "").replace("-", " "),
                build_wiki_page_url(
                    page_name,
                    bot_help_wiki_url=bot_help_wiki_url,
                    bot_help_wiki_root_url=bot_help_wiki_root_url,
                ),
            )
        )
    if ("Wiki Home", bot_help_wiki_url) not in links:
        links.append(("Wiki Home", bot_help_wiki_url))
    return links


def suppress_discord_link_embed(url: str):
    text = str(url or "").strip()
    if not text.startswith(("http://", "https://")):
        return text
    return f"<{text}>"


def build_help_message_for_command(
    command_name: str | None,
    *,
    bot_public_name: str,
    bot_help_wiki_url: str,
    bot_help_wiki_root_url: str,
    command_permission_defaults: dict[str, str],
    moderator_policy_value: str,
    command_permission_metadata: dict[str, dict[str, str]],
):
    normalized_command = normalize_help_command_name(command_name)
    if not normalized_command:
        return "\n".join(
            [
                f"🤖 **{bot_public_name} Help**",
                "",
                "Use `/help command:<name>` for details on a specific command.",
                "",
                "Common command groups:",
                "- Role access and invites (`/submitrole`, `/enter_role`, `/restore_code`, `/getaccess`)",
                "- Search (`/search_reddit`, `/search_forum`, `/search_openwrt_forum`, `/search_kvm`, `/search_iot`, `/search_router`, `/search_astrowarp`)",
                "- Utilities (`/ping`, `/sayhi`, `/happy`, `/coin_flip`, `/eight_ball`, `/meme`, `/dad_joke`, `/shorten`, `/expand`, `/uptime`, `/stats`)",
                "- Country nickname tools (`/country`, `/clear_country`)",
                "- Moderation and role management (`/ban_member`, `/kick_member`, `/timeout_member`, `/create_role`, `/random_choice`)",
                "",
                "Docs:",
                f"- Command Reference: {suppress_discord_link_embed(build_wiki_page_url('Command-Reference.md', bot_help_wiki_url=bot_help_wiki_url, bot_help_wiki_root_url=bot_help_wiki_root_url))}",
                f"- Search and Docs: {suppress_discord_link_embed(build_wiki_page_url('Search-and-Docs.md', bot_help_wiki_url=bot_help_wiki_url, bot_help_wiki_root_url=bot_help_wiki_root_url))}",
                f"- Role Access and Invites: {suppress_discord_link_embed(build_wiki_page_url('Role-Access-and-Invites.md', bot_help_wiki_url=bot_help_wiki_url, bot_help_wiki_root_url=bot_help_wiki_root_url))}",
                f"- Moderation and Logs: {suppress_discord_link_embed(build_wiki_page_url('Moderation-and-Logs.md', bot_help_wiki_url=bot_help_wiki_url, bot_help_wiki_root_url=bot_help_wiki_root_url))}",
                f"- Wiki Home: {suppress_discord_link_embed(bot_help_wiki_url)}",
            ]
        )

    metadata = command_permission_metadata.get(normalized_command)
    if metadata is None and normalized_command not in {"list"}:
        return "\n".join(
            [
                f"❌ I do not have help details for `{command_name}`.",
                f"Use `/help` for the overview or check the full command reference: {suppress_discord_link_embed(build_wiki_page_url('Command-Reference.md', bot_help_wiki_url=bot_help_wiki_url, bot_help_wiki_root_url=bot_help_wiki_root_url))}",
            ]
        )

    label = metadata["label"] if metadata else "!list"
    description = metadata["description"] if metadata else "Lists configured tag commands."
    wiki_links = build_help_wiki_links(
        normalized_command,
        bot_help_wiki_url=bot_help_wiki_url,
        bot_help_wiki_root_url=bot_help_wiki_root_url,
    )
    lines = [
        f"🤖 **Help: {label}**",
        f"- Default Access: `{command_default_access_label(normalized_command, command_permission_defaults=command_permission_defaults, moderator_policy_value=moderator_policy_value)}`",
        f"- Description: {description}",
        "",
        "More info:",
    ]
    for link_label, link_url in wiki_links:
        lines.append(f"- {link_label}: {suppress_discord_link_embed(link_url)}")
    return "\n".join(lines)

from app.help_content import build_help_message_for_command


def test_help_content_overview_includes_docs():
    message = build_help_message_for_command(
        None,
        bot_public_name="GL.iNet UnOfficial Discord Bot",
        bot_help_wiki_url="https://example.com/wiki/Home.md",
        bot_help_wiki_root_url="https://example.com/wiki",
        command_permission_defaults={"help": "public", "ban_member": "moderator"},
        moderator_policy_value="moderator",
        command_permission_metadata={
            "help": {"label": "/help", "description": "Show help"},
            "ban_member": {"label": "/ban_member", "description": "Ban a member"},
        },
    )
    assert "GL.iNet UnOfficial Discord Bot Help" in message
    assert "Command Reference" in message
    assert "Role Access and Invites" in message


def test_help_content_command_specific_entry():
    message = build_help_message_for_command(
        "ban_member",
        bot_public_name="GL.iNet UnOfficial Discord Bot",
        bot_help_wiki_url="https://example.com/wiki/Home.md",
        bot_help_wiki_root_url="https://example.com/wiki",
        command_permission_defaults={"ban_member": "moderator"},
        moderator_policy_value="moderator",
        command_permission_metadata={
            "ban_member": {"label": "/ban_member", "description": "Ban a member"},
        },
    )
    assert "Help: /ban_member" in message
    assert "Default Access: `Moderator`" in message
    assert "Moderation and Logs" in message

import os
import re
import time
import zipfile
from io import BytesIO
from pathlib import Path

from bs4 import BeautifulSoup

import web_admin
from app.service_monitor import GLINET_DOMAIN_MONITOR_PRESETS, normalize_service_monitor_targets
from web_admin import create_web_app


def _make_app(tmp_path: Path):
    env_file = tmp_path / "env.env"
    env_file.write_text(
        "\n".join(
            [
                "WEB_ENFORCE_CSRF=false",
                "WEB_ENFORCE_SAME_ORIGIN_POSTS=false",
                "WEB_TRUST_PROXY_HEADERS=true",
            ]
        )
    )

    def guilds():
        return {
            "ok": True,
            "guilds": [
                {
                    "id": "1234567890",
                    "name": "Test Guild",
                    "member_count": 42,
                    "icon_url": "",
                    "is_primary": True,
                },
                {
                    "id": "2222222222",
                    "name": "Support Guild",
                    "member_count": 17,
                    "icon_url": "",
                    "is_primary": False,
                }
            ],
            "primary_guild_id": "1234567890",
        }

    def catalog(guild_id):
        guild_name = "Support Guild" if str(guild_id) == "2222222222" else "Test Guild"
        return {
            "ok": True,
            "guild": {"id": str(guild_id), "name": guild_name},
            "channels": [
                {
                    "id": "9999",
                    "name": "alerts",
                    "type": "text",
                    "label": "#alerts [text]",
                }
            ],
            "roles": [
                {"id": "111", "name": "Member", "label": "@Member"},
                {"id": "222", "name": "Employee", "label": "@Employee"},
            ],
        }

    guild_settings_state = {
        "bot_log_channel_id": "",
        "mod_log_channel_id": "",
        "firmware_notify_channel_id": "",
        "bad_words_enabled": "0",
        "bad_words_list_json": "[]",
        "bad_words_warning_window_hours": "72",
        "bad_words_warning_threshold": "3",
        "bad_words_action": "timeout",
        "bad_words_timeout_minutes": "60",
        "firmware_monitor_enabled": "-1",
        "reddit_feed_notify_enabled": "-1",
        "youtube_notify_enabled": "-1",
        "linkedin_notify_enabled": "-1",
        "beta_program_notify_enabled": "-1",
        "discourse_enabled": "-1",
        "discourse_base_url": "",
        "discourse_api_key": "",
        "discourse_api_username": "",
        "discourse_profile_name": "",
        "discourse_request_timeout_seconds": "15",
        "discourse_features_json": '[\"search\",\"topic_lookup\",\"categories\"]',
        "discourse_api_key_configured": "",
        "access_role_id": "",
        "welcome_channel_id": "",
        "welcome_dm_enabled": "",
        "welcome_channel_image_enabled": "",
        "welcome_dm_image_enabled": "",
        "welcome_channel_message": "",
        "welcome_dm_message": "",
        "welcome_image_filename": "",
        "welcome_image_media_type": "",
        "welcome_image_size_bytes": "",
        "welcome_image_width": "",
        "welcome_image_height": "",
        "welcome_image_configured": "",
    }

    def get_guild_settings(guild_id):
        settings = dict(guild_settings_state)
        return {
            "ok": True,
            "guild_id": str(guild_id),
            "settings": settings,
            "effective": settings,
        }

    def save_guild_settings(payload, actor_email, guild_id):
        guild_settings_state.update(
            {
                "bot_log_channel_id": str(payload.get("bot_log_channel_id") or "").strip(),
                "mod_log_channel_id": str(payload.get("mod_log_channel_id") or "").strip(),
                "firmware_notify_channel_id": str(payload.get("firmware_notify_channel_id") or "").strip(),
                "bad_words_enabled": str(payload.get("bad_words_enabled", guild_settings_state.get("bad_words_enabled", "0"))).strip(),
                "bad_words_list_json": str(payload.get("bad_words_list_json", guild_settings_state.get("bad_words_list_json", "[]"))).strip(),
                "bad_words_warning_window_hours": str(payload.get("bad_words_warning_window_hours", guild_settings_state.get("bad_words_warning_window_hours", "72"))).strip(),
                "bad_words_warning_threshold": str(payload.get("bad_words_warning_threshold", guild_settings_state.get("bad_words_warning_threshold", "3"))).strip(),
                "bad_words_action": str(payload.get("bad_words_action", guild_settings_state.get("bad_words_action", "timeout"))).strip(),
                "bad_words_timeout_minutes": str(payload.get("bad_words_timeout_minutes", guild_settings_state.get("bad_words_timeout_minutes", "60"))).strip(),
                "firmware_monitor_enabled": str(payload.get("firmware_monitor_enabled", guild_settings_state.get("firmware_monitor_enabled", "-1"))),
                "reddit_feed_notify_enabled": str(payload.get("reddit_feed_notify_enabled", guild_settings_state.get("reddit_feed_notify_enabled", "-1"))),
                "youtube_notify_enabled": str(payload.get("youtube_notify_enabled", guild_settings_state.get("youtube_notify_enabled", "-1"))),
                "linkedin_notify_enabled": str(payload.get("linkedin_notify_enabled", guild_settings_state.get("linkedin_notify_enabled", "-1"))),
                "beta_program_notify_enabled": str(payload.get("beta_program_notify_enabled", guild_settings_state.get("beta_program_notify_enabled", "-1"))),
                "discourse_enabled": str(payload.get("discourse_enabled", guild_settings_state.get("discourse_enabled", "-1"))),
                "discourse_base_url": str(payload.get("discourse_base_url", guild_settings_state.get("discourse_base_url", ""))).strip(),
                "discourse_api_username": str(payload.get("discourse_api_username", guild_settings_state.get("discourse_api_username", ""))).strip(),
                "discourse_profile_name": str(payload.get("discourse_profile_name", guild_settings_state.get("discourse_profile_name", ""))).strip(),
                "discourse_request_timeout_seconds": str(
                    payload.get("discourse_request_timeout_seconds", guild_settings_state.get("discourse_request_timeout_seconds", "15"))
                ).strip(),
                "discourse_features_json": str(
                    payload.get("discourse_features_json", guild_settings_state.get("discourse_features_json", '[\"search\",\"topic_lookup\",\"categories\"]'))
                ).strip(),
                "access_role_id": str(payload.get("access_role_id") or "").strip(),
                "welcome_channel_id": str(payload.get("welcome_channel_id") or "").strip(),
                "welcome_dm_enabled": str(payload.get("welcome_dm_enabled") or "").strip(),
                "welcome_channel_image_enabled": str(payload.get("welcome_channel_image_enabled") or "").strip(),
                "welcome_dm_image_enabled": str(payload.get("welcome_dm_image_enabled") or "").strip(),
                "welcome_channel_message": str(payload.get("welcome_channel_message") or "").strip(),
                "welcome_dm_message": str(payload.get("welcome_dm_message") or "").strip(),
                "welcome_image_filename": str(payload.get("welcome_image_filename") or "").strip(),
                "welcome_image_media_type": str(payload.get("welcome_image_media_type") or "").strip(),
                "welcome_image_size_bytes": str(payload.get("welcome_image_size_bytes") or "").strip(),
                "welcome_image_width": str(payload.get("welcome_image_width") or "").strip(),
                "welcome_image_height": str(payload.get("welcome_image_height") or "").strip(),
                "welcome_image_configured": "1" if payload.get("welcome_image_bytes") else (
                    "" if str(payload.get("welcome_image_remove") or "").strip() else guild_settings_state.get("welcome_image_configured", "")
                ),
            }
        )
        if "discourse_api_key" in payload:
            guild_settings_state["discourse_api_key"] = str(payload.get("discourse_api_key") or "").strip()
        elif str(payload.get("discourse_api_key_clear") or "").strip():
            guild_settings_state["discourse_api_key"] = ""
        guild_settings_state["discourse_api_key_configured"] = "1" if guild_settings_state.get("discourse_api_key") else ""
        if str(payload.get("welcome_image_remove") or "").strip():
            guild_settings_state["welcome_image_filename"] = ""
            guild_settings_state["welcome_image_media_type"] = ""
            guild_settings_state["welcome_image_configured"] = ""
        settings = dict(guild_settings_state)
        return {
            "ok": True,
            "guild_id": str(guild_id),
            "message": f"Guild settings updated by {actor_email}.",
            "settings": settings,
            "effective": settings,
        }

    bot_profile_updates = []
    bot_profile_state = {
        "id": "1478110480806576259",
        "name": "GL.iNet UnOfficial Discord Bot",
        "display_name": "GL.iNet UnOfficial Discord Bot",
        "global_name": "GL.iNet UnOfficial Discord Bot",
        "guild_id": "1234567890",
        "guild_name": "Test Guild",
        "server_display_name": "GL.iNet UnOfficial Discord Bot",
        "server_nickname": "",
        "avatar_url": "",
    }

    def get_bot_profile(guild_id):
        return {"ok": True, **bot_profile_state, "guild_id": str(guild_id), "guild_name": "Test Guild"}

    def update_bot_profile(guild_id, username, server_nickname, clear_server_nickname, actor_email):
        bot_profile_updates.append(
            {
                "guild_id": str(guild_id),
                "username": username,
                "server_nickname": server_nickname,
                "clear_server_nickname": clear_server_nickname,
                "actor_email": actor_email,
            }
        )
        if username is not None:
            bot_profile_state["name"] = username
            bot_profile_state["display_name"] = username
            bot_profile_state["global_name"] = username
            bot_profile_state["server_display_name"] = bot_profile_state.get("server_nickname") or username
            return {
                "ok": True,
                "message": "Updated username.",
                **bot_profile_state,
            }
        if clear_server_nickname:
            bot_profile_state["server_nickname"] = ""
        elif server_nickname is not None:
            bot_profile_state["server_nickname"] = server_nickname
        bot_profile_state["server_display_name"] = bot_profile_state.get("server_nickname") or bot_profile_state["name"]
        return {
            "ok": True,
            "message": "Updated server nickname.",
            **bot_profile_state,
        }

    reddit_feeds_state = [
        {
            "id": 1,
            "guild_id": 1234567890,
            "subreddit": "glinet",
            "channel_id": 9999,
            "enabled": True,
            "created_at": "2026-03-01T00:00:00+00:00",
            "updated_at": "2026-03-01T00:00:00+00:00",
            "created_by_email": "admin@example.com",
            "updated_by_email": "admin@example.com",
            "last_checked_at": "",
            "last_posted_at": "",
            "last_error": "",
        }
    ]

    def get_reddit_feeds(guild_id):
        return {"ok": True, "feeds": [dict(item) for item in reddit_feeds_state if int(item["guild_id"]) == int(guild_id)]}

    def manage_reddit_feeds(payload, actor_email, guild_id):
        action = str(payload.get("action") or "").strip().lower()
        safe_guild_id = int(guild_id)
        if action == "add":
            reddit_feeds_state.append(
                {
                    "id": max((item["id"] for item in reddit_feeds_state), default=0) + 1,
                    "guild_id": safe_guild_id,
                    "subreddit": str(payload.get("subreddit") or "").strip().lower(),
                    "channel_id": int(str(payload.get("channel_id") or "0")),
                    "enabled": True,
                    "created_at": "",
                    "updated_at": "",
                    "created_by_email": actor_email,
                    "updated_by_email": actor_email,
                    "last_checked_at": "",
                    "last_posted_at": "",
                    "last_error": "",
                }
            )
            return get_reddit_feeds(safe_guild_id) | {"message": "Reddit feed saved."}
        if action == "edit":
            feed_id = int(str(payload.get("feed_id") or "0"))
            for item in reddit_feeds_state:
                if item["id"] == feed_id and int(item["guild_id"]) == safe_guild_id:
                    item["subreddit"] = str(payload.get("subreddit") or "").strip().lower()
                    item["channel_id"] = int(str(payload.get("channel_id") or "0"))
                    item["updated_by_email"] = actor_email
                    return get_reddit_feeds(safe_guild_id) | {"message": "Reddit feed updated."}
            return {"ok": False, "error": "Reddit feed entry was not found."}
        if action == "toggle":
            feed_id = int(str(payload.get("feed_id") or "0"))
            for item in reddit_feeds_state:
                if item["id"] == feed_id and int(item["guild_id"]) == safe_guild_id:
                    item["enabled"] = str(payload.get("enabled") or "").strip() in {"1", "true", "True"}
                    return get_reddit_feeds(safe_guild_id) | {"message": "Reddit feed updated."}
            return {"ok": False, "error": "Reddit feed entry was not found."}
        if action == "delete":
            feed_id = int(str(payload.get("feed_id") or "0"))
            reddit_feeds_state[:] = [item for item in reddit_feeds_state if not (item["id"] == feed_id and int(item["guild_id"]) == safe_guild_id)]
            return get_reddit_feeds(safe_guild_id) | {"message": "Reddit feed deleted."}
        return {"ok": False, "error": "Invalid Reddit feed action."}

    youtube_subscriptions_state = [
        {
            "id": 1,
            "guild_id": 1234567890,
            "source_url": "https://www.youtube.com/@glinet",
            "channel_id": "UC123",
            "channel_title": "GL.iNet",
            "target_channel_id": 9999,
            "target_channel_name": "#alerts",
            "last_video_id": "vid1",
            "last_video_title": "Latest Video",
            "last_published_at": "2026-03-20T00:00:00+00:00",
            "enabled": True,
            "created_at": "",
            "updated_at": "",
            "created_by_email": "admin@example.com",
            "updated_by_email": "admin@example.com",
        }
    ]

    def get_youtube_subscriptions(guild_id):
        return {"ok": True, "subscriptions": [dict(item) for item in youtube_subscriptions_state if int(item["guild_id"]) == int(guild_id)]}

    def manage_youtube_subscriptions(payload, actor_email, guild_id):
        action = str(payload.get("action") or "").strip().lower()
        safe_guild_id = int(guild_id)
        if action == "edit":
            subscription_id = int(str(payload.get("subscription_id") or "0"))
            for item in youtube_subscriptions_state:
                if item["id"] == subscription_id and int(item["guild_id"]) == safe_guild_id:
                    item["source_url"] = str(payload.get("source_url") or "").strip()
                    item["target_channel_id"] = int(str(payload.get("channel_id") or "0"))
                    item["target_channel_name"] = "#alerts"
                    item["updated_by_email"] = actor_email
                    return get_youtube_subscriptions(safe_guild_id) | {"message": "YouTube subscription updated."}
            return {"ok": False, "error": "YouTube subscription entry was not found."}
        if action == "delete":
            subscription_id = int(str(payload.get("subscription_id") or "0"))
            youtube_subscriptions_state[:] = [item for item in youtube_subscriptions_state if not (item["id"] == subscription_id and int(item["guild_id"]) == safe_guild_id)]
            return get_youtube_subscriptions(safe_guild_id) | {"message": "YouTube subscription deleted."}
        if action == "add":
            youtube_subscriptions_state.append(
                {
                    "id": max((item["id"] for item in youtube_subscriptions_state), default=0) + 1,
                    "guild_id": safe_guild_id,
                    "source_url": str(payload.get("source_url") or "").strip(),
                    "channel_id": "UCNEW",
                    "channel_title": "New Channel",
                    "target_channel_id": int(str(payload.get("channel_id") or "0")),
                    "target_channel_name": "#alerts",
                    "last_video_id": "",
                    "last_video_title": "",
                    "last_published_at": "",
                    "enabled": True,
                    "created_at": "",
                    "updated_at": "",
                    "created_by_email": actor_email,
                    "updated_by_email": actor_email,
                }
            )
            return get_youtube_subscriptions(safe_guild_id) | {"message": "YouTube subscription saved."}
        return {"ok": False, "error": "Invalid YouTube subscription action."}

    linkedin_subscriptions_state = [
        {
            "id": 1,
            "guild_id": 1234567890,
            "source_url": "https://www.linkedin.com/in/glinet",
            "profile_name": "GL.iNet",
            "target_channel_id": 9999,
            "target_channel_name": "#alerts",
            "last_post_id": "post1",
            "last_post_url": "https://www.linkedin.com/posts/example",
            "last_post_text": "Recent update text",
            "last_published_at": "2026-03-20T00:00:00+00:00",
            "last_checked_at": "2026-03-20T01:00:00+00:00",
            "last_posted_at": "2026-03-20T01:00:00+00:00",
            "last_error": "",
            "enabled": True,
            "created_at": "",
            "updated_at": "",
            "created_by_email": "admin@example.com",
            "updated_by_email": "admin@example.com",
        }
    ]

    def get_linkedin_subscriptions(guild_id):
        return {"ok": True, "subscriptions": [dict(item) for item in linkedin_subscriptions_state if int(item["guild_id"]) == int(guild_id)]}

    def manage_linkedin_subscriptions(payload, actor_email, guild_id):
        action = str(payload.get("action") or "").strip().lower()
        safe_guild_id = int(guild_id)
        if action == "edit":
            subscription_id = int(str(payload.get("subscription_id") or "0"))
            for item in linkedin_subscriptions_state:
                if item["id"] == subscription_id and int(item["guild_id"]) == safe_guild_id:
                    item["source_url"] = str(payload.get("source_url") or "").strip()
                    item["target_channel_id"] = int(str(payload.get("channel_id") or "0"))
                    item["target_channel_name"] = "#alerts"
                    item["updated_by_email"] = actor_email
                    return get_linkedin_subscriptions(safe_guild_id) | {"message": "LinkedIn subscription updated."}
            return {"ok": False, "error": "LinkedIn subscription entry was not found."}
        if action == "delete":
            subscription_id = int(str(payload.get("subscription_id") or "0"))
            linkedin_subscriptions_state[:] = [item for item in linkedin_subscriptions_state if not (item["id"] == subscription_id and int(item["guild_id"]) == safe_guild_id)]
            return get_linkedin_subscriptions(safe_guild_id) | {"message": "LinkedIn subscription deleted."}
        if action == "add":
            linkedin_subscriptions_state.append(
                {
                    "id": max((item["id"] for item in linkedin_subscriptions_state), default=0) + 1,
                    "guild_id": safe_guild_id,
                    "source_url": str(payload.get("source_url") or "").strip(),
                    "profile_name": "New Profile",
                    "target_channel_id": int(str(payload.get("channel_id") or "0")),
                    "target_channel_name": "#alerts",
                    "last_post_id": "",
                    "last_post_url": "",
                    "last_post_text": "",
                    "last_published_at": "",
                    "last_checked_at": "",
                    "last_posted_at": "",
                    "last_error": "",
                    "enabled": True,
                    "created_at": "",
                    "updated_at": "",
                    "created_by_email": actor_email,
                    "updated_by_email": actor_email,
                }
            )
            return get_linkedin_subscriptions(safe_guild_id) | {"message": "LinkedIn subscription saved."}
        return {"ok": False, "error": "Invalid LinkedIn subscription action."}

    role_access_state = [
        {
            "guild_id": 1234567890,
            "code": "531580",
            "invite_code": "Xjkd246SYq",
            "invite_url": "https://discord.gg/Xjkd246SYq",
            "role_id": 111,
            "created_at": "2026-03-20T00:00:00+00:00",
            "updated_at": "2026-03-20T00:00:00+00:00",
            "status": "active",
        }
    ]

    def get_role_access_mappings(guild_id):
        safe_guild_id = int(guild_id)
        return {"ok": True, "mappings": [dict(item) for item in role_access_state if int(item["guild_id"]) == safe_guild_id]}

    def manage_role_access_mappings(payload, actor_email, guild_id):
        safe_guild_id = int(guild_id)
        action = str(payload.get("action") or "").strip().lower()
        if action == "set_status":
            code = str(payload.get("code") or "").strip()
            invite_code = str(payload.get("invite") or "").strip()
            status = str(payload.get("status") or "").strip().lower()
            for item in role_access_state:
                if int(item["guild_id"]) == safe_guild_id and item["code"] == code and item["invite_code"] == invite_code:
                    item["status"] = status
                    return get_role_access_mappings(safe_guild_id) | {"message": f"Role access mapping marked {status}."}
            return {"ok": False, "error": "Role access mapping was not found."}
        if action == "save":
            code = str(payload.get("code") or "").strip()
            invite_code = str(payload.get("invite") or "").strip()
            role_id = int(str(payload.get("role_id") or "0"))
            status = str(payload.get("status") or "active").strip().lower() or "active"
            for item in role_access_state:
                if int(item["guild_id"]) == safe_guild_id and item["code"] == code:
                    item.update(
                        {
                            "invite_code": invite_code,
                            "invite_url": f"https://discord.gg/{invite_code}",
                            "role_id": role_id,
                            "status": status,
                        }
                    )
                    return get_role_access_mappings(safe_guild_id) | {"message": "Role access mapping saved."}
            role_access_state.append(
                {
                    "guild_id": safe_guild_id,
                    "code": code,
                    "invite_code": invite_code,
                    "invite_url": f"https://discord.gg/{invite_code}",
                    "role_id": role_id,
                    "created_at": "",
                    "updated_at": "",
                    "status": status,
                }
            )
            return get_role_access_mappings(safe_guild_id) | {"message": "Role access mapping saved."}
        return {"ok": False, "error": "Invalid role access action."}

    app = create_web_app(
        data_dir=str(tmp_path),
        env_file_path=str(env_file),
        tag_responses_file=str(tmp_path / "tags.json"),
        default_admin_email="admin@example.com",
        default_admin_password="Ab!12xy",
        on_get_guilds=guilds,
        on_get_discord_catalog=catalog,
        on_get_guild_settings=get_guild_settings,
        on_save_guild_settings=save_guild_settings,
        on_get_bot_profile=get_bot_profile,
        on_update_bot_profile=update_bot_profile,
        on_get_actions=lambda guild_id: {
            "ok": True,
            "actions": [
                {
                    "created_at": "2026-03-15T00:00:00+00:00",
                    "action": "test_action",
                    "status": "success",
                    "moderator": "admin@example.com",
                    "target": "target",
                    "reason": "reason",
                }
            ],
        },
        on_get_member_activity=lambda guild_id, role_id=None: {
            "ok": True,
            "top_limit": 20,
            "selected_role_id": int(role_id or 0),
            "excluded_role_ids": [],
            "excluded_role_names": ["Employee", "Admin", "Gl.iNet Moderator"],
            "windows": [
                {
                    "key": "last_180_days",
                    "label": "Last 180 Days",
                    "members": [
                        {
                            "rank": 1,
                            "display_name": "Tester",
                            "username": "tester",
                            "message_count": 456,
                            "active_days": 42,
                            "last_message_at": "2026-03-15T00:00:00+00:00",
                        }
                    ],
                },
                {
                    "key": "last_90_days",
                    "label": "Last 90 Days",
                    "members": [
                        {
                            "rank": 1,
                            "display_name": "Tester",
                            "username": "tester",
                            "message_count": 123,
                            "active_days": 14,
                            "last_message_at": "2026-03-15T00:00:00+00:00",
                        }
                    ],
                }
            ],
        },
        on_export_member_activity=lambda guild_id, role_id=None: {
            "ok": True,
            "filename": "member_activity_test.zip",
            "content_type": "application/zip",
            "data": b"PK\x05\x06" + (b"\x00" * 18),
        },
        on_get_reddit_feeds=get_reddit_feeds,
        on_manage_reddit_feeds=manage_reddit_feeds,
        on_get_command_permissions=lambda guild_id: {
            "ok": True,
            "commands": [
                {
                    "key": "ping",
                    "label": "/ping",
                    "description": "Check that the bot is online.",
                    "default_policy": "public",
                    "default_policy_label": "Public",
                    "mode": "disabled",
                    "role_ids": [],
                },
                {
                    "key": "ban_member",
                    "label": "/ban_member",
                    "description": "Ban a member from the server.",
                    "default_policy": "moderator_role_ids",
                    "default_policy_label": "Mod Only",
                    "mode": "default",
                    "role_ids": [],
                },
                {
                    "key": "help",
                    "label": "/help",
                    "description": "Show command help.",
                    "default_policy": "public",
                    "default_policy_label": "Public",
                    "mode": "public",
                    "role_ids": [],
                }
            ],
            "allowed_role_names": ["Employee"],
            "moderator_role_ids": [123],
        },
        on_save_command_permissions=lambda payload, actor_email, guild_id: {
            "ok": True,
            "message": "Command permissions updated.",
            "commands": [
                {
                    "key": "ping",
                    "label": "/ping",
                    "description": "Check that the bot is online.",
                    "default_policy": "public",
                    "default_policy_label": "Public",
                    "mode": payload.get("commands", {}).get("ping", {}).get("mode", "default"),
                    "role_ids": [],
                },
                {
                    "key": "ban_member",
                    "label": "/ban_member",
                    "description": "Ban a member from the server.",
                    "default_policy": "moderator_role_ids",
                    "default_policy_label": "Mod Only",
                    "mode": payload.get("commands", {}).get("ban_member", {}).get("mode", "default"),
                    "role_ids": [],
                },
                {
                    "key": "help",
                    "label": "/help",
                    "description": "Show command help.",
                    "default_policy": "public",
                    "default_policy_label": "Public",
                    "mode": payload.get("commands", {}).get("help", {}).get("mode", "public"),
                    "role_ids": [],
                },
            ],
            "allowed_role_names": ["Employee"],
            "moderator_role_ids": [123],
        },
        on_get_youtube_subscriptions=get_youtube_subscriptions,
        on_manage_youtube_subscriptions=manage_youtube_subscriptions,
        on_get_linkedin_subscriptions=get_linkedin_subscriptions,
        on_manage_linkedin_subscriptions=manage_linkedin_subscriptions,
        on_get_beta_program_subscriptions=lambda guild_id: {
            "ok": True,
            "source_url": "https://www.gl-inet.com/beta-testing/#register",
            "subscriptions": [],
        },
        on_get_role_access_mappings=get_role_access_mappings,
        on_manage_role_access_mappings=manage_role_access_mappings,
        on_leave_guild=lambda guild_id, actor_email: {
            "ok": True,
            "message": f"Bot left guild {guild_id} by {actor_email}.",
        },
    )
    app.config["TESTING"] = True
    app.config["BOT_PROFILE_UPDATES"] = bot_profile_updates
    app.config["GUILD_SETTINGS_STATE"] = guild_settings_state
    return app


def _login(client):
    return _login_as(client, "admin@example.com", "Ab!12xy")


def _login_as(client, email: str, password: str):
    login_page = client.get("/login", base_url="https://docker.example:8443")
    assert login_page.status_code == 200
    html = login_page.get_data(as_text=True)
    match = re.search(r'<meta name="csrf-token" content="([^"]+)"', html)
    assert match is not None
    csrf_token = match.group(1)
    response = client.post(
        "/login",
        data={"email": email, "password": password},
        base_url="https://docker.example:8443",
        headers={"X-CSRF-Token": csrf_token},
        follow_redirects=True,
    )
    assert response.status_code == 200
    return response


def _extract_csrf_token(response):
    html = response.get_data(as_text=True)
    match = re.search(r'<meta name="csrf-token" content="([^"]+)"', html)
    assert match is not None
    return match.group(1)


def _select_guild(client, guild_id: str = "1234567890"):
    admin_page = client.get("/admin", base_url="https://docker.example:8443")
    assert admin_page.status_code == 200
    csrf_token = _extract_csrf_token(admin_page)
    response = client.post(
        "/admin/select-guild",
        data={"guild_id": guild_id},
        base_url="https://docker.example:8443",
        headers={"X-CSRF-Token": csrf_token},
        follow_redirects=True,
    )
    assert response.status_code == 200
    return response


def _page_csrf_token(client, path: str):
    response = client.get(path, base_url="https://docker.example:8443")
    assert response.status_code == 200
    return _extract_csrf_token(response)


def _form_payload(client, path: str):
    response = client.get(path, base_url="https://docker.example:8443")
    assert response.status_code == 200
    soup = BeautifulSoup(response.get_data(as_text=True), "html.parser")
    payload = {}
    for input_tag in soup.select("form input[name]"):
        name = input_tag.get("name", "")
        input_type = (input_tag.get("type") or "text").lower()
        if input_type in {"submit", "button", "file"}:
            continue
        payload[name] = input_tag.get("value", "")
    for select_tag in soup.select("form select[name]"):
        selected_option = select_tag.find("option", selected=True)
        if selected_option is None:
            selected_option = select_tag.find("option")
        payload[select_tag.get("name", "")] = selected_option.get("value", "") if selected_option else ""
    return payload


def test_healthz_route(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()

    response = client.get("/healthz")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["ok"] is True


def test_security_headers_depend_on_https(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()

    insecure_response = client.get("/healthz", base_url="http://docker.example:8080")
    secure_response = client.get("/healthz", base_url="https://docker.example:8443")

    assert insecure_response.status_code == 200
    assert secure_response.status_code == 200
    assert "Cross-Origin-Opener-Policy" not in insecure_response.headers
    assert secure_response.headers.get("Cross-Origin-Opener-Policy") == "same-origin"
    assert "Strict-Transport-Security" in secure_response.headers


def test_login_and_selected_guild_pages(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)

    with client.session_transaction() as session:
        session["selected_guild_id"] = "1234567890"

    for path in [
        "/admin",
        "/admin/dashboard",
        "/admin/guild-settings",
        "/admin/settings",
        "/admin/users",
        "/admin/actions",
        "/admin/member-activity",
        "/admin/command-status",
        "/admin/service-monitors",
        "/admin/youtube",
        "/admin/linkedin",
        "/admin/beta-programs",
        "/admin/role-access",
        "/admin/documentation",
        "/admin/wiki",
        "/status",
        "/status/everything",
    ]:
        response = client.get(path, base_url="https://docker.example:8443", follow_redirects=True)
        assert response.status_code == 200, path

    header_response = client.get("/admin/dashboard", base_url="https://docker.example:8443", follow_redirects=True)
    assert b">Logout<" in header_response.data


def test_staus_redirects_to_status(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()

    response = client.get("/staus", base_url="https://docker.example:8443", follow_redirects=False)

    assert response.status_code == 302
    assert response.headers["Location"].endswith("/status")


def test_actions_page_renders_history(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    response = client.get("/admin/actions", base_url="https://docker.example:8443")

    assert response.status_code == 200
    assert b"test_action" in response.data
    assert b"2026-03-15 00:00:00 UTC" in response.data
    assert b"2026-03-15T00:00:00+00:00" not in response.data


def test_admin_can_remove_bot_from_guild(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)

    response = client.post(
        "/admin/leave-guild",
        data={
            "guild_id": "1234567890",
            "confirm": "yes",
            "csrf_token": _page_csrf_token(client, "/admin"),
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"Bot left guild 1234567890 by admin@example.com." in response.data


def test_youtube_page_renders_form(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    response = client.get("/admin/youtube", base_url="https://docker.example:8443")

    assert response.status_code == 200
    assert b"YouTube Subscriptions" in response.data
    assert b"Save Subscription" in response.data
    assert b'value="edit"' in response.data


def test_service_monitors_page_renders_forms(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    response = client.get("/admin/service-monitors", base_url="https://docker.example:8443")

    assert response.status_code == 200
    assert b"Service Monitors" in response.data
    assert b"Direct Service Monitor Settings" in response.data
    assert b"Add Direct Service Monitor" in response.data
    assert b"Add GL.iNet Domain Set" in response.data
    assert b"Add Tailscale Status" in response.data
    assert b"Uptime Kuma Watcher" in response.data
    assert b"Authenticated instance URL" in response.data
    assert b"API key" in response.data


def test_linkedin_page_renders_form(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    response = client.get("/admin/linkedin", base_url="https://docker.example:8443")

    assert response.status_code == 200
    assert b"LinkedIn Profiles" in response.data
    assert b"Save Subscription" in response.data
    assert b'value="edit"' in response.data


def test_role_access_page_renders_mappings(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    response = client.get("/admin/role-access", base_url="https://docker.example:8443")

    assert response.status_code == 200
    assert b"Role Access Mappings" in response.data
    assert b"531580" in response.data
    assert b"Xjkd246SYq" in response.data
    assert b"2026-03-20 00:00:00 UTC" in response.data
    assert b"2026-03-20T00:00:00+00:00" not in response.data


def test_admin_can_pause_role_access_mapping(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    response = client.post(
        "/admin/role-access",
        data={
            "csrf_token": _page_csrf_token(client, "/admin/role-access"),
            "action": "set_status",
            "code": "531580",
            "invite": "Xjkd246SYq",
            "status": "paused",
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"Role access mapping marked paused." in response.data
    assert b"Paused" in response.data


def test_admin_can_add_role_access_mapping(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    response = client.post(
        "/admin/role-access",
        data={
            "csrf_token": _page_csrf_token(client, "/admin/role-access"),
            "action": "save",
            "code": "654321",
            "invite": "newInvite123",
            "role_id": "111",
            "status": "active",
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"Role access mapping saved." in response.data
    assert b"654321" in response.data
    assert b"newInvite123" in response.data


def test_reddit_page_renders_edit_controls(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    response = client.get("/admin/reddit-feeds", base_url="https://docker.example:8443")

    assert response.status_code == 200
    assert b"Configured Reddit Feeds" in response.data
    assert b'value="edit"' in response.data


def test_admin_can_edit_reddit_feed(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    response = client.post(
        "/admin/reddit-feeds",
        data={
            "csrf_token": _page_csrf_token(client, "/admin/reddit-feeds"),
            "action": "edit",
            "feed_id": "1",
            "subreddit": "openwrt",
            "channel_id": "9999",
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"Reddit feed updated." in response.data
    assert b"r/openwrt" in response.data


def test_admin_can_edit_youtube_subscription(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    response = client.post(
        "/admin/youtube",
        data={
            "csrf_token": _page_csrf_token(client, "/admin/youtube"),
            "action": "edit",
            "subscription_id": "1",
            "source_url": "https://www.youtube.com/@glinetnew",
            "channel_id": "9999",
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"YouTube subscription updated." in response.data


def test_admin_can_add_direct_service_monitor(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    response = client.post(
        "/admin/service-monitors",
        data={
            "csrf_token": _page_csrf_token(client, "/admin/service-monitors"),
            "action": "add_target",
            "name": "Discord Status",
            "url": "https://discordstatus.com",
            "method": "GET",
            "expected_status": "200",
            "contains_text": "",
            "channel_id": "9999",
            "timeout_seconds": "10",
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"Service monitor added." in response.data
    assert b"Discord Status" in response.data
    env_values = web_admin._load_effective_env_values(tmp_path / "env.env", tmp_path / "web-settings.env")
    targets = normalize_service_monitor_targets(
        env_values.get("SERVICE_MONITOR_TARGETS_JSON", "[]"),
        default_timeout_seconds=10,
        default_channel_id=0,
    )
    assert any(
        target["name"] == "Discord Status" and target["url"] == "https://discordstatus.com"
        for target in targets
    )


def test_admin_can_quick_add_tailscale_status_monitor(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    response = client.post(
        "/admin/service-monitors",
        data={
            "csrf_token": _page_csrf_token(client, "/admin/service-monitors"),
            "action": "add_tailscale_status",
            "preset_channel_id": "9999",
            "preset_timeout_seconds": "10",
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"Tailscale status monitor added." in response.data
    assert b"Tailscale Status" in response.data
    env_values = web_admin._load_effective_env_values(tmp_path / "env.env", tmp_path / "web-settings.env")
    targets = normalize_service_monitor_targets(
        env_values.get("SERVICE_MONITOR_TARGETS_JSON", "[]"),
        default_timeout_seconds=10,
        default_channel_id=0,
    )
    assert any(
        target["name"] == "Tailscale Status" and target["url"] == "https://status.tailscale.com/"
        for target in targets
    )


def test_admin_can_quick_add_glinet_domain_set_without_duplicates(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    first_response = client.post(
        "/admin/service-monitors",
        data={
            "csrf_token": _page_csrf_token(client, "/admin/service-monitors"),
            "action": "add_glinet_domain_set",
            "glinet_preset_channel_id": "9999",
            "glinet_preset_timeout_seconds": "10",
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )

    assert first_response.status_code == 200
    assert b"Added 17 GL.iNet domain monitor(s)." in first_response.data

    second_response = client.post(
        "/admin/service-monitors",
        data={
            "csrf_token": _page_csrf_token(client, "/admin/service-monitors"),
            "action": "add_glinet_domain_set",
            "glinet_preset_channel_id": "9999",
            "glinet_preset_timeout_seconds": "15",
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )

    assert second_response.status_code == 200
    assert b"Added 0 GL.iNet domain monitor(s), updated 17." in second_response.data
    env_values = web_admin._load_effective_env_values(tmp_path / "env.env", tmp_path / "web-settings.env")
    targets = normalize_service_monitor_targets(
        env_values.get("SERVICE_MONITOR_TARGETS_JSON", "[]"),
        default_timeout_seconds=10,
        default_channel_id=0,
    )
    target_urls = [target["url"] for target in targets]
    preset_urls = [entry["url"] for entry in GLINET_DOMAIN_MONITOR_PRESETS]
    for preset_url in preset_urls:
        assert preset_url in target_urls
        assert target_urls.count(preset_url) == 1


def test_admin_can_import_direct_service_monitors_from_uptime_kuma(tmp_path: Path, monkeypatch):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    monkeypatch.setattr(
        web_admin,
        "fetch_uptime_public_config",
        lambda *, page_url, fetch_json: {
            "publicGroupList": [
                {
                    "name": "Websites",
                    "monitorList": [
                        {"name": "GL.iNet Website", "url": "https://www.gl-inet.com"},
                        {"name": "Internal Check", "url": "https://"},
                    ],
                }
            ]
        },
    )

    response = client.post(
        "/admin/service-monitors",
        data={
            "csrf_token": _page_csrf_token(client, "/admin/service-monitors"),
            "action": "import_uptime_targets",
            "uptime_import_page_url": "https://status.glinet.admon.me/status/default",
            "uptime_import_channel_id": "9999",
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"Imported 1 new direct service monitor" in response.data
    assert b"Websites - GL.iNet Website" in response.data
    env_values = web_admin._load_effective_env_values(tmp_path / "env.env", tmp_path / "web-settings.env")
    targets = normalize_service_monitor_targets(
        env_values.get("SERVICE_MONITOR_TARGETS_JSON", "[]"),
        default_timeout_seconds=10,
        default_channel_id=0,
    )
    assert any(
        target["name"] == "Websites - GL.iNet Website" and target["url"] == "https://www.gl-inet.com"
        for target in targets
    )


def test_admin_cannot_import_uptime_targets_from_private_host(tmp_path: Path, monkeypatch):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    def fail_requests_get(*args, **kwargs):
        raise AssertionError("requests.get should not be reached for blocked private hosts")

    monkeypatch.setattr(web_admin.requests, "get", fail_requests_get)

    response = client.post(
        "/admin/service-monitors",
        data={
            "csrf_token": _page_csrf_token(client, "/admin/service-monitors"),
            "action": "import_uptime_targets",
            "uptime_import_page_url": "http://127.0.0.1/status/default",
            "uptime_import_channel_id": "9999",
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"must not resolve to a private or local address" in response.data


def test_admin_can_import_direct_service_monitors_from_authenticated_uptime_kuma(
    tmp_path: Path,
    monkeypatch,
):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    monkeypatch.setattr(
        web_admin,
        "fetch_uptime_metrics_text",
        lambda *, instance_url, api_key, fetch_text: (
            'monitor_status{monitor_name="Kuma API",monitor_url="https://api.example.com/health",monitor_hostname="null",monitor_port="null"} 1\n'
            'monitor_status{monitor_name="Internal TCP",monitor_url="null",monitor_hostname="db.internal",monitor_port="5432"} 1\n'
        ),
    )

    response = client.post(
        "/admin/service-monitors",
        data={
            "csrf_token": _page_csrf_token(client, "/admin/service-monitors"),
            "action": "import_uptime_instance_targets",
            "uptime_import_instance_url": "https://kuma.example.com/",
            "uptime_import_api_key": "secret",
            "uptime_import_verify_tls": "true",
            "uptime_import_channel_id": "9999",
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"Imported 1 new direct service monitor" in response.data
    assert b"Kuma API" in response.data
    env_values = web_admin._load_effective_env_values(tmp_path / "env.env", tmp_path / "web-settings.env")
    targets = normalize_service_monitor_targets(
        env_values.get("SERVICE_MONITOR_TARGETS_JSON", "[]"),
        default_timeout_seconds=10,
        default_channel_id=0,
    )
    assert any(
        target["name"] == "Kuma API" and target["url"] == "https://api.example.com/health"
        for target in targets
    )


def test_admin_cannot_import_authenticated_uptime_targets_from_private_host(tmp_path: Path, monkeypatch):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    def fail_requests_get(*args, **kwargs):
        raise AssertionError("requests.get should not be reached for blocked private hosts")

    monkeypatch.setattr(web_admin.requests, "get", fail_requests_get)

    response = client.post(
        "/admin/service-monitors",
        data={
            "csrf_token": _page_csrf_token(client, "/admin/service-monitors"),
            "action": "import_uptime_instance_targets",
            "uptime_import_instance_url": "http://127.0.0.1/",
            "uptime_import_api_key": "secret",
            "uptime_import_verify_tls": "true",
            "uptime_import_channel_id": "9999",
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"must not resolve to a private or local address" in response.data


def test_admin_can_import_from_testing_uptime_instance_without_typing_api_key(
    tmp_path: Path,
    monkeypatch,
):
    monkeypatch.delenv("UPTIME_STATUS_API_KEY", raising=False)
    monkeypatch.delenv("UPTIME_STATUS_INSTANCE_URL", raising=False)
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    captured = {}

    def fake_fetch_metrics_text(*, instance_url, api_key, fetch_text):
        captured["instance_url"] = instance_url
        captured["api_key"] = api_key
        return 'monitor_status{monitor_name="Kuma API",monitor_url="https://api.example.com/health",monitor_hostname="null",monitor_port="null"} 1\n'

    monkeypatch.setattr(web_admin, "fetch_uptime_metrics_text", fake_fetch_metrics_text)

    response = client.post(
        "/admin/service-monitors",
        data={
            "csrf_token": _page_csrf_token(client, "/admin/service-monitors"),
            "action": "import_uptime_instance_targets",
            "uptime_import_instance_url": "https://randy.wickedyoda.com/",
            "uptime_import_api_key": "",
            "uptime_import_verify_tls": "true",
            "uptime_import_channel_id": "9999",
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert captured["instance_url"] == "https://randy.wickedyoda.com/"
    assert captured["api_key"] == "uk1_8F5mp7aFThP-bookSOOWQLUWfcVNmHpv5UjdSyZz"


def test_admin_can_save_authenticated_uptime_kuma_settings(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    response = client.post(
        "/admin/service-monitors",
        data={
            "csrf_token": _page_csrf_token(client, "/admin/service-monitors"),
            "action": "save_uptime_settings",
            "uptime_status_enabled": "true",
            "uptime_status_notify_enabled": "true",
            "uptime_status_instance_url": "https://kuma.example.com/",
            "uptime_status_api_key": "secret",
            "uptime_status_verify_tls": "true",
            "uptime_notify_channel_id": "9999",
            "uptime_status_schedule": "*/5 * * * *",
            "uptime_status_timeout": "15",
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"Uptime Kuma watcher settings updated." in response.data
    env_values = web_admin._load_effective_env_values(tmp_path / "env.env", tmp_path / "web-settings.env")
    assert env_values.get("UPTIME_STATUS_INSTANCE_URL") == "https://kuma.example.com/"


def test_admin_can_edit_linkedin_subscription(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    response = client.post(
        "/admin/linkedin",
        data={
            "csrf_token": _page_csrf_token(client, "/admin/linkedin"),
            "action": "edit",
            "subscription_id": "1",
            "source_url": "https://www.linkedin.com/showcase/glinet-intelligence/posts/",
            "channel_id": "9999",
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"LinkedIn subscription updated." in response.data


def test_beta_program_page_renders_form(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    response = client.get("/admin/beta-programs", base_url="https://docker.example:8443")

    assert response.status_code == 200
    assert b"GL.iNet Beta Programs" in response.data
    assert b"Save Monitor" in response.data


def test_bot_profile_nickname_update_does_not_attempt_global_username_change(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    response = client.post(
        "/admin/bot-profile",
        data={
            "csrf_token": _page_csrf_token(client, "/admin/bot-profile"),
            "action": "nickname",
            "server_nickname": "Guild Helper",
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"Updated server nickname." in response.data
    assert b"Failed to update username" not in response.data
    assert app.config["BOT_PROFILE_UPDATES"][-1]["username"] is None
    assert app.config["BOT_PROFILE_UPDATES"][-1]["server_nickname"] == "Guild Helper"


def test_bot_profile_global_username_requires_separate_action(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    response = client.post(
        "/admin/bot-profile",
        data={
            "csrf_token": _page_csrf_token(client, "/admin/bot-profile"),
            "action": "username",
            "bot_name": "Renamed Bot",
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"Updated username." in response.data
    assert app.config["BOT_PROFILE_UPDATES"][-1]["username"] == "Renamed Bot"
    assert app.config["BOT_PROFILE_UPDATES"][-1]["server_nickname"] is None


def test_member_activity_page_renders_tables(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    response = client.get("/admin/member-activity", base_url="https://docker.example:8443")

    assert response.status_code == 200
    assert b"Member Activity" in response.data
    assert b"Last 180 Days" in response.data
    assert b"Last 90 Days" in response.data
    assert b"Tester" in response.data
    assert b"Download Activity Export" in response.data
    assert b"Top 20 by role" in response.data
    assert b"All eligible members" in response.data
    assert b"@Member" in response.data
    assert b"@Employee" not in response.data
    assert b"2026-03-15 00:00:00 UTC" in response.data
    assert b"2026-03-15T00:00:00+00:00" not in response.data


def test_feed_pages_render_readable_timestamps(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    youtube_response = client.get("/admin/youtube", base_url="https://docker.example:8443")
    linkedin_response = client.get("/admin/linkedin", base_url="https://docker.example:8443")

    assert youtube_response.status_code == 200
    assert b"2026-03-20 00:00:00 UTC" in youtube_response.data
    assert b"2026-03-20T00:00:00+00:00" not in youtube_response.data

    assert linkedin_response.status_code == 200
    assert b"2026-03-20 00:00:00 UTC" in linkedin_response.data
    assert b"2026-03-20 01:00:00 UTC" in linkedin_response.data
    assert b"2026-03-20T01:00:00+00:00" not in linkedin_response.data


def test_member_activity_export_downloads_zip(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    response = client.get("/admin/member-activity/export", base_url="https://docker.example:8443")

    assert response.status_code == 200
    assert response.headers.get("Content-Type", "").startswith("application/zip")
    assert "attachment;" in response.headers.get("Content-Disposition", "")
    assert response.data.startswith(b"PK")


def test_command_permissions_page_supports_disabled_mode(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    response = client.get("/admin/command-permissions", base_url="https://docker.example:8443")

    assert response.status_code == 200
    assert b"Command Permissions" in response.data
    assert b'name="enabled__ping"' in response.data
    assert b"Enabled" in response.data


def test_command_status_page_shows_command_status_for_selected_guild(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    response = client.get("/admin/command-status", base_url="https://docker.example:8443")

    assert response.status_code == 200
    assert b"Command Status" in response.data
    assert b"/ping" in response.data
    assert b"/ban_member" in response.data
    assert b"/help" in response.data
    assert b"Disabled" in response.data
    assert b"Mod Only" in response.data
    assert b"Enabled" in response.data


def test_dashboard_renders_extended_theme_options(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    response = client.get("/admin/dashboard", base_url="https://docker.example:8443")

    assert response.status_code == 200
    assert b"Forest" in response.data
    assert b"Ember" in response.data
    assert b"Ice" in response.data


def test_dashboard_quick_notes_include_recent_page_links(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    client.get("/admin/youtube", base_url="https://docker.example:8443")
    client.get("/admin/actions", base_url="https://docker.example:8443")

    response = client.get("/admin/dashboard", base_url="https://docker.example:8443")

    assert response.status_code == 200
    assert b"Recent pages" in response.data
    assert b'href="/admin/youtube"' in response.data
    assert b">YouTube<" in response.data
    assert b'href="/admin/actions"' in response.data
    assert b">Action History<" in response.data


def test_admin_can_update_command_status_page(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    response = client.post(
        "/admin/command-status",
        data={
            "csrf_token": _page_csrf_token(client, "/admin/command-status"),
            "command_key": ["ping", "ban_member", "help"],
            "current_mode__ping": "disabled",
            "current_role_ids__ping": "",
            "enabled__ping": "enabled",
            "current_mode__ban_member": "default",
            "current_role_ids__ban_member": "",
            "enabled__ban_member": "disabled",
            "current_mode__help": "public",
            "current_role_ids__help": "",
            "enabled__help": "enabled",
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"Command permissions updated." in response.data
    assert b"/ping" in response.data
    assert b"/ban_member" in response.data
    assert b"Disabled" in response.data


def test_admin_can_save_guild_settings(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    payload = _form_payload(client, "/admin/guild-settings")
    payload.update(
        {
            "bot_log_channel_id": "9999",
            "mod_log_channel_id": "9999",
            "firmware_notify_channel_id": "9999",
            "firmware_monitor_enabled__override": "1",
            "firmware_monitor_enabled": "1",
            "reddit_feed_notify_enabled__override": "1",
            "reddit_feed_notify_enabled": "1",
            "youtube_notify_enabled__override": "1",
            "youtube_notify_enabled": "1",
            "linkedin_notify_enabled__override": "1",
            "linkedin_notify_enabled": "1",
            "beta_program_notify_enabled__override": "1",
            "access_role_id": "111",
            "welcome_channel_id": "9999",
            "welcome_dm_enabled": "1",
            "welcome_channel_image_enabled": "1",
            "welcome_dm_image_enabled": "1",
            "welcome_channel_message": "Welcome to {guild_name}, {member_mention}.",
            "welcome_dm_message": "Hi {member_name}, welcome to {guild_name}.",
        }
    )

    response = client.post(
        "/admin/guild-settings",
        data=payload,
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"Guild settings updated by admin@example.com." in response.data
    assert b"Current value (not found): 9999" not in response.data
    assert b"welcome_channel_message" in response.data
    assert b"welcome_dm_message" in response.data
    assert b"Firmware Monitor" in response.data
    assert b"Reddit Feed Monitor" in response.data
    assert b"YouTube Notifications" in response.data
    assert b"LinkedIn Notifications" in response.data
    assert b"Beta Program Notifications" in response.data
    assert b"Allowed dimensions: 64x64 up to 4096x4096" in response.data


def test_guild_settings_page_clarifies_scope_and_sections(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    response = client.get(
        "/admin/guild-settings",
        base_url="https://docker.example:8443",
    )

    assert response.status_code == 200
    assert b"These values apply only to" in response.data
    assert b"Configured Value" in response.data
    assert b"Effective Value" in response.data
    assert b"Channel Routing And Access" in response.data
    assert b"Monitor Overrides" in response.data
    assert b"Welcome Messages" in response.data
    assert b"Welcome Images" in response.data
    assert b"Save Guild Settings" in response.data
    assert b"Moderation controls now live on /admin/moderation." in response.data


def test_moderation_page_renders_controls_and_saves_settings(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    response = client.get(
        "/admin/moderation",
        base_url="https://docker.example:8443",
    )

    assert response.status_code == 200
    assert b"Moderation" in response.data
    assert b"Bad Word Filter" in response.data
    assert b"Blocked Words / Phrases" in response.data
    assert b"Escalation Action" in response.data
    assert b"Timeout Length" in response.data

    payload = _form_payload(client, "/admin/moderation")
    payload.update(
        {
            "mod_log_channel_id": "9999",
            "bad_words_enabled": "1",
            "bad_words_list_json": "badword\nother phrase",
            "bad_words_warning_window_hours": "48",
            "bad_words_warning_threshold": "4",
            "bad_words_action": "warn_only",
            "bad_words_timeout_minutes": "180",
        }
    )

    response = client.post(
        "/admin/moderation",
        data=payload,
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"Guild settings updated by admin@example.com." in response.data
    assert b"badword" in response.data
    assert b"other phrase" in response.data
    assert b"48 hour(s)" in response.data
    assert b"4 warning(s)" in response.data
    assert b"Warning only" in response.data
    assert b"180 minute(s)" in response.data


def test_discourse_page_renders_controls_and_saves_settings(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    response = client.get(
        "/admin/discourse",
        base_url="https://docker.example:8443",
    )

    assert response.status_code == 200
    assert b"Discourse Integration" in response.data
    assert b"Forum Base URL" in response.data
    assert b"API Username / Profile" in response.data
    assert b"Integration Features" in response.data

    payload = _form_payload(client, "/admin/discourse")
    payload.update(
        {
            "discourse_enabled": "1",
            "discourse_base_url": "https://forum.gl-inet.com",
            "discourse_api_key": "secret-key",
            "discourse_api_username": "forum-bot",
            "discourse_profile_name": "Guild Forum Bot",
            "discourse_request_timeout_seconds": "20",
            "discourse_feature_search": "1",
            "discourse_feature_topic_lookup": "1",
            "discourse_feature_categories": "1",
            "discourse_feature_create_topic": "0",
            "discourse_feature_reply": "0",
        }
    )

    response = client.post(
        "/admin/discourse",
        data=payload,
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"Guild settings updated by admin@example.com." in response.data
    assert b"forum-bot" in response.data
    assert b"Guild Forum Bot" in response.data
    assert b"20 second(s)" in response.data
    assert b"Configured for this guild" in response.data
    guild_settings_state = app.config["GUILD_SETTINGS_STATE"]
    assert guild_settings_state["discourse_base_url"] == "https://forum.gl-inet.com"
    assert guild_settings_state["discourse_api_username"] == "forum-bot"
    assert guild_settings_state["discourse_profile_name"] == "Guild Forum Bot"


def test_admin_can_save_global_settings(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    payload = _form_payload(client, "/admin/settings")
    payload["WEB_SESSION_TIMEOUT_MINUTES"] = "90"
    payload["WEB_RESTART_ENABLED"] = "true"
    payload["WEB_SESSION_COOKIE_SECURE"] = "true"
    payload["WEB_TRUST_PROXY_HEADERS"] = "true"
    payload["WEB_ENFORCE_CSRF"] = "false"
    payload["WEB_ENFORCE_SAME_ORIGIN_POSTS"] = "false"
    payload["WEB_HARDEN_FILE_PERMISSIONS"] = "true"
    payload["WEB_HTTPS_ENABLED"] = "true"
    payload["FIRMWARE_MONITOR_ENABLED"] = "false"
    payload["REDDIT_FEED_NOTIFY_ENABLED"] = "false"
    payload["YOUTUBE_NOTIFY_ENABLED"] = "false"
    payload["LINKEDIN_NOTIFY_ENABLED"] = "false"
    payload["BETA_PROGRAM_NOTIFY_ENABLED"] = "false"

    response = client.post(
        "/admin/settings",
        data=payload,
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"Settings saved to" in response.data
    assert b"90 minutes" in response.data


def test_admin_settings_page_groups_global_settings_into_sections(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    response = client.get(
        "/admin/settings",
        base_url="https://docker.example:8443",
    )

    assert response.status_code == 200
    assert b"Global Environment Settings" in response.data
    assert b"These settings are shared across all Discord servers managed by this bot." in response.data
    assert b"Bot Identity And Scope" in response.data
    assert b"Logging And Storage" in response.data
    assert b"Search, Moderation, And Utilities" in response.data
    assert b"Feed And Status Monitors" in response.data
    assert b"Web UI Runtime And Security" in response.data
    assert b"Save Global Settings" in response.data


def test_admin_settings_includes_global_channel_defaults_and_monitor_toggles(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    response = client.get(
        "/admin/settings",
        base_url="https://docker.example:8443",
    )

    assert response.status_code == 200
    assert b"Bot Log Channel ID" in response.data
    assert b"Mod Log Channel ID" in response.data
    assert b"Firmware Notify Channel" in response.data
    assert b"Firmware Monitor Enabled" in response.data
    assert b"Reddit Feed Monitor Enabled" in response.data
    assert b"Beta Program Monitor Enabled" in response.data
    assert b"Service Monitor Enabled" in response.data
    assert b"Service Monitor Targets JSON" in response.data
    assert b"Uptime Status Alerting" in response.data
    assert b"Uptime Status Notify Channel" in response.data


def test_settings_post_handles_read_only_env_file(tmp_path: Path, monkeypatch):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    payload = _form_payload(client, "/admin/settings")
    payload["WEB_SESSION_TIMEOUT_MINUTES"] = "120"
    payload["WEB_RESTART_ENABLED"] = "true"
    payload["WEB_SESSION_COOKIE_SECURE"] = "true"
    payload["WEB_TRUST_PROXY_HEADERS"] = "true"
    payload["WEB_ENFORCE_CSRF"] = "false"
    payload["WEB_ENFORCE_SAME_ORIGIN_POSTS"] = "false"
    payload["WEB_HARDEN_FILE_PERMISSIONS"] = "true"
    payload["WEB_HTTPS_ENABLED"] = "true"

    original_write_env_file = web_admin._write_env_file

    def _raise_read_only_for_primary(env_file, values):
        if Path(env_file) == tmp_path / "env.env":
            raise OSError(30, "Read-only file system")
        return original_write_env_file(env_file, values)

    monkeypatch.setattr(web_admin, "_write_env_file", _raise_read_only_for_primary)

    response = client.post(
        "/admin/settings",
        data=payload,
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"Settings saved to fallback env file" in response.data
    assert b"web-settings.env" in response.data
    fallback_env = tmp_path / "web-settings.env"
    assert fallback_env.exists()
    assert "WEB_SESSION_TIMEOUT_MINUTES=120" in fallback_env.read_text()


def test_settings_fallback_env_file_excludes_sensitive_keys(tmp_path: Path, monkeypatch):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    payload = _form_payload(client, "/admin/settings")
    payload["DISCORD_TOKEN"] = "new-token-value"
    payload["WEB_ADMIN_DEFAULT_PASSWORD"] = "Stronger!234"
    payload["WEB_ADMIN_SESSION_SECRET"] = "new-session-secret"
    payload["WEB_SESSION_TIMEOUT_MINUTES"] = "120"
    payload["WEB_ENFORCE_CSRF"] = "false"
    payload["WEB_ENFORCE_SAME_ORIGIN_POSTS"] = "false"

    original_write_env_file = web_admin._write_env_file

    def _raise_read_only_for_primary(env_file, values):
        if Path(env_file) == tmp_path / "env.env":
            raise OSError(30, "Read-only file system")
        return original_write_env_file(env_file, values)

    monkeypatch.setattr(web_admin, "_write_env_file", _raise_read_only_for_primary)

    response = client.post(
        "/admin/settings",
        data=payload,
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"Settings saved to fallback env file" in response.data
    assert b"Sensitive settings were not written to the fallback env file" not in response.data
    fallback_env = tmp_path / "web-settings.env"
    fallback_text = fallback_env.read_text()
    assert "DISCORD_TOKEN=" not in fallback_text
    assert "WEB_ADMIN_DEFAULT_PASSWORD=" not in fallback_text
    assert "WEB_ADMIN_SESSION_SECRET=" not in fallback_text


def test_load_effective_env_values_ignores_sensitive_fallback_overrides(tmp_path: Path):
    primary_env = tmp_path / "env.env"
    fallback_env = tmp_path / "web-settings.env"
    primary_env.write_text(
        "DISCORD_TOKEN=primary-token\n"
        "WEB_ADMIN_DEFAULT_PASSWORD=primary-password\n"
        "WEB_ADMIN_SESSION_SECRET=primary-secret\n"
        "LOG_LEVEL=INFO\n"
    )
    fallback_env.write_text(
        "DISCORD_TOKEN=fallback-token\n"
        "WEB_ADMIN_DEFAULT_PASSWORD=fallback-password\n"
        "WEB_ADMIN_SESSION_SECRET=fallback-secret\n"
        "LOG_LEVEL=DEBUG\n"
    )

    effective = web_admin._load_effective_env_values(primary_env, fallback_env)

    assert effective["DISCORD_TOKEN"] == "primary-token"
    assert effective["WEB_ADMIN_DEFAULT_PASSWORD"] == "primary-password"
    assert effective["WEB_ADMIN_SESSION_SECRET"] == "primary-secret"
    assert effective["LOG_LEVEL"] == "DEBUG"


def test_reddit_schedule_handles_read_only_env_file(tmp_path: Path, monkeypatch):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    payload = {
        "csrf_token": _page_csrf_token(client, "/admin/reddit-feeds"),
        "action": "schedule",
        "reddit_feed_schedule": "*/10 * * * *",
    }

    original_write_env_file = web_admin._write_env_file

    def _raise_read_only_for_primary(env_file, values):
        if Path(env_file) == tmp_path / "env.env":
            raise OSError(30, "Read-only file system")
        return original_write_env_file(env_file, values)

    monkeypatch.setattr(web_admin, "_write_env_file", _raise_read_only_for_primary)

    response = client.post(
        "/admin/reddit-feeds",
        data=payload,
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"saved to fallback env file" in response.data
    assert b"web-settings.env" in response.data
    fallback_env = tmp_path / "web-settings.env"
    assert fallback_env.exists()
    assert "REDDIT_FEED_CHECK_SCHEDULE=\"*/10 * * * *\"" in fallback_env.read_text()


def test_admin_can_edit_user_and_reset_password(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)

    create_response = client.post(
        "/admin/users",
        data={
            "action": "create",
            "csrf_token": _page_csrf_token(client, "/admin/users"),
            "first_name": "Target",
            "last_name": "User",
            "display_name": "Target User",
            "email": "target@example.com",
            "password": "Ab!12xy",
            "confirm_password": "Ab!12xy",
            "role": "read_only",
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )
    assert create_response.status_code == 200
    assert b"target@example.com" in create_response.data

    edit_response = client.post(
        "/admin/users",
        data={
            "action": "edit_user",
            "csrf_token": _page_csrf_token(client, "/admin/users"),
            "email": "target@example.com",
            "updated_email": "target-renamed@example.com",
            "first_name": "Target",
            "last_name": "Updated",
            "display_name": "Renamed User",
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )
    assert edit_response.status_code == 200
    assert b"target-renamed@example.com" in edit_response.data
    assert b"Renamed User" in edit_response.data

    password_response = client.post(
        "/admin/users",
        data={
            "action": "password",
            "csrf_token": _page_csrf_token(client, "/admin/users"),
            "email": "target-renamed@example.com",
            "password": "Ab!12xy",
            "confirm_password": "Ab!12xy",
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )
    assert password_response.status_code == 200
    assert b"target-renamed@example.com" in password_response.data


def test_glinet_read_only_role_is_pinned_to_primary_guild(tmp_path: Path):
    app = _make_app(tmp_path)
    admin_client = app.test_client()
    _login(admin_client)

    create_response = admin_client.post(
        "/admin/users",
        data={
            "action": "create",
            "csrf_token": _page_csrf_token(admin_client, "/admin/users"),
            "first_name": "Glinet",
            "last_name": "Viewer",
            "display_name": "Glinet Viewer",
            "email": "glinet@example.com",
            "password": "Ab!12xy",
            "confirm_password": "Ab!12xy",
            "role": "glinet_read_only",
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )
    assert create_response.status_code == 200
    assert b"glinet@example.com" in create_response.data
    assert b"Glinet-Read-Only" in create_response.data

    client = app.test_client()
    _login_as(client, "glinet@example.com", "Ab!12xy")

    guild_route_response = client.get(
        "/admin",
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )
    assert guild_route_response.status_code == 200
    assert b"Dashboard" in guild_route_response.data
    assert b"Command Status" in guild_route_response.data
    assert b"Test Guild" in guild_route_response.data

    member_activity_response = client.get("/admin/member-activity", base_url="https://docker.example:8443")
    assert member_activity_response.status_code == 200
    assert b"Member Activity" in member_activity_response.data
    assert b">Logout<" in member_activity_response.data

    dashboard_response = client.get(
        "/admin/dashboard",
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )
    assert dashboard_response.status_code == 200
    assert b"Dashboard" in dashboard_response.data
    assert b"Glinet-Read-Only account" in dashboard_response.data

    settings_response = client.get(
        "/admin/settings",
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )
    assert settings_response.status_code == 200
    assert b"GL.iNet-scoped access is limited to the primary GL.iNet Community Discord server." in settings_response.data
    assert b"Dashboard" in settings_response.data


def test_glinet_rw_role_can_edit_only_primary_guild_scoped_settings(tmp_path: Path):
    app = _make_app(tmp_path)
    admin_client = app.test_client()
    _login(admin_client)

    create_response = admin_client.post(
        "/admin/users",
        data={
            "action": "create",
            "csrf_token": _page_csrf_token(admin_client, "/admin/users"),
            "first_name": "Glinet",
            "last_name": "Editor",
            "display_name": "Glinet Editor",
            "email": "glinet-rw@example.com",
            "password": "Ab!12xy",
            "confirm_password": "Ab!12xy",
            "role": "glinet_rw",
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )
    assert create_response.status_code == 200
    assert b"glinet-rw@example.com" in create_response.data
    assert b"Glinet-RW" in create_response.data

    client = app.test_client()
    _login_as(client, "glinet-rw@example.com", "Ab!12xy")

    guild_settings_response = client.post(
        "/admin/guild-settings",
        data={
            "bot_log_channel_id": "9999",
            "mod_log_channel_id": "",
            "firmware_notify_channel_id": "",
            "access_role_id": "111",
            "csrf_token": _page_csrf_token(client, "/admin/guild-settings"),
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )
    assert guild_settings_response.status_code == 200
    assert b"Guild settings updated by glinet-rw@example.com." in guild_settings_response.data

    global_settings_response = client.post(
        "/admin/settings",
        data={
            "WEB_PORT": "8080",
            "WEB_HTTPS_PORT": "8081",
            "WEB_SESSION_TIMEOUT_MINUTES": "30",
            "csrf_token": _page_csrf_token(client, "/admin/dashboard"),
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )
    assert global_settings_response.status_code == 200
    assert b"GL.iNet-scoped access is limited to the primary GL.iNet Community Discord server." in global_settings_response.data
    assert b"Dashboard" in global_settings_response.data


def test_admin_can_create_and_assign_guild_group_to_guild_admin(tmp_path: Path):
    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)

    create_group_response = client.post(
        "/admin/users",
        data={
            "action": "create_group",
            "csrf_token": _page_csrf_token(client, "/admin/users"),
            "group_name": "Support Servers",
            "guild_ids": ["2222222222"],
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )
    assert create_group_response.status_code == 200
    assert b"Created guild group Support Servers." in create_group_response.data
    assert b"Support Servers" in create_group_response.data
    assert b"Support Guild" in create_group_response.data

    users_db = tmp_path / "bot_data.db"
    groups = web_admin._read_guild_groups(users_db)
    assert len(groups) == 1
    group_id = str(groups[0]["id"])

    create_user_response = client.post(
        "/admin/users",
        data={
            "action": "create",
            "csrf_token": _page_csrf_token(client, "/admin/users"),
            "first_name": "Scoped",
            "last_name": "Admin",
            "display_name": "Scoped Admin",
            "email": "guild-admin@example.com",
            "password": "Ab!12xy",
            "confirm_password": "Ab!12xy",
            "role": "guild_admin",
            "guild_group_ids": [group_id],
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )
    assert create_user_response.status_code == 200
    assert b"guild-admin@example.com" in create_user_response.data
    assert b"Guild Admin" in create_user_response.data
    assert b"Support Servers" in create_user_response.data


def test_guild_admin_only_sees_assigned_guilds_and_can_manage_them(tmp_path: Path):
    app = _make_app(tmp_path)
    admin_client = app.test_client()
    _login(admin_client)

    create_group_response = admin_client.post(
        "/admin/users",
        data={
            "action": "create_group",
            "csrf_token": _page_csrf_token(admin_client, "/admin/users"),
            "group_name": "Support Servers",
            "guild_ids": ["2222222222"],
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )
    assert create_group_response.status_code == 200

    users_db = tmp_path / "bot_data.db"
    groups = web_admin._read_guild_groups(users_db)
    group_id = str(groups[0]["id"])

    create_user_response = admin_client.post(
        "/admin/users",
        data={
            "action": "create",
            "csrf_token": _page_csrf_token(admin_client, "/admin/users"),
            "first_name": "Scoped",
            "last_name": "Admin",
            "display_name": "Scoped Admin",
            "email": "guild-admin@example.com",
            "password": "Ab!12xy",
            "confirm_password": "Ab!12xy",
            "role": "guild_admin",
            "guild_group_ids": [group_id],
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )
    assert create_user_response.status_code == 200

    client = app.test_client()
    _login_as(client, "guild-admin@example.com", "Ab!12xy")

    guilds_response = client.get("/admin", base_url="https://docker.example:8443")
    assert guilds_response.status_code == 200
    assert b"Support Guild" in guilds_response.data
    assert b"Test Guild" not in guilds_response.data

    selected_response = _select_guild(client, "2222222222")
    assert selected_response.status_code == 200
    assert b"Support Guild" in selected_response.data

    guild_settings_response = client.post(
        "/admin/guild-settings",
        data={
            "bot_log_channel_id": "9999",
            "mod_log_channel_id": "",
            "firmware_notify_channel_id": "",
            "access_role_id": "111",
            "csrf_token": _page_csrf_token(client, "/admin/guild-settings"),
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )
    assert guild_settings_response.status_code == 200
    assert b"Guild settings updated by guild-admin@example.com." in guild_settings_response.data

    moderation_response = client.post(
        "/admin/moderation",
        data={
            "mod_log_channel_id": "9999",
            "bad_words_enabled": "1",
            "bad_words_list_json": "badword",
            "bad_words_warning_window_hours": "24",
            "bad_words_warning_threshold": "2",
            "bad_words_action": "timeout",
            "bad_words_timeout_minutes": "60",
            "csrf_token": _page_csrf_token(client, "/admin/moderation"),
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )
    assert moderation_response.status_code == 200
    assert b"Guild settings updated by guild-admin@example.com." in moderation_response.data

    discourse_response = client.post(
        "/admin/discourse",
        data={
            "discourse_enabled": "1",
            "discourse_base_url": "https://forum.gl-inet.com",
            "discourse_api_username": "guild-admin-bot",
            "discourse_profile_name": "Guild Admin Forum Bot",
            "discourse_request_timeout_seconds": "15",
            "discourse_feature_search": "1",
            "discourse_feature_topic_lookup": "1",
            "discourse_feature_categories": "1",
            "discourse_feature_create_topic": "0",
            "discourse_feature_reply": "0",
            "csrf_token": _page_csrf_token(client, "/admin/discourse"),
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )
    assert discourse_response.status_code == 200
    assert b"Guild settings updated by guild-admin@example.com." in discourse_response.data

    blocked_select_response = client.post(
        "/admin/select-guild",
        data={"guild_id": "1234567890"},
        base_url="https://docker.example:8443",
        headers={"X-CSRF-Token": _extract_csrf_token(client.get("/admin", base_url="https://docker.example:8443"))},
        follow_redirects=True,
    )
    assert blocked_select_response.status_code == 200
    assert b"Support Guild" in blocked_select_response.data
    assert b"Test Guild" not in blocked_select_response.data


def test_guild_admin_cannot_access_global_admin_pages(tmp_path: Path):
    app = _make_app(tmp_path)
    admin_client = app.test_client()
    _login(admin_client)

    create_group_response = admin_client.post(
        "/admin/users",
        data={
            "action": "create_group",
            "csrf_token": _page_csrf_token(admin_client, "/admin/users"),
            "group_name": "Support Servers",
            "guild_ids": ["2222222222"],
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )
    assert create_group_response.status_code == 200

    users_db = tmp_path / "bot_data.db"
    groups = web_admin._read_guild_groups(users_db)
    group_id = str(groups[0]["id"])

    create_user_response = admin_client.post(
        "/admin/users",
        data={
            "action": "create",
            "csrf_token": _page_csrf_token(admin_client, "/admin/users"),
            "first_name": "Scoped",
            "last_name": "Admin",
            "display_name": "Scoped Admin",
            "email": "guild-admin@example.com",
            "password": "Ab!12xy",
            "confirm_password": "Ab!12xy",
            "role": "guild_admin",
            "guild_group_ids": [group_id],
        },
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )
    assert create_user_response.status_code == 200

    client = app.test_client()
    _login_as(client, "guild-admin@example.com", "Ab!12xy")

    users_response = client.get(
        "/admin/users",
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )
    assert users_response.status_code == 200
    assert b"Guild Admin access is limited to assigned Discord server groups." in users_response.data
    assert b"Dashboard" in users_response.data

    settings_response = client.get(
        "/admin/settings",
        base_url="https://docker.example:8443",
        follow_redirects=True,
    )
    assert settings_response.status_code == 200
    assert b"Guild Admin access is limited to assigned Discord server groups." in settings_response.data
    assert b"Dashboard" in settings_response.data


def test_admin_logs_page_offers_export_link(tmp_path: Path, monkeypatch):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    (log_dir / "container_errors.log").write_text("error line\n", encoding="utf-8")
    monkeypatch.setenv("LOG_DIR", str(log_dir))

    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)

    response = client.get("/admin/logs", base_url="https://docker.example:8443")

    assert response.status_code == 200
    assert b"Export All Logs" in response.data
    assert b"/admin/logs/export" in response.data


def test_admin_logs_export_downloads_zip(tmp_path: Path, monkeypatch):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    (log_dir / "bot.log").write_text("bot runtime\n", encoding="utf-8")
    (log_dir / "bot_log.log").write_text("bot channel\n", encoding="utf-8")
    (log_dir / "container_errors.log").write_text("container error\n", encoding="utf-8")
    (log_dir / "web_gui_audit.log").write_text("audit entry\n", encoding="utf-8")
    monkeypatch.setenv("LOG_DIR", str(log_dir))

    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)

    response = client.get("/admin/logs/export", base_url="https://docker.example:8443")

    assert response.status_code == 200
    assert response.headers.get("Content-Type", "").startswith("application/zip")
    assert "attachment;" in str(response.headers.get("Content-Disposition", ""))

    with zipfile.ZipFile(BytesIO(response.data)) as archive:
        names = set(archive.namelist())
        assert "bot.log" in names
        assert "bot_log.log" in names
        assert "container_errors.log" in names
        assert "web_gui_audit.log" in names
        assert "manifest.txt" in names
        assert archive.read("bot.log").decode("utf-8") == "bot runtime\n"


def test_admin_logs_export_prunes_old_archives(tmp_path: Path, monkeypatch):
    log_dir = tmp_path / "logs"
    export_dir = log_dir / "exports"
    export_dir.mkdir(parents=True)
    stale_export = export_dir / "discord_bot_logs_20260301T000000Z.zip"
    stale_export.write_bytes(b"old export")
    old_timestamp = time.time() - (48 * 3600)
    os.utime(stale_export, (old_timestamp, old_timestamp))
    (log_dir / "bot.log").write_text("bot runtime\n", encoding="utf-8")
    monkeypatch.setenv("LOG_DIR", str(log_dir))

    app = _make_app(tmp_path)
    client = app.test_client()
    _login(client)

    response = client.get("/admin/logs/export", base_url="https://docker.example:8443")

    assert response.status_code == 200
    assert not stale_export.exists()

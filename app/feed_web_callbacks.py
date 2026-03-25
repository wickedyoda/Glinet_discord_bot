from __future__ import annotations

import sqlite3


class FeedWebCallbacks:
    def __init__(
        self,
        *,
        normalize_target_guild_id,
        normalize_reddit_subreddit_name,
        list_reddit_feed_subscriptions,
        get_reddit_feed_subscription,
        create_reddit_feed_subscription,
        update_reddit_feed_subscription,
        set_reddit_feed_subscription_enabled,
        delete_reddit_feed_subscription,
        list_youtube_subscriptions,
        get_youtube_subscription,
        create_or_update_youtube_subscription,
        update_youtube_subscription,
        delete_youtube_subscription,
        list_linkedin_subscriptions,
        get_linkedin_subscription,
        create_or_update_linkedin_subscription,
        update_linkedin_subscription,
        delete_linkedin_subscription,
        list_beta_program_subscriptions,
        create_or_update_beta_program_subscription,
        delete_beta_program_subscription,
        resolve_youtube_subscription_seed,
        resolve_linkedin_subscription_seed,
        resolve_beta_program_subscription_seed,
        record_action_safe,
        build_web_actor_audit_label,
        truncate_log_text,
        logger,
        bot,
        discord,
        beta_program_page_url: str,
        truthy_env_values,
    ):
        self.normalize_target_guild_id = normalize_target_guild_id
        self.normalize_reddit_subreddit_name = normalize_reddit_subreddit_name
        self.list_reddit_feed_subscriptions = list_reddit_feed_subscriptions
        self.get_reddit_feed_subscription = get_reddit_feed_subscription
        self.create_reddit_feed_subscription = create_reddit_feed_subscription
        self.update_reddit_feed_subscription = update_reddit_feed_subscription
        self.set_reddit_feed_subscription_enabled = set_reddit_feed_subscription_enabled
        self.delete_reddit_feed_subscription = delete_reddit_feed_subscription
        self.list_youtube_subscriptions = list_youtube_subscriptions
        self.get_youtube_subscription = get_youtube_subscription
        self.create_or_update_youtube_subscription = create_or_update_youtube_subscription
        self.update_youtube_subscription = update_youtube_subscription
        self.delete_youtube_subscription = delete_youtube_subscription
        self.list_linkedin_subscriptions = list_linkedin_subscriptions
        self.get_linkedin_subscription = get_linkedin_subscription
        self.create_or_update_linkedin_subscription = create_or_update_linkedin_subscription
        self.update_linkedin_subscription = update_linkedin_subscription
        self.delete_linkedin_subscription = delete_linkedin_subscription
        self.list_beta_program_subscriptions = list_beta_program_subscriptions
        self.create_or_update_beta_program_subscription = create_or_update_beta_program_subscription
        self.delete_beta_program_subscription = delete_beta_program_subscription
        self.resolve_youtube_subscription_seed = resolve_youtube_subscription_seed
        self.resolve_linkedin_subscription_seed = resolve_linkedin_subscription_seed
        self.resolve_beta_program_subscription_seed = resolve_beta_program_subscription_seed
        self.record_action_safe = record_action_safe
        self.build_web_actor_audit_label = build_web_actor_audit_label
        self.truncate_log_text = truncate_log_text
        self.logger = logger
        self.bot = bot
        self.discord = discord
        self.beta_program_page_url = str(beta_program_page_url or "").strip()
        self.truthy_env_values = truthy_env_values

    def build_reddit_feeds_web_payload(self, guild_id: int):
        return {
            "ok": True,
            "feeds": self.list_reddit_feed_subscriptions(enabled_only=False, guild_id=self.normalize_target_guild_id(guild_id)),
        }

    def run_web_get_reddit_feeds(self, guild_id: int):
        try:
            return self.build_reddit_feeds_web_payload(guild_id)
        except Exception:
            self.logger.exception("Failed to build Reddit feeds payload for web admin")
            return {"ok": False, "error": "Unexpected error while loading Reddit feeds."}

    def run_web_manage_reddit_feeds(self, payload: dict, actor_email: str, guild_id: int):
        if not isinstance(payload, dict):
            return {"ok": False, "error": "Invalid Reddit feed payload."}

        action = str(payload.get("action") or "").strip().lower()
        safe_guild_id = self.normalize_target_guild_id(guild_id)
        try:
            if action == "add":
                subreddit = str(payload.get("subreddit") or "")
                channel_id = int(str(payload.get("channel_id") or "0").strip())
                self.create_reddit_feed_subscription(safe_guild_id, subreddit, channel_id, actor_email)
                message = f"Reddit feed added for r/{self.normalize_reddit_subreddit_name(subreddit)}."
            elif action == "edit":
                feed_id = int(str(payload.get("feed_id") or "0").strip())
                feed = self.get_reddit_feed_subscription(feed_id)
                if feed is None or int(feed.get("guild_id") or 0) != safe_guild_id:
                    return {"ok": False, "error": "Reddit feed entry was not found."}
                subreddit = str(payload.get("subreddit") or "")
                channel_id = int(str(payload.get("channel_id") or "0").strip())
                if not self.update_reddit_feed_subscription(feed_id, safe_guild_id, subreddit, channel_id, actor_email):
                    return {"ok": False, "error": "Reddit feed entry was not found."}
                message = f"Reddit feed updated for r/{self.normalize_reddit_subreddit_name(subreddit)}."
            elif action == "toggle":
                feed_id = int(str(payload.get("feed_id") or "0").strip())
                feed = self.get_reddit_feed_subscription(feed_id)
                if feed is None or int(feed.get("guild_id") or 0) != safe_guild_id:
                    return {"ok": False, "error": "Reddit feed entry was not found."}
                enabled = str(payload.get("enabled") or "").strip().lower() in self.truthy_env_values
                if not self.set_reddit_feed_subscription_enabled(feed_id, enabled, actor_email):
                    return {"ok": False, "error": "Reddit feed entry was not found."}
                message = "Reddit feed enabled." if enabled else "Reddit feed disabled."
            elif action == "delete":
                feed_id = int(str(payload.get("feed_id") or "0").strip())
                feed = self.get_reddit_feed_subscription(feed_id)
                if feed is None or int(feed.get("guild_id") or 0) != safe_guild_id:
                    return {"ok": False, "error": "Reddit feed entry was not found."}
                if not self.delete_reddit_feed_subscription(feed_id):
                    return {"ok": False, "error": "Reddit feed entry was not found."}
                message = "Reddit feed deleted."
            else:
                return {"ok": False, "error": "Invalid Reddit feed action."}
        except ValueError as exc:
            return {"ok": False, "error": str(exc)}
        except sqlite3.IntegrityError:
            return {
                "ok": False,
                "error": "That subreddit/channel feed already exists.",
            }
        except Exception:
            self.logger.exception("Failed to manage Reddit feeds from web admin")
            return {"ok": False, "error": "Failed to update Reddit feeds."}

        self.logger.info("Reddit feeds updated via web admin action=%s", action)
        response = self.build_reddit_feeds_web_payload(safe_guild_id)
        response["message"] = message
        return response

    def build_youtube_subscriptions_web_payload(self, guild_id: int):
        return {
            "ok": True,
            "subscriptions": self.list_youtube_subscriptions(
                guild_id=self.normalize_target_guild_id(guild_id),
                enabled_only=False,
            ),
        }

    def run_web_get_youtube_subscriptions(self, guild_id: int):
        try:
            return self.build_youtube_subscriptions_web_payload(guild_id)
        except Exception:
            self.logger.exception("Failed to build YouTube subscriptions payload for web admin")
            return {"ok": False, "error": "Unexpected error while loading YouTube subscriptions."}

    def run_web_manage_youtube_subscriptions(self, payload: dict, actor_email: str, guild_id: int):
        if not isinstance(payload, dict):
            return {"ok": False, "error": "Invalid YouTube subscription payload."}
        action = str(payload.get("action") or "").strip().lower()
        safe_guild_id = self.normalize_target_guild_id(guild_id)
        audit_actor = self.build_web_actor_audit_label(actor_email)
        try:
            if action == "add":
                source_url = str(payload.get("source_url") or "").strip()
                target_channel_id = int(str(payload.get("channel_id") or "0").strip())
                if target_channel_id <= 0:
                    return {"ok": False, "error": "Choose a valid Discord channel."}
                resolved = self.resolve_youtube_subscription_seed(source_url)
                guild = self.bot.get_guild(safe_guild_id)
                target_channel = guild.get_channel(target_channel_id) if guild else None
                target_channel_name = (
                    f"#{target_channel.name}"
                    if isinstance(target_channel, self.discord.TextChannel)
                    else str(target_channel_id)
                )
                self.create_or_update_youtube_subscription(
                    safe_guild_id,
                    source_url=resolved["source_url"],
                    channel_id=resolved["channel_id"],
                    channel_title=resolved["channel_title"],
                    target_channel_id=target_channel_id,
                    target_channel_name=target_channel_name,
                    last_video_id=resolved["last_video_id"],
                    last_video_title=resolved["last_video_title"],
                    last_published_at=resolved["last_published_at"],
                    actor_email=actor_email,
                )
                self.record_action_safe(
                    action="youtube_subscription_add",
                    status="success",
                    moderator=audit_actor,
                    target=resolved["channel_title"],
                    reason=self.truncate_log_text(resolved["source_url"]),
                    guild_id=safe_guild_id,
                )
                message = "YouTube subscription saved."
            elif action == "edit":
                subscription_id = int(str(payload.get("subscription_id") or "0").strip())
                if self.get_youtube_subscription(subscription_id, guild_id=safe_guild_id) is None:
                    return {"ok": False, "error": "YouTube subscription entry was not found."}
                source_url = str(payload.get("source_url") or "").strip()
                target_channel_id = int(str(payload.get("channel_id") or "0").strip())
                if target_channel_id <= 0:
                    return {"ok": False, "error": "Choose a valid Discord channel."}
                resolved = self.resolve_youtube_subscription_seed(source_url)
                guild = self.bot.get_guild(safe_guild_id)
                target_channel = guild.get_channel(target_channel_id) if guild else None
                target_channel_name = (
                    f"#{target_channel.name}"
                    if isinstance(target_channel, self.discord.TextChannel)
                    else str(target_channel_id)
                )
                if not self.update_youtube_subscription(
                    subscription_id,
                    safe_guild_id,
                    source_url=resolved["source_url"],
                    channel_id=resolved["channel_id"],
                    channel_title=resolved["channel_title"],
                    target_channel_id=target_channel_id,
                    target_channel_name=target_channel_name,
                    last_video_id=resolved["last_video_id"],
                    last_video_title=resolved["last_video_title"],
                    last_published_at=resolved["last_published_at"],
                    actor_email=actor_email,
                ):
                    return {"ok": False, "error": "YouTube subscription entry was not found."}
                self.record_action_safe(
                    action="youtube_subscription_edit",
                    status="success",
                    moderator=audit_actor,
                    target=resolved["channel_title"],
                    reason=self.truncate_log_text(resolved["source_url"]),
                    guild_id=safe_guild_id,
                )
                message = "YouTube subscription updated."
            elif action == "delete":
                subscription_id = int(str(payload.get("subscription_id") or "0").strip())
                if not self.delete_youtube_subscription(subscription_id, guild_id=safe_guild_id):
                    return {"ok": False, "error": "YouTube subscription entry was not found."}
                self.record_action_safe(
                    action="youtube_subscription_delete",
                    status="success",
                    moderator=audit_actor,
                    target=str(subscription_id),
                    reason="Deleted via web admin",
                    guild_id=safe_guild_id,
                )
                message = "YouTube subscription deleted."
            else:
                return {"ok": False, "error": "Invalid YouTube subscription action."}
        except ValueError as exc:
            return {"ok": False, "error": str(exc)}
        except Exception:
            self.logger.exception("Failed to manage YouTube subscriptions from web admin")
            return {"ok": False, "error": "Failed to manage YouTube subscriptions."}

        response = self.build_youtube_subscriptions_web_payload(safe_guild_id)
        response["message"] = message
        return response

    def build_linkedin_subscriptions_web_payload(self, guild_id: int):
        return {
            "ok": True,
            "subscriptions": self.list_linkedin_subscriptions(
                guild_id=self.normalize_target_guild_id(guild_id),
                enabled_only=False,
            ),
        }

    def run_web_get_linkedin_subscriptions(self, guild_id: int):
        try:
            return self.build_linkedin_subscriptions_web_payload(guild_id)
        except Exception:
            self.logger.exception("Failed to build LinkedIn subscriptions payload for web admin")
            return {"ok": False, "error": "Unexpected error while loading LinkedIn subscriptions."}

    def run_web_manage_linkedin_subscriptions(self, payload: dict, actor_email: str, guild_id: int):
        if not isinstance(payload, dict):
            return {"ok": False, "error": "Invalid LinkedIn subscription payload."}
        action = str(payload.get("action") or "").strip().lower()
        safe_guild_id = self.normalize_target_guild_id(guild_id)
        audit_actor = self.build_web_actor_audit_label(actor_email)
        try:
            if action == "add":
                source_url = str(payload.get("source_url") or "").strip()
                target_channel_id = int(str(payload.get("channel_id") or "0").strip())
                if target_channel_id <= 0:
                    return {"ok": False, "error": "Choose a valid Discord channel."}
                resolved = self.resolve_linkedin_subscription_seed(source_url)
                guild = self.bot.get_guild(safe_guild_id)
                target_channel = guild.get_channel(target_channel_id) if guild else None
                if not isinstance(target_channel, self.discord.TextChannel):
                    return {"ok": False, "error": "Choose a valid Discord text channel."}
                target_channel_name = f"#{target_channel.name}"
                self.create_or_update_linkedin_subscription(
                    safe_guild_id,
                    source_url=resolved["source_url"],
                    profile_name=resolved["profile_name"],
                    target_channel_id=target_channel_id,
                    target_channel_name=target_channel_name,
                    last_post_id=resolved["last_post_id"],
                    last_post_url=resolved["last_post_url"],
                    last_post_text=resolved["last_post_text"],
                    last_published_at=resolved["last_published_at"],
                    actor_email=actor_email,
                )
                self.record_action_safe(
                    action="linkedin_subscription_add",
                    status="success",
                    moderator=audit_actor,
                    target=resolved["profile_name"],
                    reason=self.truncate_log_text(resolved["source_url"]),
                    guild_id=safe_guild_id,
                )
                message = "LinkedIn subscription saved."
            elif action == "edit":
                subscription_id = int(str(payload.get("subscription_id") or "0").strip())
                if self.get_linkedin_subscription(subscription_id, guild_id=safe_guild_id) is None:
                    return {"ok": False, "error": "LinkedIn subscription entry was not found."}
                source_url = str(payload.get("source_url") or "").strip()
                target_channel_id = int(str(payload.get("channel_id") or "0").strip())
                if target_channel_id <= 0:
                    return {"ok": False, "error": "Choose a valid Discord channel."}
                resolved = self.resolve_linkedin_subscription_seed(source_url)
                guild = self.bot.get_guild(safe_guild_id)
                target_channel = guild.get_channel(target_channel_id) if guild else None
                if not isinstance(target_channel, self.discord.TextChannel):
                    return {"ok": False, "error": "Choose a valid Discord text channel."}
                target_channel_name = f"#{target_channel.name}"
                if not self.update_linkedin_subscription(
                    subscription_id,
                    safe_guild_id,
                    source_url=resolved["source_url"],
                    profile_name=resolved["profile_name"],
                    target_channel_id=target_channel_id,
                    target_channel_name=target_channel_name,
                    last_post_id=resolved["last_post_id"],
                    last_post_url=resolved["last_post_url"],
                    last_post_text=resolved["last_post_text"],
                    last_published_at=resolved["last_published_at"],
                    actor_email=actor_email,
                ):
                    return {"ok": False, "error": "LinkedIn subscription entry was not found."}
                self.record_action_safe(
                    action="linkedin_subscription_edit",
                    status="success",
                    moderator=audit_actor,
                    target=resolved["profile_name"],
                    reason=self.truncate_log_text(resolved["source_url"]),
                    guild_id=safe_guild_id,
                )
                message = "LinkedIn subscription updated."
            elif action == "delete":
                subscription_id = int(str(payload.get("subscription_id") or "0").strip())
                if not self.delete_linkedin_subscription(subscription_id, guild_id=safe_guild_id):
                    return {"ok": False, "error": "LinkedIn subscription entry was not found."}
                self.record_action_safe(
                    action="linkedin_subscription_delete",
                    status="success",
                    moderator=audit_actor,
                    target=str(subscription_id),
                    reason="Deleted via web admin",
                    guild_id=safe_guild_id,
                )
                message = "LinkedIn subscription deleted."
            else:
                return {"ok": False, "error": "Invalid LinkedIn subscription action."}
        except ValueError as exc:
            return {"ok": False, "error": str(exc)}
        except Exception:
            self.logger.exception("Failed to manage LinkedIn subscriptions from web admin")
            return {"ok": False, "error": "Failed to manage LinkedIn subscriptions."}

        response = self.build_linkedin_subscriptions_web_payload(safe_guild_id)
        response["message"] = message
        return response

    def build_beta_program_subscriptions_web_payload(self, guild_id: int):
        subscriptions = self.list_beta_program_subscriptions(
            guild_id=self.normalize_target_guild_id(guild_id),
            enabled_only=False,
        )
        for subscription in subscriptions:
            subscription["program_count"] = len(subscription.get("programs") or [])
        return {
            "ok": True,
            "source_url": self.beta_program_page_url,
            "subscriptions": subscriptions,
        }

    def run_web_get_beta_program_subscriptions(self, guild_id: int):
        try:
            return self.build_beta_program_subscriptions_web_payload(guild_id)
        except Exception:
            self.logger.exception("Failed to build GL.iNet beta program subscriptions payload for web admin")
            return {
                "ok": False,
                "error": "Unexpected error while loading GL.iNet beta program subscriptions.",
            }

    def run_web_manage_beta_program_subscriptions(self, payload: dict, actor_email: str, guild_id: int):
        if not isinstance(payload, dict):
            return {"ok": False, "error": "Invalid GL.iNet beta program payload."}
        action = str(payload.get("action") or "").strip().lower()
        safe_guild_id = self.normalize_target_guild_id(guild_id)
        audit_actor = self.build_web_actor_audit_label(actor_email)
        try:
            if action == "add":
                target_channel_id = int(str(payload.get("channel_id") or "0").strip())
                if target_channel_id <= 0:
                    return {"ok": False, "error": "Choose a valid Discord channel."}
                resolved = self.resolve_beta_program_subscription_seed(self.beta_program_page_url)
                guild = self.bot.get_guild(safe_guild_id)
                target_channel = guild.get_channel(target_channel_id) if guild else None
                if not isinstance(target_channel, self.discord.TextChannel):
                    return {"ok": False, "error": "Choose a valid Discord text channel."}
                target_channel_name = f"#{target_channel.name}"
                self.create_or_update_beta_program_subscription(
                    safe_guild_id,
                    source_url=resolved["source_url"],
                    source_name=resolved["source_name"],
                    target_channel_id=target_channel_id,
                    target_channel_name=target_channel_name,
                    last_snapshot_json=resolved["last_snapshot_json"],
                    actor_email=actor_email,
                )
                self.record_action_safe(
                    action="beta_program_subscription_add",
                    status="success",
                    moderator=audit_actor,
                    target=target_channel_name,
                    reason=self.truncate_log_text(resolved["source_url"]),
                    guild_id=safe_guild_id,
                )
                message = "GL.iNet beta program monitor saved."
            elif action == "delete":
                subscription_id = int(str(payload.get("subscription_id") or "0").strip())
                if not self.delete_beta_program_subscription(subscription_id, guild_id=safe_guild_id):
                    return {"ok": False, "error": "GL.iNet beta program entry was not found."}
                self.record_action_safe(
                    action="beta_program_subscription_delete",
                    status="success",
                    moderator=audit_actor,
                    target=str(subscription_id),
                    reason="Deleted via web admin",
                    guild_id=safe_guild_id,
                )
                message = "GL.iNet beta program monitor deleted."
            else:
                return {"ok": False, "error": "Invalid GL.iNet beta program action."}
        except ValueError as exc:
            return {"ok": False, "error": str(exc)}
        except Exception:
            self.logger.exception("Failed to manage GL.iNet beta program subscriptions from web admin")
            return {"ok": False, "error": "Failed to manage GL.iNet beta program subscriptions."}

        response = self.build_beta_program_subscriptions_web_payload(safe_guild_id)
        response["message"] = message
        return response

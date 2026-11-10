from __future__ import annotations

import sqlite3


class ForumTagWebCallbacks:
    """Web admin callbacks for forum tag cross-post subscriptions."""

    def __init__(
        self,
        *,
        normalize_target_guild_id,
        normalize_forum_tag_name,
        list_forum_tag_subscriptions,
        get_forum_tag_subscription,
        create_forum_tag_subscription,
        update_forum_tag_subscription,
        set_forum_tag_subscription_enabled,
        delete_forum_tag_subscription,
        record_action_safe,
        build_web_actor_audit_label,
        truncate_log_text,
        logger,
    ):
        self.normalize_target_guild_id = normalize_target_guild_id
        self.normalize_forum_tag_name = normalize_forum_tag_name
        self.list_forum_tag_subscriptions = list_forum_tag_subscriptions
        self.get_forum_tag_subscription = get_forum_tag_subscription
        self.create_forum_tag_subscription = create_forum_tag_subscription
        self.update_forum_tag_subscription = update_forum_tag_subscription
        self.set_forum_tag_subscription_enabled = set_forum_tag_subscription_enabled
        self.delete_forum_tag_subscription = delete_forum_tag_subscription
        self.record_action_safe = record_action_safe
        self.build_web_actor_audit_label = build_web_actor_audit_label
        self.truncate_log_text = truncate_log_text
        self.logger = logger

    def build_forum_tags_web_payload(self, guild_id: int):
        return {
            "ok": True,
            "subscriptions": self.list_forum_tag_subscriptions(
                guild_id=self.normalize_target_guild_id(guild_id),
                enabled_only=False,
            ),
        }

    def run_web_get_forum_tags(self, guild_id: int):
        try:
            return self.build_forum_tags_web_payload(guild_id)
        except Exception:
            self.logger.exception("Failed to build forum tag subscriptions payload for web admin")
            return {"ok": False, "error": "Unexpected error while loading forum tag subscriptions."}

    def run_web_manage_forum_tags(self, payload: dict, actor_email: str, guild_id: int, actor_display_name: str = ""):
        if not isinstance(payload, dict):
            return {"ok": False, "error": "Invalid forum tag payload."}

        action = str(payload.get("action") or "").strip().lower()
        safe_guild_id = self.normalize_target_guild_id(guild_id)
        audit_actor = self.build_web_actor_audit_label(actor_email, actor_display_name)
        try:
            if action == "add":
                tag = self.normalize_forum_tag_name(payload.get("tag"))
                if not tag:
                    return {"ok": False, "error": "Enter a valid forum tag."}
                channel_id = int(str(payload.get("channel_id") or "0").strip())
                if channel_id <= 0:
                    return {"ok": False, "error": "Choose a valid Discord channel."}
                self.create_forum_tag_subscription(safe_guild_id, tag, channel_id, actor_email)
                self.record_action_safe(
                    action="forum_tag_add",
                    status="success",
                    moderator=audit_actor,
                    target=f"#{tag}",
                    reason=self.truncate_log_text(f"Cross-post to channel {channel_id}"),
                    guild_id=safe_guild_id,
                )
                message = f"Forum tag #{tag} will now be cross-posted to the selected channel."
            elif action == "edit":
                subscription_id = int(str(payload.get("subscription_id") or "0").strip())
                existing = self.get_forum_tag_subscription(subscription_id, guild_id=safe_guild_id)
                if existing is None or int(existing.get("guild_id") or 0) != safe_guild_id:
                    return {"ok": False, "error": "Forum tag subscription was not found."}
                tag = self.normalize_forum_tag_name(payload.get("tag"))
                if not tag:
                    return {"ok": False, "error": "Enter a valid forum tag."}
                channel_id = int(str(payload.get("channel_id") or "0").strip())
                if channel_id <= 0:
                    return {"ok": False, "error": "Choose a valid Discord channel."}
                if not self.update_forum_tag_subscription(subscription_id, safe_guild_id, tag, channel_id, actor_email):
                    return {"ok": False, "error": "Forum tag subscription was not found."}
                self.record_action_safe(
                    action="forum_tag_edit",
                    status="success",
                    moderator=audit_actor,
                    target=f"#{tag}",
                    reason=self.truncate_log_text(f"Cross-post to channel {channel_id}"),
                    guild_id=safe_guild_id,
                )
                message = f"Forum tag #{tag} subscription updated."
            elif action == "toggle":
                subscription_id = int(str(payload.get("subscription_id") or "0").strip())
                existing = self.get_forum_tag_subscription(subscription_id, guild_id=safe_guild_id)
                if existing is None or int(existing.get("guild_id") or 0) != safe_guild_id:
                    return {"ok": False, "error": "Forum tag subscription was not found."}
                enabled = str(payload.get("enabled") or "").strip().lower() in {"1", "true", "yes", "on"}
                if not self.set_forum_tag_subscription_enabled(subscription_id, enabled, actor_email):
                    return {"ok": False, "error": "Forum tag subscription was not found."}
                self.record_action_safe(
                    action="forum_tag_toggle",
                    status="success",
                    moderator=audit_actor,
                    target=f"#{existing.get('tag') or subscription_id}",
                    reason=f"Enabled={enabled} via web admin",
                    guild_id=safe_guild_id,
                )
                message = "Forum tag subscription enabled." if enabled else "Forum tag subscription disabled."
            elif action == "delete":
                subscription_id = int(str(payload.get("subscription_id") or "0").strip())
                existing = self.get_forum_tag_subscription(subscription_id, guild_id=safe_guild_id)
                if existing is None or int(existing.get("guild_id") or 0) != safe_guild_id:
                    return {"ok": False, "error": "Forum tag subscription was not found."}
                tag = existing.get("tag") or str(subscription_id)
                if not self.delete_forum_tag_subscription(subscription_id):
                    return {"ok": False, "error": "Forum tag subscription was not found."}
                self.record_action_safe(
                    action="forum_tag_delete",
                    status="success",
                    moderator=audit_actor,
                    target=f"#{tag}",
                    reason="Deleted via web admin",
                    guild_id=safe_guild_id,
                )
                message = f"Forum tag #{tag} subscription deleted."
            else:
                return {"ok": False, "error": "Invalid forum tag action."}
        except ValueError as exc:
            return {"ok": False, "error": str(exc)}
        except sqlite3.IntegrityError:
            return {"ok": False, "error": "That forum tag is already configured for this server."}
        except Exception:
            self.logger.exception("Failed to manage forum tag subscriptions from web admin")
            return {"ok": False, "error": "Failed to update forum tag subscriptions."}

        self.logger.info("Forum tag subscriptions updated via web admin actor=%s action=%s", audit_actor, action)
        response = self.build_forum_tags_web_payload(safe_guild_id)
        response["message"] = message
        return response

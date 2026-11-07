from __future__ import annotations


class ReactionRolesWebCallbacks:
    def __init__(
        self,
        *,
        normalize_target_guild_id,
        normalize_reaction_role_emoji,
        normalize_reaction_role_message_id,
        list_reaction_role_mappings,
        save_reaction_role_mapping,
        set_reaction_role_mapping_status,
        delete_reaction_role_mapping,
        build_web_actor_audit_label,
        record_action_safe,
        truncate_log_text,
        logger,
        validate_reaction_role_message_for_guild,
        post_reaction_role_message_for_guild=None,
    ):
        self.normalize_target_guild_id = normalize_target_guild_id
        self.normalize_reaction_role_emoji = normalize_reaction_role_emoji
        self.normalize_reaction_role_message_id = normalize_reaction_role_message_id
        self.list_reaction_role_mappings = list_reaction_role_mappings
        self.save_reaction_role_mapping = save_reaction_role_mapping
        self.set_reaction_role_mapping_status = set_reaction_role_mapping_status
        self.delete_reaction_role_mapping = delete_reaction_role_mapping
        self.build_web_actor_audit_label = build_web_actor_audit_label
        self.record_action_safe = record_action_safe
        self.truncate_log_text = truncate_log_text
        self.logger = logger
        self.validate_reaction_role_message_for_guild = validate_reaction_role_message_for_guild
        self.post_reaction_role_message_for_guild = post_reaction_role_message_for_guild

    def build_reaction_roles_web_payload(self, guild_id: int):
        safe_guild_id = self.normalize_target_guild_id(guild_id)
        return {
            "ok": True,
            "mappings": self.list_reaction_role_mappings(safe_guild_id),
        }

    def run_web_get_reaction_roles(self, guild_id: int):
        try:
            return self.build_reaction_roles_web_payload(guild_id)
        except Exception:
            self.logger.exception("Failed to build reaction role payload for web admin")
            return {"ok": False, "error": "Unexpected error while loading reaction roles."}

    def run_web_manage_reaction_roles(self, payload: dict, actor_email: str, guild_id: int):
        if not isinstance(payload, dict):
            return {"ok": False, "error": "Invalid reaction role payload."}

        safe_guild_id = self.normalize_target_guild_id(guild_id)
        action = str(payload.get("action") or "").strip().lower()
        audit_actor = self.build_web_actor_audit_label(actor_email)
        try:
            if action == "post_message":
                channel_id = int(str(payload.get("channel_id") or "0").strip())
                if channel_id <= 0:
                    return {"ok": False, "error": "Please choose a valid text channel."}
                message_content = str(payload.get("message_content") or "").strip()
                if not message_content:
                    return {"ok": False, "error": "Message content cannot be empty."}

                raw_pairs = payload.get("pairs") or []
                if not isinstance(raw_pairs, list) or not raw_pairs:
                    return {"ok": False, "error": "Please configure at least one emoji and role connection."}

                valid_pairs = []
                seen_emoji_keys = set()
                for pair in raw_pairs:
                    if not isinstance(pair, dict):
                        continue
                    emoji_norm = self.normalize_reaction_role_emoji(pair.get("emoji"))
                    if emoji_norm is None:
                        return {"ok": False, "error": f"Invalid emoji: {pair.get('emoji')}"}
                    emoji_key = emoji_norm["emoji_key"]
                    if emoji_key in seen_emoji_keys:
                        return {
                            "ok": False,
                            "error": f"Duplicate emoji specified: {pair.get('emoji')}. Each emoji can only be mapped to one role per message.",
                        }
                    seen_emoji_keys.add(emoji_key)

                    try:
                        role_id = int(str(pair.get("role_id") or "0").strip())
                    except (TypeError, ValueError):
                        role_id = 0
                    if role_id <= 0:
                        return {"ok": False, "error": "Please choose a valid Discord role for each emoji."}

                    valid_pairs.append({
                        "emoji": pair.get("emoji"),
                        "emoji_key": emoji_norm["emoji_key"],
                        "emoji_text": emoji_norm["emoji_text"],
                        "role_id": role_id,
                    })

                if not valid_pairs:
                    return {"ok": False, "error": "Please configure at least one valid emoji and role connection."}

                if not callable(self.post_reaction_role_message_for_guild):
                    return {"ok": False, "error": "Discord message posting is not available in this environment."}

                post_result = self.post_reaction_role_message_for_guild(
                    safe_guild_id,
                    channel_id=channel_id,
                    message_content=message_content,
                    pairs=valid_pairs,
                )
                if not isinstance(post_result, dict) or not post_result.get("ok"):
                    err = str(
                        post_result.get("error")
                        if isinstance(post_result, dict)
                        else "Failed to post reaction role message to Discord."
                    )
                    return {"ok": False, "error": err}

                msg_id = post_result.get("message_id")
                saved_count = post_result.get("saved_count")
                count = int(saved_count if saved_count is not None else len(valid_pairs))
                failed_emojis = post_result.get("failed_emojis") or []

                if count == 0 and len(valid_pairs) > 0:
                    return {"ok": False, "error": "Failed to save reaction role mappings for the posted message."}

                self.record_action_safe(
                    action="reaction_role_post_create",
                    status="success",
                    moderator=audit_actor,
                    target=str(msg_id or channel_id),
                    reason=self.truncate_log_text(f"Posted to channel {channel_id} with {count} reaction role(s)"),
                    guild_id=safe_guild_id,
                )
                if failed_emojis:
                    failed_str = ", ".join(failed_emojis)
                    message = f"Reaction role message posted ({msg_id}) with {count} role(s). Note: Could not add reactions for: {failed_str}."
                else:
                    message = f"Reaction role message successfully posted ({msg_id}) with {count} reaction role(s)!"
            elif action == "save":
                message_id = self.normalize_reaction_role_message_id(payload.get("message_id"))
                if message_id is None:
                    return {"ok": False, "error": "Message ID must be a valid Discord message ID."}
                channel_id = int(str(payload.get("channel_id") or "0").strip())
                emoji = self.normalize_reaction_role_emoji(payload.get("emoji"))
                if emoji is None:
                    return {"ok": False, "error": "Emoji is required."}
                role_id = int(str(payload.get("role_id") or "0").strip())
                if role_id <= 0:
                    return {"ok": False, "error": "Choose a valid Discord role."}
                status = str(payload.get("status") or "active").strip().lower()
                if status not in {"active", "paused", "disabled"}:
                    return {"ok": False, "error": "Status must be active, paused, or disabled."}

                validation = self.validate_reaction_role_message_for_guild(safe_guild_id, channel_id, message_id)
                if not isinstance(validation, dict) or not validation.get("ok"):
                    return {
                        "ok": False,
                        "error": str(
                            validation.get("error")
                            if isinstance(validation, dict)
                            else "Discord could not validate that message right now."
                        ),
                    }

                original_message_id = self.normalize_reaction_role_message_id(payload.get("original_message_id"))
                original_emoji = self.normalize_reaction_role_emoji(payload.get("original_emoji"))
                if (
                    original_message_id is not None
                    and original_emoji is not None
                    and (original_message_id != message_id or original_emoji["emoji_key"] != emoji["emoji_key"])
                ):
                    self.delete_reaction_role_mapping(
                        safe_guild_id,
                        message_id=original_message_id,
                        emoji=payload.get("original_emoji"),
                    )

                self.save_reaction_role_mapping(
                    safe_guild_id,
                    channel_id=channel_id,
                    message_id=message_id,
                    emoji=payload.get("emoji"),
                    role_id=role_id,
                    status=status,
                    emoji_text=emoji["emoji_text"],
                )
                self.record_action_safe(
                    action="reaction_role_save",
                    status="success",
                    moderator=audit_actor,
                    target=str(role_id),
                    reason=self.truncate_log_text(f"{message_id} {emoji['emoji_key']} {status}"),
                    guild_id=safe_guild_id,
                )
                message = "Reaction role mapping saved."
            elif action == "set_status":
                message_id = self.normalize_reaction_role_message_id(payload.get("message_id"))
                emoji = self.normalize_reaction_role_emoji(payload.get("emoji"))
                status = str(payload.get("status") or "").strip().lower()
                if message_id is None or emoji is None:
                    return {"ok": False, "error": "A valid message ID and emoji are required."}
                if status not in {"active", "paused", "disabled"}:
                    return {"ok": False, "error": "Status must be active, paused, or disabled."}
                if not self.set_reaction_role_mapping_status(
                    safe_guild_id,
                    message_id=message_id,
                    emoji=payload.get("emoji"),
                    status=status,
                ):
                    return {"ok": False, "error": "Reaction role mapping was not found."}
                self.record_action_safe(
                    action="reaction_role_status",
                    status="success",
                    moderator=audit_actor,
                    target=str(payload.get("role_id") or ""),
                    reason=self.truncate_log_text(f"{message_id} {emoji['emoji_key']} -> {status}"),
                    guild_id=safe_guild_id,
                )
                message = f"Reaction role mapping marked {status}."
            elif action == "delete":
                message_id = self.normalize_reaction_role_message_id(payload.get("message_id"))
                emoji = self.normalize_reaction_role_emoji(payload.get("emoji"))
                if message_id is None or emoji is None:
                    return {"ok": False, "error": "A valid message ID and emoji are required."}
                if not self.delete_reaction_role_mapping(safe_guild_id, message_id=message_id, emoji=payload.get("emoji")):
                    return {"ok": False, "error": "Reaction role mapping was not found."}
                self.record_action_safe(
                    action="reaction_role_delete",
                    status="success",
                    moderator=audit_actor,
                    target=str(payload.get("role_id") or ""),
                    reason=self.truncate_log_text(f"{message_id} {emoji['emoji_key']}"),
                    guild_id=safe_guild_id,
                )
                message = "Reaction role mapping deleted."
            else:
                return {"ok": False, "error": "Invalid reaction role action."}
        except ValueError as exc:
            return {"ok": False, "error": str(exc)}
        except Exception:
            self.logger.exception("Failed to manage reaction role mappings from web admin")
            return {"ok": False, "error": "Failed to update reaction role mappings."}

        response = self.build_reaction_roles_web_payload(safe_guild_id)
        response["message"] = message
        return response

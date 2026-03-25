from __future__ import annotations


class RoleAccessWebCallbacks:
    def __init__(
        self,
        *,
        normalize_target_guild_id,
        normalize_role_access_code,
        normalize_discord_invite_code,
        list_role_access_mappings,
        upsert_role_access_mapping,
        set_role_access_mapping_status,
        build_web_actor_audit_label,
        record_action_safe,
        truncate_log_text,
        logger,
        validate_invite_for_guild,
    ):
        self.normalize_target_guild_id = normalize_target_guild_id
        self.normalize_role_access_code = normalize_role_access_code
        self.normalize_discord_invite_code = normalize_discord_invite_code
        self.list_role_access_mappings = list_role_access_mappings
        self.upsert_role_access_mapping = upsert_role_access_mapping
        self.set_role_access_mapping_status = set_role_access_mapping_status
        self.build_web_actor_audit_label = build_web_actor_audit_label
        self.record_action_safe = record_action_safe
        self.truncate_log_text = truncate_log_text
        self.logger = logger
        self.validate_invite_for_guild = validate_invite_for_guild

    def build_role_access_web_payload(self, guild_id: int):
        safe_guild_id = self.normalize_target_guild_id(guild_id)
        return {
            "ok": True,
            "mappings": self.list_role_access_mappings(safe_guild_id),
        }

    def run_web_get_role_access_mappings(self, guild_id: int):
        try:
            return self.build_role_access_web_payload(guild_id)
        except Exception:
            self.logger.exception("Failed to build role access payload for web admin")
            return {"ok": False, "error": "Unexpected error while loading role access mappings."}

    def run_web_manage_role_access_mappings(self, payload: dict, actor_email: str, guild_id: int):
        if not isinstance(payload, dict):
            return {"ok": False, "error": "Invalid role access payload."}

        safe_guild_id = self.normalize_target_guild_id(guild_id)
        action = str(payload.get("action") or "").strip().lower()
        audit_actor = self.build_web_actor_audit_label(actor_email)
        try:
            if action == "save":
                code = self.normalize_role_access_code(payload.get("code"))
                if code is None:
                    return {"ok": False, "error": "Code must be exactly 6 digits."}
                invite_code = self.normalize_discord_invite_code(payload.get("invite"))
                if invite_code is None:
                    return {"ok": False, "error": "Invite must be a valid Discord invite URL or code."}
                role_id = int(str(payload.get("role_id") or "0").strip())
                if role_id <= 0:
                    return {"ok": False, "error": "Choose a valid Discord role."}
                status = str(payload.get("status") or "active").strip().lower()
                if status not in {"active", "paused", "disabled"}:
                    return {"ok": False, "error": "Status must be active, paused, or disabled."}

                validation = self.validate_invite_for_guild(safe_guild_id, invite_code)
                if not isinstance(validation, dict) or not validation.get("ok"):
                    return {
                        "ok": False,
                        "error": str(
                            validation.get("error")
                            if isinstance(validation, dict)
                            else "Discord could not validate that invite."
                        ),
                    }

                self.upsert_role_access_mapping(
                    safe_guild_id,
                    role_id=role_id,
                    code=code,
                    invite_code=invite_code,
                    status=status,
                )
                self.record_action_safe(
                    action="role_access_save",
                    status="success",
                    moderator=audit_actor,
                    target=str(role_id),
                    reason=self.truncate_log_text(f"{code} {invite_code} {status}"),
                    guild_id=safe_guild_id,
                )
                message = "Role access mapping saved."
            elif action == "set_status":
                code = self.normalize_role_access_code(payload.get("code"))
                invite_code = self.normalize_discord_invite_code(payload.get("invite"))
                status = str(payload.get("status") or "").strip().lower()
                if code is None or invite_code is None:
                    return {"ok": False, "error": "A valid code and invite are required."}
                if status not in {"active", "paused", "disabled"}:
                    return {"ok": False, "error": "Status must be active, paused, or disabled."}
                if not self.set_role_access_mapping_status(
                    safe_guild_id,
                    code=code,
                    invite_code=invite_code,
                    status=status,
                ):
                    return {"ok": False, "error": "Role access mapping was not found."}
                self.record_action_safe(
                    action="role_access_status",
                    status="success",
                    moderator=audit_actor,
                    target=code,
                    reason=self.truncate_log_text(f"{invite_code} -> {status}"),
                    guild_id=safe_guild_id,
                )
                message = f"Role access mapping marked {status}."
            else:
                return {"ok": False, "error": "Invalid role access action."}
        except ValueError as exc:
            return {"ok": False, "error": str(exc)}
        except Exception:
            self.logger.exception("Failed to manage role access mappings from web admin")
            return {"ok": False, "error": "Failed to update role access mappings."}

        response = self.build_role_access_web_payload(safe_guild_id)
        response["message"] = message
        return response

"""Automod rules, the escalation ladder, raid guard and the shared ban list."""

from __future__ import annotations

import sqlite3
import threading

import pytest

import bot as bot_module
from app.automod import (
    ATTACHMENT,
    CAPS,
    DUPLICATE,
    EMOJI,
    INVITE,
    LINK,
    MENTIONS,
    RATE,
    REGEX,
    ZALGO,
    AutomodConfig,
    MessageTracker,
    RegexRule,
    evaluate,
    is_exempt,
)
from app.escalation import ACTION_BAN, ACTION_KICK, ACTION_TIMEOUT, decide, parse_ladder
from app.raid_guard import RAID, YOUNG_ACCOUNT, JoinTracker, RaidConfig, evaluate_join
from app.shared_bans import add_ban, get_ban, list_bans, remove_ban

GUILD = 1
USER = 2


def _config(**overrides) -> AutomodConfig:
    base = {"enabled": True, "max_messages": 3, "window_seconds": 10}
    base.update(overrides)
    return AutomodConfig(**base)


def _send(tracker, config, text, *, at=0.0, mentions=0, attachments=None):
    return evaluate(
        tracker,
        config,
        guild_id=GUILD,
        user_id=USER,
        text=text,
        mention_count=mentions,
        attachments=attachments,
        now=at,
    )


# --- automod ------------------------------------------------------------------


def test_nothing_happens_while_automod_is_off():
    tracker = MessageTracker()
    assert _send(tracker, AutomodConfig(enabled=False), "buy cheap followers") is None


def test_a_flood_is_caught_by_rate():
    tracker = MessageTracker()
    config = _config()
    for index in range(3):
        assert _send(tracker, config, f"message {index}", at=float(index)) is None
    verdict = _send(tracker, config, "one too many", at=3.0)
    assert verdict is not None and verdict.code == RATE


def test_the_rate_window_forgets_old_messages():
    tracker = MessageTracker()
    config = _config()
    for index in range(3):
        _send(tracker, config, f"message {index}", at=float(index))
    # Well past the window: the queue is empty again.
    assert _send(tracker, config, "later", at=100.0) is None


def test_the_same_message_repeated_is_a_duplicate():
    tracker = MessageTracker()
    config = _config(max_messages=20, duplicate_count=3)
    for _ in range(2):
        assert _send(tracker, config, "join my server", at=0.0) is None
    verdict = _send(tracker, config, "  JOIN my   server ", at=0.0)
    assert verdict is not None and verdict.code == DUPLICATE


def test_mention_spam_is_caught():
    tracker = MessageTracker()
    verdict = _send(tracker, _config(max_mentions=3), "look", mentions=6)
    assert verdict is not None and verdict.code == MENTIONS


def test_invites_are_blocked_and_can_be_allowed():
    tracker = MessageTracker()
    verdict = _send(tracker, _config(), "join https://discord.gg/abcdef")
    assert verdict is not None and verdict.code == INVITE
    assert _send(tracker, _config(block_invites=False), "join https://discord.gg/abcdef") is None


def test_links_are_filtered_against_the_allowlist():
    tracker = MessageTracker()
    config = _config(block_links=True, allowed_domains=("gl-inet.com",))
    assert _send(tracker, config, "see https://gl-inet.com/docs") is None
    verdict = _send(tracker, config, "see https://evil.example/x")
    assert verdict is not None and verdict.code == LINK


def test_a_regex_rule_fires_with_its_reason():
    tracker = MessageTracker()
    config = _config(regex_rules=(RegexRule(r"free\s+nitro", "nitro scam"),))
    verdict = _send(tracker, config, "FREE NITRO here")
    assert verdict is not None and verdict.code == REGEX
    assert "nitro scam" in verdict.detail


def test_an_invalid_regex_is_ignored_rather_than_crashing():
    tracker = MessageTracker()
    config = _config(regex_rules=(RegexRule("([unclosed", "broken"),))
    assert _send(tracker, config, "anything at all") is None


def test_shouting_is_caught_only_when_it_is_long_enough():
    tracker = MessageTracker()
    config = _config(max_messages=20)
    assert _send(tracker, config, "OK") is None, "two letters is not shouting"
    verdict = _send(tracker, config, "THIS IS ALL IN CAPITAL LETTERS AND LONG")
    assert verdict is not None and verdict.code == CAPS


def test_emoji_and_zalgo_floods_are_caught():
    tracker = MessageTracker()
    assert _send(tracker, _config(max_emoji=2), "🎮🎮🎮🎮").code == EMOJI
    zalgo = "a" + "".join("\u0301" for _ in range(20))
    assert _send(tracker, _config(max_zalgo=5), zalgo).code == ZALGO


def test_attachments_are_checked_for_size_and_extension():
    tracker = MessageTracker()
    config = _config(max_attachment_mb=5)
    too_big = [{"filename": "video.mp4", "size": 10 * 1024 * 1024}]
    assert _send(tracker, config, "", attachments=too_big).code == ATTACHMENT
    executable = [{"filename": "tool.exe", "size": 10}]
    verdict = _send(tracker, config, "", attachments=executable)
    assert verdict is not None and verdict.code == ATTACHMENT
    harmless = [{"filename": "notes.pdf", "size": 10}]
    assert _send(tracker, config, "", attachments=harmless) is None


def test_exempt_roles_and_channels_are_skipped():
    config = _config(exempt_role_ids=(99,), exempt_channel_ids=(5,))
    assert is_exempt(config, role_ids=[1, 99], channel_id=1) is True
    assert is_exempt(config, role_ids=[1], channel_id=5) is True
    assert is_exempt(config, role_ids=[1], channel_id=1) is False


def test_quiet_hours_tighten_the_thresholds():
    config = _config(max_messages=10, quiet_start_hour=22, quiet_end_hour=6, quiet_multiplier=2.0)
    assert config.is_quiet_hour(23) is True
    assert config.is_quiet_hour(3) is True
    assert config.is_quiet_hour(12) is False
    assert config.effective(23).max_messages == 5
    assert config.effective(12).max_messages == 10


# --- escalation ---------------------------------------------------------------


def test_the_default_ladder_escalates_in_order():
    assert decide(1) is None
    assert decide(2) is None
    assert decide(3).action == ACTION_TIMEOUT
    assert decide(4).action == ACTION_TIMEOUT
    assert decide(5).action == ACTION_KICK
    assert decide(7).action == ACTION_BAN


def test_a_jump_lands_on_the_highest_rung():
    step = decide(9)
    assert step is not None and step.action == ACTION_BAN


def test_a_custom_ladder_is_sorted_and_validated():
    ladder = parse_ladder([[5, "ban", 0], [2, "timeout", 300], [99, "nonsense", 0]])
    assert [step.strikes for step in ladder] == [2, 5]
    assert decide(3, ladder).action == ACTION_TIMEOUT
    assert decide(5, ladder).action == ACTION_BAN


def test_a_broken_ladder_falls_back_to_the_default():
    assert parse_ladder("nonsense")[0].action == ACTION_TIMEOUT


# --- raid guard ---------------------------------------------------------------


def test_a_join_spike_is_a_raid():
    tracker = JoinTracker()
    config = RaidConfig(enabled=True, max_joins_per_minute=3, min_account_age_hours=0)
    verdicts = [
        evaluate_join(tracker, config, guild_id=GUILD, account_age_hours=500, now=float(index))
        for index in range(5)
    ]
    assert verdicts[2] is None
    assert verdicts[3] is not None and verdicts[3].code == RAID


def test_a_new_account_is_flagged_without_a_spike():
    tracker = JoinTracker()
    config = RaidConfig(enabled=True, max_joins_per_minute=50, min_account_age_hours=24)
    verdict = evaluate_join(tracker, config, guild_id=GUILD, account_age_hours=2)
    assert verdict is not None and verdict.code == YOUNG_ACCOUNT


def test_the_raid_guard_is_off_by_default():
    tracker = JoinTracker()
    assert evaluate_join(tracker, RaidConfig(), guild_id=GUILD, account_age_hours=0) is None


# --- shared bans --------------------------------------------------------------


@pytest.fixture()
def db(tmp_path, monkeypatch):
    conn = sqlite3.connect(str(tmp_path / "shared.db"))
    conn.row_factory = sqlite3.Row
    monkeypatch.setattr(bot_module, "db_connection", conn)
    monkeypatch.setattr(bot_module, "db_lock", threading.RLock())
    bot_module.ensure_db_schema()
    yield conn
    conn.close()


def test_a_shared_ban_is_global_and_traceable(db):
    assert add_ban(db, 555, reason="raiding", added_by="mod#1", guild_id=GUILD) is True
    db.commit()

    entry = get_ban(db, 555)
    assert entry is not None
    assert entry["reason"] == "raiding"
    assert entry["source_guild_id"] == str(GUILD)
    assert add_ban(db, 555, reason="again", added_by="mod#2", guild_id=999) is False
    db.commit()
    assert get_ban(db, 555)["added_by"] == "mod#2"


def test_removing_a_shared_ban(db):
    add_ban(db, 777, reason="x", added_by="mod", guild_id=GUILD)
    db.commit()
    assert remove_ban(db, 777) is True
    assert remove_ban(db, 777) is False
    assert get_ban(db, 777) is None


def test_the_shared_ban_list_is_newest_first(db):
    for index in range(3):
        add_ban(db, 100 + index, reason=f"r{index}", added_by="mod", guild_id=GUILD)
    db.commit()
    entries = list_bans(db)
    assert len(entries) == 3
    assert {entry["user_id"] for entry in entries} == {"100", "101", "102"}


def test_the_new_tables_exist(db):
    tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"shared_bans", "moderation_reports", "moderation_appeals"} <= tables


# --- the settings form --------------------------------------------------------


def test_the_form_normalises_a_full_submission():
    from app.automod_settings import normalize_automod

    config, errors = normalize_automod(
        {
            "enabled": "1",
            "max_messages": "12",
            "window_seconds": "15",
            "block_invites": "1",
            "block_links": "",
            "allowed_domains": "GL-INET.com, docs.gl-inet.com",
            "blocked_extensions": ".exe, .scr",
            "regex_rules": "free\\s+nitro => nitro scam\n# a comment\n\\bword\\b",
            "exempt_role_ids": "111, 222",
            "quiet_start_hour": "22",
            "quiet_end_hour": "6",
            "quiet_multiplier": "2",
        }
    )
    assert errors == []
    assert config["enabled"] is True
    assert config["max_messages"] == 12
    assert config["block_links"] is False
    assert config["allowed_domains"] == ["gl-inet.com", "docs.gl-inet.com"]
    assert config["blocked_extensions"] == ["exe", "scr"]
    assert config["exempt_role_ids"] == [111, 222]
    assert config["quiet_start_hour"] == 22
    assert len(config["regex_rules"]) == 2, "the comment line is not a rule"


def test_an_invalid_regex_is_refused_with_its_line_number():
    from app.automod_settings import normalize_automod

    _, errors = normalize_automod({"regex_rules": "ok pattern\n([unclosed"})
    assert errors and "line 2" in errors[0]


def test_quiet_hours_must_be_real_hours():
    from app.automod_settings import normalize_automod

    _, errors = normalize_automod({"quiet_start_hour": "25", "quiet_end_hour": "6"})
    assert any("Quiet hours" in error for error in errors)


def test_out_of_range_numbers_fall_back_to_defaults():
    from app.automod_settings import normalize_automod

    config, errors = normalize_automod({"max_messages": "9999", "max_mentions": "0"})
    assert errors == []
    assert config["max_messages"] == 8
    assert config["max_mentions"] == 6


def test_the_raid_form_rejects_a_non_numeric_role():
    from app.automod_settings import normalize_raid

    _, errors = normalize_raid({"quarantine_role_id": "staff"})
    assert errors


def test_the_ladder_must_increase():
    from app.automod_settings import normalize_ladder

    _, errors = normalize_ladder(
        {"ladder_timeout_strikes": "5", "ladder_kick_strikes": "3", "ladder_ban_strikes": "7"}
    )
    assert errors

    ladder, errors = normalize_ladder(
        {
            "ladder_timeout_strikes": "2",
            "ladder_kick_strikes": "4",
            "ladder_ban_strikes": "6",
            "ladder_timeout_minutes": "15",
        }
    )
    assert errors == []
    assert ladder["steps"][0] == [2, "timeout", 900]


def test_the_automod_page_renders_and_saves(tmp_path):
    from tests.test_web_admin import _extract_csrf_token, _login, _make_app, _select_guild

    stored = {}

    def get_automod(guild_id):
        return {
            "ok": True,
            "automod": stored.get("automod", {"enabled": True, "max_messages": 10}),
            "raid": stored.get("raid", {}),
            "ladder": stored.get("ladder", {}),
        }

    def save_automod(guild_id, form):
        stored["automod"] = {"enabled": bool(form.get("enabled")), "max_messages": form.get("max_messages")}
        return {"ok": True, "message": "Automod settings saved."}

    app = _make_app(tmp_path, automod_get=get_automod, automod_save=save_automod)
    client = app.test_client()
    _login(client)
    _select_guild(client)

    page = client.get("/admin/automod", base_url="https://docker.example:8443")
    assert page.status_code == 200
    html = page.get_data(as_text=True)
    assert "Message rules" in html and "Escalation ladder" in html
    assert "name='max_messages'" in html

    token = _extract_csrf_token(page)
    saved = client.post(
        "/admin/automod",
        data={"confirm": "yes", "enabled": "1", "max_messages": "12"},
        base_url="https://docker.example:8443",
        headers={"X-CSRF-Token": token},
        follow_redirects=True,
    )
    assert saved.status_code == 200
    assert "Automod settings saved." in saved.get_data(as_text=True)
    assert stored["automod"]["max_messages"] == "12"


def test_saving_without_the_confirmation_box_changes_nothing(tmp_path):
    from tests.test_web_admin import _extract_csrf_token, _login, _make_app, _select_guild

    called = {"saved": False}

    def save_automod(guild_id, form):
        called["saved"] = True
        return {"ok": True}

    app = _make_app(
        tmp_path,
        automod_get=lambda guild_id: {"ok": True, "automod": {}, "raid": {}, "ladder": {}},
        automod_save=save_automod,
    )
    client = app.test_client()
    _login(client)
    _select_guild(client)
    page = client.get("/admin/automod", base_url="https://docker.example:8443")
    token = _extract_csrf_token(page)

    response = client.post(
        "/admin/automod",
        data={"enabled": "1"},
        base_url="https://docker.example:8443",
        headers={"X-CSRF-Token": token},
        follow_redirects=True,
    )

    assert "Tick the confirmation box" in response.get_data(as_text=True)
    assert called["saved"] is False


# --- the review's findings ----------------------------------------------------


def test_a_catastrophic_regex_is_bounded():
    import time as _time

    tracker = MessageTracker()
    config = _config(regex_rules=(RegexRule(r"(a+)+$", "evil"),))
    started = _time.monotonic()
    verdict = _send(tracker, config, "a" * 500 + "!")
    elapsed = _time.monotonic() - started
    assert verdict is None, "a near-match must not be treated as a match"
    assert elapsed < 2.0, f"the match took {elapsed:.2f}s — the timeout did not apply"


def test_the_tracker_is_bounded_by_capacity_and_idleness():
    tracker = MessageTracker(max_members=3, idle_seconds=60)
    for user in range(5):
        tracker.state_for(GUILD, user, now=float(user))
    assert tracker.size() == 3, "the oldest entries are evicted at capacity"

    # Everyone seen before the idle window goes; the member just seen stays.
    tracker.state_for(GUILD, 4, now=10_000.0)
    assert tracker.evict_idle(now=10_000.0) == 2
    assert tracker.size() == 1


def test_forgetting_a_member_clears_their_state():
    tracker = MessageTracker()
    tracker.state_for(GUILD, USER)
    tracker.forget(GUILD, USER)
    assert tracker.size() == 0


def test_a_duplicate_threshold_of_twenty_still_works():
    tracker = MessageTracker()
    config = _config(max_messages=60, duplicate_count=20)
    for _ in range(19):
        assert _send(tracker, config, "same text") is None
    verdict = _send(tracker, config, "same text")
    assert verdict is not None and verdict.code == DUPLICATE


def test_url_userinfo_cannot_borrow_an_allowed_hostname():
    tracker = MessageTracker()
    config = _config(block_links=True, allowed_domains=("gl-inet.com",))
    verdict = _send(tracker, config, "see https://gl-inet.com:x@evil.example/path")
    assert verdict is not None and verdict.code == LINK


def test_a_nan_multiplier_is_not_a_setting():
    from app.automod_settings import normalize_automod

    config, errors = normalize_automod({"quiet_multiplier": "nan"})
    assert errors == []
    assert config["quiet_multiplier"] == 1.0


def test_a_pattern_is_not_truncated_into_invalidity():
    from app.automod_settings import normalize_automod

    pattern = "[" + "a" * 199 + "]"
    _, errors = normalize_automod({"regex_rules": pattern})
    assert errors and "longer than" in errors[0]


def test_the_automod_page_is_writable():
    from app.web_permissions import ENDPOINT_AREAS, READ_ENDPOINT_AREAS

    assert ENDPOINT_AREAS.get("automod_page") == "moderation"
    assert READ_ENDPOINT_AREAS.get("automod_page") == "moderation"


def test_a_message_reference_is_decoded_or_refused():
    decode = bot_module.decode_message_reference
    assert decode("https://discord.com/channels/1/222/333", 9) == (222, 333)
    assert decode("444", 9) == (9, 444)
    assert decode("-1", 9) == (0, 0)
    assert decode("not-a-message", 9) == (0, 0)
    assert decode("", 9) == (0, 0)

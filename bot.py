import discord
from discord.ext import commands
from discord import app_commands
import logging
import os
import json
import asyncio
import re
import time
import csv
import io
from datetime import timedelta, datetime, timezone
from html import unescape
from urllib.parse import urljoin
from dotenv import load_dotenv
import random
import requests
from bs4 import BeautifulSoup
from croniter import croniter

load_dotenv()

# Directory to persist data files. This folder is mounted as a Docker volume
# so codes and invites survive container rebuilds.
DATA_DIR = os.getenv("DATA_DIR", "data")
os.makedirs(DATA_DIR, exist_ok=True)

# Set up logging to console and persistent file
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logger = logging.getLogger("invite_bot")
logger.setLevel(LOG_LEVEL)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

file_handler = logging.FileHandler(os.path.join(DATA_DIR, "bot.log"))
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

TOKEN = os.getenv("DISCORD_TOKEN")
GUILD_ID = int(os.getenv("GUILD_ID"))
GENERAL_CHANNEL_ID = int(os.getenv("GENERAL_CHANNEL_ID", "0"))
FORUM_BASE_URL = os.getenv("FORUM_BASE_URL", "https://forum.gl-inet.com").rstrip("/")
FORUM_MAX_RESULTS = int(os.getenv("FORUM_MAX_RESULTS", "5"))
DOCS_MAX_RESULTS_PER_SITE = int(os.getenv("DOCS_MAX_RESULTS_PER_SITE", "2"))
DOCS_INDEX_TTL_SECONDS = int(os.getenv("DOCS_INDEX_TTL_SECONDS", "3600"))
SEARCH_RESPONSE_MAX_CHARS = int(os.getenv("SEARCH_RESPONSE_MAX_CHARS", "1900"))
FIRMWARE_FEED_URL = os.getenv("FIRMWARE_FEED_URL", "https://gl-fw.remotetohome.io/").strip()
FIRMWARE_NOTIFICATION_CHANNEL_RAW = os.getenv(
    "firmware_notification_channel",
    os.getenv("FIRMWARE_NOTIFY_CHANNEL_ID", ""),
).strip()
if FIRMWARE_NOTIFICATION_CHANNEL_RAW.startswith("<#") and FIRMWARE_NOTIFICATION_CHANNEL_RAW.endswith(">"):
    FIRMWARE_NOTIFICATION_CHANNEL_RAW = FIRMWARE_NOTIFICATION_CHANNEL_RAW[2:-1]
try:
    FIRMWARE_NOTIFY_CHANNEL_ID = int(FIRMWARE_NOTIFICATION_CHANNEL_RAW) if FIRMWARE_NOTIFICATION_CHANNEL_RAW else 0
except ValueError:
    logger.warning("Invalid firmware_notification_channel value: %s", FIRMWARE_NOTIFICATION_CHANNEL_RAW)
    FIRMWARE_NOTIFY_CHANNEL_ID = 0

FIRMWARE_CHECK_SCHEDULE = os.getenv("firmware_check_schedule", "").strip()
if not FIRMWARE_CHECK_SCHEDULE:
    legacy_interval_raw = os.getenv("FIRMWARE_CHECK_INTERVAL_SECONDS", "").strip()
    if legacy_interval_raw:
        try:
            interval_seconds = max(60, int(legacy_interval_raw))
            interval_minutes = max(1, interval_seconds // 60)
            FIRMWARE_CHECK_SCHEDULE = f"*/{interval_minutes} * * * *"
        except ValueError:
            logger.warning("Invalid FIRMWARE_CHECK_INTERVAL_SECONDS value: %s", legacy_interval_raw)
if not FIRMWARE_CHECK_SCHEDULE:
    FIRMWARE_CHECK_SCHEDULE = "*/30 * * * *"

FIRMWARE_REQUEST_TIMEOUT_SECONDS = int(os.getenv("FIRMWARE_REQUEST_TIMEOUT_SECONDS", "30"))
FIRMWARE_RELEASE_NOTES_MAX_CHARS = max(200, int(os.getenv("FIRMWARE_RELEASE_NOTES_MAX_CHARS", "900")))
COUNTRY_CODE_PATTERN = re.compile(r"^[A-Za-z]{2}$")
COUNTRY_LEGACY_SUFFIX_PATTERN = re.compile(r"_[A-Z]{2}$")
COUNTRY_FLAG_SUFFIX_PATTERN = re.compile(r"\s*-\s*[\U0001F1E6-\U0001F1FF]{2}$")
COUNTRY_CODE_SUFFIX_PATTERN = re.compile(r"\s*-\s*[A-Z]{2}$")
TIMEOUT_DURATION_PATTERN = re.compile(r"^\s*(\d+)\s*([mhd]?)\s*$", re.IGNORECASE)
MODERATOR_ROLE_IDS = {
    int(os.getenv("MODERATOR_ROLE_ID", "1294957416294645771")),
    int(os.getenv("ADMIN_ROLE_ID", "1138302148292116551")),
}
MOD_LOG_CHANNEL_ID = int(os.getenv("MOD_LOG_CHANNEL_ID", "1311820410269995009"))
KICK_PRUNE_HOURS = int(os.getenv("KICK_PRUNE_HOURS", "72"))
TIMEOUT_MAX_MINUTES = 28 * 24 * 60
CSV_ROLE_ASSIGN_MAX_NAMES = int(os.getenv("CSV_ROLE_ASSIGN_MAX_NAMES", "500"))
DOCS_SITE_MAP = {
    "kvm": ("KVM Docs", "https://docs.gl-inet.com/kvm/en"),
    "iot": ("IoT Docs", "https://docs.gl-inet.com/iot/en"),
    "router": ("Router Docs v4", "https://docs.gl-inet.com/router/en/4"),
}
DOCS_SOURCES = [DOCS_SITE_MAP["kvm"], DOCS_SITE_MAP["iot"], DOCS_SITE_MAP["router"]]

ROLE_FILE = os.path.join(DATA_DIR, "access_role.txt")
INVITE_FILE = os.path.join(DATA_DIR, "permanent_invite.txt")
CODES_FILE = os.path.join(DATA_DIR, "role_codes.txt")
INVITE_ROLE_FILE = os.path.join(DATA_DIR, "invite_roles.json")
TAG_RESPONSES_FILE = os.path.join(DATA_DIR, "tag_responses.json")
FIRMWARE_STATE_FILE = os.path.join(DATA_DIR, "firmware_seen.json")

DEFAULT_TAG_RESPONSES = {
    "!betatest": "✅ Thanks for your interest in the beta! We'll share more details soon.",
    "!support": "🛠️ Need help? Please open a ticket or message a moderator.",
}

intents = discord.Intents.default()
intents.members = True
intents.message_content = True

bot = commands.Bot(command_prefix='!', intents=intents, case_insensitive=True)
tree = bot.tree

tag_responses = {}
tag_responses_mtime = None
tag_command_names = set()
docs_index_cache = {}
firmware_monitor_task = None


def normalize_tag(tag: str) -> str:
    return tag.strip().lower()


def tag_to_command_name(tag: str) -> str:
    normalized = normalize_tag(tag)
    if normalized.startswith("!"):
        normalized = normalized[1:]
    if normalized.startswith("/"):
        normalized = normalized[1:]
    return normalized.replace(" ", "_")


def load_tag_responses():
    if not os.path.exists(TAG_RESPONSES_FILE):
        save_tag_responses(DEFAULT_TAG_RESPONSES)
        return {normalize_tag(k): str(v) for k, v in DEFAULT_TAG_RESPONSES.items()}
    try:
        with open(TAG_RESPONSES_FILE, "r") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            logger.warning("Tag responses file does not contain an object. Using empty mapping.")
            return {}
        return {normalize_tag(k): str(v) for k, v in data.items()}
    except Exception:
        logger.exception("Failed to load tag responses")
        return {}


def save_tag_responses(mapping):
    with open(TAG_RESPONSES_FILE, "w") as f:
        json.dump(mapping, f, indent=2)


def get_tag_responses():
    global tag_responses, tag_responses_mtime
    try:
        current_mtime = os.path.getmtime(TAG_RESPONSES_FILE)
    except FileNotFoundError:
        tag_responses = load_tag_responses()
        try:
            tag_responses_mtime = os.path.getmtime(TAG_RESPONSES_FILE)
        except FileNotFoundError:
            tag_responses_mtime = None
        return tag_responses
    if tag_responses_mtime != current_mtime:
        tag_responses = load_tag_responses()
        tag_responses_mtime = current_mtime
    return tag_responses


def build_command_list():
    tags = sorted(get_tag_responses().keys())
    if not tags:
        return "No tag commands are available yet."
    return "Tag commands:\n" + "\n".join(tags)


def register_tag_commands():
    global tag_command_names
    tag_commands = {}
    for tag, response in get_tag_responses().items():
        command_name = tag_to_command_name(tag)
        if not command_name:
            continue
        tag_commands[command_name] = response

    existing_names = {cmd.name for cmd in tree.get_commands()}
    for command_name, response in tag_commands.items():
        if command_name in existing_names:
            logger.warning("Skipping tag slash command /%s due to name conflict", command_name)
            continue

        def make_tag_reply(tag_response: str):
            async def tag_reply(interaction: discord.Interaction):
                await interaction.response.send_message(tag_response)

            return tag_reply

        try:
            tree.add_command(
                app_commands.Command(
                    name=command_name,
                    description=f"Tag response for {command_name}",
                    callback=make_tag_reply(response),
                ),
                guild=discord.Object(id=GUILD_ID),
            )
            tag_command_names.add(command_name)
        except app_commands.errors.CommandAlreadyRegistered:
            logger.info("Tag slash command /%s already registered", command_name)
        except TypeError:
            logger.exception("Failed to register tag slash command /%s", command_name)


def generate_code():
    while True:
        code = ""
        last_digit = None
        streak = 1
        for _ in range(6):
            digit = str(random.randint(0, 9))
            if digit == last_digit:
                streak += 1
            else:
                streak = 1
            if streak > 2:
                break
            code += digit
            last_digit = digit
        if len(code) == 6:
            logger.debug("Generated code %s", code)
            return code

def save_role_code(code, role_id):
    with open(CODES_FILE, 'a') as f:
        f.write(f"{code}:{role_id}\n")
    logger.info("Saved code %s for role %s", code, role_id)

def get_role_id_by_code(code):
    if not os.path.exists(CODES_FILE):
        return None
    with open(CODES_FILE, 'r') as f:
        for line in f:
            if line.startswith(code + ":"):
                role_id = int(line.strip().split(":")[1])
                logger.info("Code %s matched role %s", code, role_id)
                return role_id
    return None

def load_invite_roles():
    if not os.path.exists(INVITE_ROLE_FILE):
        return {}
    try:
        with open(INVITE_ROLE_FILE, "r") as f:
            import json
            return json.load(f)
    except Exception:
        logger.exception("Failed to load invite-role mappings")
        return {}

def save_invite_role(invite_code, role_id):
    mapping = load_invite_roles()
    mapping[invite_code] = role_id
    with open(INVITE_ROLE_FILE, "w") as f:
        import json
        json.dump(mapping, f)
    logger.info("Saved invite %s for role %s", invite_code, role_id)

def has_allowed_role(member: discord.Member):
    allowed = {"Employee", "Admin", "Gl.iNet Moderator"}
    has_role = any(role.name in allowed for role in member.roles)
    logger.debug("User %s allowed: %s", member, has_role)
    return has_role


def has_moderator_access(member: discord.Member):
    return any(role.id in MODERATOR_ROLE_IDS for role in member.roles)


def validate_moderation_target(actor: discord.Member, target: discord.Member, bot_member: discord.Member):
    if target.id == actor.id:
        return False, "❌ You cannot moderate yourself."
    if target.id == actor.guild.owner_id:
        return False, "❌ You cannot moderate the server owner."
    if target.id == bot_member.id:
        return False, "❌ You cannot moderate the bot."
    if actor.id != actor.guild.owner_id and actor.top_role <= target.top_role:
        return False, "❌ You can only moderate members below your top role."
    if bot_member.top_role <= target.top_role:
        return False, "❌ I can only moderate members below my top role."
    return True, None


def normalize_member_lookup_name(value: str):
    if not value:
        return None
    normalized = re.sub(r"\s+", " ", value.strip().lstrip("@")).casefold()
    return normalized or None


def parse_member_names_from_csv_bytes(data: bytes):
    decoded = None
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            decoded = data.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    if decoded is None:
        return []

    names = []
    reader = csv.reader(io.StringIO(decoded))
    for row in reader:
        for cell in row:
            candidate = cell.strip()
            if candidate:
                names.append(candidate)
    return names


def build_member_name_lookup(guild: discord.Guild):
    lookup = {}
    for member in guild.members:
        candidates = {
            member.name,
            member.display_name,
            member.global_name,
            str(member),
        }
        if member.discriminator and member.discriminator != "0":
            candidates.add(f"{member.name}#{member.discriminator}")

        for candidate in candidates:
            key = normalize_member_lookup_name(candidate)
            if not key:
                continue
            lookup.setdefault(key, []).append(member)
    return lookup


def unique_member_names(values: list[str]):
    seen = set()
    unique = []
    for value in values:
        key = normalize_member_lookup_name(value)
        if not key or key in seen:
            continue
        seen.add(key)
        unique.append(value.strip())
    return unique


def parse_timeout_duration(value: str):
    match = TIMEOUT_DURATION_PATTERN.fullmatch(value or "")
    if not match:
        return None, None, "❌ Invalid duration. Use `30m`, `2h`, or `1d`."

    amount = int(match.group(1))
    unit = (match.group(2) or "m").lower()
    multiplier = {"m": 1, "h": 60, "d": 1440}
    total_minutes = amount * multiplier[unit]
    if total_minutes < 1:
        return None, None, "❌ Duration must be at least 1 minute."
    if total_minutes > TIMEOUT_MAX_MINUTES:
        return None, None, "❌ Duration cannot exceed 28 days."
    return timedelta(minutes=total_minutes), f"{amount}{unit}", None


def normalize_country_code(value: str):
    normalized = value.strip().upper()
    if COUNTRY_CODE_PATTERN.fullmatch(normalized):
        return normalized
    return None


def strip_country_suffix(name: str):
    without_flag = COUNTRY_FLAG_SUFFIX_PATTERN.sub("", name)
    without_code = COUNTRY_CODE_SUFFIX_PATTERN.sub("", without_flag)
    without_legacy = COUNTRY_LEGACY_SUFFIX_PATTERN.sub("", without_code)
    return without_legacy.rstrip(" _-")


def build_country_nickname(member: discord.Member, country_code: str):
    base_name = member.nick or member.display_name or member.name
    base_name = strip_country_suffix(base_name)
    if not base_name:
        base_name = member.name or "user"

    suffix = f" - {country_code}"
    max_base_length = 32 - len(suffix)
    trimmed_base = base_name[:max_base_length].rstrip() or base_name[:max_base_length]
    if not trimmed_base:
        trimmed_base = "user"[:max_base_length]
    return f"{trimmed_base}{suffix}"


async def set_member_country(member: discord.Member, country_code: str):
    nickname = build_country_nickname(member, country_code)
    if member.nick == nickname:
        return False, f"ℹ️ Your nickname already includes `{country_code}`."
    await member.edit(nick=nickname, reason=f"Set country code to {country_code}")
    return True, f"✅ Country updated. Your nickname is now `{nickname}`."


async def clear_member_country(member: discord.Member):
    if not member.nick:
        return False, "❌ You do not currently have a server nickname to update."

    stripped = strip_country_suffix(member.nick)
    if stripped == member.nick:
        return False, "❌ Your nickname does not end with a country code suffix."

    await member.edit(nick=stripped or None, reason="Clear country code suffix")
    if stripped:
        return True, f"✅ Country removed. Your nickname is now `{stripped}`."
    return True, "✅ Country removed. Your nickname has been reset."


async def prune_user_messages(guild: discord.Guild, user_id: int, hours: int):
    cutoff = discord.utils.utcnow() - timedelta(hours=hours)
    deleted_count = 0
    scanned_channels = 0
    reason = f"Prune last {hours}h messages for kicked member {user_id}"
    channels = list(guild.text_channels) + list(guild.threads)
    seen_channel_ids = set()

    for channel in channels:
        if channel.id in seen_channel_ids:
            continue
        seen_channel_ids.add(channel.id)

        perms = channel.permissions_for(guild.me)
        if not (perms.view_channel and perms.read_message_history and perms.manage_messages):
            continue

        scanned_channels += 1
        try:
            deleted = await channel.purge(
                limit=None,
                after=cutoff,
                check=lambda message: message.author.id == user_id,
                bulk=True,
                reason=reason,
            )
            deleted_count += len(deleted)
        except discord.Forbidden:
            logger.warning("Skipping channel %s while pruning: missing permissions", channel.id)
        except discord.HTTPException:
            logger.exception("Failed to prune messages in channel %s", channel.id)

    return deleted_count, scanned_channels


async def resolve_mod_log_channel(guild: discord.Guild):
    channel = guild.get_channel(MOD_LOG_CHANNEL_ID)
    if channel is None:
        channel = bot.get_channel(MOD_LOG_CHANNEL_ID)
    if channel is None:
        try:
            channel = await bot.fetch_channel(MOD_LOG_CHANNEL_ID)
        except discord.NotFound:
            logger.warning("Moderation log channel %s not found", MOD_LOG_CHANNEL_ID)
            return None
        except discord.Forbidden:
            logger.warning("No permission to access moderation log channel %s", MOD_LOG_CHANNEL_ID)
            return None
        except discord.HTTPException:
            logger.exception("Failed to fetch moderation log channel %s", MOD_LOG_CHANNEL_ID)
            return None

    if isinstance(channel, (discord.TextChannel, discord.Thread)):
        return channel

    logger.warning("Moderation log channel %s is not a text channel", MOD_LOG_CHANNEL_ID)
    return None


async def send_moderation_log(
    guild: discord.Guild,
    actor: discord.Member,
    action: str,
    target: discord.Member | None = None,
    reason: str | None = None,
    outcome: str = "success",
    details: str | None = None,
):
    channel = await resolve_mod_log_channel(guild)
    if channel is None:
        return False

    target_text = f"{target} (`{target.id}`)" if target else "N/A"
    reason_text = reason or "N/A"
    details_text = details or "N/A"
    message = (
        "🛡️ **Moderation Action**\n"
        f"**Moderator:** {actor.mention} (`{actor.id}`)\n"
        f"**Action:** `{action}`\n"
        f"**Target:** {target_text}\n"
        f"**Outcome:** `{outcome}`\n"
        f"**Reason:** {reason_text}\n"
        f"**Details:** {details_text}"
    )
    try:
        await channel.send(message)
        return True
    except discord.Forbidden:
        logger.warning("No permission to send moderation logs to channel %s", MOD_LOG_CHANNEL_ID)
        return False
    except discord.HTTPException:
        logger.exception("Failed to send moderation log for action %s", action)
        return False


def clip_text(value: str, max_chars: int = 250):
    if not value:
        return "N/A"
    cleaned = value.strip().replace("\n", " ")
    if len(cleaned) <= max_chars:
        return cleaned
    return f"{cleaned[: max_chars - 3]}..."


async def send_server_event_log(guild: discord.Guild, event_name: str, details: str):
    channel = await resolve_mod_log_channel(guild)
    if channel is None:
        return False

    message = f"📌 **Server Event:** `{event_name}`\n{details}"
    try:
        await channel.send(message)
        return True
    except discord.Forbidden:
        logger.warning("No permission to send server event logs to channel %s", MOD_LOG_CHANNEL_ID)
        return False
    except discord.HTTPException:
        logger.exception("Failed to send server event log for %s", event_name)
        return False


def normalize_release_notes_text(value: str):
    lines = []
    for raw_line in (value or "").splitlines():
        cleaned = re.sub(r"\s+", " ", raw_line).strip()
        if cleaned:
            lines.append(cleaned)
    return "\n".join(lines)


def load_firmware_seen_ids():
    if not os.path.exists(FIRMWARE_STATE_FILE):
        return None
    try:
        with open(FIRMWARE_STATE_FILE, "r") as f:
            data = json.load(f)
        seen_ids = data.get("seen_ids", [])
        if not isinstance(seen_ids, list):
            return None
        return {str(item) for item in seen_ids if item}
    except Exception:
        logger.exception("Failed to load firmware state from %s", FIRMWARE_STATE_FILE)
        return None


def save_firmware_state(seen_ids: set[str], sync_label: str = ""):
    payload = {
        "source_url": FIRMWARE_FEED_URL,
        "updated_at": int(time.time()),
        "seen_ids": sorted(seen_ids),
    }
    if sync_label:
        payload["last_synced"] = sync_label
    with open(FIRMWARE_STATE_FILE, "w") as f:
        json.dump(payload, f, indent=2)


def parse_firmware_entries(page_html: str):
    soup = BeautifulSoup(page_html, "html.parser")
    sync_line = soup.select_one(".sync-line")
    sync_label = clean_search_text(sync_line.get_text(" ", strip=True)) if sync_line else ""
    entries = []

    for section in soup.select("section.model-section"):
        model_code = (section.get("id") or "").strip()
        model_name = model_code.upper() if model_code else "Unknown Model"
        heading = section.find("h2")
        if heading:
            code_tag = heading.find("span", class_="code")
            if code_tag is not None:
                code_tag.extract()
            model_name = clean_search_text(heading.get_text(" ", strip=True)) or model_name

        for row in section.find_all("div", class_="fw-row"):
            stage = (row.get("data-stage") or "unknown").strip().lower()
            version_tag = row.find("span", class_="fw-version")
            date_tag = row.find("span", class_="fw-date")
            version = clean_search_text(version_tag.get_text(" ", strip=True)) if version_tag else "unknown"
            published_date = clean_search_text(date_tag.get_text(" ", strip=True)) if date_tag else "unknown"

            files = []
            for link in row.select(".fw-files a[href]"):
                label = clean_search_text(link.get_text(" ", strip=True)) or "Download"
                url = link["href"].strip()
                if url:
                    files.append({"label": label, "url": url})

            sha256_values = []
            for badge in row.select(".fw-sha .sha-badge"):
                title = (badge.get("title") or "").strip()
                if title:
                    sha256_values.append(title.split(" —", 1)[0].strip())

            release_notes = ""
            notes_block = row.find_next_sibling()
            if notes_block and notes_block.name == "details" and "release-notes" in (notes_block.get("class") or []):
                notes_content = notes_block.find("div", class_="content")
                if notes_content:
                    release_notes = normalize_release_notes_text(notes_content.get_text("\n", strip=True))

            file_key = "|".join(sorted(file_info["url"] for file_info in files))
            sha_key = "|".join(sorted(value for value in sha256_values if value))
            entry_id = f"{model_code}|{stage}|{version}|{published_date}|{file_key or sha_key}"
            entries.append(
                {
                    "id": entry_id,
                    "model_code": model_code or "unknown",
                    "model_name": model_name,
                    "stage": stage,
                    "version": version,
                    "published_date": published_date,
                    "files": files,
                    "sha256": sha256_values,
                    "release_notes": release_notes,
                }
            )

    return entries, sync_label


def fetch_firmware_entries():
    response = requests.get(FIRMWARE_FEED_URL, timeout=FIRMWARE_REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    return parse_firmware_entries(response.text)


def format_release_notes_excerpt(release_notes: str):
    if not release_notes:
        return "- No release notes available on source page."

    excerpt_lines = []
    used_chars = 0
    for raw_line in release_notes.splitlines():
        line = clip_text(raw_line, max_chars=220)
        if not line or line == "N/A":
            continue
        projected = used_chars + len(line) + 3
        if excerpt_lines and projected > FIRMWARE_RELEASE_NOTES_MAX_CHARS:
            excerpt_lines.append("- ...")
            break
        excerpt_lines.append(f"- {line}")
        used_chars = projected
        if len(excerpt_lines) >= 12:
            excerpt_lines.append("- ...")
            break

    if not excerpt_lines:
        return "- No release notes available on source page."
    return "\n".join(excerpt_lines)


def trim_discord_message(message: str, max_chars: int = 1900):
    if len(message) <= max_chars:
        return message
    return message[: max_chars - 4].rstrip() + " ..."


def format_firmware_notification(entry: dict, sync_label: str):
    stage_text = "Stable" if entry["stage"] == "release" else "Testing" if entry["stage"] == "testing" else entry["stage"].title()
    lines = [
        "🆕 **New firmware mirrored**",
        f"**Model:** {entry['model_name']} (`{entry['model_code']}`)",
        f"**Track:** `{stage_text}`",
        f"**Version:** `{entry['version']}`",
        f"**Date:** `{entry['published_date']}`",
    ]

    if entry["files"]:
        lines.append("**Files:**")
        for file_info in entry["files"]:
            lines.append(f"- [{file_info['label']}]({file_info['url']})")

    if entry["sha256"]:
        lines.append("**SHA256:**")
        for sha_value in entry["sha256"][:2]:
            lines.append(f"- `{sha_value}`")

    lines.append("**Release Notes (excerpt):**")
    lines.append(format_release_notes_excerpt(entry["release_notes"]))
    if sync_label:
        lines.append(f"`{sync_label}`")
    lines.append(f"Source: {FIRMWARE_FEED_URL}")
    return trim_discord_message("\n".join(lines))


async def resolve_firmware_notify_channel():
    if FIRMWARE_NOTIFY_CHANNEL_ID <= 0:
        return None

    channel = bot.get_channel(FIRMWARE_NOTIFY_CHANNEL_ID)
    if channel is None:
        try:
            channel = await bot.fetch_channel(FIRMWARE_NOTIFY_CHANNEL_ID)
        except discord.NotFound:
            logger.warning("Firmware notify channel %s not found", FIRMWARE_NOTIFY_CHANNEL_ID)
            return None
        except discord.Forbidden:
            logger.warning("No permission to access firmware notify channel %s", FIRMWARE_NOTIFY_CHANNEL_ID)
            return None
        except discord.HTTPException:
            logger.exception("Failed to fetch firmware notify channel %s", FIRMWARE_NOTIFY_CHANNEL_ID)
            return None

    if isinstance(channel, (discord.TextChannel, discord.Thread)):
        return channel

    logger.warning("Firmware notify channel %s is not a text channel", FIRMWARE_NOTIFY_CHANNEL_ID)
    return None


async def check_firmware_updates_once():
    try:
        entries, sync_label = await asyncio.to_thread(fetch_firmware_entries)
    except requests.RequestException:
        logger.exception("Firmware fetch failed from %s", FIRMWARE_FEED_URL)
        return
    except Exception:
        logger.exception("Unexpected firmware parsing failure")
        return

    if not entries:
        logger.warning("Firmware monitor parsed no entries from %s", FIRMWARE_FEED_URL)
        return

    current_ids = {entry["id"] for entry in entries}
    seen_ids = load_firmware_seen_ids()
    if seen_ids is None:
        save_firmware_state(current_ids, sync_label)
        logger.info("Firmware monitor baseline initialized with %d entries", len(current_ids))
        return

    new_entries = [entry for entry in entries if entry["id"] not in seen_ids]
    if not new_entries:
        save_firmware_state(current_ids, sync_label)
        return

    channel = await resolve_firmware_notify_channel()
    if channel is None:
        logger.warning(
            "Firmware monitor found %d new entries but channel is unavailable; retrying on next check",
            len(new_entries),
        )
        return

    new_entries.sort(key=lambda item: (item["published_date"], item["model_code"], item["version"]))
    logger.info("Firmware monitor found %d new entries", len(new_entries))
    for entry in new_entries:
        try:
            await channel.send(format_firmware_notification(entry, sync_label))
        except discord.Forbidden:
            logger.warning("No permission to post firmware notification in channel %s", channel.id)
            return
        except discord.HTTPException:
            logger.exception("Failed to post firmware notification for %s %s", entry["model_code"], entry["version"])
            return

    save_firmware_state(current_ids, sync_label)


async def firmware_monitor_loop():
    if FIRMWARE_NOTIFY_CHANNEL_ID <= 0:
        logger.info("Firmware monitor disabled: set firmware_notification_channel to enable it.")
        return

    if not croniter.is_valid(FIRMWARE_CHECK_SCHEDULE):
        logger.error("Firmware monitor disabled: invalid firmware_check_schedule '%s'", FIRMWARE_CHECK_SCHEDULE)
        return

    logger.info(
        "Firmware monitor active: checking %s on cron '%s' (UTC)",
        FIRMWARE_FEED_URL,
        FIRMWARE_CHECK_SCHEDULE,
    )
    await check_firmware_updates_once()

    while not bot.is_closed():
        now_utc = datetime.now(timezone.utc)
        next_run_utc = croniter(FIRMWARE_CHECK_SCHEDULE, now_utc).get_next(datetime)
        wait_seconds = max(1, int((next_run_utc - now_utc).total_seconds()))
        logger.debug("Next firmware check scheduled for %s UTC", next_run_utc.isoformat())
        await asyncio.sleep(wait_seconds)
        await check_firmware_updates_once()


def search_forum_links(query: str):
    search_url = f"{FORUM_BASE_URL}/search.json"
    try:
        response = requests.get(search_url, params={"q": query}, timeout=10)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException:
        logger.exception("Forum search request failed for query: %s", query)
        return ["❌ Failed to fetch forum results."]
    except ValueError:
        logger.exception("Forum search returned invalid JSON for query: %s", query)
        return ["❌ Forum returned an invalid response."]

    topics = data.get("topics", [])
    links = []
    for topic in topics[:FORUM_MAX_RESULTS]:
        topic_id = topic.get("id")
        slug = topic.get("slug")
        if topic_id and slug:
            links.append(f"{FORUM_BASE_URL}/t/{slug}/{topic_id}")

    return links if links else ["No results found."]


def normalize_search_terms(query: str):
    terms = [term.lower() for term in re.findall(r"[a-zA-Z0-9]+", query)]
    return list(dict.fromkeys(terms))


def clean_search_text(value: str):
    no_html = re.sub(r"<[^>]+>", " ", value or "")
    return re.sub(r"\s+", " ", unescape(no_html)).strip()


def load_docs_index(base_url: str):
    now = time.time()
    cached = docs_index_cache.get(base_url)
    if cached and now - cached["fetched_at"] < DOCS_INDEX_TTL_SECONDS:
        return cached["docs"]

    index_url = f"{base_url}/search/search_index.json"
    try:
        response = requests.get(index_url, timeout=15)
        response.raise_for_status()
        data = response.json()
        docs = data.get("docs", [])
        if not isinstance(docs, list):
            docs = []
        docs_index_cache[base_url] = {"fetched_at": now, "docs": docs}
        return docs
    except requests.RequestException:
        logger.exception("Docs index request failed for %s", base_url)
        return []
    except ValueError:
        logger.exception("Docs index returned invalid JSON for %s", base_url)
        return []


def score_document(title: str, text: str, terms):
    title_lc = title.lower()
    text_lc = text.lower()
    score = 0
    for term in terms:
        if term in title_lc:
            score += 8
        if term in text_lc:
            score += min(4, text_lc.count(term))
    return score


def search_docs_site_links(query: str, base_url: str):
    terms = normalize_search_terms(query)
    docs = load_docs_index(base_url)
    ranked = []
    for doc in docs:
        location = doc.get("location")
        if not location:
            continue
        title = clean_search_text(str(doc.get("title", "")))
        text = clean_search_text(str(doc.get("text", "")))
        score = score_document(title, text, terms)
        if score <= 0:
            continue
        resolved_url = urljoin(f"{base_url}/", location)
        ranked.append((score, title or location, resolved_url))
    ranked.sort(key=lambda item: item[0], reverse=True)
    return ranked[:DOCS_MAX_RESULTS_PER_SITE]


def search_docs_links(query: str):
    by_site = {}
    for site_name, base_url in DOCS_SOURCES:
        by_site[site_name] = search_docs_site_links(query, base_url)
    return by_site


def trim_search_message(message: str):
    if len(message) <= SEARCH_RESPONSE_MAX_CHARS:
        return message
    trimmed = message[: SEARCH_RESPONSE_MAX_CHARS - 24].rsplit("\n", 1)[0]
    return f"{trimmed}\n...results truncated."


def build_search_message(query: str):
    forum_results = search_forum_links(query)
    docs_results = search_docs_links(query)

    lines = [f"🔎 Results for: `{query}`", "", "**Forum**"]
    forum_links = [item for item in forum_results if item.startswith("http")]
    if forum_links:
        lines.extend([f"- {link}" for link in forum_links])
    else:
        lines.append(f"- {forum_results[0]}")

    lines.append("")
    lines.append("**Docs**")
    docs_found = 0
    for site_name, _ in DOCS_SOURCES:
        site_results = docs_results.get(site_name, [])
        if not site_results:
            continue
        docs_found += len(site_results)
        lines.append(f"{site_name}:")
        for _, title, link in site_results:
            lines.append(f"- {title} - {link}")

    if docs_found == 0:
        lines.append("No matching docs results found.")

    return trim_search_message("\n".join(lines))


def build_forum_search_message(query: str):
    forum_results = search_forum_links(query)
    lines = [f"🔎 Forum results for: `{query}`", "", "**Forum**"]
    forum_links = [item for item in forum_results if item.startswith("http")]
    if forum_links:
        lines.extend([f"- {link}" for link in forum_links])
    else:
        lines.append(f"- {forum_results[0]}")
    return trim_search_message("\n".join(lines))


def build_docs_site_search_message(query: str, site_key: str):
    site_info = DOCS_SITE_MAP.get(site_key)
    if not site_info:
        return "❌ Invalid documentation site."

    site_name, base_url = site_info
    site_results = search_docs_site_links(query, base_url)
    lines = [f"🔎 {site_name} results for: `{query}`", "", f"**{site_name}**"]
    if site_results:
        for _, title, link in site_results:
            lines.append(f"- {title} - {link}")
    else:
        lines.append("- No matching docs results found.")
    return trim_search_message("\n".join(lines))

# Runtime caches for invite tracking
invite_roles = load_invite_roles()
invite_uses = {}

@bot.event
async def on_ready():
    global firmware_monitor_task
    logger.info("Logged in as %s", bot.user.name)
    guild = bot.get_guild(GUILD_ID)
    if callable(globals().get("register_tag_commands")):
        register_tag_commands()
    else:
        logger.warning("Tag slash commands not registered: register_tag_commands missing")
    synced = await tree.sync(guild=guild)
    logger.info("Synced %d command(s) to guild %s", len(synced), GUILD_ID)
    get_tag_responses()

    # Cache invite uses for tracking
    try:
        invites = await guild.invites()
        for inv in invites:
            invite_uses[inv.code] = inv.uses
    except Exception:
        logger.exception("Failed to cache invites on startup")

    if firmware_monitor_task is None or firmware_monitor_task.done():
        firmware_monitor_task = asyncio.create_task(firmware_monitor_loop(), name="firmware_monitor")

@bot.event
async def on_member_join(member: discord.Member):
    """Assign role based on the invite used to join."""
    guild = member.guild
    if guild.id != GUILD_ID:
        return

    used_invite = None
    try:
        invites = await guild.invites()
        for inv in invites:
            if inv.code in invite_roles and inv.uses > invite_uses.get(inv.code, 0):
                invite_uses[inv.code] = inv.uses
                used_invite = inv
                break
    except Exception:
        logger.exception("Failed to fetch invites on member join")

    if used_invite:
        role_id = invite_roles.get(used_invite.code)
        role = guild.get_role(role_id)
        if role:
            try:
                await member.add_roles(role)
                logger.info("Assigned role %s to %s via invite %s", role.id, member, used_invite.code)
            except Exception:
                logger.exception("Failed to assign role on join for %s", member)

    join_details = (
        f"**Member:** {member.mention} (`{member.id}`)\n"
        f"**Created:** <t:{int(member.created_at.timestamp())}:f>\n"
    )
    if used_invite:
        join_details += f"**Invite:** `{used_invite.code}`\n"
    await send_server_event_log(guild, "member_join", join_details)


@bot.event
async def on_member_remove(member: discord.Member):
    guild = member.guild
    if guild.id != GUILD_ID:
        return

    details = (
        f"**Member:** {member} (`{member.id}`)\n"
        f"**Nickname:** {clip_text(member.nick or 'N/A')}\n"
    )
    await send_server_event_log(guild, "member_leave", details)


@bot.event
async def on_message_delete(message: discord.Message):
    guild = message.guild
    if guild is None or guild.id != GUILD_ID:
        return

    channel_name = message.channel.mention if hasattr(message.channel, "mention") else f"`{message.channel.id}`"
    details = (
        f"**Author:** {message.author} (`{message.author.id}`)\n"
        f"**Channel:** {channel_name}\n"
        f"**Message ID:** `{message.id}`\n"
        f"**Content:** {clip_text(message.content)}\n"
        f"**Attachments:** `{len(message.attachments)}`\n"
    )
    await send_server_event_log(guild, "message_delete", details)


@bot.event
async def on_bulk_message_delete(messages: list[discord.Message]):
    if not messages:
        return
    guild = messages[0].guild
    if guild is None or guild.id != GUILD_ID:
        return

    channel = messages[0].channel
    channel_name = channel.mention if hasattr(channel, "mention") else f"`{channel.id}`"
    details = (
        f"**Channel:** {channel_name}\n"
        f"**Messages Deleted:** `{len(messages)}`\n"
    )
    await send_server_event_log(guild, "bulk_message_delete", details)


@bot.event
async def on_user_update(before: discord.User, after: discord.User):
    guild = bot.get_guild(GUILD_ID)
    if guild is None:
        return
    member = guild.get_member(after.id)
    if member is None:
        return

    if before.name != after.name or before.global_name != after.global_name:
        details = (
            f"**User:** {member.mention} (`{after.id}`)\n"
            f"**Username:** {clip_text(before.name)} -> {clip_text(after.name)}\n"
            f"**Global Name:** {clip_text(before.global_name or 'N/A')} -> {clip_text(after.global_name or 'N/A')}\n"
        )
        await send_server_event_log(guild, "user_name_change", details)

    if before.display_avatar != after.display_avatar:
        details = (
            f"**User:** {member.mention} (`{after.id}`)\n"
            f"**New Avatar:** {after.display_avatar.url}\n"
        )
        await send_server_event_log(guild, "user_avatar_change", details)


@bot.event
async def on_member_update(before: discord.Member, after: discord.Member):
    guild = after.guild
    if guild.id != GUILD_ID:
        return

    if before.nick != after.nick:
        details = (
            f"**Member:** {after.mention} (`{after.id}`)\n"
            f"**Nickname:** {clip_text(before.nick or 'N/A')} -> {clip_text(after.nick or 'N/A')}\n"
        )
        await send_server_event_log(guild, "member_nickname_change", details)

    before_role_map = {role.id: role for role in before.roles}
    after_role_map = {role.id: role for role in after.roles}
    added_role_ids = sorted(set(after_role_map) - set(before_role_map))
    removed_role_ids = sorted(set(before_role_map) - set(after_role_map))

    for role_id in added_role_ids:
        role = after_role_map[role_id]
        details = (
            f"**Member:** {after.mention} (`{after.id}`)\n"
            f"**Role Added:** {role.mention} (`{role.id}`)\n"
        )
        await send_server_event_log(guild, "member_role_added", details)

    for role_id in removed_role_ids:
        role = before_role_map[role_id]
        details = (
            f"**Member:** {after.mention} (`{after.id}`)\n"
            f"**Role Removed:** {role.name} (`{role.id}`)\n"
        )
        await send_server_event_log(guild, "member_role_removed", details)


@bot.event
async def on_invite_create(invite: discord.Invite):
    guild = invite.guild
    if guild is None or guild.id != GUILD_ID:
        return

    inviter_text = f"{invite.inviter} (`{invite.inviter.id}`)" if invite.inviter else "Unknown"
    channel_text = invite.channel.mention if getattr(invite, "channel", None) else "N/A"
    details = (
        f"**Invite Code:** `{invite.code}`\n"
        f"**Inviter:** {inviter_text}\n"
        f"**Channel:** {channel_text}\n"
        f"**Max Uses:** `{invite.max_uses}`\n"
        f"**Max Age:** `{invite.max_age}`\n"
    )
    await send_server_event_log(guild, "invite_created", details)


@bot.event
async def on_guild_channel_create(channel: discord.abc.GuildChannel):
    guild = channel.guild
    if guild.id != GUILD_ID:
        return

    if isinstance(channel, discord.CategoryChannel):
        event_name = "category_created"
    else:
        event_name = "channel_created"

    parent_name = channel.category.name if channel.category else "N/A"
    details = (
        f"**Name:** {clip_text(channel.name)}\n"
        f"**ID:** `{channel.id}`\n"
        f"**Type:** `{channel.type}`\n"
        f"**Category:** {clip_text(parent_name)}\n"
    )
    await send_server_event_log(guild, event_name, details)


@bot.event
async def on_guild_channel_delete(channel: discord.abc.GuildChannel):
    guild = channel.guild
    if guild.id != GUILD_ID:
        return

    if isinstance(channel, discord.CategoryChannel):
        event_name = "category_deleted"
    else:
        event_name = "channel_deleted"

    parent_name = channel.category.name if channel.category else "N/A"
    details = (
        f"**Name:** {clip_text(channel.name)}\n"
        f"**ID:** `{channel.id}`\n"
        f"**Type:** `{channel.type}`\n"
        f"**Category:** {clip_text(parent_name)}\n"
    )
    await send_server_event_log(guild, event_name, details)


@bot.event
async def on_guild_role_create(role: discord.Role):
    guild = role.guild
    if guild.id != GUILD_ID:
        return

    details = (
        f"**Role:** {role.mention} (`{role.id}`)\n"
        f"**Color:** `{role.color}`\n"
        f"**Position:** `{role.position}`\n"
    )
    await send_server_event_log(guild, "role_created", details)


@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return
    if message.content:
        tag = normalize_tag(message.content.strip().split()[0])
        if tag == "!list":
            await bot.process_commands(message)
            return
        response = get_tag_responses().get(tag)
        if response:
            await message.channel.send(response)
    await bot.process_commands(message)


@bot.command(name="list")
async def list_commands(ctx: commands.Context):
    await ctx.send(build_command_list())

@tree.command(name="submitrole", description="Submit a role for invite/code linking", guild=discord.Object(id=GUILD_ID))
async def submitrole(interaction: discord.Interaction):
    logger.info("/submitrole invoked by %s", interaction.user)
    if not has_allowed_role(interaction.user):
        await interaction.response.send_message("❌ You do not have permission to use this command.", ephemeral=True)
        return

    await interaction.response.send_message("Please mention the role you want to assign.", ephemeral=True)

    def check(m):
        return m.author.id == interaction.user.id and m.channel.id == interaction.channel.id

    try:
        msg = await bot.wait_for("message", timeout=30.0, check=check)
        if not msg.role_mentions:
            await interaction.followup.send("❌ No role mentioned.", ephemeral=True)
            return

        role = msg.role_mentions[0]
        channel = bot.get_channel(GENERAL_CHANNEL_ID) or interaction.channel
        invite = await channel.create_invite(max_age=0, max_uses=0, unique=True)
        code = generate_code()
        save_role_code(code, role.id)
        save_invite_role(invite.code, role.id)
        invite_roles[invite.code] = role.id
        invite_uses[invite.code] = invite.uses

        logger.info(
            "Generated invite %s and code %s for role %s using channel %s",
            invite.url,
            code,
            role.id,
            channel.id,
        )

        await interaction.followup.send(
            f"✅ Invite link: {invite.url}\n🔢 6-digit code: `{code}`", ephemeral=True
        )
    except Exception:
        logger.exception("Error in /submitrole")
        await interaction.followup.send("❌ Something went wrong. Try again.", ephemeral=True)


@tree.command(
    name="bulk_assign_role_csv",
    description="Assign a role to members listed in an uploaded CSV file",
    guild=discord.Object(id=GUILD_ID),
)
async def bulk_assign_role_csv(interaction: discord.Interaction):
    logger.info("/bulk_assign_role_csv invoked by %s", interaction.user)
    if not isinstance(interaction.user, discord.Member) or not has_moderator_access(interaction.user):
        await interaction.response.send_message("❌ Only moderators can use this command.", ephemeral=True)
        return

    if interaction.guild is None or interaction.channel is None:
        await interaction.response.send_message("❌ This command can only be used in a server channel.", ephemeral=True)
        return

    await interaction.response.send_message("Please mention the role you want to assign.", ephemeral=True)

    channel_id = interaction.channel.id

    def role_message_check(message: discord.Message):
        return message.author.id == interaction.user.id and message.channel.id == channel_id

    try:
        role_message = await bot.wait_for("message", timeout=60.0, check=role_message_check)
    except asyncio.TimeoutError:
        await interaction.followup.send("❌ Timed out waiting for role mention.", ephemeral=True)
        return

    if not role_message.role_mentions:
        await interaction.followup.send("❌ No role mentioned. Run the command again and mention a role.", ephemeral=True)
        return

    role = role_message.role_mentions[0]
    bot_user_id = bot.user.id if bot.user else None
    bot_member = interaction.guild.me or (interaction.guild.get_member(bot_user_id) if bot_user_id else None)
    actor = interaction.user if isinstance(interaction.user, discord.Member) else None
    if bot_member is None:
        await interaction.followup.send("❌ Could not resolve bot member in this guild.", ephemeral=True)
        return
    if role == interaction.guild.default_role:
        await interaction.followup.send("❌ The @everyone role cannot be assigned this way.", ephemeral=True)
        return
    if role.managed:
        await interaction.followup.send("❌ That role is managed by an integration and cannot be assigned manually.", ephemeral=True)
        return
    if bot_member.top_role <= role:
        await interaction.followup.send(
            "❌ I can't assign that role because it's above my top role.",
            ephemeral=True,
        )
        return
    if actor and actor.id != interaction.guild.owner_id and actor.top_role <= role:
        await interaction.followup.send(
            "❌ You can only bulk-assign roles below your top role.",
            ephemeral=True,
        )
        return

    await interaction.followup.send(
        "Upload a `.csv` file with Discord names separated by commas.",
        ephemeral=True,
    )

    def attachment_message_check(message: discord.Message):
        return (
            message.author.id == interaction.user.id
            and message.channel.id == channel_id
            and len(message.attachments) > 0
        )

    try:
        csv_message = await bot.wait_for("message", timeout=120.0, check=attachment_message_check)
    except asyncio.TimeoutError:
        await interaction.followup.send("❌ Timed out waiting for CSV upload.", ephemeral=True)
        return

    attachment = csv_message.attachments[0]
    try:
        payload = await attachment.read()
    except Exception:
        logger.exception("Failed reading CSV attachment for /bulk_assign_role_csv")
        await interaction.followup.send("❌ Could not read that file. Please try again.", ephemeral=True)
        return

    raw_names = parse_member_names_from_csv_bytes(payload)
    unique_names = unique_member_names(raw_names)
    if not unique_names:
        await interaction.followup.send(
            "❌ The uploaded file did not contain any names.",
            ephemeral=True,
        )
        return
    if len(unique_names) > CSV_ROLE_ASSIGN_MAX_NAMES:
        await interaction.followup.send(
            f"❌ Too many names. Limit is `{CSV_ROLE_ASSIGN_MAX_NAMES}` unique names per file.",
            ephemeral=True,
        )
        return

    member_lookup = build_member_name_lookup(interaction.guild)
    matched_members = {}
    duplicate_member_inputs = []
    ambiguous_names = []
    unmatched_names = []
    for raw_name in unique_names:
        key = normalize_member_lookup_name(raw_name)
        matches = member_lookup.get(key, [])
        if len(matches) == 1:
            member = matches[0]
            if member.id in matched_members:
                duplicate_member_inputs.append(raw_name)
                continue
            matched_members[member.id] = (member, raw_name)
        elif len(matches) > 1:
            ambiguous_names.append(f"{raw_name} ({len(matches)} matches)")
        else:
            unmatched_names.append(raw_name)

    assigned = []
    already_had_role = []
    assignment_failures = []
    for member, matched_name in matched_members.values():
        if role in member.roles:
            already_had_role.append(f"{matched_name} ({member})")
            continue
        try:
            await member.add_roles(
                role,
                reason=f"Bulk CSV role assignment by {interaction.user} ({interaction.user.id})",
            )
            assigned.append(f"{matched_name} ({member})")
        except discord.Forbidden:
            assignment_failures.append(f"{matched_name} (permission denied)")
        except discord.HTTPException:
            assignment_failures.append(f"{matched_name} (Discord API error)")

    logger.info(
        "CSV role assignment by %s role=%s processed=%s assigned=%s already=%s unmatched=%s ambiguous=%s failed=%s",
        interaction.user,
        role.id,
        len(unique_names),
        len(assigned),
        len(already_had_role),
        len(unmatched_names),
        len(ambiguous_names),
        len(assignment_failures),
    )

    def format_preview(title: str, values: list[str], limit: int = 20):
        if not values:
            return None
        preview = ", ".join(f"`{clip_text(value, 40)}`" for value in values[:limit])
        remaining = len(values) - limit
        if remaining > 0:
            preview = f"{preview} ... (+{remaining} more)"
        return f"**{title}:** {preview}"

    summary_lines = [
        f"✅ Finished processing `{attachment.filename}` for role {role.mention}.",
        f"- Unique names processed: `{len(unique_names)}`",
        f"- Matched members: `{len(matched_members)}`",
        f"- Assigned now: `{len(assigned)}`",
        f"- Already had role: `{len(already_had_role)}`",
        f"- Unmatched names: `{len(unmatched_names)}`",
        f"- Ambiguous names: `{len(ambiguous_names)}`",
        f"- Assignment failures: `{len(assignment_failures)}`",
    ]

    for line in (
        format_preview("Unmatched", unmatched_names),
        format_preview("Ambiguous", ambiguous_names),
        format_preview("Failed", assignment_failures),
        format_preview("Duplicate member inputs", duplicate_member_inputs),
    ):
        if line:
            summary_lines.append(line)

    await interaction.followup.send("\n".join(summary_lines), ephemeral=True)


class CodeEntryModal(discord.ui.Modal):
    def __init__(self):
        super().__init__(title="Enter Role Code")
        self.code = discord.ui.TextInput(label="6-digit code", min_length=6, max_length=6)
        self.add_item(self.code)

    async def on_submit(self, interaction: discord.Interaction):
        role_id = get_role_id_by_code(self.code.value.strip())
        if not role_id:
            await interaction.response.send_message("❌ Invalid code.", ephemeral=True)
            return


        role = interaction.guild.get_role(role_id)
        if not role:
            await interaction.response.send_message("❌ Role not found.", ephemeral=True)
            return

        await interaction.user.add_roles(role)
        await interaction.response.send_message(
            f"✅ You've been given the **{role.name}** role!", ephemeral=True
        )


@tree.command(
    name="enter_role",
    description="Enter a 6-digit code to receive a role",
    guild=discord.Object(id=GUILD_ID),
)
async def enter_role(interaction: discord.Interaction):
    """Prompt the user to enter their code via a modal."""
    logger.info("/enter_role invoked by %s", interaction.user)
    await interaction.response.send_modal(CodeEntryModal())


@tree.command(name="getaccess", description="Assign yourself the protected role", guild=discord.Object(id=GUILD_ID))
async def getaccess(interaction: discord.Interaction):
    logger.info("/getaccess invoked by %s", interaction.user)
    try:
        with open(ROLE_FILE, "r") as f:
            role_id = int(f.read().strip())
        role = interaction.guild.get_role(role_id)
        await interaction.user.add_roles(role)
        logger.info("Assigned default role %s to user %s", role.id, interaction.user)
        await interaction.response.send_message(f"✅ You've been given the **{role.name}** role!", ephemeral=True)
    except Exception:
        logger.exception("Error in /getaccess")
        await interaction.response.send_message(
            "❌ Could not assign role. Contact an admin.", ephemeral=True
        )


@tree.command(name="country", description="Add your country code to your nickname", guild=discord.Object(id=GUILD_ID))
@app_commands.describe(code="2-letter country code (e.g. US, CA, DE)")
async def country_slash(interaction: discord.Interaction, code: str):
    logger.info("/country invoked by %s with code %s", interaction.user, code)
    normalized = normalize_country_code(code)
    if not normalized:
        await interaction.response.send_message(
            "❌ Please provide a valid 2-letter country code (A-Z).",
            ephemeral=True,
        )
        return

    try:
        success, message = await set_member_country(interaction.user, normalized)
        await interaction.response.send_message(message, ephemeral=True)
        logger.info("/country result for %s success=%s", interaction.user, success)
    except discord.Forbidden:
        logger.exception("Missing permission to edit nickname for %s", interaction.user)
        await interaction.response.send_message(
            "❌ I can't edit your nickname. Check role hierarchy and nickname permissions.",
            ephemeral=True,
        )
    except discord.HTTPException:
        logger.exception("Failed to update nickname for %s", interaction.user)
        await interaction.response.send_message(
            "❌ Could not update your nickname right now. Try again.",
            ephemeral=True,
        )


@bot.command(name="country")
async def country_prefix(ctx: commands.Context, code: str):
    logger.info("!country invoked by %s with code %s", ctx.author, code)
    normalized = normalize_country_code(code)
    if not normalized:
        await ctx.send("❌ Please provide a valid 2-letter country code (A-Z).")
        return

    try:
        _, message = await set_member_country(ctx.author, normalized)
        await ctx.send(message)
    except discord.Forbidden:
        logger.exception("Missing permission to edit nickname for %s", ctx.author)
        await ctx.send("❌ I can't edit your nickname. Check role hierarchy and nickname permissions.")
    except discord.HTTPException:
        logger.exception("Failed to update nickname for %s", ctx.author)
        await ctx.send("❌ Could not update your nickname right now. Try again.")


@tree.command(name="clear_country", description="Remove country code suffix from your nickname", guild=discord.Object(id=GUILD_ID))
async def clear_country_slash(interaction: discord.Interaction):
    logger.info("/clear_country invoked by %s", interaction.user)
    try:
        _, message = await clear_member_country(interaction.user)
        await interaction.response.send_message(message, ephemeral=True)
    except discord.Forbidden:
        logger.exception("Missing permission to edit nickname for %s", interaction.user)
        await interaction.response.send_message(
            "❌ I can't edit your nickname. Check role hierarchy and nickname permissions.",
            ephemeral=True,
        )
    except discord.HTTPException:
        logger.exception("Failed to clear nickname suffix for %s", interaction.user)
        await interaction.response.send_message(
            "❌ Could not update your nickname right now. Try again.",
            ephemeral=True,
        )


@bot.command(name="clearcountry")
async def clear_country_prefix(ctx: commands.Context):
    logger.info("!clearcountry invoked by %s", ctx.author)
    try:
        _, message = await clear_member_country(ctx.author)
        await ctx.send(message)
    except discord.Forbidden:
        logger.exception("Missing permission to edit nickname for %s", ctx.author)
        await ctx.send("❌ I can't edit your nickname. Check role hierarchy and nickname permissions.")
    except discord.HTTPException:
        logger.exception("Failed to clear nickname suffix for %s", ctx.author)
        await ctx.send("❌ Could not update your nickname right now. Try again.")


@tree.command(
    name="modlog_test",
    description="Send a test moderation log entry",
    guild=discord.Object(id=GUILD_ID),
)
async def modlog_test_slash(interaction: discord.Interaction):
    logger.info("/modlog_test invoked by %s", interaction.user)
    if not has_moderator_access(interaction.user):
        await interaction.response.send_message("❌ Only moderators can use this command.", ephemeral=True)
        return

    sent = await send_moderation_log(
        interaction.guild,
        interaction.user,
        action="modlog_test",
        target=interaction.user,
        reason="Manual moderation log test",
        outcome="success",
        details="Triggered via /modlog_test",
    )
    if sent:
        await interaction.response.send_message(
            f"✅ Test moderation log sent to <#{MOD_LOG_CHANNEL_ID}>.",
            ephemeral=True,
        )
    else:
        await interaction.response.send_message(
            f"❌ Could not send test log to channel ID `{MOD_LOG_CHANNEL_ID}`. "
            "Check channel ID and bot permissions.",
            ephemeral=True,
        )


@bot.command(name="modlogtest")
async def modlog_test_prefix(ctx: commands.Context):
    logger.info("!modlogtest invoked by %s", ctx.author)
    if not has_moderator_access(ctx.author):
        await ctx.send("❌ Only moderators can use this command.")
        return

    sent = await send_moderation_log(
        ctx.guild,
        ctx.author,
        action="modlog_test",
        target=ctx.author,
        reason="Manual moderation log test",
        outcome="success",
        details="Triggered via !modlogtest",
    )
    if sent:
        await ctx.send(f"✅ Test moderation log sent to <#{MOD_LOG_CHANNEL_ID}>.")
    else:
        await ctx.send(
            f"❌ Could not send test log to channel ID `{MOD_LOG_CHANNEL_ID}`. "
            "Check channel ID and bot permissions."
        )


@tree.command(
    name="ban_member",
    description="Ban a member from the server",
    guild=discord.Object(id=GUILD_ID),
)
@app_commands.describe(member="Member to ban", reason="Reason for ban")
async def ban_member_slash(interaction: discord.Interaction, member: discord.Member, reason: str | None = None):
    logger.info("/ban_member invoked by %s targeting %s", interaction.user, member)
    if not has_moderator_access(interaction.user):
        await interaction.response.send_message("❌ Only moderators can use this command.", ephemeral=True)
        return

    can_moderate, error_message = validate_moderation_target(interaction.user, member, interaction.guild.me)
    if not can_moderate:
        await send_moderation_log(
            interaction.guild,
            interaction.user,
            "ban_member",
            member,
            reason,
            outcome="blocked",
            details=error_message,
        )
        await interaction.response.send_message(error_message, ephemeral=True)
        return

    action_reason = (reason or "").strip() or f"Banned by {interaction.user} via bot"
    try:
        await member.ban(reason=action_reason, delete_message_seconds=0)
    except discord.Forbidden:
        logger.exception("Missing permission to ban member %s", member)
        await send_moderation_log(
            interaction.guild,
            interaction.user,
            "ban_member",
            member,
            action_reason,
            outcome="failed",
            details="Bot missing `Ban Members` permission or role hierarchy block.",
        )
        await interaction.response.send_message(
            "❌ I can't ban that member. Check role hierarchy and `Ban Members` permission.",
            ephemeral=True,
        )
        return
    except discord.HTTPException:
        logger.exception("Failed to ban member %s", member)
        await send_moderation_log(
            interaction.guild,
            interaction.user,
            "ban_member",
            member,
            action_reason,
            outcome="failed",
            details="Discord API error while banning member.",
        )
        await interaction.response.send_message("❌ Failed to ban the member. Try again.", ephemeral=True)
        return

    await send_moderation_log(
        interaction.guild,
        interaction.user,
        "ban_member",
        target=member,
        reason=action_reason,
        details="Banned successfully.",
    )
    await interaction.response.send_message(f"✅ Banned **{member}**.", ephemeral=True)


@bot.command(name="banmember")
async def ban_member_prefix(ctx: commands.Context, member: discord.Member, *, reason: str = ""):
    logger.info("!banmember invoked by %s targeting %s", ctx.author, member)
    if not has_moderator_access(ctx.author):
        await ctx.send("❌ Only moderators can use this command.")
        return

    can_moderate, error_message = validate_moderation_target(ctx.author, member, ctx.guild.me)
    if not can_moderate:
        await send_moderation_log(
            ctx.guild,
            ctx.author,
            "ban_member",
            member,
            reason.strip() or None,
            outcome="blocked",
            details=error_message,
        )
        await ctx.send(error_message)
        return

    action_reason = reason.strip() or f"Banned by {ctx.author} via bot"
    try:
        await member.ban(reason=action_reason, delete_message_seconds=0)
    except discord.Forbidden:
        logger.exception("Missing permission to ban member %s", member)
        await send_moderation_log(
            ctx.guild,
            ctx.author,
            "ban_member",
            member,
            action_reason,
            outcome="failed",
            details="Bot missing `Ban Members` permission or role hierarchy block.",
        )
        await ctx.send("❌ I can't ban that member. Check role hierarchy and `Ban Members` permission.")
        return
    except discord.HTTPException:
        logger.exception("Failed to ban member %s", member)
        await send_moderation_log(
            ctx.guild,
            ctx.author,
            "ban_member",
            member,
            action_reason,
            outcome="failed",
            details="Discord API error while banning member.",
        )
        await ctx.send("❌ Failed to ban the member. Try again.")
        return

    await send_moderation_log(
        ctx.guild,
        ctx.author,
        "ban_member",
        target=member,
        reason=action_reason,
        details="Banned successfully.",
    )
    await ctx.send(f"✅ Banned **{member}**.")


@tree.command(
    name="kick_member",
    description="Kick a member and prune their last 72 hours of messages",
    guild=discord.Object(id=GUILD_ID),
)
@app_commands.describe(member="Member to kick", reason="Reason for kicking")
async def kick_member_slash(interaction: discord.Interaction, member: discord.Member, reason: str | None = None):
    logger.info("/kick_member invoked by %s targeting %s", interaction.user, member)
    if not has_moderator_access(interaction.user):
        await interaction.response.send_message(
            "❌ Only moderators can use this command.",
            ephemeral=True,
        )
        return

    can_moderate, error_message = validate_moderation_target(interaction.user, member, interaction.guild.me)
    if not can_moderate:
        await send_moderation_log(
            interaction.guild,
            interaction.user,
            "kick_member",
            member,
            reason,
            outcome="blocked",
            details=error_message,
        )
        await interaction.response.send_message(error_message, ephemeral=True)
        return

    await interaction.response.defer(thinking=True, ephemeral=True)
    action_reason = (reason or "").strip() or f"Kicked by {interaction.user} via bot"
    target_id = member.id
    target_name = str(member)

    try:
        await member.kick(reason=action_reason)
    except discord.Forbidden:
        logger.exception("Missing permission to kick member %s", member)
        await send_moderation_log(
            interaction.guild,
            interaction.user,
            "kick_member",
            member,
            action_reason,
            outcome="failed",
            details="Bot missing `Kick Members` permission or role hierarchy block.",
        )
        await interaction.followup.send(
            "❌ I can't kick that member. Check role hierarchy and `Kick Members` permission.",
            ephemeral=True,
        )
        return
    except discord.HTTPException:
        logger.exception("Failed to kick member %s", member)
        await send_moderation_log(
            interaction.guild,
            interaction.user,
            "kick_member",
            member,
            action_reason,
            outcome="failed",
            details="Discord API error while kicking member.",
        )
        await interaction.followup.send("❌ Failed to kick the member. Try again.", ephemeral=True)
        return

    deleted_count, scanned_channels = await prune_user_messages(interaction.guild, target_id, KICK_PRUNE_HOURS)
    await send_moderation_log(
        interaction.guild,
        interaction.user,
        "kick_member",
        target=member,
        reason=action_reason,
        details=(
            f"Kicked successfully; pruned {deleted_count} messages "
            f"from last {KICK_PRUNE_HOURS}h across {scanned_channels} channels."
        ),
    )
    await interaction.followup.send(
        f"✅ Kicked **{target_name}** and pruned **{deleted_count}** messages "
        f"from the last **{KICK_PRUNE_HOURS}** hours across **{scanned_channels}** channels.",
        ephemeral=True,
    )


@bot.command(name="kickmember")
async def kick_member_prefix(ctx: commands.Context, member: discord.Member, *, reason: str = ""):
    logger.info("!kickmember invoked by %s targeting %s", ctx.author, member)
    if not has_moderator_access(ctx.author):
        await ctx.send("❌ Only moderators can use this command.")
        return

    can_moderate, error_message = validate_moderation_target(ctx.author, member, ctx.guild.me)
    if not can_moderate:
        await send_moderation_log(
            ctx.guild,
            ctx.author,
            "kick_member",
            member,
            reason.strip() or None,
            outcome="blocked",
            details=error_message,
        )
        await ctx.send(error_message)
        return

    action_reason = reason.strip() or f"Kicked by {ctx.author} via bot"
    target_id = member.id
    target_name = str(member)
    try:
        await member.kick(reason=action_reason)
    except discord.Forbidden:
        logger.exception("Missing permission to kick member %s", member)
        await send_moderation_log(
            ctx.guild,
            ctx.author,
            "kick_member",
            member,
            action_reason,
            outcome="failed",
            details="Bot missing `Kick Members` permission or role hierarchy block.",
        )
        await ctx.send("❌ I can't kick that member. Check role hierarchy and `Kick Members` permission.")
        return
    except discord.HTTPException:
        logger.exception("Failed to kick member %s", member)
        await send_moderation_log(
            ctx.guild,
            ctx.author,
            "kick_member",
            member,
            action_reason,
            outcome="failed",
            details="Discord API error while kicking member.",
        )
        await ctx.send("❌ Failed to kick the member. Try again.")
        return

    deleted_count, scanned_channels = await prune_user_messages(ctx.guild, target_id, KICK_PRUNE_HOURS)
    await send_moderation_log(
        ctx.guild,
        ctx.author,
        "kick_member",
        target=member,
        reason=action_reason,
        details=(
            f"Kicked successfully; pruned {deleted_count} messages "
            f"from last {KICK_PRUNE_HOURS}h across {scanned_channels} channels."
        ),
    )
    await ctx.send(
        f"✅ Kicked **{target_name}** and pruned **{deleted_count}** messages "
        f"from the last **{KICK_PRUNE_HOURS}** hours across **{scanned_channels}** channels."
    )


@tree.command(
    name="timeout_member",
    description="Timeout a member for a duration (e.g. 30m, 2h, 1d)",
    guild=discord.Object(id=GUILD_ID),
)
@app_commands.describe(
    member="Member to timeout",
    duration="Duration like 30m, 2h, or 1d",
    reason="Reason for timeout",
)
async def timeout_member_slash(
    interaction: discord.Interaction,
    member: discord.Member,
    duration: str,
    reason: str | None = None,
):
    logger.info("/timeout_member invoked by %s targeting %s for %s", interaction.user, member, duration)
    if not has_moderator_access(interaction.user):
        await interaction.response.send_message("❌ Only moderators can use this command.", ephemeral=True)
        return

    can_moderate, error_message = validate_moderation_target(interaction.user, member, interaction.guild.me)
    if not can_moderate:
        await send_moderation_log(
            interaction.guild,
            interaction.user,
            "timeout_member",
            member,
            reason,
            outcome="blocked",
            details=error_message,
        )
        await interaction.response.send_message(error_message, ephemeral=True)
        return

    timeout_delta, duration_text, parse_error = parse_timeout_duration(duration)
    if parse_error:
        await send_moderation_log(
            interaction.guild,
            interaction.user,
            "timeout_member",
            member,
            reason,
            outcome="blocked",
            details=parse_error,
        )
        await interaction.response.send_message(parse_error, ephemeral=True)
        return

    until = discord.utils.utcnow() + timeout_delta
    action_reason = (reason or "").strip() or f"Timed out by {interaction.user} via bot"
    try:
        await member.timeout(until, reason=action_reason)
    except discord.Forbidden:
        logger.exception("Missing permission to timeout member %s", member)
        await send_moderation_log(
            interaction.guild,
            interaction.user,
            "timeout_member",
            member,
            action_reason,
            outcome="failed",
            details="Bot missing `Moderate Members` permission or role hierarchy block.",
        )
        await interaction.response.send_message(
            "❌ I can't timeout that member. Check role hierarchy and `Moderate Members` permission.",
            ephemeral=True,
        )
        return
    except discord.HTTPException:
        logger.exception("Failed to timeout member %s", member)
        await send_moderation_log(
            interaction.guild,
            interaction.user,
            "timeout_member",
            member,
            action_reason,
            outcome="failed",
            details="Discord API error while applying timeout.",
        )
        await interaction.response.send_message("❌ Failed to timeout the member. Try again.", ephemeral=True)
        return

    timestamp = int(until.timestamp())
    await send_moderation_log(
        interaction.guild,
        interaction.user,
        "timeout_member",
        target=member,
        reason=action_reason,
        details=f"Timed out for {duration_text} until <t:{timestamp}:f>.",
    )
    await interaction.response.send_message(
        f"✅ Timed out **{member}** for **{duration_text}** (until <t:{timestamp}:f>).",
        ephemeral=True,
    )


@bot.command(name="timeoutmember")
async def timeout_member_prefix(ctx: commands.Context, member: discord.Member, duration: str, *, reason: str = ""):
    logger.info("!timeoutmember invoked by %s targeting %s for %s", ctx.author, member, duration)
    if not has_moderator_access(ctx.author):
        await ctx.send("❌ Only moderators can use this command.")
        return

    can_moderate, error_message = validate_moderation_target(ctx.author, member, ctx.guild.me)
    if not can_moderate:
        await send_moderation_log(
            ctx.guild,
            ctx.author,
            "timeout_member",
            member,
            reason.strip() or None,
            outcome="blocked",
            details=error_message,
        )
        await ctx.send(error_message)
        return

    timeout_delta, duration_text, parse_error = parse_timeout_duration(duration)
    if parse_error:
        await send_moderation_log(
            ctx.guild,
            ctx.author,
            "timeout_member",
            member,
            reason.strip() or None,
            outcome="blocked",
            details=parse_error,
        )
        await ctx.send(parse_error)
        return

    until = discord.utils.utcnow() + timeout_delta
    action_reason = reason.strip() or f"Timed out by {ctx.author} via bot"
    try:
        await member.timeout(until, reason=action_reason)
    except discord.Forbidden:
        logger.exception("Missing permission to timeout member %s", member)
        await send_moderation_log(
            ctx.guild,
            ctx.author,
            "timeout_member",
            member,
            action_reason,
            outcome="failed",
            details="Bot missing `Moderate Members` permission or role hierarchy block.",
        )
        await ctx.send("❌ I can't timeout that member. Check role hierarchy and `Moderate Members` permission.")
        return
    except discord.HTTPException:
        logger.exception("Failed to timeout member %s", member)
        await send_moderation_log(
            ctx.guild,
            ctx.author,
            "timeout_member",
            member,
            action_reason,
            outcome="failed",
            details="Discord API error while applying timeout.",
        )
        await ctx.send("❌ Failed to timeout the member. Try again.")
        return

    timestamp = int(until.timestamp())
    await send_moderation_log(
        ctx.guild,
        ctx.author,
        "timeout_member",
        target=member,
        reason=action_reason,
        details=f"Timed out for {duration_text} until <t:{timestamp}:f>.",
    )
    await ctx.send(f"✅ Timed out **{member}** for **{duration_text}** (until <t:{timestamp}:f>).")


@tree.command(name="search", description="Search GL.iNet forum and docs", guild=discord.Object(id=GUILD_ID))
@app_commands.describe(query="Enter search keywords")
async def search_slash(interaction: discord.Interaction, query: str):
    logger.info("/search invoked by %s with query %s", interaction.user, query)
    query = query.strip()
    if not query:
        await interaction.response.send_message("❌ Please provide a search query.", ephemeral=True)
        return
    await interaction.response.defer(thinking=True)
    message = await asyncio.to_thread(build_search_message, query)
    await interaction.followup.send(message)


@bot.command(name="search")
async def search_prefix(ctx: commands.Context, *, query: str):
    logger.info("!search invoked by %s with query %s", ctx.author, query)
    query = query.strip()
    if not query:
        await ctx.send("❌ Please provide a search query.")
        return
    await ctx.send("🔍 Searching forum + docs...")
    message = await asyncio.to_thread(build_search_message, query)
    await ctx.send(message)


@tree.command(name="search_forum", description="Search the GL.iNet forum only", guild=discord.Object(id=GUILD_ID))
@app_commands.describe(query="Enter search keywords")
async def search_forum_slash(interaction: discord.Interaction, query: str):
    logger.info("/search_forum invoked by %s with query %s", interaction.user, query)
    query = query.strip()
    if not query:
        await interaction.response.send_message("❌ Please provide a search query.", ephemeral=True)
        return
    await interaction.response.defer(thinking=True)
    message = await asyncio.to_thread(build_forum_search_message, query)
    await interaction.followup.send(message)


@bot.command(name="searchforum")
async def search_forum_prefix(ctx: commands.Context, *, query: str):
    logger.info("!searchforum invoked by %s with query %s", ctx.author, query)
    query = query.strip()
    if not query:
        await ctx.send("❌ Please provide a search query.")
        return
    await ctx.send("🔍 Searching forum...")
    message = await asyncio.to_thread(build_forum_search_message, query)
    await ctx.send(message)


@tree.command(name="search_kvm", description="Search KVM docs only", guild=discord.Object(id=GUILD_ID))
@app_commands.describe(query="Enter search keywords")
async def search_kvm_slash(interaction: discord.Interaction, query: str):
    logger.info("/search_kvm invoked by %s with query %s", interaction.user, query)
    query = query.strip()
    if not query:
        await interaction.response.send_message("❌ Please provide a search query.", ephemeral=True)
        return
    await interaction.response.defer(thinking=True)
    message = await asyncio.to_thread(build_docs_site_search_message, query, "kvm")
    await interaction.followup.send(message)


@bot.command(name="searchkvm")
async def search_kvm_prefix(ctx: commands.Context, *, query: str):
    logger.info("!searchkvm invoked by %s with query %s", ctx.author, query)
    query = query.strip()
    if not query:
        await ctx.send("❌ Please provide a search query.")
        return
    await ctx.send("🔍 Searching KVM docs...")
    message = await asyncio.to_thread(build_docs_site_search_message, query, "kvm")
    await ctx.send(message)


@tree.command(name="search_iot", description="Search IoT docs only", guild=discord.Object(id=GUILD_ID))
@app_commands.describe(query="Enter search keywords")
async def search_iot_slash(interaction: discord.Interaction, query: str):
    logger.info("/search_iot invoked by %s with query %s", interaction.user, query)
    query = query.strip()
    if not query:
        await interaction.response.send_message("❌ Please provide a search query.", ephemeral=True)
        return
    await interaction.response.defer(thinking=True)
    message = await asyncio.to_thread(build_docs_site_search_message, query, "iot")
    await interaction.followup.send(message)


@bot.command(name="searchiot")
async def search_iot_prefix(ctx: commands.Context, *, query: str):
    logger.info("!searchiot invoked by %s with query %s", ctx.author, query)
    query = query.strip()
    if not query:
        await ctx.send("❌ Please provide a search query.")
        return
    await ctx.send("🔍 Searching IoT docs...")
    message = await asyncio.to_thread(build_docs_site_search_message, query, "iot")
    await ctx.send(message)


@tree.command(name="search_router", description="Search Router v4 docs only", guild=discord.Object(id=GUILD_ID))
@app_commands.describe(query="Enter search keywords")
async def search_router_slash(interaction: discord.Interaction, query: str):
    logger.info("/search_router invoked by %s with query %s", interaction.user, query)
    query = query.strip()
    if not query:
        await interaction.response.send_message("❌ Please provide a search query.", ephemeral=True)
        return
    await interaction.response.defer(thinking=True)
    message = await asyncio.to_thread(build_docs_site_search_message, query, "router")
    await interaction.followup.send(message)


@bot.command(name="searchrouter")
async def search_router_prefix(ctx: commands.Context, *, query: str):
    logger.info("!searchrouter invoked by %s with query %s", ctx.author, query)
    query = query.strip()
    if not query:
        await ctx.send("❌ Please provide a search query.")
        return
    await ctx.send("🔍 Searching Router v4 docs...")
    message = await asyncio.to_thread(build_docs_site_search_message, query, "router")
    await ctx.send(message)


bot.run(TOKEN)

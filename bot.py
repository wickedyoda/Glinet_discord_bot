import discord
from discord.ext import commands
from discord import app_commands
import logging
import os
import json
import asyncio
import re
import time
from datetime import timedelta
from html import unescape
from urllib.parse import urljoin
from dotenv import load_dotenv
import random
import requests

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
COUNTRY_CODE_PATTERN = re.compile(r"^[A-Za-z]{2}$")
COUNTRY_LEGACY_SUFFIX_PATTERN = re.compile(r"_[A-Z]{2}$")
COUNTRY_FLAG_SUFFIX_PATTERN = re.compile(r"\s*-\s*[\U0001F1E6-\U0001F1FF]{2}$")
COUNTRY_CODE_SUFFIX_PATTERN = re.compile(r"\s*-\s*[A-Z]{2}$")
TIMEOUT_DURATION_PATTERN = re.compile(r"^\s*(\d+)\s*([mhd]?)\s*$", re.IGNORECASE)
MODERATOR_ROLE_IDS = {
    int(os.getenv("MODERATOR_ROLE_ID", "1294957416294645771")),
    int(os.getenv("ADMIN_ROLE_ID", "1138302148292116551")),
}
KICK_PRUNE_HOURS = int(os.getenv("KICK_PRUNE_HOURS", "72"))
TIMEOUT_MAX_MINUTES = 28 * 24 * 60
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

@bot.event
async def on_member_join(member: discord.Member):
    """Assign role based on the invite used to join."""
    guild = member.guild
    try:
        invites = await guild.invites()
    except Exception:
        logger.exception("Failed to fetch invites on member join")
        return

    used_invite = None
    for inv in invites:
        if inv.code in invite_roles and inv.uses > invite_uses.get(inv.code, 0):
            invite_uses[inv.code] = inv.uses
            used_invite = inv
            break

    if used_invite:
        role_id = invite_roles.get(used_invite.code)
        role = guild.get_role(role_id)
        if role:
            try:
                await member.add_roles(role)
                logger.info("Assigned role %s to %s via invite %s", role.id, member, used_invite.code)
            except Exception:
                logger.exception("Failed to assign role on join for %s", member)


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
        await interaction.followup.send(
            "❌ I can't kick that member. Check role hierarchy and `Kick Members` permission.",
            ephemeral=True,
        )
        return
    except discord.HTTPException:
        logger.exception("Failed to kick member %s", member)
        await interaction.followup.send("❌ Failed to kick the member. Try again.", ephemeral=True)
        return

    deleted_count, scanned_channels = await prune_user_messages(interaction.guild, target_id, KICK_PRUNE_HOURS)
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
        await ctx.send(error_message)
        return

    action_reason = reason.strip() or f"Kicked by {ctx.author} via bot"
    target_id = member.id
    target_name = str(member)
    try:
        await member.kick(reason=action_reason)
    except discord.Forbidden:
        logger.exception("Missing permission to kick member %s", member)
        await ctx.send("❌ I can't kick that member. Check role hierarchy and `Kick Members` permission.")
        return
    except discord.HTTPException:
        logger.exception("Failed to kick member %s", member)
        await ctx.send("❌ Failed to kick the member. Try again.")
        return

    deleted_count, scanned_channels = await prune_user_messages(ctx.guild, target_id, KICK_PRUNE_HOURS)
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
        await interaction.response.send_message(error_message, ephemeral=True)
        return

    timeout_delta, duration_text, parse_error = parse_timeout_duration(duration)
    if parse_error:
        await interaction.response.send_message(parse_error, ephemeral=True)
        return

    until = discord.utils.utcnow() + timeout_delta
    action_reason = (reason or "").strip() or f"Timed out by {interaction.user} via bot"
    try:
        await member.timeout(until, reason=action_reason)
    except discord.Forbidden:
        logger.exception("Missing permission to timeout member %s", member)
        await interaction.response.send_message(
            "❌ I can't timeout that member. Check role hierarchy and `Moderate Members` permission.",
            ephemeral=True,
        )
        return
    except discord.HTTPException:
        logger.exception("Failed to timeout member %s", member)
        await interaction.response.send_message("❌ Failed to timeout the member. Try again.", ephemeral=True)
        return

    timestamp = int(until.timestamp())
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
        await ctx.send(error_message)
        return

    timeout_delta, duration_text, parse_error = parse_timeout_duration(duration)
    if parse_error:
        await ctx.send(parse_error)
        return

    until = discord.utils.utcnow() + timeout_delta
    action_reason = reason.strip() or f"Timed out by {ctx.author} via bot"
    try:
        await member.timeout(until, reason=action_reason)
    except discord.Forbidden:
        logger.exception("Missing permission to timeout member %s", member)
        await ctx.send("❌ I can't timeout that member. Check role hierarchy and `Moderate Members` permission.")
        return
    except discord.HTTPException:
        logger.exception("Failed to timeout member %s", member)
        await ctx.send("❌ Failed to timeout the member. Try again.")
        return

    timestamp = int(until.timestamp())
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

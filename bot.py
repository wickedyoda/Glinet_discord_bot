import discord
from discord.ext import commands
from discord import app_commands
import logging
import os
import json
from dotenv import load_dotenv
import random

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

bot = commands.Bot(command_prefix='!', intents=intents)
tree = bot.tree

tag_responses = {}
tag_responses_mtime = None


def normalize_tag(tag: str) -> str:
    return tag.strip().lower()


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
    return "Tag commands: " + ", ".join(tags)


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

# Runtime caches for invite tracking
invite_roles = load_invite_roles()
invite_uses = {}

@bot.event
async def on_ready():
    logger.info("Logged in as %s", bot.user.name)
    guild = bot.get_guild(GUILD_ID)
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
        tag = message.content.strip().split()[0]
        if normalize_tag(tag) == "!list":
            await bot.process_commands(message)
            return
        response = get_tag_responses().get(normalize_tag(tag))
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


bot.run(TOKEN)

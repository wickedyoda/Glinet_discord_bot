import discord
from discord.ext import commands
from discord import app_commands
import logging
import os
from dotenv import load_dotenv
import random

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("invite_bot")

TOKEN = os.getenv("DISCORD_TOKEN")
GUILD_ID = int(os.getenv("GUILD_ID"))

# Directory to persist data files. This folder is mounted as a Docker volume
# so codes and invites survive container rebuilds.
DATA_DIR = os.getenv("DATA_DIR", "data")
os.makedirs(DATA_DIR, exist_ok=True)

ROLE_FILE = os.path.join(DATA_DIR, "access_role.txt")
INVITE_FILE = os.path.join(DATA_DIR, "permanent_invite.txt")
CODES_FILE = os.path.join(DATA_DIR, "role_codes.txt")

intents = discord.Intents.default()
intents.members = True
intents.message_content = True

bot = commands.Bot(command_prefix='!', intents=intents)
tree = bot.tree

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

def has_allowed_role(member: discord.Member):
    allowed = {"Employee", "Admin", "Gl.iNet Moderator"}
    has_role = any(role.name in allowed for role in member.roles)
    logger.debug("User %s allowed: %s", member, has_role)
    return has_role

@bot.event
async def on_ready():
    logger.info("Logged in as %s", bot.user.name)
    guild = discord.Object(id=GUILD_ID)
    synced = await tree.sync(guild=guild)
    logger.info("Synced %d command(s) to guild %s", len(synced), GUILD_ID)

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
        invite = await interaction.channel.create_invite(max_age=0, max_uses=0, unique=True)
        code = generate_code()
        save_role_code(code, role.id)

        logger.info("Generated invite %s and code %s for role %s", invite.url, code, role.id)

        await interaction.followup.send(
            f"✅ Invite link: {invite.url}\n🔢 6-digit code: `{code}`", ephemeral=True
        )
    except Exception as e:
        logger.exception("Error in /submitrole")
        await interaction.followup.send("❌ Something went wrong. Try again.", ephemeral=True)


@tree.command(name="enter_role", description="Enter a 6-digit code to receive a role", guild=discord.Object(id=GUILD_ID))
@app_commands.describe(code="The 6-digit code provided to you")
async def enter_role(interaction: discord.Interaction, code: str):
    logger.info("/enter_role invoked by %s with code %s", interaction.user, code)
    role_id = get_role_id_by_code(code)
    if not role_id:
        await interaction.response.send_message("❌ Invalid code.", ephemeral=True)
        return
=======
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


    await interaction.user.add_roles(role)
    logger.info("Assigned role %s to user %s via code", role.id, interaction.user)
    await interaction.response.send_message(f"✅ You've been given the **{role.name}** role!", ephemeral=True)

@tree.command(
    name="enter_role",
    description="Enter a 6-digit code to receive a role",
    guild=discord.Object(id=GUILD_ID),
)
async def enter_role(interaction: discord.Interaction):
    """Prompt the user to enter their code via a modal."""

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
    except Exception as e:
        print("Error in /getaccess:", e)
        await interaction.response.send_message(
            "❌ Could not assign role. Contact an admin.", ephemeral=True
        )

        logger.exception("Error in /getaccess")
        await interaction.response.send_message("❌ Could not assign role. Contact an admin.", ephemeral=True)


bot.run(TOKEN)

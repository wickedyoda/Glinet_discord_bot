import discord
from discord.ext import commands
from discord import app_commands
import os
from dotenv import load_dotenv
import random

load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN")
GUILD_ID = int(os.getenv("GUILD_ID"))
ROLE_FILE = "access_role.txt"
INVITE_FILE = "permanent_invite.txt"
CODES_FILE = "role_codes.txt"

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
            return code


def save_role_code(code, role_id):
    with open(CODES_FILE, 'a') as f:
        f.write(f"{code}:{role_id}\n")


def get_role_id_by_code(code):
    if not os.path.exists(CODES_FILE):
        return None
    with open(CODES_FILE, 'r') as f:
        for line in f:
            if line.startswith(code + ":"):
                return int(line.strip().split(":")[1])
    return None


@bot.event
async def on_ready():
    print(f"Logged in as {bot.user} (ID: {bot.user.id})")
    guild = discord.Object(id=GUILD_ID)
    try:
        synced = await tree.sync(guild=guild)
        print(f"✅ Slash commands synced to guild {GUILD_ID}. Total: {len(synced)}")
    except Exception as e:
        print("❌ Failed to sync slash commands:", e)


@tree.command(name="submitrole", description="Submit a role for invite/code linking")
async def submitrole(interaction: discord.Interaction):
    allowed_roles = ["Employee", "Admin", "Gl.iNet Moderators"]
    if not any(role.name in allowed_roles for role in interaction.user.roles):
        await interaction.response.send_message("❌ You do not have permission to use this command.", ephemeral=True)
        return

    await interaction.response.send_message("Please mention the role you want to assign.", ephemeral=True)

    def check(m):
        return m.author == interaction.user and m.channel == interaction.channel

    try:
        msg = await bot.wait_for('message', timeout=30.0, check=check)
        if not msg.role_mentions:
            await interaction.followup.send("❌ No role mentioned.", ephemeral=True)
            return
        role = msg.role_mentions[0]
        invite = await interaction.channel.create_invite(max_age=0, max_uses=0, unique=True)
        code = generate_code()
        save_role_code(code, role.id)
        await interaction.followup.send(f"✅ Invite link: {invite.url}\n6-digit code: `{code}`", ephemeral=True)
    except Exception as e:
        await interaction.followup.send("❌ Something went wrong.", ephemeral=True)
        print("Error in /submitrole:", e)


@tree.command(name="enter_role", description="Enter a 6-digit code to receive a role")
async def enter_role(interaction: discord.Interaction, code: str):
    role_id = get_role_id_by_code(code)
    if not role_id:
        await interaction.response.send_message("❌ Invalid code.", ephemeral=True)
        return
    role = interaction.guild.get_role(role_id)
    if not role:
        await interaction.response.send_message("❌ Role not found.", ephemeral=True)
        return
    await interaction.user.add_roles(role)
    await interaction.response.send_message(f"✅ You've been given the **{role.name}** role!", ephemeral=True)


@tree.command(name="getaccess", description="Assign yourself the protected role")
async def getaccess(interaction: discord.Interaction):
    try:
        with open(ROLE_FILE, "r") as f:
            role_id = int(f.read().strip())
        role = interaction.guild.get_role(role_id)
        await interaction.user.add_roles(role)
        await interaction.response.send_message(f"✅ You've been given the **{role.name}** role!", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message("❌ Could not assign role. Contact an admin.", ephemeral=True)
        print("Error in /getaccess:", e)


bot.run(TOKEN)
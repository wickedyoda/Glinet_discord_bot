import discord
from discord.ext import commands
from discord import app_commands
import os
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN")
GUILD_ID = int(os.getenv("GUILD_ID"))
ROLE_FILE = "access_role.txt"
INVITE_FILE = "permanent_invite.txt"

intents = discord.Intents.default()
intents.members = True
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents=intents)
tree = bot.tree

invite_code = None
previous_uses = {}

def save_role_id(role_id):
    with open(ROLE_FILE, 'w') as f:
        f.write(str(role_id))

def load_role_id():
    if not os.path.exists(ROLE_FILE):
        return None
    with open(ROLE_FILE, 'r') as f:
        return int(f.read().strip())

def save_invite(invite):
    with open(INVITE_FILE, 'w') as f:
        f.write(invite.code)

def load_invite():
    if not os.path.exists(INVITE_FILE):
        return None
    with open(INVITE_FILE, 'r') as f:
        return f.read().strip()

@bot.event
async def on_ready():
    print(f"Logged in as {bot.user.name}")
    guild = discord.Object(id=GUILD_ID)
    await tree.sync(guild=guild)
    print("Slash commands synced")
    full_guild = bot.get_guild(GUILD_ID)
    invites = await full_guild.invites()
    global previous_uses
    previous_uses = {invite.code: invite.uses for invite in invites}

@tree.command(name="setaccessrole", description="Set the role to assign via invite", guild=discord.Object(id=GUILD_ID))
@app_commands.describe(role="The role to assign")
async def setaccessrole(interaction: discord.Interaction, role: discord.Role):
    save_role_id(role.id)
    await interaction.response.send_message(f"Access role set to {role.name}.", ephemeral=True)

@tree.command(name="generateinvite", description="Generate a permanent invite link", guild=discord.Object(id=GUILD_ID))
async def generateinvite(interaction: discord.Interaction):
    invite = await interaction.channel.create_invite(max_age=0, max_uses=0, unique=True)
    save_invite(invite)
    await interaction.response.send_message(f"Permanent invite created: {invite.url}", ephemeral=True)

@bot.event
async def on_member_join(member):
    role_id = load_role_id()
    if role_id:
        role = member.guild.get_role(role_id)
        if role:
            await member.add_roles(role)
            print(f"Assigned role to new member {member.name}")

@bot.event
async def on_invite_create(invite):
    global previous_uses
    previous_uses[invite.code] = 0

@bot.event
async def on_member_update(before, after):
    global previous_uses
    guild = after.guild
    invites = await guild.invites()
    role_id = load_role_id()
    target_role = guild.get_role(role_id) if role_id else None
    if not target_role:
        return

    for invite in invites:
        if invite.code in previous_uses and invite.uses > previous_uses[invite.code]:
            if target_role not in after.roles:
                await after.add_roles(target_role)
                print(f"Assigned role to existing member {after.name} via invite")
        previous_uses[invite.code] = invite.uses

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
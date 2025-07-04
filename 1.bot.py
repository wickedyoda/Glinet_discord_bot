import discord
from discord.ext import commands
import os
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN")
GUILD_ID = int(os.getenv("GUILD_ID"))
ROLE_FILE = "access_role.txt"
INVITE_FILE = "permanent_invite.txt"

intents = discord.Intents.default()
intents.members = True
intents.message_content = True  # ✅ this is critical for reading `!commands`
bot = commands.Bot(command_prefix='!', intents=intents)

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
    guild = bot.get_guild(GUILD_ID)
    invites = await guild.invites()
    global previous_uses
    previous_uses = {invite.code: invite.uses for invite in invites}

@bot.command()
@commands.has_permissions(administrator=True)
async def setaccessrole(ctx, role: discord.Role):
    save_role_id(role.id)
    await ctx.send(f"Access role set to {role.name}.")

@bot.command()
@commands.has_permissions(administrator=True)
async def generateinvite(ctx):
    invite = await ctx.channel.create_invite(max_age=0, max_uses=0, unique=True)
    save_invite(invite)
    await ctx.send(f"Permanent invite created: {invite.url}")

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

bot.run(TOKEN)
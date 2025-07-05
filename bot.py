import discord
from discord.ext import commands
from discord import app_commands
import os
import json
import random
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN")
GUILD_ID = int(os.getenv("GUILD_ID"))
ROLEMAP_FILE = "rolemap.json"

intents = discord.Intents.default()
intents.members = True
bot = commands.Bot(command_prefix="!", intents=intents)
tree = bot.tree

# Load/save role-code-invite mappings
def load_rolemap():
    if not os.path.exists(ROLEMAP_FILE):
        return {}
    with open(ROLEMAP_FILE, "r") as f:
        return json.load(f)

def save_rolemap(data):
    with open(ROLEMAP_FILE, "w") as f:
        json.dump(data, f, indent=2)

def generate_code():
    while True:
        code = ''.join(random.choices("0123456789", k=6))
        if all(code[i] != code[i+1] or code[i] != code[i+2] for i in range(4)):  # no more than 2 in a row
            return code

@bot.event
async def on_ready():
    await tree.sync(guild=discord.Object(id=GUILD_ID))
    print(f"✅ Bot is ready as {bot.user.name}")

@tree.command(name="submitrole", description="Begin protected role invite setup", guild=discord.Object(id=GUILD_ID))
async def submitrole(interaction: discord.Interaction):
    await interaction.response.send_message("🛠 Please mention the role you'd like to protect access to.", ephemeral=True)

    def check(msg):
        return msg.author.id == interaction.user.id and msg.channel == interaction.channel and len(msg.role_mentions) > 0

    try:
        msg = await bot.wait_for("message", timeout=60.0, check=check)
        role = msg.role_mentions[0]

        invite = await msg.channel.create_invite(max_age=0, max_uses=0, unique=True)
        code = generate_code()

        rolemap = load_rolemap()
        rolemap[code] = {
            "role_id": role.id,
            "invite": invite.code
        }
        save_rolemap(rolemap)

        await interaction.followup.send(
            f"✅ Invite created: {invite.url}\n🔢 6-digit code for existing members: `{code}`\n\n"
            f"New members who use the invite will be assigned **{role.name}**.\n"
            f"Existing members can use `/enter_role` and submit the code.",
            ephemeral=True
        )

    except Exception as e:
        await interaction.followup.send("❌ Timeout or error occurred. Please try again.", ephemeral=True)
        print(f"Error in /submitrole: {e}")

@bot.event
async def on_member_join(member):
    rolemap = load_rolemap()
    invites = await member.guild.invites()
    used = None

    for invite in invites:
        if invite.uses > 0:
            for code, data in rolemap.items():
                if data["invite"] == invite.code:
                    role = member.guild.get_role(data["role_id"])
                    if role:
                        await member.add_roles(role)
                        print(f"Assigned {role.name} to {member.name}")
                        return

@tree.command(name="enter_role", description="Submit your 6-digit access code", guild=discord.Object(id=GUILD_ID))
@app_commands.describe(code="6-digit code given to you")
async def enter_role(interaction: discord.Interaction, code: str):
    rolemap = load_rolemap()
    data = rolemap.get(code)

    if data:
        role = interaction.guild.get_role(data["role_id"])
        if role:
            await interaction.user.add_roles(role)
            await interaction.response.send_message(f"✅ You've been given the **{role.name}** role!", ephemeral=True)
        else:
            await interaction.response.send_message("❌ Role no longer exists.", ephemeral=True)
    else:
        await interaction.response.send_message("❌ Invalid or expired code.", ephemeral=True)

bot.run(TOKEN)
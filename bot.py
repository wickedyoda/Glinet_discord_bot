
import discord
from discord.ext import commands
from discord import app_commands
import os
from dotenv import load_dotenv
import random
import string
import json
from pathlib import Path
from datetime import datetime

load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN")
GUILD_ID = int(os.getenv("GUILD_ID"))
ROLEMAP_FILE = "rolemap.json"

intents = discord.Intents.default()
intents.members = True
bot = commands.Bot(command_prefix='!', intents=intents)
tree = bot.tree

def load_rolemap():
    if not os.path.exists(ROLEMAP_FILE):
        return []
    with open(ROLEMAP_FILE, "r") as f:
        return json.load(f).get("entries", [])

def save_rolemap(entries):
    with open(ROLEMAP_FILE, "w") as f:
        json.dump({"entries": entries}, f, indent=2)

def generate_code():
    return ''.join(random.choices(string.digits, k=6))

@bot.event
async def on_ready():
    await tree.sync(guild=discord.Object(id=GUILD_ID))
    print(f"Logged in as {bot.user.name}, slash commands synced.")

@tree.command(name="protectrole", description="Generate an invite and code to assign a role", guild=discord.Object(id=GUILD_ID))
@app_commands.describe(role="Role to protect")
async def protectrole(interaction: discord.Interaction, role: discord.Role):
    try:
        invite = await interaction.channel.create_invite(max_age=0, max_uses=0, unique=True)
        code = generate_code()
        entries = load_rolemap()
        entries.append({
            "role_id": role.id,
            "role_name": role.name,
            "invite_link": invite.url,
            "code": code,
            "created_at": datetime.utcnow().isoformat()
        })
        save_rolemap(entries)
        await interaction.response.send_message(
            f"✅ Protected role setup!
Invite: {invite.url}
Code: `{code}`", ephemeral=True
        )
    except Exception as e:
        await interaction.response.send_message("❌ Failed to generate invite/code.", ephemeral=True)
        print("Error in /protectrole:", e)

@tree.command(name="usecode", description="Use a code to claim access to a protected role", guild=discord.Object(id=GUILD_ID))
@app_commands.describe(code="6-digit access code")
async def usecode(interaction: discord.Interaction, code: str):
    try:
        entries = load_rolemap()
        match = next((e for e in entries if e["code"] == code), None)
        if not match:
            await interaction.response.send_message("❌ Invalid code.", ephemeral=True)
            return
        role = interaction.guild.get_role(int(match["role_id"]))
        if role:
            await interaction.user.add_roles(role)
            await interaction.response.send_message(f"✅ Role **{role.name}** assigned!", ephemeral=True)
        else:
            await interaction.response.send_message("❌ Role no longer exists.", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message("❌ Could not assign role.", ephemeral=True)
        print("Error in /usecode:", e)

bot.run(TOKEN)

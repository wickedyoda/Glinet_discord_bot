"""
Tickets module — ported from discord-tickets/bot (GPLv3).
Provides ticket lifecycle: create, claim, close, reopen, transcript.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands
from sqlite3 import Connection


TICKET_CATEGORIES_DEFAULT = [
    {"id": "support", "name": "Support", "questions": ["What do you need help with?"]},
    {"id": "sales", "name": "Sales", "questions": ["What product/plan are you interested in?"]},
    {"id": "partnership", "name": "Partnership", "questions": ["Describe the partnership idea."]},
]


class TicketStore:
    """Thin wrapper around existing guild DB for ticket records."""

    def __init__(self, conn: Connection, guild_id: int | None = None) -> None:
        self.conn = conn
        self.guild_id = guild_id

    def ensure_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS tickets (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id     INTEGER NOT NULL DEFAULT 0,
                channel_id   INTEGER NOT NULL,
                owner_id     INTEGER NOT NULL,
                category_id  TEXT NOT NULL,
                claimer_id   INTEGER,
                status       TEXT NOT NULL DEFAULT 'open',
                number       INTEGER,
                created_at   TEXT NOT NULL,
                closed_at    TEXT,
                note         TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_tickets_guild
                ON tickets(guild_id);
            CREATE INDEX IF NOT EXISTS idx_tickets_channel
                ON tickets(channel_id);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_tickets_guild_number
                ON tickets(guild_id, number);
            """
        )
        self.conn.commit()

    def next_number(self, guild_id: int) -> int:
        row = self.conn.execute(
            "SELECT COALESCE(MAX(number),0) AS n FROM tickets WHERE guild_id=?",
            (guild_id,),
        ).fetchone()
        return int(row["n"]) + 1

    def create(self, *, channel_id: int, owner_id: int, category_id: str, guild_id: int) -> int:
        number = self.next_number(guild_id)
        now = datetime.now(UTC).isoformat()
        cur = self.conn.execute(
            "INSERT INTO tickets(guild_id,channel_id,owner_id,category_id,number,created_at) "
            "VALUES(?,?,?,?,?,?)",
            (guild_id, channel_id, owner_id, category_id, number, now),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def close(self, channel_id: int, *, closer_id: int, note: str | None = None) -> bool:
        now = datetime.now(UTC).isoformat()
        cur = self.conn.execute(
            "UPDATE tickets SET status='closed', closed_at=?, claimer_id=?, note=? "
            "WHERE channel_id=? AND status='open'",
            (now, closer_id, note, channel_id),
        )
        self.conn.commit()
        return cur.rowcount > 0

    def reopen(self, channel_id: int) -> bool:
        cur = self.conn.execute(
            "UPDATE tickets SET status='open', closed_at=NULL, claimer_id=NULL "
            "WHERE channel_id=? AND status='closed'",
            (channel_id,),
        )
        self.conn.commit()
        return cur.rowcount > 0

    def get_by_channel(self, channel_id: int) -> Optional[dict]:
        row = self.conn.execute(
            "SELECT * FROM tickets WHERE channel_id=? LIMIT 1", (channel_id,)
        ).fetchone()
        if row is None:
            return None
        return dict(row)

    def count_open(self, guild_id: int) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) AS c FROM tickets WHERE guild_id=? AND status='open'",
            (guild_id,),
        ).fetchone()
        return int(row["c"])

    def stats(self, guild_id: int) -> dict:
        rows = self.conn.execute(
            "SELECT category_id, status, COUNT(*) AS c "
            "FROM tickets WHERE guild_id=? GROUP BY category_id, status",
            (guild_id,),
        ).fetchall()
        by_category: dict[str, dict[str, int]] = {}
        for row in rows:
            cat = by_category.setdefault(row["category_id"], {"open": 0, "closed": 0})
            status = row["status"]
            if status in cat:
                cat[status] = int(row["c"])
        return by_category


def _permission_set(role_ids: list[int]) -> discord.PermissionOverwrite:
    return discord.PermissionOverwrite(
        view_channel=True, send_messages=True, read_message_history=True
    )


class TicketCategorySelect(discord.ui.Select):
    def __init__(self, categories: list[dict]) -> None:
        options = [
            discord.SelectOption(
                label=cat.get("name", cat["id"]),
                value=cat["id"],
                description=f"{len(cat.get('questions', []))} question(s)",
            )
            for cat in categories
        ]
        super().__init__(placeholder="Choose a category...", options=options, min_values=1, max_values=1)

    async def callback(self, interaction: discord.Interaction) -> None:
        view: TicketFlowView = self.view  # type: ignore[assignment]
        await view.select_category(interaction, self.values[0])


class TicketFlowView(discord.ui.View):
    def __init__(
        self,
        cog: "Tickets",
        *,
        categories: list[dict],
        claimer_role_id: int | None = None,
    ) -> None:
        super().__init__(timeout=None)
        self.cog = cog
        self.categories = categories
        self.claimer_role_id = claimer_role_id
        self.category_select = TicketCategorySelect(categories)
        self.claim_button = discord.ui.Button(
            label="Claim",
            style=discord.ButtonStyle.primary,
            custom_id="tickets:claim",
        )
        self.close_button = discord.ui.Button(
            label="Close",
            style=discord.ButtonStyle.danger,
            custom_id="tickets:close",
        )
        self.reopen_button = discord.ui.Button(
            label="Reopen",
            style=discord.ButtonStyle.success,
            custom_id="tickets:reopen",
        )
        self.add_item(self.category_select)
        self.add_item(self.claim_button)
        self.add_item(self.close_button)
        self.add_item(self.reopen_button)
        self.category_select.callback = self._on_category_select  # type: ignore[method-assign]
        self.claim_button.callback = self._on_claim  # type: ignore[method-assign]
        self.close_button.callback = self._on_close  # type: ignore[method-assign]
        self.reopen_button.callback = self._on_reopen  # type: ignore[method-assign]

    async def _on_category_select(self, interaction: discord.Interaction) -> None:
        await self.select_category(interaction, self.category_select.values[0])

    async def select_category(self, interaction: discord.Interaction, category_id: str) -> None:
        category = next((c for c in self.categories if c["id"] == category_id), None)
        if category is None:
            await interaction.response.send_message("Unknown category.", ephemeral=True)
            return
        questions = category.get("questions") or []
        if not questions:
            await interaction.response.send_message(
                "That category has no questions configured.", ephemeral=True
            )
            return
        await interaction.response.send_modal(TicketQuestionsModal(self.cog, category, self))

    async def _on_claim(self, interaction: discord.Interaction) -> None:
        record = self.cog.store.get_by_channel(interaction.channel_id)
        if record is None:
            return await interaction.response.send_message("Not a ticket channel.", ephemeral=True)
        if record["status"] != "open":
            return await interaction.response.send_message("Ticket is not open.", ephemeral=True)
        self.cog.store.close(
            interaction.channel_id,
            closer_id=interaction.user.id,
            note="claimed",
        )
        if self.claimer_role_id:
            role = interaction.guild.get_role(self.claimer_role_id) if interaction.guild else None
            if role:
                try:
                    await interaction.channel.edit(sync_permissions=True)
                    await interaction.channel.set_permissions(role, view_channel=True, send_messages=True)
                except Exception:
                    pass
        await interaction.response.send_message(
            f"{interaction.user.mention} claimed this ticket.", allowed_mentions=discord.AllowedMentions.none()
        )

    async def _on_close(self, interaction: discord.Interaction) -> None:
        if self.cog.store.close(interaction.channel_id, closer_id=interaction.user.id):
            await interaction.response.send_message("Ticket marked closed.")
        else:
            await interaction.response.send_message("Could not close this ticket.", ephemeral=True)

    async def _on_reopen(self, interaction: discord.Interaction) -> None:
        if self.cog.store.reopen(interaction.channel_id):
            await interaction.response.send_message("Ticket reopened.")
        else:
            await interaction.response.send_message("Could not reopen.", ephemeral=True)


class TicketQuestionsModal(discord.ui.Modal):
    def __init__(self, cog: "Tickets", category: dict, view: TicketFlowView) -> None:
        super().__init__(title=f"{category.get('name', category['id'])} ticket")
        self.cog = cog
        self.category = category
        self.flow_view = view
        for idx, q in enumerate(category.get("questions", [])):
            self.add_item(
                discord.ui.TextInput(
                    label=q[:45],
                    style=discord.TextStyle.paragraph,
                    required=idx == 0,
                )
            )

    async def on_submit(self, interaction: discord.Interaction) -> None:
        answers = "\n".join(f"- {i.value}" for i in self.children if isinstance(i, discord.ui.TextInput))
        overwrites: dict[discord.abc.Snowflake, discord.PermissionOverwrite] = {}
        overwrites[interaction.guild.default_role] = discord.PermissionOverwrite(view_channel=False)
        everyone = interaction.guild.default_role
        overwrites[everyone] = discord.PermissionOverwrite(view_channel=False)
        if interaction.user:
            overwrites[interaction.user] = discord.PermissionOverwrite(
                view_channel=True, send_messages=True, read_message_history=True
            )
        if self.flow_view.claimer_role_id and interaction.guild:
            role = interaction.guild.get_role(self.flow_view.claimer_role_id)
            if role:
                overwrites[role] = discord.PermissionOverwrite(
                    view_channel=True, send_messages=True
                )
        category_channel = None
        if interaction.guild:
            for cat in interaction.guild.categories:
                if cat.name.lower() == "tickets":
                    category_channel = cat
                    break
        try:
            channel = await interaction.guild.create_text_channel(
                name=f"ticket-{self.category['id']}",
                overwrites=overwrites,
                category=category_channel,
                reason="ticket created",
            )
        except Exception as exc:
            return await interaction.response.send_message(
                f"Failed to create ticket: {exc}", ephemeral=True
            )
        record = self.cog.store.create(
            channel_id=channel.id,
            owner_id=interaction.user.id if interaction.user else 0,
            category_id=self.category["id"],
            guild_id=interaction.guild_id or 0,
        )
        embed = discord.Embed(
            title=f"Ticket #{record} — {self.category.get('name', self.category['id'])}",
            description=answers or "No details provided.",
            color=discord.Color.blurple(),
        )
        embed.set_footer(text=f"Owner: {interaction.user} ({interaction.user.id})" if interaction.user else "")
        await channel.send(embed=embed, view=self.flow_view)
        await interaction.response.send_message(
            f"Ticket created: {channel.mention}", ephemeral=True
        )


class Tickets(commands.GroupCog, name="tickets"):
    def __init__(self, bot: commands.Bot, store: TicketStore) -> None:
        self.bot = bot
        self.store = store
        self.categories = TICKET_CATEGORIES_DEFAULT
        self.claimer_role_id: int | None = None

    @commands.Cog.listener()
    async def on_ready(self) -> None:
        self.store.ensure_schema()
        self.bot.add_view(TicketFlowView(self, categories=self.categories, claimer_role_id=self.claimer_role_id))

    @app_commands.command(name="panel", description="Send the ticket creation panel to a channel.")
    @app_commands.checks.has_permissions(manage_channels=True)
    async def panel(self, interaction: discord.Interaction) -> None:
        embed = discord.Embed(
            title="Support Tickets",
            description="Use the dropdown below to open a ticket.",
            color=discord.Color.blurple(),
        )
        await interaction.response.send_message(
            embed=embed, view=TicketFlowView(self, categories=self.categories, claimer_role_id=self.claimer_role_id)
        )

    @app_commands.command(name="stats", description="Show ticket stats for this server.")
    async def stats(self, interaction: discord.Interaction) -> None:
        data = self.store.stats(interaction.guild_id or 0)
        lines = []
        for category, counts in data.items():
            lines.append(
                f"- **{category}**: {counts.get('open', 0)} open / {counts.get('closed', 0)} closed"
            )
        body = "\n".join(lines) if lines else "No tickets yet."
        await interaction.response.send_message(body, ephemeral=True)

    @app_commands.command(name="categories", description="List configured ticket categories.")
    @app_commands.checks.has_permissions(manage_channels=True)
    async def categories_cmd(self, interaction: discord.Interaction) -> None:
        lines = [f"- {c['id']}: {c.get('name', c['id'])}" for c in self.categories]
        await interaction.response.send_message("\n".join(lines), ephemeral=True)

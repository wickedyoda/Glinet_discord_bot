from __future__ import annotations


def ensure_reaction_roles_schema_locked(conn):
    columns = {str(row["name"]) for row in conn.execute("PRAGMA table_info(reaction_roles)").fetchall()}
    if not columns:
        conn.executescript(
            """
            CREATE TABLE reaction_roles (
                guild_id INTEGER NOT NULL DEFAULT 0,
                channel_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                emoji_key TEXT NOT NULL,
                emoji_text TEXT NOT NULL DEFAULT '',
                role_id INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (guild_id, message_id, emoji_key)
            );
            CREATE INDEX IF NOT EXISTS idx_reaction_roles_guild_id ON reaction_roles(guild_id);
            CREATE INDEX IF NOT EXISTS idx_reaction_roles_message_id ON reaction_roles(message_id);
            CREATE INDEX IF NOT EXISTS idx_reaction_roles_role_id ON reaction_roles(role_id);
            CREATE INDEX IF NOT EXISTS idx_reaction_roles_status ON reaction_roles(status);
            """
        )
        return

    if "guild_id" not in columns:
        conn.executescript(
            """
            CREATE TABLE reaction_roles_new (
                guild_id INTEGER NOT NULL DEFAULT 0,
                channel_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                emoji_key TEXT NOT NULL,
                emoji_text TEXT NOT NULL DEFAULT '',
                role_id INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (guild_id, message_id, emoji_key)
            );
            INSERT INTO reaction_roles_new (guild_id, channel_id, message_id, emoji_key, emoji_text, role_id, status, created_at, updated_at)
            SELECT 0, channel_id, message_id, emoji_key, COALESCE(emoji_text, ''), role_id, COALESCE(status, 'active'), created_at, COALESCE(updated_at, created_at)
            FROM reaction_roles;
            DROP TABLE reaction_roles;
            ALTER TABLE reaction_roles_new RENAME TO reaction_roles;
            """
        )
        columns = {str(row["name"]) for row in conn.execute("PRAGMA table_info(reaction_roles)").fetchall()}

    if "channel_id" not in columns:
        conn.execute("ALTER TABLE reaction_roles ADD COLUMN channel_id INTEGER NOT NULL DEFAULT 0")
    if "emoji_key" not in columns:
        conn.execute("ALTER TABLE reaction_roles ADD COLUMN emoji_key TEXT NOT NULL DEFAULT ''")
    if "emoji_text" not in columns:
        conn.execute("ALTER TABLE reaction_roles ADD COLUMN emoji_text TEXT NOT NULL DEFAULT ''")
    if "status" not in columns:
        conn.execute("ALTER TABLE reaction_roles ADD COLUMN status TEXT NOT NULL DEFAULT 'active'")
    if "updated_at" not in columns:
        conn.execute("ALTER TABLE reaction_roles ADD COLUMN updated_at TEXT NOT NULL DEFAULT ''")
    conn.execute("UPDATE reaction_roles SET updated_at = created_at WHERE TRIM(COALESCE(updated_at, '')) = ''")
    conn.execute("UPDATE reaction_roles SET status = 'active' WHERE TRIM(COALESCE(status, '')) = ''")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_reaction_roles_guild_id ON reaction_roles(guild_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_reaction_roles_message_id ON reaction_roles(message_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_reaction_roles_role_id ON reaction_roles(role_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_reaction_roles_status ON reaction_roles(status)")
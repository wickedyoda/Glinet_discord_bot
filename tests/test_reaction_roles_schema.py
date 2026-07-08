import sqlite3

from app.reaction_roles_schema import ensure_reaction_roles_schema_locked


def test_ensure_reaction_roles_schema_creates_table_and_indexes():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    ensure_reaction_roles_schema_locked(conn)

    columns = {str(row["name"]) for row in conn.execute("PRAGMA table_info(reaction_roles)").fetchall()}
    assert {"guild_id", "channel_id", "message_id", "emoji_key", "emoji_text", "role_id", "status", "created_at", "updated_at"}.issubset(columns)

    indexes = {str(row["name"]) for row in conn.execute("PRAGMA index_list(reaction_roles)").fetchall()}
    assert "idx_reaction_roles_guild_id" in indexes
    assert "idx_reaction_roles_message_id" in indexes
    assert "idx_reaction_roles_role_id" in indexes
    assert "idx_reaction_roles_status" in indexes
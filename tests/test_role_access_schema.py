import sqlite3

from app.role_access_schema import ensure_role_access_schema_locked


def test_ensure_role_access_schema_migrates_legacy_tables_without_status_columns():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE role_codes (
            guild_id INTEGER NOT NULL DEFAULT 0,
            code TEXT NOT NULL,
            role_id INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT '',
            invite_code TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (guild_id, code)
        );
        CREATE TABLE invite_roles (
            guild_id INTEGER NOT NULL DEFAULT 0,
            invite_code TEXT NOT NULL,
            role_id INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT '',
            code TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (guild_id, invite_code)
        );
        INSERT INTO role_codes (guild_id, code, role_id, created_at, updated_at, invite_code)
        VALUES (123, '123456', 10, '2026-03-25T00:00:00+00:00', '', 'abc123');
        INSERT INTO invite_roles (guild_id, invite_code, role_id, created_at, updated_at, code)
        VALUES (123, 'abc123', 10, '2026-03-25T00:00:00+00:00', '', '123456');
        """
    )

    ensure_role_access_schema_locked(conn)

    role_code_columns = {str(row["name"]) for row in conn.execute("PRAGMA table_info(role_codes)").fetchall()}
    invite_role_columns = {str(row["name"]) for row in conn.execute("PRAGMA table_info(invite_roles)").fetchall()}
    assert "status" in role_code_columns
    assert "status" in invite_role_columns

    role_row = conn.execute(
        "SELECT guild_id, code, role_id, updated_at, invite_code, status FROM role_codes"
    ).fetchone()
    invite_row = conn.execute(
        "SELECT guild_id, invite_code, role_id, updated_at, code, status FROM invite_roles"
    ).fetchone()

    assert dict(role_row)["status"] == "active"
    assert dict(invite_row)["status"] == "active"
    assert dict(role_row)["updated_at"] == "2026-03-25T00:00:00+00:00"
    assert dict(invite_row)["updated_at"] == "2026-03-25T00:00:00+00:00"

    role_indexes = {str(row["name"]) for row in conn.execute("PRAGMA index_list(role_codes)").fetchall()}
    invite_indexes = {str(row["name"]) for row in conn.execute("PRAGMA index_list(invite_roles)").fetchall()}
    assert "idx_role_codes_status" in role_indexes
    assert "idx_invite_roles_status" in invite_indexes

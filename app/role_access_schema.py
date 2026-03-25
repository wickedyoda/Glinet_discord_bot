def ensure_role_access_schema_locked(conn):
    role_code_columns = {str(row["name"]) for row in conn.execute("PRAGMA table_info(role_codes)").fetchall()}
    if "guild_id" not in role_code_columns:
        conn.executescript(
            """
            CREATE TABLE role_codes_new (
                guild_id INTEGER NOT NULL,
                code TEXT NOT NULL,
                role_id INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT '',
                invite_code TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'active',
                PRIMARY KEY (guild_id, code)
            );
            INSERT INTO role_codes_new (guild_id, code, role_id, created_at, updated_at, invite_code, status)
            SELECT 0, code, role_id, created_at, created_at, '', 'active'
            FROM role_codes;
            DROP TABLE role_codes;
            ALTER TABLE role_codes_new RENAME TO role_codes;
            """
        )
        role_code_columns = {str(row["name"]) for row in conn.execute("PRAGMA table_info(role_codes)").fetchall()}
    if "updated_at" not in role_code_columns:
        conn.execute("ALTER TABLE role_codes ADD COLUMN updated_at TEXT NOT NULL DEFAULT ''")
    if "invite_code" not in role_code_columns:
        conn.execute("ALTER TABLE role_codes ADD COLUMN invite_code TEXT NOT NULL DEFAULT ''")
    if "status" not in role_code_columns:
        conn.execute("ALTER TABLE role_codes ADD COLUMN status TEXT NOT NULL DEFAULT 'active'")
    conn.execute(
        "UPDATE role_codes SET updated_at = created_at WHERE TRIM(COALESCE(updated_at, '')) = ''"
    )
    conn.execute(
        "UPDATE role_codes SET status = 'active' WHERE TRIM(COALESCE(status, '')) = ''"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_role_codes_role_id ON role_codes(role_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_role_codes_guild_id ON role_codes(guild_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_role_codes_status ON role_codes(status)")

    invite_role_columns = {str(row["name"]) for row in conn.execute("PRAGMA table_info(invite_roles)").fetchall()}
    if "guild_id" not in invite_role_columns:
        conn.executescript(
            """
            CREATE TABLE invite_roles_new (
                guild_id INTEGER NOT NULL,
                invite_code TEXT NOT NULL,
                role_id INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT '',
                code TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'active',
                PRIMARY KEY (guild_id, invite_code)
            );
            INSERT INTO invite_roles_new (guild_id, invite_code, role_id, created_at, updated_at, code, status)
            SELECT 0, invite_code, role_id, created_at, created_at, '', 'active'
            FROM invite_roles;
            DROP TABLE invite_roles;
            ALTER TABLE invite_roles_new RENAME TO invite_roles;
            """
        )
        invite_role_columns = {str(row["name"]) for row in conn.execute("PRAGMA table_info(invite_roles)").fetchall()}
    if "updated_at" not in invite_role_columns:
        conn.execute("ALTER TABLE invite_roles ADD COLUMN updated_at TEXT NOT NULL DEFAULT ''")
    if "code" not in invite_role_columns:
        conn.execute("ALTER TABLE invite_roles ADD COLUMN code TEXT NOT NULL DEFAULT ''")
    if "status" not in invite_role_columns:
        conn.execute("ALTER TABLE invite_roles ADD COLUMN status TEXT NOT NULL DEFAULT 'active'")
    conn.execute(
        "UPDATE invite_roles SET updated_at = created_at WHERE TRIM(COALESCE(updated_at, '')) = ''"
    )
    conn.execute(
        "UPDATE invite_roles SET status = 'active' WHERE TRIM(COALESCE(status, '')) = ''"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_invite_roles_role_id ON invite_roles(role_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_invite_roles_guild_id ON invite_roles(guild_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_invite_roles_status ON invite_roles(status)")

from __future__ import annotations

import json
import os
import sqlite3
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path


def current_time_iso() -> str:
    return datetime.now(UTC).isoformat()


def normalize_id_string_list(raw_values):
    if raw_values is None:
        items = []
    elif isinstance(raw_values, (list, tuple, set)):
        items = list(raw_values)
    else:
        raw_text = str(raw_values or "").strip()
        if not raw_text:
            items = []
        else:
            try:
                parsed = json.loads(raw_text)
            except json.JSONDecodeError:
                parsed = None
            if isinstance(parsed, list):
                items = parsed
            else:
                items = [part.strip() for part in raw_text.split(",")]

    normalized = []
    seen = set()
    for item in items:
        value = str(item or "").strip()
        if not value or not value.isdigit() or value in seen:
            continue
        normalized.append(value)
        seen.add(value)
    return normalized


def serialize_id_string_list(raw_values):
    return json.dumps(normalize_id_string_list(raw_values), separators=(",", ":"))


def normalize_string_id_list(raw_values):
    if raw_values is None:
        items = []
    elif isinstance(raw_values, (list, tuple, set)):
        items = list(raw_values)
    else:
        raw_text = str(raw_values or "").strip()
        if not raw_text:
            items = []
        else:
            try:
                parsed = json.loads(raw_text)
            except json.JSONDecodeError:
                parsed = None
            if isinstance(parsed, list):
                items = parsed
            else:
                items = [part.strip() for part in raw_text.split(",")]

    normalized = []
    seen = set()
    for item in items:
        value = str(item or "").strip()
        if not value or value in seen:
            continue
        normalized.append(value)
        seen.add(value)
    return normalized


def serialize_string_id_list(raw_values):
    return json.dumps(normalize_string_id_list(raw_values), separators=(",", ":"))


def normalize_guild_group_name(value: str, *, clean_profile_text: Callable[..., str]):
    return clean_profile_text(str(value or ""), max_length=80)


def ensure_users_table_columns(conn: sqlite3.Connection) -> None:
    rows = conn.execute("PRAGMA table_info(web_users)").fetchall()
    columns = {str(row["name"]) for row in rows}
    alter_statements = []
    if "role" not in columns:
        alter_statements.append("ALTER TABLE web_users ADD COLUMN role TEXT NOT NULL DEFAULT ''")
    if "first_name" not in columns:
        alter_statements.append("ALTER TABLE web_users ADD COLUMN first_name TEXT NOT NULL DEFAULT ''")
    if "last_name" not in columns:
        alter_statements.append("ALTER TABLE web_users ADD COLUMN last_name TEXT NOT NULL DEFAULT ''")
    if "display_name" not in columns:
        alter_statements.append("ALTER TABLE web_users ADD COLUMN display_name TEXT NOT NULL DEFAULT ''")
    if "password_changed_at" not in columns:
        alter_statements.append("ALTER TABLE web_users ADD COLUMN password_changed_at TEXT NOT NULL DEFAULT ''")
    if "previous_password_hash" not in columns:
        alter_statements.append("ALTER TABLE web_users ADD COLUMN previous_password_hash TEXT NOT NULL DEFAULT ''")
    if "email_changed_at" not in columns:
        alter_statements.append("ALTER TABLE web_users ADD COLUMN email_changed_at TEXT NOT NULL DEFAULT ''")
    if "updated_at" not in columns:
        alter_statements.append("ALTER TABLE web_users ADD COLUMN updated_at TEXT NOT NULL DEFAULT ''")
    if "created_at" not in columns:
        alter_statements.append("ALTER TABLE web_users ADD COLUMN created_at TEXT NOT NULL DEFAULT ''")
    if "guild_group_ids_json" not in columns:
        alter_statements.append("ALTER TABLE web_users ADD COLUMN guild_group_ids_json TEXT NOT NULL DEFAULT '[]'")
    for statement in alter_statements:
        conn.execute(statement)

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS web_guild_groups (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            guild_ids_json TEXT NOT NULL DEFAULT '[]',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )

    now_iso = current_time_iso()
    conn.execute(
        """
        UPDATE web_users
        SET created_at = COALESCE(NULLIF(TRIM(created_at), ''), ?)
        """,
        (now_iso,),
    )
    conn.execute(
        """
        UPDATE web_users
        SET updated_at = COALESCE(NULLIF(TRIM(updated_at), ''), created_at, ?)
        """,
        (now_iso,),
    )
    conn.execute(
        """
        UPDATE web_users
        SET password_changed_at = COALESCE(
            NULLIF(TRIM(password_changed_at), ''),
            NULLIF(TRIM(updated_at), ''),
            NULLIF(TRIM(created_at), ''),
            ?
        )
        """,
        (now_iso,),
    )
    conn.execute(
        """
        UPDATE web_users
        SET email_changed_at = COALESCE(
            NULLIF(TRIM(email_changed_at), ''),
            NULLIF(TRIM(updated_at), ''),
            NULLIF(TRIM(created_at), ''),
            ?
        )
        """,
        (now_iso,),
    )
    conn.execute(
        """
        UPDATE web_users
        SET role = CASE
            WHEN TRIM(COALESCE(role, '')) = '' THEN CASE WHEN is_admin = 1 THEN 'admin' ELSE 'read_only' END
            ELSE LOWER(TRIM(role))
        END
        """
    )
    conn.execute(
        """
        UPDATE web_users
        SET first_name = COALESCE(first_name, ''),
            last_name = COALESCE(last_name, ''),
            display_name = COALESCE(display_name, ''),
            guild_group_ids_json = COALESCE(NULLIF(TRIM(guild_group_ids_json), ''), '[]')
        """
    )
    conn.commit()


def open_users_db(users_db_file: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(users_db_file), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS web_users (
            email TEXT PRIMARY KEY,
            password_hash TEXT NOT NULL,
            is_admin INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    ensure_users_table_columns(conn)
    try:
        os.chmod(users_db_file, 0o600)
    except (PermissionError, OSError):
        pass
    return conn


def read_users(
    users_db_file: Path,
    *,
    normalize_role: Callable[..., str],
    clean_profile_text: Callable[..., str],
    default_display_name: Callable[[str], str],
):
    conn = open_users_db(users_db_file)
    try:
        rows = conn.execute(
            """
            SELECT
                email,
                password_hash,
                previous_password_hash,
                is_admin,
                role,
                first_name,
                last_name,
                display_name,
                guild_group_ids_json,
                password_changed_at,
                email_changed_at,
                created_at,
                updated_at
            FROM web_users
            ORDER BY created_at ASC, email ASC
            """
        ).fetchall()
    finally:
        conn.close()

    now_iso = current_time_iso()
    users = []
    for row in rows:
        email = str(row["email"]).strip().lower()
        password_hash = str(row["password_hash"]).strip()
        if not email or not password_hash:
            continue
        role = normalize_role(str(row["role"] or ""), is_admin=bool(row["is_admin"]))
        users.append(
            {
                "email": email,
                "password_hash": password_hash,
                "previous_password_hash": str(row["previous_password_hash"] or "").strip(),
                "role": role,
                "is_admin": role == "admin",
                "first_name": clean_profile_text(str(row["first_name"] or ""), max_length=80),
                "last_name": clean_profile_text(str(row["last_name"] or ""), max_length=80),
                "display_name": clean_profile_text(
                    str(row["display_name"] or "") or default_display_name(email),
                    max_length=80,
                ),
                "guild_group_ids": normalize_string_id_list(str(row["guild_group_ids_json"] or "[]")),
                "password_changed_at": str(row["password_changed_at"] or row["updated_at"] or row["created_at"] or now_iso),
                "email_changed_at": str(row["email_changed_at"] or row["updated_at"] or row["created_at"] or now_iso),
                "created_at": str(row["created_at"] or now_iso),
                "updated_at": str(row["updated_at"] or row["created_at"] or now_iso),
            }
        )
    return users


def save_users(
    users_db_file: Path,
    users,
    *,
    normalize_email: Callable[[str], str],
    normalize_role: Callable[..., str],
    clean_profile_text: Callable[..., str],
    default_display_name: Callable[[str], str],
) -> None:
    now_iso = current_time_iso()
    conn = open_users_db(users_db_file)
    try:
        with conn:
            conn.execute("DELETE FROM web_users")
            for entry in users:
                email = normalize_email(entry.get("email", ""))
                password_hash = str(entry.get("password_hash", "")).strip()
                previous_password_hash = str(entry.get("previous_password_hash", "")).strip()
                if not email or not password_hash:
                    continue
                role = normalize_role(str(entry.get("role", "")), is_admin=bool(entry.get("is_admin", False)))
                is_admin = 1 if role == "admin" else 0
                first_name = clean_profile_text(str(entry.get("first_name", "")), max_length=80)
                last_name = clean_profile_text(str(entry.get("last_name", "")), max_length=80)
                display_name = clean_profile_text(str(entry.get("display_name", "")), max_length=80) or default_display_name(email)
                guild_group_ids_json = serialize_string_id_list(entry.get("guild_group_ids", []))
                created_at = str(entry.get("created_at") or now_iso)
                password_changed_at = str(entry.get("password_changed_at") or created_at or now_iso)
                email_changed_at = str(entry.get("email_changed_at") or created_at or now_iso)
                conn.execute(
                    """
                    INSERT INTO web_users (
                        email,
                        password_hash,
                        previous_password_hash,
                        is_admin,
                        role,
                        first_name,
                        last_name,
                        display_name,
                        guild_group_ids_json,
                        password_changed_at,
                        email_changed_at,
                        created_at,
                        updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        email,
                        password_hash,
                        previous_password_hash,
                        is_admin,
                        role,
                        first_name,
                        last_name,
                        display_name,
                        guild_group_ids_json,
                        password_changed_at,
                        email_changed_at,
                        created_at,
                        now_iso,
                    ),
                )
    finally:
        conn.close()


def ensure_default_admin(
    users_db_file: Path,
    default_email: str,
    default_password: str,
    logger,
    *,
    read_users_func: Callable[[Path], list[dict]],
    normalize_email: Callable[[str], str],
    is_valid_email: Callable[[str], bool],
    password_policy_errors: Callable[[str], list[str]],
    hash_password: Callable[[str], str],
    default_display_name: Callable[[str], str],
) -> None:
    if read_users_func(users_db_file):
        return

    email = normalize_email(default_email) or "admin@example.com"
    if not is_valid_email(email):
        email = "admin@example.com"

    password = default_password or ""
    if password_policy_errors(password):
        message = (
            "WEB_ADMIN_DEFAULT_PASSWORD is missing or does not meet password policy. "
            "Set a strong password before first boot so the initial admin user can be created securely."
        )
        if logger:
            logger.error(message)
        raise ValueError(message)

    now_iso = current_time_iso()
    conn = open_users_db(users_db_file)
    try:
        with conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO web_users (
                    email,
                    password_hash,
                    previous_password_hash,
                    is_admin,
                    role,
                    first_name,
                    last_name,
                    display_name,
                    guild_group_ids_json,
                    password_changed_at,
                    email_changed_at,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    email,
                    hash_password(password),
                    "",
                    1,
                    "admin",
                    "",
                    "",
                    default_display_name(email),
                    "[]",
                    now_iso,
                    now_iso,
                    now_iso,
                    now_iso,
                ),
            )
    finally:
        conn.close()


def read_guild_groups(users_db_file: Path, *, clean_profile_text: Callable[..., str]):
    conn = open_users_db(users_db_file)
    try:
        rows = conn.execute(
            """
            SELECT
                id,
                name,
                guild_ids_json,
                created_at,
                updated_at
            FROM web_guild_groups
            ORDER BY LOWER(name) ASC, created_at ASC, id ASC
            """
        ).fetchall()
    finally:
        conn.close()

    now_iso = current_time_iso()
    groups = []
    for row in rows:
        group_id = str(row["id"] or "").strip()
        if not group_id:
            continue
        groups.append(
            {
                "id": group_id,
                "name": normalize_guild_group_name(str(row["name"] or ""), clean_profile_text=clean_profile_text) or "Guild Group",
                "guild_ids": normalize_id_string_list(str(row["guild_ids_json"] or "[]")),
                "created_at": str(row["created_at"] or now_iso),
                "updated_at": str(row["updated_at"] or row["created_at"] or now_iso),
            }
        )
    return groups


def save_guild_groups(users_db_file: Path, groups, *, clean_profile_text: Callable[..., str]) -> None:
    now_iso = current_time_iso()
    conn = open_users_db(users_db_file)
    try:
        with conn:
            conn.execute("DELETE FROM web_guild_groups")
            for entry in groups:
                group_id = str(entry.get("id") or "").strip()
                group_name = normalize_guild_group_name(str(entry.get("name") or ""), clean_profile_text=clean_profile_text)
                if not group_id or not group_name:
                    continue
                conn.execute(
                    """
                    INSERT INTO web_guild_groups (
                        id,
                        name,
                        guild_ids_json,
                        created_at,
                        updated_at
                    )
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        group_id,
                        group_name,
                        serialize_id_string_list(entry.get("guild_ids", [])),
                        str(entry.get("created_at") or now_iso),
                        now_iso,
                    ),
                )
    finally:
        conn.close()

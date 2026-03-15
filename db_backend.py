import os
import re
import sqlite3
from pathlib import Path

import pymysql
from pymysql.cursors import DictCursor


DEFAULT_DB_PORT = 3306
SQLITE_BACKEND = "sqlite"
MYSQL_BACKEND = "mysql"
VALID_DB_BACKENDS = {SQLITE_BACKEND, MYSQL_BACKEND}
DB_INTEGRITY_ERROR = (sqlite3.IntegrityError, pymysql.err.IntegrityError)


def _normalize_backend_name(raw_value):
    backend = str(raw_value or SQLITE_BACKEND).strip().lower()
    if backend in {"mariadb", "mysql"}:
        return MYSQL_BACKEND
    if backend not in VALID_DB_BACKENDS:
        return SQLITE_BACKEND
    return backend


def get_database_backend():
    return _normalize_backend_name(os.getenv("DB_BACKEND", SQLITE_BACKEND))


def is_mysql_backend():
    return get_database_backend() == MYSQL_BACKEND


def get_sqlite_db_path():
    data_dir = os.getenv("DATA_DIR", "data")
    raw_path = str(os.getenv("DB_SQLITE_PATH", "")).strip()
    if raw_path:
        return raw_path
    return os.path.join(data_dir, "bot_data.db")


def get_database_settings():
    return {
        "backend": get_database_backend(),
        "sqlite_path": get_sqlite_db_path(),
        "host": str(os.getenv("DB_HOST", "mysql")).strip() or "mysql",
        "port": _parse_int_env("DB_PORT", DEFAULT_DB_PORT, minimum=1),
        "name": str(os.getenv("DB_NAME", "discord_bot")).strip() or "discord_bot",
        "user": str(os.getenv("DB_USER", "discord_bot")).strip() or "discord_bot",
        "password": str(os.getenv("DB_PASSWORD", "")).strip(),
        "charset": str(os.getenv("DB_CHARSET", "utf8mb4")).strip() or "utf8mb4",
        "connect_timeout": _parse_int_env(
            "DB_CONNECT_TIMEOUT_SECONDS", 10, minimum=1
        ),
    }


def _parse_int_env(name: str, default_value: int, minimum: int | None = None):
    try:
        parsed = int(str(os.getenv(name, str(default_value))).strip())
    except (TypeError, ValueError):
        return default_value
    if minimum is not None and parsed < minimum:
        return default_value
    return parsed


def _translate_mysql_sql(statement: str):
    translated = str(statement or "").strip()
    if not translated:
        return translated
    translated = translated.replace("?", "%s")
    translated = re.sub(
        r"\bINSERT\s+OR\s+IGNORE\s+INTO\b", "INSERT IGNORE INTO", translated, flags=re.I
    )
    translated = re.sub(
        r"\bINSERT\s+OR\s+REPLACE\s+INTO\b", "REPLACE INTO", translated, flags=re.I
    )
    translated = translated.replace("COLLATE NOCASE", "")
    translated = translated.replace(
        "INTEGER PRIMARY KEY AUTOINCREMENT",
        "BIGINT PRIMARY KEY AUTO_INCREMENT",
    )
    translated = re.sub(
        r"ALTER TABLE\s+([A-Za-z0-9_]+)\s+RENAME TO\s+([A-Za-z0-9_]+)",
        r"RENAME TABLE \1 TO \2",
        translated,
        flags=re.I,
    )
    translated = re.sub(
        r"ON\s+CONFLICT\s*\(\s*`?key`?\s*\)\s+DO\s+UPDATE\s+SET\s+value\s*=\s*excluded\.value\s*,\s*updated_at\s*=\s*excluded\.updated_at",
        "ON DUPLICATE KEY UPDATE value=VALUES(value), updated_at=VALUES(updated_at)",
        translated,
        flags=re.I,
    )
    return translated


class DatabaseCursor:
    def __init__(self, backend: str, cursor):
        self.backend = backend
        self.cursor = cursor

    @property
    def rowcount(self):
        return self.cursor.rowcount

    @property
    def lastrowid(self):
        return getattr(self.cursor, "lastrowid", None)

    def fetchone(self):
        row = self.cursor.fetchone()
        if row is None:
            return None
        return row

    def fetchall(self):
        return self.cursor.fetchall()


class DatabaseConnection:
    def __init__(self, backend: str, raw_connection, connect_fn):
        self.backend = backend
        self.raw_connection = raw_connection
        self._connect_fn = connect_fn

    def ensure_ready(self):
        if self.backend != MYSQL_BACKEND:
            return
        try:
            self.raw_connection.ping(reconnect=True)
        except Exception:
            self.raw_connection = self._connect_fn()

    def execute(self, statement: str, params=()):
        self.ensure_ready()
        if self.backend == MYSQL_BACKEND:
            translated = _translate_mysql_sql(statement)
            if not translated or translated.upper().startswith("PRAGMA "):
                return DatabaseCursor(self.backend, _EmptyCursor())
            cursor = self.raw_connection.cursor()
            cursor.execute(translated, params or ())
            return DatabaseCursor(self.backend, cursor)
        return DatabaseCursor(self.backend, self.raw_connection.execute(statement, params or ()))

    def executescript(self, script: str):
        self.ensure_ready()
        if self.backend == MYSQL_BACKEND:
            for statement in _split_sql_script(script):
                self.execute(statement)
            return
        self.raw_connection.executescript(script)

    def commit(self):
        self.raw_connection.commit()

    def rollback(self):
        self.raw_connection.rollback()

    def close(self):
        self.raw_connection.close()

    def __enter__(self):
        self.ensure_ready()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.commit()
        else:
            self.rollback()
        return False


class _EmptyCursor:
    rowcount = 0
    lastrowid = None

    def fetchone(self):
        return None

    def fetchall(self):
        return []


def _split_sql_script(script: str):
    parts = []
    current = []
    for line in str(script or "").splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        current.append(line)
        if stripped.endswith(";"):
            statement = "\n".join(current).strip().rstrip(";").strip()
            if statement:
                parts.append(statement)
            current = []
    trailing = "\n".join(current).strip().rstrip(";").strip()
    if trailing:
        parts.append(trailing)
    return parts


def open_database_connection():
    settings = get_database_settings()
    if settings["backend"] == MYSQL_BACKEND:
        return DatabaseConnection(MYSQL_BACKEND, _open_mysql_connection(settings), lambda: _open_mysql_connection(settings))
    sqlite_path = Path(settings["sqlite_path"])
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    raw_connection = _open_sqlite_connection(sqlite_path)
    return DatabaseConnection(
        SQLITE_BACKEND,
        raw_connection,
        lambda: _open_sqlite_connection(sqlite_path),
    )


def _open_mysql_connection(settings: dict):
    return pymysql.connect(
        host=settings["host"],
        port=int(settings["port"]),
        user=settings["user"],
        password=settings["password"],
        database=settings["name"],
        charset=settings["charset"],
        connect_timeout=int(settings["connect_timeout"]),
        autocommit=False,
        cursorclass=DictCursor,
    )


def _open_sqlite_connection(sqlite_path: Path):
    raw_connection = sqlite3.connect(str(sqlite_path), check_same_thread=False, timeout=30)
    raw_connection.row_factory = sqlite3.Row
    return raw_connection


def get_table_columns(conn: DatabaseConnection, table_name: str):
    safe_table_name = str(table_name or "").strip()
    if not safe_table_name:
        return set()
    if conn.backend == MYSQL_BACKEND:
        rows = conn.execute(
            """
            SELECT COLUMN_NAME
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = %s
            """,
            (safe_table_name,),
        ).fetchall()
        return {str(row["COLUMN_NAME"]) for row in rows}
    rows = conn.execute(f"PRAGMA table_info({safe_table_name})").fetchall()
    return {str(row["name"]) for row in rows}


def table_exists(conn: DatabaseConnection, table_name: str):
    return bool(get_table_columns(conn, table_name))


def index_exists(conn: DatabaseConnection, table_name: str, index_name: str):
    safe_table_name = str(table_name or "").strip()
    safe_index_name = str(index_name or "").strip()
    if not safe_table_name or not safe_index_name:
        return False
    if conn.backend == MYSQL_BACKEND:
        row = conn.execute(
            """
            SELECT 1
            FROM information_schema.statistics
            WHERE table_schema = DATABASE()
              AND table_name = %s
              AND index_name = %s
            LIMIT 1
            """,
            (safe_table_name, safe_index_name),
        ).fetchone()
        return row is not None
    row = conn.execute(
        f"PRAGMA index_list({safe_table_name})"
    ).fetchall()
    return any(str(item["name"]) == safe_index_name for item in row)

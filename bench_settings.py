"""Measure the guild-settings cache-hit path.

Run: PYTHONPATH=. python3 bench_settings.py
"""
from __future__ import annotations

import sqlite3
import statistics
import sys
import tempfile
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import bot


def setup():
    tmp = Path(tempfile.mkdtemp()) / "bench.db"
    conn = sqlite3.connect(str(tmp), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    # Match get_db_connection() exactly: without synchronous=NORMAL every commit
    # pays a full fsync and the per-message numbers are ~8x too pessimistic.
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA foreign_keys=ON")
    bot.db_connection = conn
    bot.db_lock = threading.RLock()
    bot.ensure_db_schema()
    bot.save_guild_settings(123, {"hi_channel_id": 999}, actor_email="bench@example.com")
    return conn


def timeit(label, fn, n=2000, runs=5):
    samples = []
    for _ in range(runs):
        started = time.perf_counter()
        for _ in range(n):
            fn()
        samples.append((time.perf_counter() - started) / n * 1e6)
    med = statistics.median(samples)
    print(f"  {label:44}: {med:8.2f} us/call")
    return med




def bench_member_activity(conn):
    """The per-message write path, for comparison against the version read."""
    from unittest.mock import MagicMock

    mgr = bot._get_member_activity_manager()
    import discord

    def make_message(i):
        guild = MagicMock()
        guild.id = 123
        author = MagicMock()
        author.bot = False
        author.id = 4242
        author.__str__ = lambda self: "benchuser"
        author.display_name = "Bench User"
        msg = MagicMock()
        msg.guild = guild
        msg.author = author
        msg.id = 900000 + i
        msg.created_at = discord.utils.utcnow()
        return msg

    # warm
    for i in range(20):
        mgr.record_member_message_activity(make_message(i))

    n = 200
    started = time.perf_counter()
    for i in range(1000, 1000 + n):
        mgr.record_member_message_activity(make_message(i))
    per = (time.perf_counter() - started) / n * 1e6
    print(f"  {'record_member_message_activity (per message)':44}: {per:8.0f} us/call")
    return per


def main():
    conn = setup()
    print("Guild settings hot path")
    print(f"  cached guilds: {len(bot.guild_state_manager.guild_settings_cache)}")

    timeit("db_kv_get (version key read)", lambda: bot.guild_state_manager.db_kv_get("guild_settings_updated_at:123"))
    timeit("load_guild_settings (cache hit)", lambda: bot.load_guild_settings(123))
    timeit("get_effective_guild_setting (cache hit)", lambda: bot.get_effective_guild_setting(123, "hi_channel_id", 0))

    # Contention: a writer holding the lock is what a cache hit queues behind.
    def contended():
        def hold():
            with bot.db_lock:
                time.sleep(0.002)
        t = threading.Thread(target=hold)
        t.start()
        time.sleep(0.0005)
        started = time.perf_counter()
        bot.get_effective_guild_setting(123, "hi_channel_id", 0)
        elapsed = (time.perf_counter() - started) * 1e6
        t.join()
        return elapsed

    bench_member_activity(conn)

    vals = [contended() for _ in range(20)]
    print(f"  {'get_effective_guild_setting under 2ms lock':44}: {statistics.median(vals):8.0f} us/call (blocked)")
    conn.close()


if __name__ == "__main__":
    main()

"""Benchmark the forum-tag cross-post hot path.

Run: PYTHONPATH=. python3 bench_forum_tags.py
"""
from __future__ import annotations

import asyncio
import sqlite3
import sys
import tempfile
import threading
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent))

import bot

N_TOPICS = 30
HTTP_LATENCY = 0.25  # a realistic Discourse round-trip


def make_db():
    tmp = Path(tempfile.mkdtemp()) / "bench.db"
    conn = sqlite3.connect(str(tmp))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")


    bot.db_connection = conn
    bot.db_lock = threading.RLock()
    bot.ensure_db_schema()
    return conn


def config():
    return {
        "base_url": "https://forum.gl-inet.com",
        "timeout_seconds": 20,
        "api_key": "bench-key",
        "api_username": "",
        "enabled": True,
    }


def make_subscription():
    bot.create_forum_tag_subscription(123, "kb", 999, "bench@example.com", feed_type="tag")
    sub = bot.list_forum_tag_subscriptions(guild_id=123)[0]
    bot.mark_forum_tag_baseline_initialized(sub["id"])
    return sub


def bench_poller():
    conn = make_db()
    sub = make_subscription()
    topics = [
        {"id": i, "title": f"T{i}", "url": f"https://forum.gl-inet.com/t/x/{i}",
         "author": "a", "excerpt": ""}
        for i in range(1000, 1000 + N_TOPICS)
    ]
    channel = MagicMock()
    channel.send = AsyncMock()

    def slow_topic(**kw):
        time.sleep(HTTP_LATENCY)
        return {
            "id": kw["topic_id"], "title": "T", "url": "https://forum.gl-inet.com/t/x/1",
            "posts": [{"id": 1, "username": "a", "text": "Body.", "cooked": "<p>Body.</p>"}],
        }

    with (
        patch.object(bot, "resolve_discourse_forum_settings", return_value=config()),
        patch.object(bot, "fetch_discourse_tag_topics", return_value=topics),
        patch.object(bot, "fetch_discourse_topic", side_effect=slow_topic),
        patch.object(bot, "resolve_forum_tag_channel", new=AsyncMock(return_value=(channel, ""))),
        patch.object(bot, "is_managed_guild_id", return_value=True),
    ):
        started = time.perf_counter()
        asyncio.run(bot.process_forum_tag_subscription(sub))
        elapsed = time.perf_counter() - started

    print(f"  poller, {N_TOPICS} topics @ {HTTP_LATENCY}s latency : {elapsed:.2f}s")
    conn.close()
    return elapsed


def bench_merge(n=N_TOPICS):
    conn = make_db()
    sub = make_subscription()
    ids = list(range(5000, 5000 + n))
    started = time.perf_counter()
    bot.merge_forum_tag_seen_topic_ids(sub["id"], ids)
    elapsed = time.perf_counter() - started
    print(f"  merge {n} seen ids                       : {elapsed * 1000:.2f} ms")
    conn.close()
    return elapsed


def bench_load(n=500):
    conn = make_db()
    sub = make_subscription()
    bot.merge_forum_tag_seen_topic_ids(sub["id"], list(range(9000, 9000 + n)))
    started = time.perf_counter()
    for _ in range(100):
        bot.load_forum_tag_seen_topic_ids(sub["id"])
    elapsed = (time.perf_counter() - started) / 100
    print(f"  load {n} seen ids                       : {elapsed * 1000:.2f} ms")
    conn.close()
    return elapsed


if __name__ == "__main__":
    print("Forum tag hot-path benchmark")
    bench_load()
    bench_merge()
    bench_poller()

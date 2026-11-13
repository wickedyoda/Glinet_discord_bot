"""90-day Discourse staff activity — prototype backfill.

Walks /posts.json backwards with ?before=<oldest post id> (50 per page, no overlap),
counts posts per GL.iNet_Staff member, and derives "threads handled" as the first
staff post after the topic's opening post.

Read-only. Writes results to /tmp/discourse_90d.json.
"""
from __future__ import annotations

import json
import os
import time
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone

BASE = "https://forum.gl-inet.com"
DAYS = 90
GROUP = "GL.iNet_Staff"
MAX_PAGES = 500
PACE_SECONDS = 0.25

HEADERS = {"User-Agent": "GLiNetDiscordBot/1.0"}
_key = os.environ.get("FORUM_API_KEY", "")
if _key:
    HEADERS["Api-Key"] = _key
    HEADERS["Api-Username"] = "system"


def get(path: str):
    return json.load(
        urllib.request.urlopen(
            urllib.request.Request(BASE + path, headers=HEADERS), timeout=40
        )
    )


def parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def main() -> int:
    cutoff = datetime.now(timezone.utc) - timedelta(days=DAYS)

    staff = {m["username"].lower() for m in get(f"/groups/{GROUP}/members.json?limit=100")["members"]}
    print(f"staff set: {len(staff)} members from {GROUP}")

    posts = []
    before = None
    pages = 0
    oldest_seen = None

    while pages < MAX_PAGES:
        path = "/posts.json" + (f"?before={before}" if before else "")
        batch = get(path).get("latest_posts") or []
        if not batch:
            break
        pages += 1
        oldest_seen = parse_ts(batch[-1]["created_at"])
        for p in batch:
            if parse_ts(p["created_at"]) >= cutoff:
                posts.append(p)
            else:
                batch_done = True
        if oldest_seen < cutoff:
            break
        before = batch[-1]["id"]
        time.sleep(PACE_SECONDS)

    print(f"pages fetched: {pages} | posts within {DAYS}d: {len(posts)}")
    if posts:
        print(f"window: {min(parse_ts(p['created_at']) for p in posts).date()} .. "
              f"{max(parse_ts(p['created_at']) for p in posts).date()}")

    # --- activity: posts per staff member -----------------------------------
    per_user = defaultdict(int)
    topics_per_user = defaultdict(set)
    for p in posts:
        u = str(p.get("username", "")).lower()
        if u in staff:
            per_user[u] += 1
            topics_per_user[u].add(p["topic_id"])

    # --- threads handled: earliest staff post (post_number > 1) per topic ----
    first_staff = {}
    for p in posts:
        u = str(p.get("username", "")).lower()
        if u not in staff:
            continue
        if int(p.get("post_number") or 0) <= 1:
            continue  # the opening post belongs to the requester
        t = p["topic_id"]
        ts = parse_ts(p["created_at"])
        if t not in first_staff or ts < first_staff[t][1]:
            first_staff[t] = (u, ts)
    handled = defaultdict(int)
    for u, _ts in first_staff.values():
        handled[u] += 1

    print()
    print(f"staff posts: {sum(per_user.values())} | staff who posted: {len(per_user)}")
    print(f"topics with a staff reply: {len(first_staff)}")
    print()
    print(f"{'staff member':<24} {'posts':>6} {'topics':>7} {'handled':>8}")
    print("-" * 49)
    for u, n in sorted(per_user.items(), key=lambda kv: -kv[1])[:20]:
        print(f"{u:<24} {n:>6} {len(topics_per_user[u]):>7} {handled.get(u, 0):>8}")

    with open("/tmp/discourse_90d.json", "w") as fh:
        json.dump(
            {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "days": DAYS,
                "staff_count": len(staff),
                "posts_scanned": len(posts),
                "per_user_posts": dict(per_user),
                "per_user_topics": {k: len(v) for k, v in topics_per_user.items()},
                "threads_handled": dict(handled),
            },
            fh,
            indent=2,
        )
    print("\nwritten: /tmp/discourse_90d.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

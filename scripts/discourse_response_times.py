"""90-day Discourse staff activity + first-response times — prototype.

Walks /posts.json backwards with ?before=<oldest post id>. For each topic it pairs
the opening post (post_number == 1) with the first staff reply (post_number > 1 by a
GL.iNet_Staff member) and measures the delta.

Read-only. Writes /tmp/discourse_90d_detail.json.
"""
from __future__ import annotations

import json
import os
import statistics
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


def ts(value: str) -> datetime:
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def fmt(seconds: float) -> str:
    if seconds < 3600:
        return f"{seconds / 60:.0f}m"
    if seconds < 86400:
        return f"{seconds / 3600:.1f}h"
    return f"{seconds / 86400:.1f}d"


def main() -> int:
    cutoff = datetime.now(timezone.utc) - timedelta(days=DAYS)
    staff = {m["username"].lower() for m in get(f"/groups/{GROUP}/members.json?limit=100")["members"]}

    posts, before, pages = [], None, 0
    while pages < MAX_PAGES:
        batch = get("/posts.json" + (f"?before={before}" if before else "")).get("latest_posts") or []
        if not batch:
            break
        pages += 1
        posts.extend(p for p in batch if ts(p["created_at"]) >= cutoff)
        if ts(batch[-1]["created_at"]) < cutoff:
            break
        before = batch[-1]["id"]
        time.sleep(PACE_SECONDS)

    print(f"staff: {len(staff)} | pages: {pages} | posts in {DAYS}d: {len(posts)}")

    # per-topic: opening post time, first staff reply (author, time)
    opened: dict[int, datetime] = {}
    first_staff: dict[int, tuple[str, datetime]] = {}
    per_user_posts = defaultdict(int)

    for p in posts:
        u = str(p.get("username", "")).lower()
        t = p["topic_id"]
        when = ts(p["created_at"])
        num = int(p.get("post_number") or 0)
        if u in staff:
            per_user_posts[u] += 1
        if num == 1:
            opened.setdefault(t, when)
        elif u in staff:
            if t not in first_staff or when < first_staff[t][1]:
                first_staff[t] = (u, when)

    rows, no_op = [], 0
    for t, (u, when) in first_staff.items():
        start = opened.get(t)
        if start is None:
            no_op += 1  # topic opened before the window; its OP is not in this scan
            continue
        delta = (when - start).total_seconds()
        if delta >= 0:
            rows.append((u, t, delta))

    print(f"topics opened in window: {len(opened)} | with staff reply: {len(rows)}"
          f" | staff replies on older topics (OP out of window): {no_op}")

    if rows:
        all_s = sorted(r[2] for r in rows)
        print()
        print("OVERALL first staff response:")
        print(f"  median {fmt(statistics.median(all_s))} | "
              f"p90 {fmt(all_s[int(len(all_s) * 0.9)])} | "
              f"mean {fmt(statistics.mean(all_s))}")

        by_user = defaultdict(list)
        for u, _t, d in rows:
            by_user[u].append(d)

        print()
        print(f"{'staff member':<24} {'handled':>7} {'median':>8} {'p90':>8}")
        print("-" * 51)
        for u, ds in sorted(by_user.items(), key=lambda kv: -len(kv[1]))[:20]:
            ds = sorted(ds)
            print(f"{u:<24} {len(ds):>7} {fmt(statistics.median(ds)):>8} "
                  f"{fmt(ds[int(len(ds) * 0.9)]):>8}")

    with open("/tmp/discourse_90d_detail.json", "w") as fh:
        json.dump(
            {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "days": DAYS,
                "posts_scanned": len(posts),
                "per_user_posts": dict(per_user_posts),
                "topics_handled": len(rows),
                "response_seconds": {u: ds for u, ds in
                                     ((u, sorted(d for uu, _t, d in rows if uu == u))
                                      for u in set(u for u, _t, _d in rows))},
            },
            fh,
            indent=2,
        )
    print("\nwritten: /tmp/discourse_90d_detail.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

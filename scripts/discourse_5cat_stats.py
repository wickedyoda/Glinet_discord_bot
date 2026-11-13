"""GL.iNet_Staff activity in five support categories, last 90 days.

Two passes:
  1. category feeds (/c/{slug}/l/latest.json) build topic_id -> category for the
     window, because /posts.json does not carry category_id
  2. /posts.json walks backwards once, filtered to those topics

Reports staff posts per day / hour-of-day / ISO week, and first-response times
(OP -> first staff reply) for the same window. Read-only.
"""
from __future__ import annotations

import json
import os
import statistics
import time
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

BASE = "https://forum.gl-inet.com"
DAYS = 90
GROUP = "GL.iNet_Staff"
CATEGORIES = ("glrouter", "glkvm", "goodcloud", "iot", "feedback")
# Discourse throttles around 60 requests/minute per IP. 1.05s keeps us under that;
# the naive 0.25s pace earns a 429 partway through the walk.
PACE = 1.05
RETRY_BACKOFF = (10, 30, 60, 120)
MAX_PAGES = 800

H = {"User-Agent": "GLiNetDiscordBot/1.0"}
_key = os.environ.get("FORUM_API_KEY", "")
if _key:
    H["Api-Key"] = _key
    H["Api-Username"] = "system"


def get(path):
    """GET with 429 backoff -- a long walk will otherwise die partway through."""
    last = None
    for attempt, wait in enumerate((0,) + RETRY_BACKOFF):
        if wait:
            time.sleep(wait)
        try:
            return json.load(
                urllib.request.urlopen(
                    urllib.request.Request(BASE + path, headers=H), timeout=40
                )
            )
        except urllib.error.HTTPError as exc:
            if exc.code != 429:
                raise
            last = exc
            continue
    raise last


def ts(v):
    return datetime.fromisoformat(str(v).replace("Z", "+00:00"))


def fmt(sec):
    if sec < 3600:
        return f"{sec/60:.0f}m"
    if sec < 86400:
        return f"{sec/3600:.1f}h"
    return f"{sec/86400:.1f}d"


def main():
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=DAYS)

    staff = {m["username"].lower() for m in get(f"/groups/{GROUP}/members.json?limit=100")["members"]}
    print(f"staff: {len(staff)}")

    # --- pass 1: topic -> category -----------------------------------------
    topic_cat = {}
    for slug in CATEGORIES:
        page = 0
        while page < MAX_PAGES:
            d = get(f"/c/{slug}/l/latest.json?page={page}")
            topics = d.get("topic_list", {}).get("topics") or []
            if not topics:
                break
            fresh = 0
            for t in topics:
                created = ts(t["created_at"])
                if created >= cutoff:
                    topic_cat[int(t["id"])] = slug
                    fresh += 1
            if fresh == 0 and ts(topics[-1]["created_at"]) < cutoff:
                break
            page += 1
            time.sleep(PACE)
        print(f"  {slug:<12} topics in window: {sum(1 for v in topic_cat.values() if v == slug)}")

    # --- pass 2: posts in window, filtered ---------------------------------
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
        time.sleep(PACE)
    print(f"posts in {DAYS}d: {len(posts)} (from {pages} pages)")

    staff_posts = [
        p for p in posts
        if str(p.get("username", "")).lower() in staff and int(p["topic_id"]) in topic_cat
    ]
    print(f"staff posts in the five categories: {len(staff_posts)}")

    # --- counts: daily / hourly / weekly -----------------------------------
    daily = Counter()
    hourly = Counter()
    weekly = Counter()
    for p in staff_posts:
        d = ts(p["created_at"])
        daily[d.date().isoformat()] += 1
        hourly[d.hour] += 1
        iso = d.isocalendar()
        weekly[f"{iso[0]}-W{iso[1]:02d}"] += 1

    print()
    print("=== DAILY (last 14 days) ===")
    for day in sorted(daily)[-14:]:
        print(f"  {day}  {'█' * min(daily[day], 60)} {daily[day]}")
    print(f"  busiest day: {max(daily.items(), key=lambda kv: kv[1])}")
    print(f"  mean/day: {statistics.mean(daily.values()):.1f} over {len(daily)} active days")

    print()
    print("=== HOURLY (UTC hour-of-day) ===")
    for h in range(24):
        n = hourly.get(h, 0)
        print(f"  {h:02d}:00  {'█' * min(n // 2, 50)} {n}")

    print()
    print("=== WEEKLY ===")
    for w in sorted(weekly):
        print(f"  {w}  {'█' * min(weekly[w] // 2, 50)} {weekly[w]}")

    # --- response times -----------------------------------------------------
    opened, first_staff = {}, {}
    for p in posts:
        t = int(p["topic_id"])
        if t not in topic_cat:
            continue
        when = ts(p["created_at"])
        u = str(p.get("username", "")).lower()
        num = int(p.get("post_number") or 0)
        if num == 1:
            opened.setdefault(t, when)
        elif u in staff:
            if t not in first_staff or when < first_staff[t][1]:
                first_staff[t] = (u, when)

    rows = []
    for t, (u, when) in first_staff.items():
        start = opened.get(t)
        if start is None:
            continue
        delta = (when - start).total_seconds()
        if delta >= 0:
            rows.append((u, delta))
    secs = sorted(d for _u, d in rows)

    print()
    print("=== FIRST RESPONSE (five categories) ===")
    if secs:
        print(f"  topics answered: {len(secs)}")
        print(f"  median {fmt(statistics.median(secs))} | "
              f"p90 {fmt(secs[int(len(secs)*0.9)])} | mean {fmt(statistics.mean(secs))}")
        under = sum(1 for s in secs if s <= 86400)
        print(f"  answered within 24h: {under}/{len(secs)} ({under/len(secs)*100:.0f}%)")
        print()
        print(f"  {'staff':<20} {'handled':>7} {'median':>8} {'p90':>8}")
        by = defaultdict(list)
        for u, d in rows:
            by[u].append(d)
        for u, ds in sorted(by.items(), key=lambda kv: -len(kv[1]))[:15]:
            ds = sorted(ds)
            print(f"  {u:<20} {len(ds):>7} {fmt(statistics.median(ds)):>8} {fmt(ds[int(len(ds)*0.9)]):>8}")

    with open("/tmp/discourse_5cat_90d.json", "w") as fh:
        json.dump({
            "generated_at": now.isoformat(), "days": DAYS, "categories": list(CATEGORIES),
            "staff_posts": len(staff_posts), "topics_answered": len(secs),
            "daily": dict(daily), "hourly": {str(k): v for k, v in hourly.items()},
            "weekly": dict(weekly),
            "response_seconds": {u: sorted(ds) for u, ds in
                                 ((u, [d for uu, d in rows if uu == u]) for u in set(u for u, _ in rows))},
        }, fh, indent=2)
    print("\nwritten: /tmp/discourse_5cat_90d.json")


if __name__ == "__main__":
    main()

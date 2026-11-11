"""Prove every deferred import resolves at runtime.

Run: PYTHONPATH=. python3 smoke_deferred_imports.py

The deferral in this change moves bs4 / openpyxl (and therefore numpy and PIL)
off the startup path. A lazy import that never resolves is invisible until the
feature is used, so each site is exercised here in a process that starts clean.
"""
from __future__ import annotations

import io
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import bot


def check(label, fn):
    try:
        result = fn()
        detail = ""
        if isinstance(result, (list, tuple)):
            detail = f" ({len(result)} items)"
        print(f"  PASS  {label}{detail}")
        return True
    except Exception as exc:  # noqa: BLE001 - smoke harness reports everything
        print(f"  FAIL  {label}: {type(exc).__name__}: {exc}")
        return False


def make_xlsx() -> bytes:
    from openpyxl import Workbook

    wb = Workbook()
    ws = wb.active
    ws.append(["Name", "Value"])
    ws.append(["Alpha", 1])
    ws.append(["Beta", 2])
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def main():
    print("Startup state (all four must be absent before the calls below):")
    for mod in ("bs4", "openpyxl", "numpy", "PIL"):
        print(f"  {mod:10} loaded at import: {mod in sys.modules}")

    ok = True

    # 1. openpyxl path
    from app.csv_utils import parse_spreadsheet_cells

    ok &= check(
        "csv_utils.parse_spreadsheet_cells",
        lambda: parse_spreadsheet_cells(make_xlsx(), "roster.xlsx"),
    )

    # 2. bot.parse_firmware_entries (bs4, lxml)
    firmware_html = '<html><body><div class="sync-line">synced</div></body></html>'
    ok &= check("bot.parse_firmware_entries", lambda: bot.parse_firmware_entries(firmware_html))

    # 3. discourse_announcements.parse_announcement_topics (bs4)
    from app.discourse_announcements import parse_announcement_topics

    ann_html = '<html><body><a class="title raw-link" href="/t/example/123">Example</a></body></html>'
    ok &= check(
        "parse_announcement_topics",
        lambda: parse_announcement_topics(ann_html, "https://forum.gl-inet.com/c/announcement"),
    )

    # 4. beta_programs -- parse_beta_testing_programs() owns the deferred import.
    # Uses a snippet trimmed from the live page: hand-written markup does not reach
    # the section and the function raises, which would look like a broken import.
    from app.beta_programs import parse_beta_testing_programs

    bp_html = (Path(__file__).parent / "tests" / "fixtures" / "beta_programs_snippet.html").read_text(
        encoding="utf-8"
    )
    ok &= check(
        "beta_programs.parse_beta_testing_programs",
        lambda: parse_beta_testing_programs(bp_html, "https://www.gl-inet.com/beta-testing/"),
    )

    # 5. bot.fetch_reddit_search_posts_via_html -- own deferred import, and it is
    # reached through a fallback that only catches requests errors, so a broken
    # import here takes out the whole path.
    from unittest.mock import MagicMock

    reddit_html = (
        '<html><body><a href="https://www.reddit.com/r/x/comments/abc/t/"></a></body></html>'
    )
    fake_response = MagicMock()
    fake_response.text = reddit_html
    fake_response.raise_for_status.return_value = None
    original_get = bot.get
    bot.get = lambda *a, **k: fake_response
    try:
        ok &= check("bot.fetch_reddit_search_posts_via_html", lambda: bot.fetch_reddit_search_posts_via_html("test"))
    finally:
        bot.get = original_get

    print("\nAfter the calls:")
    for mod in ("bs4", "openpyxl", "numpy", "PIL"):
        print(f"  {mod:10} loaded now: {mod in sys.modules}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

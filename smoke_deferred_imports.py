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

    # 4. beta_programs (bs4, html.parser)
    import app.beta_programs as bp

    bp_html = '<html><body><select name="x"><option value="1">One</option></select></body></html>'
    ok &= check(
        "beta_programs parse",
        lambda: bp.parse_beta_programs_page(bp_html, "https://example.com")
        if hasattr(bp, "parse_beta_programs_page")
        else bp._extract_beta_programs_from_select_inputs(
            __import__("bs4").BeautifulSoup(bp_html, "html.parser"), "https://example.com"
        ),
    )

    print("\nAfter the calls:")
    for mod in ("bs4", "openpyxl", "numpy", "PIL"):
        print(f"  {mod:10} loaded now: {mod in sys.modules}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

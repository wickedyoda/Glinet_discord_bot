"""The firmware feed parser must not depend on lxml.

`parse_firmware_entries` requested `BeautifulSoup(html, "lxml")`, but lxml is not
in requirements.txt and is absent from the production image, so every real run
raised FeatureNotFound. `check_firmware_updates_once` catches bare `Exception`, so
the firmware monitor failed silently on every poll and the feature was simply dead.

These tests pin the parser to a tree builder that is actually installed, using a
snippet cut from the live feed rather than invented markup.
"""
from __future__ import annotations

from pathlib import Path

import bot

FIXTURE = Path(__file__).parent / "fixtures" / "firmware_feed_snippet.html"


def _html() -> str:
    return FIXTURE.read_text(encoding="utf-8")


def test_firmware_feed_parses_without_lxml():
    """Would raise FeatureNotFound while the parser was requested as 'lxml'."""
    entries, sync_label = bot.parse_firmware_entries(_html())

    assert entries, "parser returned no firmware entries"
    assert sync_label, "sync line was not picked up"


def test_firmware_entry_fields_are_extracted():
    entries, _ = bot.parse_firmware_entries(_html())
    entry = entries[0]

    assert entry["model_code"] == "a1300"
    assert entry["model_name"].startswith("GL-A1300")
    assert entry["stage"] in {"release", "testing"}
    assert entry["files"], "entry carried no download links"
    assert entry["id"], "entry has no stable id"


def test_no_module_requests_the_lxml_parser():
    """lxml is not a declared dependency, so nothing may ask for it by name."""
    offenders = []
    for path in [Path(bot.__file__)] + sorted((Path(bot.__file__).parent / "app").glob("*.py")):
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            if '"lxml"' in stripped or "'lxml'" in stripped:
                offenders.append(f"{path.name}:{lineno}")
    assert offenders == [], "lxml parser requested but not installed: " + ", ".join(offenders)

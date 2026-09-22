"""
Tests for the Reddit feed fetch fix.

Verifies that:
1. fetch_reddit_subreddit_new_posts tries the Atom/RSS feed FIRST (since Reddit
   now blocks unauthenticated JSON API access).
2. When Atom succeeds, the JSON endpoint is never called.
3. When Atom fails, the function falls back to the JSON endpoint.
4. fetch_reddit_json uses the app-specific REDDIT_REQUEST_USER_AGENT
   (not a fake browser User-Agent).
"""
from __future__ import annotations

import json
import logging
from unittest.mock import patch

import pytest

from bot import (
    fetch_reddit_subreddit_new_posts,
    fetch_reddit_json,
    REDDIT_REQUEST_USER_AGENT,
    REDDIT_BASE_URL,
    REDDIT_FALLBACK_BASE_URL,
)

# ---------------------------------------------------------------------------
# Atom feed XML sample (mirrors the structure Reddit actually returns)
# ---------------------------------------------------------------------------
SAMPLE_ATOM_FEED = """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:media="http://search.yahoo.com/mrss/">
  <title>newest submissions : GlInet</title>
  <id>/r/glinet/new/.rss</id>
  <updated>2026-09-22T05:09:18+00:00</updated>
  <entry>
    <author><name>/u/testuser1</name><uri>https://www.reddit.com/user/testuser1</uri></author>
    <id>t3_test123</id>
    <link href="https://www.reddit.com/r/GlInet/comments/test123/test_post/" />
    <title>Test Post 1</title>
    <published>2026-09-21T17:08:00+00:00</published>
    <updated>2026-09-21T17:08:00+00:00</updated>
  </entry>
  <entry>
    <author><name>/u/testuser2</name><uri>https://www.reddit.com/user/testuser2</uri></author>
    <id>t3_test456</id>
    <link href="https://www.reddit.com/r/GlInet/comments/test456/another_test/" />
    <title>Test Post 2</title>
    <published>2026-09-21T16:02:51+00:00</published>
    <updated>2026-09-21T16:02:51+00:00</updated>
  </entry>
</feed>
"""

# Sample JSON response (mirrors Reddit's JSON structure)
SAMPLE_JSON_RESPONSE = {
    "data": {
        "children": [
            {
                "data": {
                    "id": "abc123",
                    "title": "JSON Post 1",
                    "permalink": "/r/GlInet/comments/abc123/json_post_1/",
                    "author": "jsonuser",
                    "created_utc": 1726930000.0,
                }
            }
        ]
    }
}


class FakeResponse:
    """Minimal fake requests.Response for mocking."""

    def __init__(self, status_code=200, text="", json_data=None):
        self.status_code = status_code
        self.text = text
        self._json_data = json_data
        self.content = text.encode() if text else b""
        self.headers = {}

    def json(self):
        if self._json_data is not None:
            return self._json_data
        raise ValueError("No JSON data in this response")

    def raise_for_status(self):
        from requests import HTTPError
        if self.status_code >= 400:
            exc = HTTPError(f"HTTP {self.status_code}")
            exc.response = self
            raise exc


def make_mock_atom_success():
    """Create a mock where RSS URLs return valid Atom XML and JSON URLs are never hit."""

    def get_side_effect(url, **kwargs):
        if ".rss" in url:
            return FakeResponse(status_code=200, text=SAMPLE_ATOM_FEED)
        # JSON URLs should NOT be reached when Atom succeeds
        pytest.fail(f"JSON URL should not be called when Atom works: {url}")

    mock_get = patch("bot.get", side_effect=get_side_effect)
    return mock_get


def make_mock_atom_failure_json_success():
    """Create a mock where RSS URLs fail but JSON URLs return valid data."""

    import requests as req_lib

    def get_side_effect(url, **kwargs):
        if ".rss" in url:
            raise req_lib.ConnectionError("Atom feed unreachable")
        if ".json" in url:
            return FakeResponse(status_code=200, json_data=SAMPLE_JSON_RESPONSE)
        return FakeResponse(status_code=200, text=SAMPLE_ATOM_FEED)

    mock_get = patch("bot.get", side_effect=get_side_effect)
    return mock_get


class TestFetchRedditJsonUserAgent:
    """Test that fetch_reddit_json uses the proper app User-Agent."""

    def test_uses_app_user_agent_not_browser(self):
        """fetch_reddit_json must use REDDIT_REQUEST_USER_AGENT, not a browser UA."""
        with patch("bot.get") as mock_get:
            mock_get.return_value = FakeResponse(
                status_code=200, json_data=SAMPLE_JSON_RESPONSE
            )
            fetch_reddit_json(
                ["/r/glinet/new.json"],
                params={"limit": 10, "raw_json": 1},
                timeout_seconds=20,
            )
            assert mock_get.call_count == 1
            sent_headers = mock_get.call_args.kwargs.get("headers", {})
            user_agent = sent_headers.get("User-Agent", "")
            assert user_agent == REDDIT_REQUEST_USER_AGENT
            assert "Mozilla" not in user_agent
            assert "GlinetDiscordBot" in user_agent


class TestFetchRedditSubredditNewPosts:
    """Test the primary feed fetch logic with Atom-first ordering."""

    def test_atom_first_when_successful(self):
        """When Atom succeeds, JSON endpoints should never be called."""
        with make_mock_atom_success() as mock_get:
            subreddit, posts = fetch_reddit_subreddit_new_posts("glinet")
        assert subreddit == "glinet"
        assert len(posts) == 2
        # Posts are sorted by created_utc ascending, so Test Post 2 (16:02) < Test Post 1 (17:08)
        assert posts[0]["title"] == "Test Post 2"
        assert posts[1]["title"] == "Test Post 1"
        # Verify NO .json URLs were called
        json_calls = [
            c for c in mock_get.call_args_list
            if ".json" in (c.args[0] if c.args else "")
        ]
        assert len(json_calls) == 0, "JSON endpoint should not be called when Atom succeeds"

    def test_atom_uses_app_user_agent(self):
        """The Atom feed request must use the app-specific User-Agent."""
        with make_mock_atom_success() as mock_get:
            fetch_reddit_subreddit_new_posts("glinet")
        for call in mock_get.call_args_list:
            headers = call.kwargs.get("headers", {})
            ua = headers.get("User-Agent", "")
            assert ua == REDDIT_REQUEST_USER_AGENT, f"Expected app UA, got: {ua}"

    def test_falls_back_to_json_when_atom_fails(self, caplog):
        """When Atom fails, the function should fall back to JSON silently (INFO)."""
        with make_mock_atom_failure_json_success() as mock_get:
            with caplog.at_level(logging.INFO):
                subreddit, posts = fetch_reddit_subreddit_new_posts("glinet")
        assert subreddit == "glinet"
        assert len(posts) == 1
        assert posts[0]["id"] == "abc123"
        assert posts[0]["title"] == "JSON Post 1"

        # The fallback should be logged at INFO level, not WARNING
        info_logs = [
            record
            for record in caplog.records
            if "Atom feed fetch failed" in record.getMessage()
            and "retrying JSON feed" in record.getMessage()
        ]
        assert len(info_logs) == 1, \
            "Should log INFO about Atom failure when falling back to JSON"

        # There should be no WARNING-level log about Reddit feed
        reddit_warnings = [
            record
            for record in caplog.records
            if record.levelno == logging.WARNING and "Reddit" in record.getMessage()
        ]
        assert len(reddit_warnings) == 0, \
            "Should not emit WARNING for Reddit feed fallback; use INFO"

    def test_no_403_warning_when_atom_works(self, caplog):
        """The primary fix: no 403/WARNING should appear when Atom works."""
        with make_mock_atom_success() as mock_get:
            with caplog.at_level(logging.WARNING):
                fetch_reddit_subreddit_new_posts("glinet")
            reddit_warnings = [
                record
                for record in caplog.records
                if record.levelno == logging.WARNING and "Reddit" in record.getMessage()
            ]
            assert len(reddit_warnings) == 0, \
                "No WARNING messages about Reddit should be emitted when Atom feed works"

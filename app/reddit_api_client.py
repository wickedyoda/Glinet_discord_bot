"""
Reddit OAuth2 API client for posting comments (auto-responder feature).

This module handles:
- OAuth2 token management (refresh flow)
- Posting comments to Reddit posts
- Safe, rate-limit-aware comment submission
"""
from __future__ import annotations

import os
import time
from typing import Any

import requests

REDDIT_API_BASE_URL = "https://oauth.reddit.com"
REDDIT_TOKEN_URL = "https://www.reddit.com/api/v1/access_token"  # nosec B105
REDDIT_API_TIMEOUT_SECONDS = 15


class RedditApiError(Exception):
    """Raised for unrecoverable Reddit API errors."""


class RedditRateLimitError(Exception):
    """Raised when Reddit returns HTTP 429 (rate limited)."""


class RedditAuthError(Exception):
    """Raised when Reddit OAuth2 credentials are missing or invalid."""


def _get_reddit_oauth_config():
    """Returns (client_id, client_secret, username, password) or None if not configured."""
    client_id = str(os.getenv("REDDIT_CLIENT_ID", "") or "").strip()
    client_secret = str(os.getenv("REDDIT_CLIENT_SECRET", "") or "").strip()
    username = str(os.getenv("REDDIT_USERNAME", "") or "").strip()
    password = str(os.getenv("REDDIT_PASSWORD", "") or "").strip()
    if not client_id or not client_secret or not username or not password:
        return None
    return client_id, client_secret, username, password


def _reddit_user_agent():
    return "web:GL.iNetBot:1.0.0 (by /u/GlInetDiscordBot)"


def _refresh_reddit_access_token() -> str:
    """
    Obtain a Reddit OAuth2 access token using the password grant flow.
    Returns the access token string.
    """
    config = _get_reddit_oauth_config()
    if config is None:
        raise RedditAuthError(
            "Reddit OAuth credentials not configured. "
            "Set REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USERNAME, REDDIT_PASSWORD."
        )
    client_id, client_secret, username, password = config

    try:
        response = requests.post(
            REDDIT_TOKEN_URL,
            auth=(client_id, client_secret),
            data={
                "grant_type": "password",
                "username": username,
                "password": password,
                "scope": "submit identity",
            },
            headers={"User-Agent": _reddit_user_agent()},
            timeout=REDDIT_API_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        token_data = response.json()
        access_token = token_data.get("access_token")
        if not access_token:
            raise RedditAuthError("Reddit OAuth response missing access_token.")
        return access_token
    except requests.HTTPError as exc:
        status_code = getattr(exc.response, "status_code", None)
        if status_code == 401:
            raise RedditAuthError("Reddit OAuth authentication failed (HTTP 401).") from exc
        if status_code == 429:
            raise RedditRateLimitError("Reddit OAuth token request rate limited (HTTP 429).") from exc
        raise RedditApiError(f"Reddit OAuth token request failed (HTTP {status_code}).") from exc
    except requests.RequestException as exc:
        raise RedditApiError(f"Reddit OAuth token request network error: {exc}") from exc


# Module-level token cache with TTL
_reddit_access_token_cache: dict[str, Any] = {}
_REDDIT_TOKEN_TTL_SECONDS = 3300  # 55 minutes (tokens expire in 1 hour)


def get_reddit_access_token(force_refresh: bool = False) -> str:
    """Get a cached or fresh Reddit OAuth2 access token."""
    now = time.time()
    cached = _reddit_access_token_cache
    if (
        not force_refresh
        and cached.get("access_token")
        and cached.get("expires_at", 0) > now + 30
    ):
        return cached["access_token"]

    access_token = _refresh_reddit_access_token()
    _reddit_access_token_cache["access_token"] = access_token
    _reddit_access_token_cache["expires_at"] = now + _REDDIT_TOKEN_TTL_SECONDS
    return access_token


def post_reddit_comment(
    subreddit: str,
    post_id: str,
    body: str,
    *,
    force_refresh: bool = False,
) -> dict[str, Any]:
    """
    Post a comment to a Reddit post.

    Args:
        subreddit: The subreddit name (without r/ prefix).
        post_id: The Reddit post ID (base-36).
        body: The comment text.
        force_refresh: Force a new OAuth2 token.

    Returns:
        dict with keys: ok (bool), comment_id (str or None), error (str or None)
    """
    access_token = get_reddit_access_token(force_refresh=force_refresh)
    # Post the comment to Reddit via the submissions API.
    # The submission API accepts the post's fullname (t3_...) in the thing_id parameter.
    try:
        response = requests.post(
            f"{REDDIT_API_BASE_URL}/api/comment",
            headers={
                "Authorization": f"bearer {access_token}",
                "User-Agent": _reddit_user_agent(),
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data={
                "api_type": "json",
                "text": body,
                "thing_id": f"t3_{post_id}",
            },
            timeout=REDDIT_API_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        result = response.json()

        # Reddit returns errors in the JSON response body
        errors = ((result.get("json") or {}).get("errors") or [])
        if errors:
            error_msg = errors[0] if isinstance(errors[0], str) else str(errors[0])
            return {"ok": False, "comment_id": None, "error": error_msg}

        comment_data = ((result.get("json") or {}).get("data") or {}).get("things", [])
        if comment_data and isinstance(comment_data, list):
            first = comment_data[0] if isinstance(comment_data[0], dict) else {}
            comment_id = first.get("data", {}).get("id")
        else:
            comment_id = None

        return {"ok": True, "comment_id": comment_id, "error": None}

    except requests.HTTPError as exc:
        status_code = getattr(exc.response, "status_code", None)
        if status_code == 429:
            raise RedditRateLimitError("Reddit comment posting rate limited (HTTP 429).") from exc
        if status_code == 401:
            # Token may be expired — try once with a fresh token
            if not force_refresh:
                return post_reddit_comment(subreddit, post_id, body, force_refresh=True)
            raise RedditAuthError("Reddit OAuth token invalid (HTTP 401).") from exc
        raise RedditApiError(f"Reddit comment posting failed (HTTP {status_code}).") from exc
    except requests.RequestException as exc:
        raise RedditApiError(f"Reddit comment posting network error: {exc}") from exc


def is_reddit_oauth_configured() -> bool:
    """Check if Reddit OAuth credentials are configured."""
    return _get_reddit_oauth_config() is not None

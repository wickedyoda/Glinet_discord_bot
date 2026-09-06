"""Shared HTTP client for the Glinet Discord bot.

Wraps a single ``requests.Session`` with retry/backoff so every
outbound HTTP call reuses TCP connections instead of opening a new
one each time.
"""

from __future__ import annotations

from typing import Any

import requests
import requests.adapters
import urllib3

# Retry on transient server errors with exponential backoff
_RETRY_STRATEGY = urllib3.Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[502, 503, 504],
    allowed_methods=frozenset(["GET", "HEAD", "POST"]),
)
_ADAPTER = requests.adapters.HTTPAdapter(max_retries=_RETRY_STRATEGY)

_session: requests.Session | None = None


def get_session() -> requests.Session:
    """Return the shared :class:`requests.Session`, creating it once."""
    global _session
    if _session is None:
        _session = requests.Session()
        _session.mount("https://", _ADAPTER)
        _session.mount("http://", _ADAPTER)
    return _session


def get(url: str, **kwargs: Any) -> requests.Response:
    """GET via the shared session."""
    return get_session().get(url, **kwargs)


def post(url: str, **kwargs: Any) -> requests.Response:
    """POST via the shared session."""
    return get_session().post(url, **kwargs)

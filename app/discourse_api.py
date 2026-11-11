from __future__ import annotations

from html import unescape
from urllib.parse import urljoin

import requests


class DiscourseApiError(RuntimeError):
    pass


class DiscourseRateLimitError(DiscourseApiError):
    pass


def clean_discourse_text(value: str) -> str:
    text = str(value or "")
    cleaned = []
    in_tag = False
    for char in text:
        if char == "<":
            in_tag = True
            continue
        if char == ">":
            in_tag = False
            cleaned.append(" ")
            continue
        if not in_tag:
            cleaned.append(char)
    return " ".join(unescape("".join(cleaned)).split())


def build_discourse_headers(*, user_agent: str, api_key: str = "", api_username: str = "") -> dict[str, str]:
    headers = {
        "Accept": "application/json,text/plain,*/*",
        "User-Agent": str(user_agent or "CodexDiscourseClient/1.0"),
    }
    
    def is_valid_credential(val: str | None) -> bool:
        if not val:
            return False
        cleaned = str(val).strip()
        if not cleaned:
            return False
        if cleaned.lower() in {"none", "null", "undefined", "empty"}:
            return False
        return True

    if is_valid_credential(api_key):
        headers["Api-Key"] = str(api_key).strip()
    if is_valid_credential(api_username):
        headers["Api-Username"] = str(api_username).strip()
    return headers


def _safe_int(value) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _raise_for_discourse_status(response: requests.Response, source_name: str, query: str = ""):
    status_code = int(getattr(response, "status_code", 0) or 0)
    if status_code == 429:
        raise DiscourseRateLimitError(f"{source_name} search is rate-limited right now.")
    if status_code >= 400:
        query_note = f" for query: {query}" if query else ""
        raise DiscourseApiError(f"{source_name} request failed with HTTP {status_code}{query_note}.")


def extract_discourse_topics(payload: dict, *, base_url: str, max_results: int) -> list[dict]:
    # The search endpoint returns topics at the top level; the tag endpoint nests
    # them under topic_list. Accept both so the extractor is not endpoint-specific.
    topics = payload.get("topics", [])
    if not topics and isinstance(payload.get("topic_list"), dict):
        topics = payload["topic_list"].get("topics", [])
    if not isinstance(topics, list):
        topics = []
    category_lookup = {}
    categories = payload.get("categories", [])
    if isinstance(categories, list):
        for category in categories:
            if not isinstance(category, dict):
                continue
            category_id = category.get("id")
            if category_id is None:
                continue
            category_lookup[int(category_id)] = clean_discourse_text(str(category.get("name") or "")).strip()

    results = []
    seen_topic_ids = set()
    for topic in topics:
        if not isinstance(topic, dict):
            continue
        topic_id = topic.get("id")
        if not topic_id or topic_id in seen_topic_ids:
            continue
        slug = str(topic.get("slug") or "").strip()
        title = clean_discourse_text(str(topic.get("fancy_title") or topic.get("title") or "")).strip() or f"Topic {topic_id}"
        excerpt = clean_discourse_text(str(topic.get("excerpt") or "")).strip()
        category_id = topic.get("category_id")
        category_name = category_lookup.get(int(category_id)) if category_id is not None and str(category_id).isdigit() else ""
        topic_url = f"{base_url.rstrip('/')}/t/{slug}/{topic_id}" if slug else f"{base_url.rstrip('/')}/t/{topic_id}"
        results.append(
            {
                "id": int(topic_id),
                "title": title,
                "url": topic_url,
                "slug": slug,
                "excerpt": excerpt,
                "category_name": category_name or "",
                "posts_count": int(topic.get("posts_count") or 0),
                "last_posted_at": str(topic.get("last_posted_at") or ""),
            }
        )
        seen_topic_ids.add(topic_id)
        if len(results) >= max_results:
            break
    return results


def search_discourse_topics(
    *,
    base_url: str,
    query: str,
    max_results: int,
    source_name: str,
    timeout_seconds: int,
    user_agent: str,
    api_key: str = "",
    api_username: str = "",
) -> list[dict]:
    search_url = f"{base_url.rstrip('/')}/search.json"
    response = requests.get(
        search_url,
        params={"q": query},
        timeout=timeout_seconds,
        headers=build_discourse_headers(user_agent=user_agent, api_key=api_key, api_username=api_username),
    )
    _raise_for_discourse_status(response, source_name, query=query)
    try:
        data = response.json()
    except ValueError as exc:
        raise DiscourseApiError(f"{source_name} returned invalid JSON.") from exc
    return extract_discourse_topics(data or {}, base_url=base_url, max_results=max_results)


def fetch_discourse_category_topics(
    *,
    base_url: str,
    category_slug: str,
    max_results: int,
    source_name: str,
    timeout_seconds: int,
    user_agent: str,
    api_key: str = "",
    api_username: str = "",
) -> list[dict]:
    """Fetch the latest topics in a category.

    The bare ``/c/{slug}/l/latest.json`` form redirects to the id-qualified URL;
    ``requests`` follows that automatically.
    """
    cleaned_slug = str(category_slug or "").strip().strip("/")
    if not cleaned_slug:
        raise DiscourseApiError("A forum category is required.")

    response = requests.get(
        f"{base_url.rstrip('/')}/c/{cleaned_slug}/l/latest.json",
        timeout=timeout_seconds,
        headers=build_discourse_headers(user_agent=user_agent, api_key=api_key, api_username=api_username),
    )
    _raise_for_discourse_status(response, source_name, query=f"category:{cleaned_slug}")
    try:
        data = response.json()
    except ValueError as exc:
        raise DiscourseApiError(f"{source_name} returned invalid JSON.") from exc
    return extract_discourse_topics(data or {}, base_url=base_url, max_results=max_results)


def fetch_discourse_tags(
    *,
    base_url: str,
    timeout_seconds: int,
    source_name: str,
    user_agent: str,
    api_key: str = "",
    api_username: str = "",
) -> list[dict]:
    """Fetch the forum's available tags, most-used first.

    Used by the web admin picker so an admin chooses a real tag instead of typing
    one from memory. Slugs are used as the value because they are the canonical,
    case-insensitive form the tag feed accepts.
    """
    response = requests.get(
        f"{base_url.rstrip('/')}/tags.json",
        timeout=timeout_seconds,
        headers=build_discourse_headers(user_agent=user_agent, api_key=api_key, api_username=api_username),
    )
    _raise_for_discourse_status(response, source_name, query="tags")
    try:
        data = response.json()
    except ValueError as exc:
        raise DiscourseApiError(f"{source_name} returned invalid JSON.") from exc

    raw_tags = (data or {}).get("tags", [])
    if not isinstance(raw_tags, list):
        return []

    tags = []
    seen_slugs = set()
    for entry in raw_tags:
        if not isinstance(entry, dict):
            continue
        slug = str(entry.get("slug") or entry.get("name") or "").strip().lstrip("#")
        name = str(entry.get("name") or entry.get("text") or slug).strip()
        if not slug or slug in seen_slugs:
            continue
        seen_slugs.add(slug)
        try:
            count = int(entry.get("count") or 0)
        except (TypeError, ValueError):
            count = 0
        tags.append({"slug": slug, "name": name or slug, "count": count})

    tags.sort(key=lambda item: (-item["count"], item["name"].casefold()))
    return tags


def fetch_discourse_tag_topics(
    *,
    base_url: str,
    tag: str,
    max_results: int,
    source_name: str,
    timeout_seconds: int,
    user_agent: str,
    api_key: str = "",
    api_username: str = "",
) -> list[dict]:
    """Fetch the latest topics carrying a given tag.

    Uses ``/tag/{tag}/l/latest.json`` rather than search so results are ordered by
    activity the way the forum presents them, and so a tag with no search-index
    entry still resolves.
    """
    cleaned_tag = str(tag or "").strip().lstrip("#")
    if not cleaned_tag:
        raise DiscourseApiError("A forum tag is required.")

    response = requests.get(
        f"{base_url.rstrip('/')}/tag/{cleaned_tag}/l/latest.json",
        params={"order": "created", "ascending": "false"},
        timeout=timeout_seconds,
        headers=build_discourse_headers(user_agent=user_agent, api_key=api_key, api_username=api_username),
    )
    _raise_for_discourse_status(response, source_name, query=f"tag:{cleaned_tag}")
    try:
        data = response.json()
    except ValueError as exc:
        raise DiscourseApiError(f"{source_name} returned invalid JSON.") from exc
    return extract_discourse_topics(data or {}, base_url=base_url, max_results=max_results)


def fetch_discourse_categories(
    *,
    base_url: str,
    timeout_seconds: int,
    user_agent: str,
    api_key: str = "",
    api_username: str = "",
) -> list[dict]:
    response = requests.get(
        f"{base_url.rstrip('/')}/categories.json",
        timeout=timeout_seconds,
        headers=build_discourse_headers(user_agent=user_agent, api_key=api_key, api_username=api_username),
    )
    _raise_for_discourse_status(response, "Discourse categories")
    try:
        payload = response.json()
    except ValueError as exc:
        raise DiscourseApiError("Discourse categories returned invalid JSON.") from exc
    categories = ((payload or {}).get("category_list") or {}).get("categories") or []
    results = []
    for category in categories:
        if not isinstance(category, dict):
            continue
        category_id = category.get("id")
        if category_id is None:
            continue
        slug = str(category.get("slug") or "").strip()
        results.append(
            {
                "id": int(category_id),
                "name": clean_discourse_text(str(category.get("name") or "")).strip(),
                "slug": slug,
                "topic_count": _safe_int(category.get("topic_count")),
                "url": urljoin(f"{base_url.rstrip('/')}/", f"c/{slug}/{category_id}" if slug else f"c/{category_id}"),
            }
        )
    return results


def fetch_discourse_topic(
    *,
    base_url: str,
    topic_id: int,
    timeout_seconds: int,
    user_agent: str,
    api_key: str = "",
    api_username: str = "",
) -> dict:
    response = requests.get(
        f"{base_url.rstrip('/')}/t/{int(topic_id)}.json",
        timeout=timeout_seconds,
        headers=build_discourse_headers(user_agent=user_agent, api_key=api_key, api_username=api_username),
    )
    _raise_for_discourse_status(response, "Discourse topic")
    try:
        payload = response.json()
    except ValueError as exc:
        raise DiscourseApiError("Discourse topic returned invalid JSON.") from exc
    title = clean_discourse_text(str(payload.get("fancy_title") or payload.get("title") or "")).strip()
    post_stream = payload.get("post_stream") or {}
    posts = post_stream.get("posts") if isinstance(post_stream, dict) else []
    if not isinstance(posts, list):
        posts = []
    return {
        "id": int(payload.get("id") or topic_id),
        "title": title,
        "slug": str(payload.get("slug") or "").strip(),
        "url": urljoin(f"{base_url.rstrip('/')}/", f"t/{payload.get('slug') or payload.get('id') or topic_id}/{payload.get('id') or topic_id}"),
        "posts": [
            {
                "id": int(post.get("id") or 0),
                "username": str(post.get("username") or "").strip(),
                "cooked": str(post.get("cooked") or ""),
                "text": clean_discourse_text(str(post.get("cooked") or "")).strip(),
                "created_at": str(post.get("created_at") or ""),
            }
            for post in posts
            if isinstance(post, dict)
        ],
    }

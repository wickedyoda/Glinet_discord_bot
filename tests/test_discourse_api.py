from app.discourse_api import build_discourse_headers, clean_discourse_text, extract_discourse_topics


def test_build_discourse_headers_includes_optional_auth_values():
    headers = build_discourse_headers(
        user_agent="TestAgent/1.0",
        api_key="secret-key",
        api_username="forum-bot",
    )

    assert headers["User-Agent"] == "TestAgent/1.0"
    assert headers["Api-Key"] == "secret-key"
    assert headers["Api-Username"] == "forum-bot"


def test_clean_discourse_text_strips_tags_and_entities():
    assert clean_discourse_text("<p>Hello &amp; <strong>world</strong></p>") == "Hello & world"


def test_extract_discourse_topics_returns_structured_results():
    payload = {
        "topics": [
            {
                "id": 123,
                "slug": "example-topic",
                "title": "Example Topic",
                "excerpt": "<p>Topic excerpt</p>",
                "category_id": 7,
                "posts_count": 4,
                "last_posted_at": "2026-04-01T00:00:00Z",
            }
        ],
        "categories": [
            {"id": 7, "name": "Routers"},
        ],
    }

    results = extract_discourse_topics(payload, base_url="https://forum.gl-inet.com", max_results=5)

    assert len(results) == 1
    assert results[0]["title"] == "Example Topic"
    assert results[0]["url"] == "https://forum.gl-inet.com/t/example-topic/123"
    assert results[0]["category_name"] == "Routers"
    assert results[0]["excerpt"] == "Topic excerpt"

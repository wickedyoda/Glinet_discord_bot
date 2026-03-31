from app.moderation import find_bad_word_match, parse_bad_word_list, serialize_bad_word_list


def test_parse_bad_word_list_deduplicates_and_normalizes_lines():
    assert parse_bad_word_list("badword\n badword \nother phrase\n") == ["badword", "other phrase"]
    assert serialize_bad_word_list("badword\nother phrase") == '["badword", "other phrase"]'


def test_find_bad_word_match_uses_word_boundaries_for_single_words():
    assert find_bad_word_match("this contains badword here", ["badword"]) == "badword"
    assert find_bad_word_match("this contains badwordish here", ["badword"]) is None


def test_find_bad_word_match_supports_phrases_case_insensitively():
    assert find_bad_word_match("This has Other Phrase inside it", ["other phrase"]) == "other phrase"

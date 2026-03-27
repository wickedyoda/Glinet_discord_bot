from app.youtube_monitor import YouTubeFeedError, build_youtube_feed_error


def test_build_youtube_feed_error_disables_missing_channel_feed():
    error = build_youtube_feed_error(404)

    assert isinstance(error, YouTubeFeedError)
    assert error.status_code == 404
    assert error.disable_subscription is True


def test_build_youtube_feed_error_keeps_transient_server_errors_enabled():
    error = build_youtube_feed_error(500)

    assert isinstance(error, YouTubeFeedError)
    assert error.status_code == 500
    assert error.disable_subscription is False

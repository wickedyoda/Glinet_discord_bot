class YouTubeFeedError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None, disable_subscription: bool = False):
        super().__init__(message)
        self.status_code = status_code
        self.disable_subscription = bool(disable_subscription)


def build_youtube_feed_error(status_code: int) -> YouTubeFeedError:
    safe_status = int(status_code or 0)
    if safe_status == 404:
        return YouTubeFeedError(
            "YouTube channel feed returned HTTP 404. The saved channel no longer exists or the subscription URL resolved incorrectly.",
            status_code=safe_status,
            disable_subscription=True,
        )
    return YouTubeFeedError(
        f"YouTube feed returned HTTP {safe_status}.",
        status_code=safe_status,
        disable_subscription=False,
    )

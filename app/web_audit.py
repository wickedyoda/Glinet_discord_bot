def should_log_web_audit_event(*, endpoint: str | None, status_code: int, authenticated: bool) -> bool:
    if endpoint == "healthz":
        return False
    if (not authenticated) and int(status_code or 0) == 404 and str(endpoint or "unknown") == "unknown":
        return False
    return True

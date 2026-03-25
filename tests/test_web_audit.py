from app.web_audit import should_log_web_audit_event


def test_should_log_web_audit_event_skips_anonymous_unknown_404():
    assert not should_log_web_audit_event(endpoint=None, status_code=404, authenticated=False)


def test_should_log_web_audit_event_keeps_authenticated_and_known_routes():
    assert should_log_web_audit_event(endpoint="dashboard", status_code=200, authenticated=True)
    assert should_log_web_audit_event(endpoint="login", status_code=200, authenticated=False)

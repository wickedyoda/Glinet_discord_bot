import logging

import webui.app as webui_app


def test_gui2_listener_records_startup_failure(monkeypatch):
    class FailingApp:
        def run(self, **_kwargs):
            raise OSError(98, "Address already in use")

    monkeypatch.setattr(webui_app, "create_app", lambda **_kwargs: FailingApp())

    listener = webui_app.start_gui2_web_admin_interface(logger=logging.getLogger(__name__))
    listener.join(timeout=1)

    assert isinstance(listener.startup_error, OSError)
    assert "Address already in use" in str(listener.startup_error)


def test_gui2_listener_keeps_runtime_stop_as_normal_return(monkeypatch):
    class StoppedApp:
        def run(self, **_kwargs):
            return None

    monkeypatch.setattr(webui_app, "create_app", lambda **_kwargs: StoppedApp())

    listener = webui_app.start_gui2_web_admin_interface(logger=logging.getLogger(__name__))
    listener.join(timeout=1)

    assert listener.startup_error is None

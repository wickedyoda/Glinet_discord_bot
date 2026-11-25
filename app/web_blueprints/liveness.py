"""Liveness, readiness, favicon and the legacy redirects.

These were the smallest routes buried inside `create_web_app` and needed only four
closures, which makes them the safe proof of the blueprint pattern before the large
page builders move. Nothing here is stateful: every view either returns a computed
payload or redirects.
"""

from __future__ import annotations

from flask import Blueprint, redirect, send_file, url_for


def build_liveness_blueprint(
    *,
    build_health_payload,
    favicon_path,
    current_user,
    redirect_to_observability,
    login_required,
) -> Blueprint:
    """Blueprint for the probes and redirects.

    The dependencies are passed in rather than imported: they are closures of
    `create_web_app` (session-backed `current_user`, the health payload builder), and
    re-implementing them here would be a second source of truth for auth.
    """
    bp = Blueprint("liveness", __name__)

    @bp.get("/healthz")
    def healthz():
        payload = build_health_payload()
        return payload, 200 if bool(payload.get("ok", True)) else 503

    @bp.get("/readyz")
    def readyz():
        payload = build_health_payload()
        return payload, 200 if bool(payload.get("ready", False)) else 503

    @bp.get("/favicon.ico")
    def favicon():
        if favicon_path.exists() and favicon_path.is_file():
            return send_file(favicon_path, mimetype="image/png", max_age=86400)
        return ("", 204)

    @bp.get("/")
    def index():
        if current_user():
            return redirect(url_for("guilds_page"))
        return redirect(url_for("login"))

    @bp.get("/admin/observability")
    def observability():
        return redirect_to_observability()

    @bp.get("/status/everything")
    def public_observability_everything():
        return redirect_to_observability()

    # Kept because the typo is in links people already have; the redirect is the fix.
    @bp.get("/staus")
    def public_observability_alias():
        return redirect_to_observability()

    @bp.get("/admin/home")
    @login_required
    def legacy_gui2_home():
        return redirect(url_for("dashboard"))

    @bp.get("/admin/wiki")
    @login_required
    def wiki_viewer():
        return redirect(url_for("documentation"))

    return bp

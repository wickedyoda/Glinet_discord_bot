"""Route modules split out of the 7,617-line create_web_app.

Each module exposes a `build_*_blueprint(...)` factory that takes the closures it needs
from `create_web_app`, so auth and settings keep one source of truth while the route
bodies live somewhere a reader can find them.
"""

from app.web_blueprints.liveness import build_liveness_blueprint

__all__ = ["build_liveness_blueprint"]

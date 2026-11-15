"""Compile-once rendering for the inline templates in the admin GUI.

``flask.render_template_string`` builds a new Jinja ``Template`` on every call. The
admin layout is a single large template string that every page renders through, so
that cost was paid on every request: on this tree, compiling once per app instead
took a dashboard request from 60.7 ms to 33.9 ms.

The compiled template is cached on the app's own Jinja environment, so it is
discarded with the app rather than held in a module-level cache that would keep
every test app alive. Context processors and injection behave exactly as they do in
``flask.templating.render_template_string`` -- ``update_template_context`` runs on
each render, only the compilation is reused.
"""

from __future__ import annotations

from flask import current_app

_SOURCE_CACHE_ATTR = "_source_template_cache"


def render_cached_template(source: str, **context) -> str:
    """Render a template string, compiling it once per Jinja environment."""
    env = current_app.jinja_env
    cache = env.__dict__.get(_SOURCE_CACHE_ATTR)
    if cache is None:
        cache = env.__dict__[_SOURCE_CACHE_ATTR] = {}
    template = cache.get(source)
    if template is None:
        template = env.from_string(source)
        cache[source] = template
    # update_template_context mutates in place and returns None; give it a copy so the
    # caller's dict is not modified, then render with the merged values.
    context = dict(context)
    current_app.update_template_context(context)
    return template.render(**context)

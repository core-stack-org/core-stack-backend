"""
Central gate deciding whether a generated layer is pushed to GeoServer.

A layer is pushed only when the caller asked for it (``sync_to_geoserver``) AND
the tehsil is active. Otherwise the layer stays on local disk with
``Layer.is_sync_to_geoserver=False``, so it can be pushed once the tehsil is
activated (see ``computing.pending_geoserver_sync``).

The decision lives in a ContextVar set by the API decorator / bulk task, and is
carried over Celery hops through message headers. When no context is set (code
paths that are not layer generation), the gate is a no-op and behaviour is
unchanged.
"""

import logging
import re
import threading
import time
from contextlib import contextmanager
from contextvars import ContextVar

from django.conf import settings

logger = logging.getLogger("core_stack.layer_generation")

CELERY_HEADER = "geoserver_sync_ctx"
ACTIVE_CACHE_TTL_SECONDS = 600

_CONTEXT = ContextVar("geoserver_sync_context", default=None)
_active_cache = {"fetched_at": 0.0, "keys": frozenset()}
_active_cache_lock = threading.Lock()

_TRUE = {"1", "true", "yes", "y", "on"}
_FALSE = {"0", "false", "no", "n", "off", ""}


def parse_bool(value, default=False):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in _TRUE:
        return True
    if text in _FALSE:
        return False
    return default


def _norm(value):
    return re.sub(r"[^a-z0-9]", "", str(value or "").lower())


def _key(state, district, block):
    return (_norm(state), _norm(district), _norm(block))


def _prod_api_configured():
    return bool(getattr(settings, "PROD_BACKEND_URL", ""))


def fetch_active_tehsil_keys(refresh=False):
    """
    Normalised (state, district, block) keys of active tehsils. Reads the prod
    API when PROD_BACKEND_URL is set (the local DB is stale), else the local DB.
    Raises on API failure.
    """
    if not _prod_api_configured():
        from geoadmin.models import TehsilSOI

        rows = TehsilSOI.objects.filter(
            active_status=True,
            district__active_status=True,
            district__state__active_status=True,
        ).values_list(
            "district__state__state_name",
            "district__district_name",
            "tehsil_name",
        )
        return frozenset(_key(*row) for row in rows)

    with _active_cache_lock:
        fresh = time.time() - _active_cache["fetched_at"] < ACTIVE_CACHE_TTL_SECONDS
        if refresh or not fresh:
            from computing.bulk_layer_generation import get_active_locations_from_api

            locations = get_active_locations_from_api()
            _active_cache["keys"] = frozenset(
                _key(loc.state, loc.district, loc.block) for loc in locations
            )
            _active_cache["fetched_at"] = time.time()
        return _active_cache["keys"]


def is_tehsil_active(state, district, block, refresh=False):
    """Fails closed: an unreachable prod API means "not active"."""
    if not (state and district and block):
        return False
    try:
        return _key(state, district, block) in fetch_active_tehsil_keys(refresh)
    except Exception as exc:
        logger.warning("Could not determine active tehsils, assuming inactive: %s", exc)
        return False


def resolve_sync(requested, state, district, block):
    return bool(requested) and is_tehsil_active(state, district, block)


def _build_context(sync, state, district, block):
    return {
        "sync": bool(sync),
        "state": state,
        "district": district,
        "block": block,
    }


@contextmanager
def geoserver_sync_context(requested, state, district, block):
    """Set the gate for the enclosed work; ``requested`` is the caller's flag."""
    sync = resolve_sync(requested, state, district, block)
    if requested and not sync:
        logger.info(
            "GeoServer sync requested but tehsil %s/%s/%s is not active; "
            "layers stay local",
            state,
            district,
            block,
        )
    token = _CONTEXT.set(_build_context(sync, state, district, block))
    try:
        yield sync
    finally:
        _CONTEXT.reset(token)


def get_context():
    return _CONTEXT.get()


def set_context(ctx):
    """Returns a token for ``reset_context``. Used by the Celery worker hooks."""
    return _CONTEXT.set(ctx)


def reset_context(token):
    _CONTEXT.reset(token)


def should_skip_sync():
    ctx = _CONTEXT.get()
    return ctx is not None and not ctx["sync"]


def skip_and_record(
    layer_type,
    path,
    layer_name,
    workspace,
    style_name=None,
    file_type="gpkg",
    status_code=200,
):
    """
    If the gate says skip, return a result callers can treat as success. The
    layer stays local with is_sync_to_geoserver=False on its Layer row, which is
    what the later sync (computing.pending_geoserver_sync) looks for. Returns
    None when the push should go ahead.
    """
    if not should_skip_sync():
        return None
    logger.info("GeoServer sync skipped for %s:%s (kept local)", workspace, layer_name)
    return {"status_code": status_code, "skipped": True, "queued": False}

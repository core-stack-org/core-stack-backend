"""Shared helpers for fast local geospatial pipelines.

The modules in this package are intentionally small and dependency-light. They
support runtime pipelines that read only the requested administrative scope,
use SQLite indexes in local GeoPackages, and write standard output bundles for
API, GeoServer, STAC, EDA, and Excel handoff use.
"""

from .admin import AdminScope, CSAdminSource
from .schema import StandardRequest, api_request_payload, load_config

__all__ = [
    "AdminScope",
    "CSAdminSource",
    "StandardRequest",
    "api_request_payload",
    "load_config",
]

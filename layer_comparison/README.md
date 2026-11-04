# CoRE Stack Layer Comparison (Django app)

This app scaffold compares existing GEE assets with GeoServer WMS layers.
It creates **no database tables, Django models, or migrations**.

## Integration required
The supplied model references identify useful metadata in `computing.models`
(`Dataset`, `Layer`, `LayerMapping`) and `stats_generator.models` (`LayerInfo`).
The exact MWS model/API and all field relationships must be verified against
your checked-out branch. Implement `services/mws_service.py` first.

The metadata adapter is a starting point, not a guaranteed drop-in mapping.
Verify:
- `Layer` -> `Dataset` relationship and administrative FK names
- how the correct/latest `Layer` record is selected
- `LayerMapping.db_dataset_name` against `Dataset.name`
- exact GEE asset field and GeoServer layer/style mapping
- visualization parameters for each GEE asset

## Install
1. Copy this folder into your Django project root.
2. Add `layer_comparison` to `INSTALLED_APPS`.
3. Include `path("layer-comparison/", include("layer_comparison.urls"))` in root URLs.
4. Configure settings below.
5. Implement the MWS adapter and verify the metadata adapter.
6. Install `earthengine-api`.

No `makemigrations` or `migrate` is needed.

## Settings
```python
import os

GEE_SERVICE_ACCOUNT = os.environ.get("GEE_SERVICE_ACCOUNT")
GEE_PRIVATE_KEY_FILE = os.environ.get("GEE_PRIVATE_KEY_FILE")
GEOSERVER_PUBLIC_URL = os.environ.get(
    "GEOSERVER_PUBLIC_URL", "http://localhost:8080/geoserver"
)
LAYER_COMPARISON_GEE_CACHE_SECONDS = int(
    os.environ.get("LAYER_COMPARISON_GEE_CACHE_SECONDS", "1800")
)
```

Install:
`pip install earthengine-api`

## Endpoints
- `GET /layer-comparison/`
- `GET /layer-comparison/api/mws/`
- `GET /layer-comparison/api/config/?mws_id=<id>`

Groups are derived from `LayerMapping.theme`; no group table is created.

## Notes
- The MWS adapter must return true MWS geometry, not a tehsil polygon.
- GeoServer must be browser-accessible, or routed through a same-origin proxy.
- Never expose service-account keys or GeoServer admin credentials in JavaScript.
- GEE tile URLs are temporary; the service caches them for a limited period.
- Add your actual version-selection rule before production if multiple Layer rows match.

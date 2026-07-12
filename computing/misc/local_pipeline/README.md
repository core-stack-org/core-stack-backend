# Local Pipeline Core

Shared helpers for fast local geospatial pipelines in `computing/misc`.

The core package keeps reusable mechanics separate from dataset-specific
pipelines:

- SQLite-backed GeoPackage inspection, filtered reads, and first-run indexes.
- Standard admin scope resolution from `cs_admin_standard.gpkg`.
- CSV-to-SQLite sidecars for repeated keyed lookups.
- Standard request parsing for API, CLI, and batch execution.
- Standard output bundle writers for GPKG, CSV, README, run metadata (with
  per-output column dictionary and EDA), STAC fragments, and GeoServer link
  outputs.
- GeoLibre projects and clickable HTML maps backed by live GeoServer WFS
  layers, with optional non-redundant S3 publication.

```mermaid
flowchart TD
    A[API or batch request] --> B[StandardRequest]
    B --> C[CSAdminSource scope lookup]
    C --> D[Indexed GeoPackage read]
    D --> E[Dataset-specific pipeline]
    E --> F[OutputBundle writers]
    F --> G[Optional GeoServer publish]
    G --> H[Default local GeoLibre project and HTML]
    H --> I[Optional S3 project publication]
    H --> J[Layer DB registration with GeoLibre links]
```

Runtime scripts in `computing/misc` should not use `pyogrio`. This package reads
GeoPackage rows through SQLite and writes GeoPackages through GeoPandas/Fiona.

See [GEOLIBRE_INTEGRATION.md](GEOLIBRE_INTEGRATION.md) for the API contract,
artifact choices, AWS boundary, live demo evidence, and upstream-update process.

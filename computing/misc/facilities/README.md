# Facilities Local Pipeline

This package replaces the older GEE export path with a local GPKG/admin
pipeline:

```mermaid
flowchart TD
    A[API, CLI, or batch request] --> B[Resolve scope in cs_admin_standard.gpkg]
    B --> C[Read selected village geometries]
    C --> D[Read facility candidates using GeoPackage R-tree]
    D --> E[Spatially assign inventory facilities inside selected geography]
    E --> F[Build KD-tree by L3 facility class]
    F --> G[Find nearest facility per village and L3 class]
    G --> H[Collapse nearest L3 rows into L2 service summaries]
    H --> I[Write inventory, nearest, village service, README, EDA, STAC]
    I --> J{GeoServer enabled?}
    J -- yes --> K[Publish local GPKG]
    J -- no --> L[Return local output bundle]
```

The pipeline reads the facility GeoPackage directly through SQLite and never
loads all 4M+ facility rows for one request.

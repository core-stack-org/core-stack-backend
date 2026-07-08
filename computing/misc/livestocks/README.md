# Livestock Local Pipeline

This package replaces the older precomputed-GPKG clipping path with a keyed
runtime join:

```mermaid
flowchart TD
    A[API, CLI, or batch request] --> B[Resolve scope in cs_admin_standard.gpkg]
    B --> C[Read requested admin rows and pc11_village_id values]
    C --> D[Materialize or reuse CSV SQLite sidecar]
    D --> E[Fetch livestock rows by village_code]
    E --> F[Validate non-negative count columns]
    F --> G[Join livestock counts to admin rows]
    G --> H[Write GPKG, CSV, overview, README, EDA, STAC fragment]
    H --> I{GeoServer enabled?}
    I -- yes --> J[Publish local GPKG]
    I -- no --> K[Return local output bundle]
```

The large source CSV and generated SQLite sidecar remain under ignored `data/`.

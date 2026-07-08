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

## Request Body

The API accepts the old body:

```json
{"state": "MADHYA PRADESH", "district": "DAMOH", "block": "HATTA"}
```

It also accepts the structured body used by the local pipeline:

```json
{
  "scope": {
    "level": "tehsil",
    "state_name": "MADHYA PRADESH",
    "district_name": "DAMOH",
    "tehsil_name": "HATTA"
  },
  "outputs": {"mode": "focused"},
  "publish": {"sync_to_geoserver": true}
}
```

`focused` is the default API mode and still publishes to GeoServer unless
`publish.sync_to_geoserver` or `outputs.geoserver` is set to `false`.

## Output Modes

- `focused`: compact village-service GPKG plus focused and Excel-ready CSVs.
- `all`: inventory, nearest L3, village service, focused CSVs, methodology, EDA, STAC, and GPKG layers.
- `excel`: focused CSV only.
- `metadata`: run metadata only.
- `methodology`: documentation-oriented output.

The L3-to-L2 access grouping and `min`/`max`/`direct` rollup decisions are
tracked in `facility_classifications.yaml`.

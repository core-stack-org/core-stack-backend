# Facilities Pipeline

This directory contains the standalone scripts for building and maintaining the
pan-India facilities dataset. The source of truth is:

- `data/facilities/raw/`
- `utilities/scripts/facilities_utils/config/facilities_master.yaml`

*Facilities asset creation pipeline scripts do not depend on Django.*

## Main CLI

```bash
uv run --with pandas --with numpy --with pyyaml --with geopandas --with shapely --with pyogrio --with scipy \
  python utilities/scripts/facilities_utils/facility_pipeline.py all
```

Useful subcommands:

```bash
# Process raw source files into per-source intermediates.
uv run --with pandas --with numpy --with pyyaml \
  python utilities/scripts/facilities_utils/facility_pipeline.py clean

# Build lean CSV outputs and data/facilities/outputs/pan_india_facilities.gpkg.
uv run --with pandas --with numpy --with pyyaml --with geopandas --with shapely --with pyogrio \
  python utilities/scripts/facilities_utils/facility_pipeline.py build

# Build data/facilities/outputs/village_facility_proximity.gpkg.
# This stores L3 proximity as the durable base and materializes L2 from L3.
uv run --with pandas --with numpy --with pyyaml --with geopandas --with shapely --with pyogrio --with scipy \
  python utilities/scripts/facilities_utils/facility_pipeline.py proximity
```

## Outputs

`data/facilities/outputs/pan_india_facilities.gpkg`

- `facilities`: one valid facility point per physical facility.
- `facility_memberships`: non-spatial class membership table.
- `facility_taxonomy`: non-spatial taxonomy and filter-logic table.
- `source_summary`: non-spatial source QA table.
- `invalid_coordinates`: non-spatial invalid-coordinate audit table.

`data/facilities/outputs/village_facility_proximity.gpkg`

- `village_shapes`: village polygon geometries and key admin context columns.
- `proximity_l3`: lean durable base table with village id, L3 class, distance, and nearest facility uid.
- `proximity_l2_materialized`: lean physical L2 table derived from L3 using the configured min/max group logic.
- `proximity_nearest_facilities`: deduplicated facility detail lookup for nearest facilities referenced by L3.
- `proximity_class_map`: small YAML-derived class map from L3 to L2/L1/filter logic.
- `proximity_l2`: optional SQL view derived from L3 plus `proximity_class_map` for inspection.

L2 is materialized during the proximity build so runtime API requests do not
create working copies or evaluate derived views. L1/domain summaries are not
exported by the API; if a domain metric is needed later, add it as a cheap
derived table in this build step rather than in `computing/misc`.
The proximity source intentionally does not create or maintain a village point
layer; representative coordinates are stored as attributes on `village_shapes`
for bounds and distance traceability.

`computing/misc/facilities_proximity.py` exports tehsil GeoPackages from this
source asset. By default, those GeoPackages include a primary
`facilities_<district>_<block>` L3 proximity polygon layer, L2 proximity
polygons, village polygons, and nearest-facility point layers.
Exported feature layers carry map-facing fields such as `title`, class names,
distances, and nearest-facility details; pipeline metadata columns such as
`filter_logic` remain internal to the source tables/views.
The API does not accept level selection parameters for this layer; each request
exports the full tehsil facility package and uploads the zipped GeoPackage to
GeoServer when `sync_to_geoserver` is true.

API payload:

```json
{
  "state": "uttar pradesh",
  "district": "lucknow",
  "block": "lucknow",
  "sync_to_geoserver": true,
  "overwrite": true
}
```

## Adding Data

Use `utilities/scripts/facilities_utils/config/new_dataset_schema_template.yaml` as the starting
point for a new source block. Add the block under `sources` in
`utilities/scripts/facilities_utils/config/facilities_master.yaml`, add the class under `taxonomy`
if needed, then run:

```bash
uv run --with pandas --with numpy --with pyyaml \
  python utilities/scripts/facilities_utils/facility_pipeline.py clean --source new_source_key
```

Then rebuild the GPKG:

```bash
uv run --with pandas --with numpy --with pyyaml --with geopandas --with shapely --with pyogrio \
  python utilities/scripts/facilities_utils/facility_pipeline.py build
```

Recompute one changed L3 class without deleting the whole proximity asset:

```bash
uv run --with pandas --with numpy --with pyyaml --with geopandas --with shapely --with pyogrio --with scipy \
  python utilities/scripts/facilities_utils/facility_pipeline.py proximity \
  --classes school_primary --force
```

Refresh L2 output after only taxonomy/filter-logic YAML changes:

```bash
uv run --with pandas --with numpy --with pyyaml --with geopandas --with shapely --with pyogrio --with scipy \
  python utilities/scripts/facilities_utils/facility_pipeline.py proximity --refresh-derived-only
```

For repeated class-by-class proximity rebuilds, skip monitor generation until the
last run:

```bash
uv run --with pandas --with numpy --with pyyaml --with geopandas --with shapely --with pyogrio --with scipy \
  python utilities/scripts/facilities_utils/facility_pipeline.py proximity \
  --classes school_primary --force --skip-monitor

uv run --with pandas --with pyyaml \
  python utilities/scripts/facilities_utils/facility_pipeline.py monitor
```

## Smoke Tests

Keep smoke outputs separate:

```bash
uv run --with pandas --with numpy --with pyyaml \
  python utilities/scripts/facilities_utils/facility_pipeline.py clean --sample-rows 1000 --force

uv run --with pandas --with numpy --with pyyaml --with geopandas --with shapely --with pyogrio \
  python utilities/scripts/facilities_utils/facility_pipeline.py build --gpkg-chunksize 5000

uv run --with pandas --with numpy --with pyyaml --with geopandas --with shapely --with pyogrio --with scipy \
  python utilities/scripts/facilities_utils/facility_pipeline.py proximity \
  --sample-villages 5 --sample-classes 3 \
  --output-gpkg /tmp/village_facility_proximity.gpkg
```

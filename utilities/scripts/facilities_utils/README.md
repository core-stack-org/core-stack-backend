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
# This stores L3 proximity as the durable base and derives L2/L1 from L3.
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

- `village_points`: village representative points copied from the configured admin GPKG.
- `proximity_l3`: lean durable base table with village id, L3 class, distance, and nearest facility uid.
- `proximity_nearest_facilities`: deduplicated facility detail lookup for nearest facilities referenced by L3.
- `proximity_class_map`: small YAML-derived class map from L3 to L2/L1/filter logic.
- `proximity_l2`: SQL view derived from L3 plus `proximity_class_map`.
- `proximity_l1`: SQL view derived from L3 plus `proximity_class_map`.
- Optional materialized L1/L2 tables when `--materialize-derived` is passed after all L3 classes are complete.

The L1/L2 views are intentionally on-demand. For GEE/frontend export or repeated
large reads, rerun proximity with `--materialize-derived` after the L3 base is
complete.

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

Refresh L1/L2 outputs after only taxonomy/filter-logic YAML changes:

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

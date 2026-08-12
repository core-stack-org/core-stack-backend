# CS Admin Boundary Standard Workplan

Date: 2026-07-05
Working branch at start: `feat/facilities_v2`

## Operating Constraints

- Keep RAM usage under control during geospatial builds. Avoid running two large full-India GeoPandas/Fiona writes at the same time.
- Prefer `uv run --with ...` for Python/data runs when dependencies are needed.
- Preserve existing pipeline scripts unless the task explicitly calls for a replacement script.
- Add new scripts for the new standard path:
  - `utilities/scripts/admin_assets/build_cs_admin_boundary_standard.py`
  - `utilities/scripts/admin_assets/build_cs_admin_boundary_assets.py`
- Keep `data/admin-boundary/cs_admin_standard.gpkg` as the primary standard asset.
- Also export:
  - `data/admin-boundary/cs_admin_standard.geojson`
  - `data/admin-boundary/cs_admin_standard.csv`
- Maintain consistent village identifiers and dtypes across admin, facilities, livestock, and Antyodaya outputs.

## Admin Boundary Standard

Primary inputs to inspect before implementation:

- `data/admin-boundary/input/`
- `data/admin-boundary/multi_part_village_analysis.csv`
- `data/admin-boundary/multi_part_village_analysis_file_data.csv`
- `data/admin-boundary/cs_admin_boundary_village_shapes_analysis.py`
- `data/admin-boundary/cs_admin_boundary_village_shapes_analysis.geojson`
- `utilities/scripts/admin_assets/build_admin_boundary_assets.py`

`data/admin-boundary/cs_admin_sanitised.gpkg` is useful as a previous-build reference only. The new standard asset must maintain traceability from raw `data/admin-boundary/input/` rows to `data/admin-boundary/cs_admin_standard.gpkg`, not from `cs_admin_sanitised.gpkg`.

Expected behavior:

- Produce a reproducible standardized village asset with traceable decisions.
- Log omissions, duplicate handling, multipart handling, geometry repair, dtype coercion, and raw source rows.
- Store raw source trace in the output GeoPackage through a non-spatial table.
- Keep multipart villages when the analysis notes or geometry context indicate they are valid distinct/urban/town-like cases.
- Merge or dissolve only when rules and notes make it safe.
- Keep part indexes sorted by descending area share when multipart geometry remains.

## Facilities

Branch-aware work:

- Facilities rebuild work spans `feat/facilities_v2` and `feat/facilities_data_processing`.
- Rebuild proximity using:
  - `data/facilities/outputs/pan_india_facilities.gpkg`
  - `data/admin-boundary/cs_admin_standard.gpkg`
- Target proximity output:
  - `data/facilities/outputs/cs_village_facility_proximity.gpkg`
  - plus a useful CSV sibling for local inspection/API parity.
- Update `computing/misc/facilities_proximity.py` only in the branch where that runtime API change belongs.

## Joined Admin Assets

Create a revised builder from the useful patterns in `build_admin_boundary_assets.py`, but do not depend on the old script at runtime.

Targets:

- With `utilities/scripts/admin_assets/asset_configs/livestock.json`, rebuild:
  - `data/livestock/cs_village_livestock_census_20.gpkg`
  - `data/livestock/cs_village_livestock_census_20.geojson`
- With `utilities/scripts/admin_assets/asset_configs/antyodaya.json`, rebuild:
  - `data/antyodaya/output/cs_antyodaya_2020.gpkg`
  - `data/antyodaya/output/cs_antyodaya_2020.geojson`

The new builder resolves legacy config references to
`data/admin-boundary/cs_admin_standard.gpkg` / layer `cs_admin_standard` by
default. It prepends the standard identity columns (`cs_feature_id`,
`cs_admin_uid`, `core_admin_uid`, `pc11_village_id`, `village_id`) before the
configured joined columns and writes unmatched-admin and unmatched-source CSV
reports beside each asset.

## Monitoring

For long-running steps, run one heavy geospatial build at a time unless a companion task is clearly lightweight. Check memory/process status periodically with commands such as:

```bash
ps -eo pid,ppid,pmem,rss,etime,cmd --sort=-rss | head -20
free -h
```

Use concise monitoring notes and avoid dumping full logs unless diagnosing a failure.

Current full admin-standard build is running comfortably below the original
RAM ceiling. If a future rebuild is bottlenecked mostly by conservative
chunking, it is acceptable to tune chunk sizes for a managed target around
4 GB RAM, while still avoiding simultaneous full-India geometry writes.

External data-preparation scripts may use `pyogrio`/GeoPandas for speed. The
Django/API facilities runtime in `computing/misc/facilities_proximity.py`
should not introduce a `pyogrio` dependency.

## 2026-07-05 Build Checkpoint

Completed and validated:

- `data/admin-boundary/cs_admin_standard.gpkg`
  - Layer rows: `630,898`
  - Raw source-trace rows: `983,551`
  - Trace status counts: `971,657` output, `11,894` omitted
  - Source files read OK: `660/660`
  - `pc11_village_id = 800372`: exactly `1` retained ID row; `96`
    geometries retained with the village id removed as invalid fragments
  - SQLite `quick_check`: `ok`
  - Peak logged RSS: `3,876.5 MB`
- `data/livestock/cs_village_livestock_census_20.gpkg`
  - Layer rows: `630,898`
  - Matched admin rows: `474,240`
  - SQLite `quick_check`: `ok`
  - GeoJSON sibling written
- `data/antyodaya/output/cs_antyodaya_2020.gpkg`
  - Layer rows: `630,898`
  - Matched admin rows: `545,874`
  - SQLite `quick_check`: `ok`
  - GeoJSON sibling written

Facilities correction:

- `data/facilities/outputs/cs_village_facility_proximity.gpkg`
  - The final village feature layer is `village_shapes`, not `village_points`.
  - `village_shapes` geometry type: `MULTIPOLYGON`, CRS `EPSG:4326`
  - `village_shapes` rows: `630,898`
  - `proximity_l3` rows: `15,772,450`
  - `proximity_l2_materialized` rows: `6,939,878`
  - `proximity_l1_materialized` rows: `3,154,490`
  - `proximity_nearest_facilities` rows: `1,711,631`
  - SQLite `quick_check`: `ok`
- `data/facilities/outputs/cs_village_facility_proximity_summary.csv`
  - 40 rows: table counts, 25 L3 class summaries, metadata
- Smoke build from `utilities/scripts/facilities_utils/facility_proximity_finder.py`
  writes `village_shapes` as `MULTIPOLYGON` and does not create a
  `village_points` layer.

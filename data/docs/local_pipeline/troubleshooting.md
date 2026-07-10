# Troubleshooting Guide

Scenarios are ordered by where they occur in the request lifecycle. Each
entry: what you see, why it happens, what to do.

## Request and Scope

### 400: "Request must include a 'scope' object"
The API accepts only the structured body. Flat bodies
(`{"state": ..., "district": ..., "block": ...}`) were removed with the
legacy formats. Wrap the fields:
`{"scope": {"level": "tehsil", "state_name": ..., "district_name": ..., "tehsil_name": ...}}`.

### `ValueError: No admin rows found for scope`
The scope did not match any rows in `cs_admin_standard.gpkg`.
- Names are matched case-insensitively with whitespace collapsed, but they
  must otherwise be exact. Check spelling against the boundary:
  `sqlite3 data/admin-boundary/cs_admin_standard.gpkg "SELECT DISTINCT TEHSIL FROM cs_admin_standard WHERE lower(district_name)='ranchi'"`.
- Some blocks in `data/proposed_blocks_active_locations.json` use frontend
  labels that differ from boundary names; the active-locations test suite's
  `--admin-resolvable-only` flag reports exactly which ones resolve.
- For `level: village`, ids are matched against both `village_id` and
  `pc11_village_id`.

### `ValueError: state_name is required for state/district/tehsil scopes`
The scope level demands parent names: `tehsil` needs state + district +
tehsil; `district` needs state + district; `village` needs `village_ids`.

## Sources and Sidecars

### First run of Antyodaya/Livestock is very slow, later runs fast
Expected: the first run imports the source CSV into the SQLite sidecar
(chunked; the 634 MB Antyodaya CSV takes minutes on slow mounts). The
sidecar is reused afterwards. If you must pre-warm:
`CSVSQLiteSidecar(...).materialize()` or just run any small tehsil once.

### Sidecar rebuilds on every run
`is_fresh()` compares source CSV size and mtime with the sidecar's
metadata table. If a sync tool keeps touching the CSV's mtime, the
signature never matches. Stop the toucher, or re-copy the CSV once and let
one full materialisation complete.

### Run was killed during sidecar build — is the sidecar corrupt?
Safe: the metadata row is written only after a complete import, so a
partial sidecar fails `is_fresh()` and is rebuilt from scratch on the next
run. You can also delete `<csv>.sqlite*` files manually.

### `Missing required column` validation issues in the result
The source CSV header changed relative to the pipeline YAML
(`source_location_columns`, `metrics.count_columns`, validation suffixes).
Compare `csv_header(...)` output with the YAML and update the config — the
validation list in the result names each missing column.

### Facilities: `missing_after_supplemental` lists classes
No candidate facility of those L3 classes was found even in the widened
search box. Nearest columns for those classes will be null. If a class
should exist, check the pan-India GPKG contents; if the geography is
genuinely far from such facilities, this is correct behaviour.

## Admin and GeoPackage Reads

### `sqlite3.OperationalError: attempt to write a readonly database` during index creation
First use of a GeoPackage creates lookup indexes, which needs write access
to the `.gpkg` file. Fix file permissions, or pre-create indexes once from
a user with write access (any successful run does this).

### PROJ errors on stderr (`proj_create_from_database: Open of .../share/proj failed`)
Environment noise from conda's PROJ data path; reads/writes still work in
our flows because geometries stay in EPSG:4326. To silence, set
`PROJ_LIB=$CONDA_PREFIX/share/proj` (or install `proj-data`).

### `ValueError: No non-empty layers were provided for GeoPackage output`
Every layer frame handed to `write_gpkg` was empty — usually an admin scope
with zero geometry rows (shouldn't happen; scope resolution errors first)
or all-null geometry. Inspect the admin rows for the scope.

## Outputs and Contracts

### CSV has `l2_*`/`l3_*` or `*_feat_value` columns
You are reading the GPKG frame (or an old output directory), not the report
CSV. The report CSV carries the human contract; machine columns live in the
GPKG on purpose. Delete stale output directories if in doubt — everything
is regenerated.

### Status column missing / present where unwanted
Controlled by `output_contract.status_column.outputs` in the pipeline YAML
(`[csv, gpkg, geoserver]` by default). Remember the GeoServer layer is
published from the GPKG frame, so `gpkg` and `geoserver` cannot disagree.

### Column descriptions are null in metadata / README
The describer found no rule for that column. For facilities, check the
class/group exists in `facility_classifications.yaml` with a `label` and
the description templates are present; for antyodaya, the category title
lookup falls back to a title-cased column stem; for livestock, the column
must appear in the schema YAML structure.

### Cached result returned when you expected a fresh run
`publish.use_pregenerated: true` plus an unchanged cache key + input
signatures = cache hit. Either set `use_pregenerated: false`, change the
request, or touch/replace an input (its size/mtime signature invalidates
the manifest).

### Stale files in an output directory
`OutputBundle` overwrites by name, and stale `geoserver_links.csv` files
are removed on non-published runs, but files from removed artifact types
can linger in old directories. Output directories are disposable; delete
them.

## Publishing

### `geoserver.status = publish_failed`
The run succeeded; only publishing failed. `geoserver.error_type` /
`geoserver.error` carry the cause — usually credentials, workspace
missing, or network. Re-run with `publish.sync_to_geoserver: true` after
fixing; with `overwrite: true` to replace an existing layer.

### Layer published to the wrong workspace
Resolution order: request `publish.geoserver_workspace` → pipeline YAML
`output.geoserver_workspace` → constant in `utilities/constants.py`. Test
branches pin the YAML to `testworkspace` on purpose.

## Celery / API

### API responds success but nothing happens
The response only means the task was queued. Check a Celery worker is
consuming the `nrm` queue: `celery -A nrm_app worker -Q nrm ...`, and look
at the worker log for the task run and its result dict.

### Task raises `TypeError` about unexpected arguments
The tasks now take a single `payload` argument (legacy
state/district/block kwargs were removed). Old queued messages or callers
must send `kwargs={"payload": {...}}`.

## Performance

| Symptom | Likely cause | Lever |
| --- | --- | --- |
| Slow first request on a machine | Index/sidecar creation | Expected once; pre-warm with a small tehsil |
| Slow facilities requests everywhere | Very large scope (state level) | Scope smaller; the KD-tree work scales with villages × classes |
| Slow on WSL against `/mnt/*` | drvfs I/O is slow for big scans | Keep sidecars/data on the Linux filesystem, or accept first-run cost |
| Every run slow despite sidecar | Sidecar signature never fresh | See "Sidecar rebuilds on every run" |

The result's `timings` dict breaks down each phase
(`read_admin_seconds`, `read_*_seconds`, `build_outputs_seconds`,
`write_local_outputs_seconds`, `publish_geoserver_seconds`) — read it
before guessing.

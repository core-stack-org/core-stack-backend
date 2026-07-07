# Core Stack Asset Workflow Branch Log

Date: 2026-07-06
Current worktree branch while preparing this log: `feat/facilities_v2`

This file records the scripts/configs created or revised during the standard
asset rebuild, which branch each logical group should go to, and reviewable git
commands the maintainer can run manually.

## Actual Split Branches Created

These branches were created after the first handoff attempt so every affected
file can be reviewed as its own commit. The earlier branches
`feat/cs-admin-standard-assets` and `feat/facilities_data_processing` are still
present locally/remotely, but they contain bundled commits and should be treated
as superseded by the split branches below.

- `feat/cs-admin-standard-assets-split`
  - `7ed68ede` Add CS admin standard builder
  - `9bfca12b` Add CS admin asset join builder
  - `3f710ef6` Document CS admin standard workflow
  - `42c48039` Point livestock asset config to CS admin standard
  - `90345ca4` Point Antyodaya asset config to CS admin standard
- `feat/facilities-data-processing-cs-proximity-split`
  - `12a46cf6` Write facilities proximity village polygons
  - `06efcfb7` Point facilities master config to CS proximity outputs
  - `14788d8c` Document facilities CS proximity output
  - `abe5b130` Refresh facilities metadata monitor config
  - `814b57c0` Report facilities village shapes metadata
  - `c8d1cf18` Document facilities CS proximity workflow
- `feat/facilities_v2`
  - `6daec292` Write facilities proximity API summary CSV
  - `08a427da` Use CS facilities proximity source constant
- `feat/facilities_gee_assets`
  - `dc29fda6` Point facilities GEE proximity source to CS output
  - `127525ad` Document facilities GEE ingestion workflow
- `feat/core-stack-gee-ingestion-split`
  - `2d07c316` Add Core Stack GEE ingestion script
  - `e7035b52` Add Core Stack GEE asset config
  - `4f1dcebf` Document Core Stack GEE ingestion
  - `3b4d882c` Record Core Stack asset workflow branches

## Created Scripts

- `utilities/scripts/admin_assets/build_cs_admin_boundary_standard.py`
  - Purpose: Build `cs_admin_standard` directly from raw
    `data/admin-boundary/input/` with source trace and omission reports.
  - Suggested branch: `feat/cs-admin-standard-assets`
- `utilities/scripts/admin_assets/build_cs_admin_boundary_assets.py`
  - Purpose: Join standardized admin shapes to configured datasets for
    livestock and Antyodaya assets.
  - Suggested branch: `feat/cs-admin-standard-assets`
- `utilities/scripts/gee/core_stack_gee_ingest.py`
  - Purpose: General config-driven GPKG/GeoJSON to GEE ingestion using the
    TSV-to-GCS-to-EE-manifest path.
  - Suggested branch: `feat/core-stack-gee-ingestion`
- `utilities/scripts/facilities_utils/facilities_gee_assets.py`
  - Purpose: Facilities-specific GEE uploader/explorer support created before
    the general Core Stack ingestion wrapper.
  - Suggested branch: `feat/facilities_gee_assets` or
    `feat/facilities_data_processing`

## Created Configs And Docs

- `utilities/scripts/admin_assets/cs_admin_boundary_standard_workplan.md`
  - Suggested branch: `feat/cs-admin-standard-assets`
- `utilities/scripts/admin_assets/asset_configs/livestock.json`
  - Points livestock joins directly at `cs_admin_standard.gpkg` and the final
    `cs_village_livestock_census_20` output paths.
  - Suggested branch: `feat/cs-admin-standard-assets`
- `utilities/scripts/admin_assets/asset_configs/antyodaya.json`
  - Points Antyodaya joins directly at `cs_admin_standard.gpkg` and the final
    `cs_antyodaya_2020` output paths.
  - Suggested branch: `feat/cs-admin-standard-assets`
- `utilities/scripts/gee/core_stack_gee_assets.yaml`
  - Suggested branch: `feat/core-stack-gee-ingestion`
- `utilities/scripts/gee/core_stack_gee_ingest.md`
  - Suggested branch: `feat/core-stack-gee-ingestion`
- `utilities/scripts/gee/core_stack_asset_workflow_branch_log.md`
  - Suggested branch: `feat/core-stack-gee-ingestion`
- `utilities/scripts/facilities_utils/facilities_gee_assets.yaml`
  - Points the facilities-specific GEE proximity source at the corrected
    `cs_village_facility_proximity.gpkg`.
  - Suggested branch: `feat/facilities_gee_assets` or
    `feat/facilities_data_processing`

## Revised Existing Files

- `utilities/scripts/facilities_utils/facility_proximity_finder.py`
  - Keeps actual polygon `village_shapes` in the proximity GPKG.
  - Uses representative points only as helper latitude/longitude columns.
  - Materializes L1/L2 in logged groups to avoid long silent SQLite queries.
  - Suggested branch: `feat/facilities_data_processing`
- `utilities/scripts/facilities_utils/config/facilities_master.yaml`
  - Points proximity generation at `cs_admin_standard.gpkg` and
    `cs_village_facility_proximity.gpkg`.
  - Suggested branch: `feat/facilities_data_processing`
- `utilities/scripts/facilities_utils/config/facilities_overview.yaml`
  - Documents `village_shapes` and CS proximity output path.
  - Suggested branch: `feat/facilities_data_processing`
- `utilities/scripts/facilities_utils/config/facilities_metadata_monitor.yaml`
  - Regenerated after the corrected `cs_village_facility_proximity.gpkg`.
  - Suggested branch: `feat/facilities_data_processing`
- `utilities/scripts/facilities_utils/facility_metadata_monitor.py`
  - Reports `village_shapes` instead of `village_points`.
  - Suggested branch: `feat/facilities_data_processing`
- `utilities/scripts/facilities_utils/README.md`
  - Updates proximity output path and shape-layer description.
  - Suggested branch: `feat/facilities_data_processing`
- `utilities/scripts/facilities_utils/GEE_INGESTION_README.md`
  - Documents the facilities-specific TSV/GCS/GEE path and updates source
    proximity path to the `cs_` GPKG.
  - Suggested branch: `feat/facilities_gee_assets` or
    `feat/facilities_data_processing`
- `computing/misc/facilities_proximity.py`
  - Adds a per-tehsil summary CSV next to local API output GPKGs.
  - Suggested branch: `feat/facilities_v2`
- `utilities/constants.py`
  - Points Django/API facilities proximity source to
    `data/facilities/outputs/cs_village_facility_proximity.gpkg`.
  - Suggested branch: `feat/facilities_v2`

## Superseded Workflow Pieces

- `data/admin-boundary/cs_admin_sanitised.gpkg`
  - No longer used as lineage for the standard admin asset.
- `utilities/scripts/admin_assets/build_admin_boundary_assets.py`
  - Not deleted, but superseded for the new CS standard path by
    `build_cs_admin_boundary_standard.py` and
    `build_cs_admin_boundary_assets.py`.
- `data/facilities/outputs/village_facility_proximity.gpkg`
  - Legacy proximity asset; replaced for current work by
    `cs_village_facility_proximity.gpkg`.
- `village_points` as the stored proximity feature layer
  - Removed from the corrected CS proximity GPKG. The stored feature layer is
    `village_shapes` with actual polygons.
- `utilities/scripts/facilities_utils/facilities_gee_assets.py`
  - Still useful for the existing facilities v2 flow, but the general Core
    Stack path should use `utilities/scripts/gee/core_stack_gee_ingest.py`.

## Verification Checkpoints

- `cs_admin_standard.gpkg`: 630,898 rows, SQLite `quick_check ok`
- `cs_admin_standard.csv`: 630,898 parsed rows and 630,898 unique
  `cs_feature_id` values; raw line count is higher because one quoted text
  field contains an embedded newline.
- `cs_village_facility_proximity.gpkg`:
  - `village_shapes`: 630,898 `MULTIPOLYGON` rows
  - `village_shapes.pc11_village_id`: `INTEGER`, with no fractional non-null
    values
  - `proximity_l3`: 15,772,450 rows
  - `proximity_l2_materialized`: 6,939,878 rows
  - `proximity_l1_materialized`: 3,154,490 rows
  - SQLite `quick_check ok`
- `cs_village_livestock_census_20.gpkg`: 630,898 rows, SQLite `quick_check ok`
- `cs_antyodaya_2020.gpkg`: 630,898 rows, SQLite `quick_check ok`
- `utilities/scripts/admin_assets/asset_configs/livestock.json` and
  `utilities/scripts/admin_assets/asset_configs/antyodaya.json` now point
  directly at `cs_admin_standard.gpkg`; the new builder no longer silently
  rewrites legacy admin config paths.
- Livestock and Antyodaya were rebuilt from those explicit configs; their
  GeoPackage metadata tables record the current config hashes and
  `admin_layer=cs_admin_standard`.
- GEE ingestion smoke build:
  - `core_stack_gee_ingest.py build --asset all --limit 10 --jobs 2`
  - All five configured assets wrote 10 rows under 50 MB RSS.
- GEE full local build:
  - Completed at `2026-07-06T03:05:01Z` and wrote
    `data/gee/core_stack/core_stack_gee_build_summary.yaml`.
  - TSV line counts matched `rows + 1 header` for all five assets.
  - `cs_admin_standard`: 630,898 rows, 35 columns, 2.0 GB TSV
  - `cs_village_facility_proximity`: 630,898 rows, 100 columns, 2.9 GB TSV
  - `cs_pan_india_facilities`: 5,029,052 rows, 25 columns, 1.5 GB TSV
  - `cs_village_livestock_census_20`: 630,898 rows, 30 columns, 1.8 GB TSV
  - `cs_antyodaya_2020`: 630,898 rows, 294 columns, 2.8 GB TSV
- GEE upload/task submission:
  - Completed GCS staging and wrote
    `data/gee/core_stack/core_stack_gee_upload_summary.yaml` at
    `2026-07-06T03:27:50Z`.
  - Status check at `2026-07-06T04:03:55Z` reported all five Earth Engine
    ingestion operations as `SUCCEEDED` with no error messages.
  - Rebuilt and replaced `cs_pan_india_facilities` with
    `facility_latitude`/`facility_longitude` aliases after verifying that Earth
    Engine folds raw `latitude`/`longitude` into `longitude_latitude`.
  - Replacement status check at `2026-07-06T05:19:01Z` reported
    `cs_pan_india_facilities` as `SUCCEEDED`.
  - Final GEE verification at `2026-07-06T06:32:53Z` reported expected feature
    counts for all five assets and the corrected facilities coordinate
    properties.
  - `make-public` confirmed all five assets exist, but IAM denied public ACL
    updates with `earthengine.assets.setIamPolicy`.

## Chained Commands

Review current branch state:

```bash
git status --short --branch && git diff --stat
```

Create the admin-standard branch from `dev` and commit the admin work:

```bash
git stash push --include-untracked -m cs-admin-standard-assets -- utilities/scripts/admin_assets/build_cs_admin_boundary_standard.py utilities/scripts/admin_assets/build_cs_admin_boundary_assets.py utilities/scripts/admin_assets/cs_admin_boundary_standard_workplan.md utilities/scripts/admin_assets/asset_configs/livestock.json utilities/scripts/admin_assets/asset_configs/antyodaya.json && git fetch origin && git checkout dev && git pull --rebase origin dev && git checkout -B feat/cs-admin-standard-assets && git stash pop 'stash^{/cs-admin-standard-assets}' && git add utilities/scripts/admin_assets/build_cs_admin_boundary_standard.py utilities/scripts/admin_assets/build_cs_admin_boundary_assets.py utilities/scripts/admin_assets/cs_admin_boundary_standard_workplan.md utilities/scripts/admin_assets/asset_configs/livestock.json utilities/scripts/admin_assets/asset_configs/antyodaya.json && git commit -m "Add reproducible CS admin standard asset builders"
```

Create/update the facilities data-processing branch and commit the corrected
proximity build path:

```bash
git stash push --include-untracked -m facilities-data-processing-cs-proximity -- utilities/scripts/facilities_utils/facility_proximity_finder.py utilities/scripts/facilities_utils/config/facilities_master.yaml utilities/scripts/facilities_utils/config/facilities_overview.yaml utilities/scripts/facilities_utils/config/facilities_metadata_monitor.yaml utilities/scripts/facilities_utils/facility_metadata_monitor.py utilities/scripts/facilities_utils/README.md && git fetch origin && git checkout dev && git pull --rebase origin dev && git checkout -B feat/facilities_data_processing origin/feat/facilities_data_processing && git stash pop 'stash^{/facilities-data-processing-cs-proximity}' && git add utilities/scripts/facilities_utils/facility_proximity_finder.py utilities/scripts/facilities_utils/config/facilities_master.yaml utilities/scripts/facilities_utils/config/facilities_overview.yaml utilities/scripts/facilities_utils/config/facilities_metadata_monitor.yaml utilities/scripts/facilities_utils/facility_metadata_monitor.py utilities/scripts/facilities_utils/README.md && git commit -m "Rebuild facilities proximity on CS admin polygons"
```

Commit the facilities-specific GEE uploader separately if that branch is still
being kept:

```bash
git stash push --include-untracked -m facilities-gee-assets -- utilities/scripts/facilities_utils/facilities_gee_assets.py utilities/scripts/facilities_utils/facilities_gee_assets.yaml utilities/scripts/facilities_utils/GEE_INGESTION_README.md && git fetch origin && git checkout dev && git pull --rebase origin dev && git checkout -B feat/facilities_gee_assets && git stash pop 'stash^{/facilities-gee-assets}' && git add utilities/scripts/facilities_utils/facilities_gee_assets.py utilities/scripts/facilities_utils/facilities_gee_assets.yaml utilities/scripts/facilities_utils/GEE_INGESTION_README.md && git commit -m "Add facilities GEE asset ingestion workflow"
```

Create the general Core Stack GEE ingestion branch and commit the new standard
uploader:

```bash
git stash push --include-untracked -m core-stack-gee-ingestion -- utilities/scripts/gee/core_stack_gee_ingest.py utilities/scripts/gee/core_stack_gee_assets.yaml utilities/scripts/gee/core_stack_gee_ingest.md utilities/scripts/gee/core_stack_asset_workflow_branch_log.md && git fetch origin && git checkout dev && git pull --rebase origin dev && git checkout -B feat/core-stack-gee-ingestion && git stash pop 'stash^{/core-stack-gee-ingestion}' && git add utilities/scripts/gee/core_stack_gee_ingest.py utilities/scripts/gee/core_stack_gee_assets.yaml utilities/scripts/gee/core_stack_gee_ingest.md utilities/scripts/gee/core_stack_asset_workflow_branch_log.md && git commit -m "Add standard Core Stack GEE ingestion pipeline"
```

Commit the facilities API runtime change on `feat/facilities_v2`:

```bash
git stash push -m facilities-v2-cs-proximity-runtime -- computing/misc/facilities_proximity.py utilities/constants.py && git fetch origin && git checkout feat/facilities_v2 && git pull --rebase origin feat/facilities_v2 && git stash pop 'stash^{/facilities-v2-cs-proximity-runtime}' && git add computing/misc/facilities_proximity.py utilities/constants.py && git commit -m "Use CS village facilities proximity source in API export"
```

## GEE Staging Commands

The full local TSV build was started with:

```bash
nohup uv run --with pyyaml --with shapely python utilities/scripts/gee/core_stack_gee_ingest.py build --asset all --jobs 2 --overwrite --max-rss-mb 5000 > logs/core_stack_gee_build_full_20260706.log 2>&1 &
```

After `core_stack_gee_build_summary.yaml` confirms row counts, start GCS
staging and Earth Engine ingestion tasks:

```bash
nohup uv run --with earthengine-api --with google-cloud-storage --with pyyaml --with shapely python utilities/scripts/gee/core_stack_gee_ingest.py upload --asset all --jobs 2 --replace-existing > logs/core_stack_gee_upload_20260706.log 2>&1 &
```

Monitor submitted Earth Engine operations from the latest upload summary:

```bash
uv run --with earthengine-api --with google-cloud-storage --with pyyaml python utilities/scripts/gee/core_stack_gee_ingest.py status
```

This writes `data/gee/core_stack/core_stack_gee_status.yaml` as the latest
local status snapshot.

Make completed assets public without reuploading data:

```bash
uv run --with earthengine-api --with google-cloud-storage --with pyyaml python utilities/scripts/gee/core_stack_gee_ingest.py make-public --asset all
```

Verify completed GEE assets and sample schemas:

```bash
uv run --with earthengine-api --with google-cloud-storage --with pyyaml python utilities/scripts/gee/core_stack_gee_ingest.py verify --asset all
```

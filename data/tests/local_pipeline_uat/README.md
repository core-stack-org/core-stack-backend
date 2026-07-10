# Local Pipeline V3 UAT Handoff

This folder preserves the UAT-ready integration state for the local geospatial
pipeline work. It is intentionally tracked on the temporary test branch even
though `data/` is normally ignored.

## Branch State

- Integration branch: `test/local-pipeline-v3-uat`
- Base: freshly updated `origin/dev`
- Merged branches:
  - `feat/local_geospatial_core_v3`
  - `feat/facilities_v3`
  - `feat/antyodaya_v3`
  - `feat/livestocks`

## Postman UAT

- Collection: `core_stack_local_pipeline_uat.postman_collection.json`
- Environment: `core_stack_local_pipeline_uat.postman_environment.json`
- Source active locations: `data/proposed_blocks_active_locations.json`
- Active locations: 414
- Requests generated: 1,242

Import the collection and environment in Postman, then set:

- `base_url`: local or UAT API root, for example `http://localhost:8000/api/v1`
- `sync_to_geoserver`: `true` for Postman UAT publishing, `false` for API smoke only
- `overwrite`: `true` when re-running UAT
- `use_pregenerated`: `false` for fresh runs, `true` when checking cache reuse

All generated requests use structured payloads:

```json
{
  "scope": {
    "level": "tehsil",
    "state_name": "...",
    "district_name": "...",
    "tehsil_name": "..."
  },
  "outputs": {},
  "publish": {
    "sync_to_geoserver": true,
    "overwrite": true,
    "register_layers": false,
    "use_pregenerated": false
  }
}
```

## Validation Commands

Compile check:

```bash
/home/amitportal/miniconda3/envs/corestack-backend/bin/python -m compileall -q \
  computing/misc/local_pipeline \
  computing/misc/facilities \
  computing/misc/antyodaya \
  computing/misc/livestocks \
  computing/api.py
```

Small local runner smoke without GeoServer:

```bash
/home/amitportal/miniconda3/envs/corestack-backend/bin/python \
  computing/misc/local_pipeline/tests/local_pipeline_active_locations_test.py \
  --pipelines facilities antyodaya livestocks \
  --max-tehsils 1 \
  --all-sample 0 \
  --metadata-sample 1 \
  --district-sample 0 \
  --state-sample 0 \
  --village-sample 0 \
  --api-smoke \
  --admin-resolvable-only \
  --output-dir data/local_pipeline_test_runs/uat_smoke
```

GeoServer tests must use only the test workspace configured in the pipeline
YAML files (`testworkspace`) or passed explicitly to the local test harness.

## Validation Result

Last local smoke run:

- Date: 2026-07-09 UTC
- Output directory: `data/local_pipeline_test_runs/uat_smoke_20260709T233615Z`
- Scope selected by the harness: `Jharkhand / Ramgarh / Patratu`
- Cases: 9
- Passed: 9
- Failed: 0
- API normalization smoke: 6/6 passed
- Compile check: passed

Timing summary:

| Pipeline | Case | Cache | Wall seconds | Status |
| --- | --- | --- | ---: | --- |
| facilities | default | false | 5.302 | success |
| facilities | default | true | 0.306 | cached |
| facilities | metadata_only | false | 5.761 | success |
| antyodaya | default | false | 225.585 | success |
| antyodaya | default | true | 0.320 | cached |
| antyodaya | metadata_only | false | 0.667 | success |
| livestocks | default | false | 8.775 | success |
| livestocks | default | true | 0.229 | cached |
| livestocks | metadata_only | false | 0.416 | success |

Output checks:

- Default GPKGs had expected village layers with 76 rows for all three pipelines.
- Default CSVs existed for all three pipelines.
- CSV checks confirmed `cs_feature_id` was not present.
- Metadata-only cases correctly skipped CSV/GPKG outputs.
- No missing result paths were reported.

Performance note:

- The Antyodaya first run was slow because the ignored SQLite sidecar
  `data/base_resources/cs_antyodaya_2020_cluster_analysis.csv.sqlite` was
  being built/updated. After that, cache reuse and metadata-only runs completed
  in less than one second for the same tehsil.

GeoServer publish smoke:

- Date: 2026-07-10 UTC
- Output directory: `data/local_pipeline_test_runs/uat_geoserver_fixed_20260710T000444Z`
- Workspace: `testworkspace`
- Cases: 6
- Passed: 6
- Failed: 0
- GeoServer status: all publish attempts returned `geoserver.status = published`
- WFS verification: passed for all uploaded layers

Verified uploaded layers:

| Pipeline | Layer | Source rows | WFS rows | WFS properties |
| --- | --- | ---: | ---: | ---: |
| facilities | `facilities_ramgarh_patratu` | 76 | 76 | 131 |
| antyodaya | `antyodaya20_ramgarh_patratu` | 76 | 76 | 225 |
| livestocks | `livestocks_ramgarh_patratu` | 76 | 76 | 25 |

The GeoServer issue was traced to using `test_workspace`, which existed without
a matching namespace and caused GeoServer REST uploads to return HTTP 500. The
valid test workspace is `testworkspace`. The local pipeline publisher now
validates the workspace/namespace, uploads GeoPackages directly, publishes the
native GPKG layer under the unique pipeline layer name, and verifies WFS
GeoJSON properties before reporting success.

## PR Order

1. `feat/local_geospatial_core_v3`
2. `feat/facilities_v3`
3. `feat/antyodaya_v3`
4. `feat/livestocks`

Open PRs in this order. The three domain branches are stacked on the core
branch, so their visible diffs against `dev` will shrink after the core PR is
merged.

## PR Message Drafts

### feat/local_geospatial_core_v3

Title: Add local geospatial pipeline core

Body: Adds shared local pipeline utilities for indexed GeoPackage reads,
standard admin scope selection, keyed CSV sidecars, output bundle writing,
cache manifests, batch request parsing, and GeoServer publishing. This is the
foundation for fast local vector/tabular pipelines without `pyogrio` in runtime
API code.

### feat/facilities_v3

Title: Implement facilities local pipeline v3

Body: Replaces the facilities runtime with a structured local pipeline package
using `cs_admin_standard.gpkg` and the standard pan-India facilities asset.
The branch adds classification config, nearest-service calculations, standard
CSV/GPKG/README/metadata/STAC outputs, cache reuse, and structured API payload
support for UAT.

### feat/antyodaya_v3

Title: Implement Antyodaya local pipeline v3

Body: Adds a keyed Antyodaya 2020 local pipeline package with category mapping,
standard admin joins, focused CSV/GPKG outputs, metadata EDA, STAC fragments,
cache reuse, structured API payload support, and tracked report/blog PDFs under
`docs/reports`.

### feat/livestocks

Title: Implement livestock local pipeline v3

Body: Adds a keyed 20th Livestock Census local pipeline package with compact
schema config, derived animal totals, standard CSV/GPKG/README/metadata/STAC
outputs, cache reuse, and structured API payload support for test-workspace
UAT.

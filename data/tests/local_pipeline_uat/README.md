# Local Pipeline V3 UAT Handoff

This folder contains test assets only. The test branch can be applied on top of
`dev` or any local-pipeline implementation branch; it does not merge or copy
implementation commits. This keeps UAT reusable while the five implementation
branches progress independently toward `dev`, UAT, and `main`.

## Branch State

- Test-only branch: `test/local-pipeline-v3-uat`
- Base: `origin/dev`
- Contains: Python harness, focused module tests, Postman assets, and test docs
- Requires: merge/pull the test branch over the implementation branch being assessed

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
- `include_tehsil_layers`: `true` to exercise bounded same-tehsil GeoLibre discovery
- `geolibre_max_layers`: upper bound for GeoLibre WFS layer discovery

All generated requests use structured payloads:

```json
{
  "scope": {
    "level": "tehsil",
    "state_name": "...",
    "district_name": "...",
    "tehsil_name": "..."
  },
  "outputs": {"geolibre": true},
  "publish": {
    "sync_to_geoserver": true,
    "overwrite": true,
    "register_layers": false,
    "use_pregenerated": false
  },
  "geolibre": {
    "include_tehsil_layers": "{{include_tehsil_layers}}",
    "max_layers": "{{geolibre_max_layers}}",
    "publish_to_aws": false
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

Finalized-contract focused smoke (not the full suite):

- Date: 2026-07-12 UTC
- Location: `Gujarat / Banas Kantha / Palanpur`
- Pipeline cases: 6/6 passed (fresh plus cache reuse for all three pipelines)
- API normalization calls: 9/9 passed (simple, structured GeoLibre, and invalid-body cases)
- Shared module tests: 13/13 passed
- Facilities point package: 1,220 tehsil facilities and 2,975 village-nearest associations across exactly two layers
- Forbidden output artifacts: none
- Links manifests: valid UTF-8 with local, GeoServer, and GeoLibre sections
- Metadata: all emitted layers include columns, rename mapping, and EDA blocks

The full scale suite and live three-layer GeoServer publish were intentionally
not rerun in this pass.

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

The result above predates the finalized output contract. Current tests instead
assert GeoPackage-only data exports, one UTF-8 links manifest, no CSV or STAC
artifacts, complete column-description/rename-mapping/EDA metadata, GeoLibre
payload propagation, and exactly two layers in the facilities point package.

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

## Implementation Merge Order

1. `feat/local_geospatial_core_v3`
2. `feat/facilities_v3`
3. `feat/antyodaya_v3`
4. `feat/livestocks`
5. `feat/geolibre-local-pipeline`

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
The branch adds classification config, nearest-service calculations, village
properties plus two point-collection layers, unified metadata/links, cache
reuse, and structured API payload support for UAT.

### feat/antyodaya_v3

Title: Implement Antyodaya local pipeline v3

Body: Adds a keyed Antyodaya 2020 local pipeline package with category mapping,
standard admin joins, GeoPackage outputs, metadata EDA and column mappings,
cache reuse, structured API payload support, and tracked report/blog PDFs under
`docs/reports`.

### feat/livestocks

Title: Implement livestock local pipeline v3

Body: Adds a keyed 20th Livestock Census local pipeline package with compact
schema config, derived animal totals, GeoPackage/README/metadata/links outputs,
cache reuse, and structured API payload support for test-workspace
UAT.

### feat/geolibre-local-pipeline

Title: Add GeoLibre links to local pipeline outputs

Body: Adds small GeoLibre project and clickable HTML outputs backed by verified
GeoServer WFS layers, a single JSON links manifest, explicit facilities
multi-layer projects, and optional non-redundant AWS project hosting.

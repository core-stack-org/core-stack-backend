# Test Report: Local Pipeline v3 Output Contracts (2026-07-09/10)

Branch under test: `worktree-local-pipeline-output-contracts` (on top of
`test/local-pipeline-v3-integration`). All runs executed in-process from the
worktree with the `corestack-backend` environment; GeoServer runs published
live to `testworkspace`.

## What Was Tested

1. **Active-location coverage audit** — all 414 blocks in
   `data/proposed_blocks_active_locations.json` resolved against
   `cs_admin_standard.gpkg`.
2. **End-to-end matrix** — 96 cases across the three pipelines
   (facilities, antyodaya, livestocks) × scope levels (tehsil, district,
   state, village) × output variants (full bundle, data-only,
   metadata-only) × cache behaviour, with per-case data-quality checks.
   Full detail: [2026-07-09-matrix-sample.md](2026-07-09-matrix-sample.md).
3. **API contract smoke** — structured bodies queue on the `nrm` queue;
   flat legacy bodies are rejected with 400 and queue nothing.
4. **GeoServer publishing** — all three pipelines published for
   Jharkhand/Ranchi/Bero to `testworkspace`, verified over live WFS.

## Headline Results

| Suite | Result |
| --- | --- |
| Admin resolution of active locations | 405/414 resolve (9 label mismatches, listed below) |
| End-to-end matrix | **96/96 passed** |
| Report CSV contract checks (87 CSVs) | admin prefix 87/87, status placement 87/87, dataset contracts 87/87, zero machine/feature column leaks, zero undocumented columns |
| Cache repeats (`use_pregenerated`) | 30/30 served from manifest |
| API smoke | 6/6 (3 × structured accepted, 3 × legacy rejected) |
| GeoServer publish | 3/3 pipelines published + WFS verified; cached repeats reuse the manifest |

## Defects Found and Fixed During Testing

1. **GeoServer 500 on publish** — the pre-existing `test_workspace` on the
   server had no matching namespace, so datastore upload failed with an
   NPE-backed 500. Resolution (from the team): use the valid `testworkspace`.
   All pipeline YAMLs, test defaults, and docs now point at `testworkspace`.
   Note: `computing/utils.py` still references `test_workspace` in an
   unrelated core flow (`generate_shape_files`); left untouched here.
2. **Published feature-type name mismatch** — GeoServer names a GPKG layer
   after the GPKG's internal table, which was a generic name
   (`livestock_villages`, `antyodaya_villages`, `facilities_village_service`).
   Different tehsils collided on one feature type and the WFS/WMS links we
   emit (workspace:scoped-layer-name) pointed at nothing. Fixed by writing
   the GPKG data table under the scoped layer name (e.g.
   `livestocks_ranchi_bero`); republish verified over WFS
   (84 villages served with the new schema). Stale generic layers were
   removed from `testworkspace` by the overwrite path.
3. **Publishing from the test harness crashed with "Apps aren't loaded yet"**
   — the harness never initialised Django, which the GeoServer utility needs.
   The harness now calls `django.setup()` before resolving pipelines.

## Known Data-Quality Findings (not code defects)

- **9 active locations don't resolve** against the standard boundary —
  frontend labels differing from boundary names:
  Delhi/South/Mehrauli; Karnataka/Bengaluru Urban/Bengaluru South;
  Manipur/Senapati/Tadubi; Meghalaya East Khasi Hills, Ri-Bhoi, South West
  Khasi Hills (district names used as block labels); Nagaland Dimapur,
  Mokokchung, Mon/Tobu. These need label reconciliation in
  `proposed_blocks_active_locations.json` or boundary aliases.
- **Livestock join coverage varies**: tehsil-level mean 0.83. Lowest:
  Jharkhand/Ramgarh/Patratu at 0.526, Odisha/Keonjhar/Ghatagaon 0.624,
  Odisha/Sambalpur 0.683 — many admin villages there have no matching census
  `village_code`. The `livestock_status` column now surfaces this per row as
  `no data available for this village`.
- **Facilities state-scope runs are heavy** (~8.3 minutes for one full
  state); tehsil scope is ~3–13 s. State-level facility requests should be
  treated as batch jobs.

## Representative Timings (wall clock)

| Pipeline | Tehsil (median) | District | State | Village |
| --- | ---: | ---: | ---: | ---: |
| facilities | 3.4 s | 19.8–25.2 s | 497 s | 1.8 s |
| antyodaya | 0.8 s | 2.3–2.9 s | 84 s | 1.1 s |
| livestocks | 0.5 s | 1.0–1.1 s | 28 s | 0.9 s |

First-ever run on a machine additionally pays one-time sidecar
materialisation (the 634 MB Antyodaya CSV takes ~2–3 minutes on a WSL
`/mnt` checkout) and SQLite index creation.

## How to Reproduce

```bash
# full-coverage audit (no pipeline runs)
python computing/misc/local_pipeline/tests/local_pipeline_active_locations_test.py \
  --pipelines livestocks --max-tehsils 0 --village-sample 0 --district-sample 0 \
  --state-sample 0 --admin-resolvable-only

# sampled end-to-end matrix with API smoke
python computing/misc/local_pipeline/tests/local_pipeline_active_locations_test.py \
  --max-tehsils 10 --all-sample 3 --metadata-sample 3 --district-sample 2 \
  --state-sample 1 --village-sample 3 --admin-resolvable-only --api-smoke --overwrite

# live GeoServer publish check for one block
python computing/misc/local_pipeline/tests/local_pipeline_active_locations_test.py \
  --only-location "Jharkhand|Ranchi|Bero" --max-tehsils 1 --all-sample 0 \
  --metadata-sample 0 --district-sample 0 --state-sample 0 --village-sample 0 \
  --sync-geoserver --overwrite

# render any run directory to markdown
python computing/misc/local_pipeline/tests/render_run_report.py \
  data/local_pipeline_test_runs/<run> --output report.md
```

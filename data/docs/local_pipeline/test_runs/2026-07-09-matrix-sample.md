# Local Pipeline Test Run: `matrix_sample`

Rendered: `2026-07-10T04:07:02+00:00`

## Overall

- Cases: **96**, passed: **96** (100.0%), failed: **0**
- Pipelines: facilities, antyodaya, livestocks
- Locations available after filters: 405

## Active-Location Coverage (admin boundary resolution)

- 405/414 active locations resolve against `cs_admin_standard.gpkg`.

Unresolved locations (frontend labels not matching boundary names):

- Delhi / South / Mehrauli
- Karnataka / Bengaluru Urban / Bengaluru South
- Manipur / Senapati / Tadubi
- Meghalaya / East Khasi Hills / East Khasi Hills
- Meghalaya / Ri-Bhoi / Ribhoi
- Meghalaya / South West Khasi Hills / South West Khasi Hills
- Nagaland / Dimapur / Dimapur
- Nagaland / Mokokchung / Mokokchung
- Nagaland / Mon / Tobu

## API Contract Smoke

| Pipeline | Body | Expected | Result |
| --- | --- | --- | --- |
| facilities | legacy_rejected | 400 rejected, not queued | PASS (HTTP 400) |
| facilities | structured | 200 queued | PASS (HTTP 200) |
| antyodaya | legacy_rejected | 400 rejected, not queued | PASS (HTTP 400) |
| antyodaya | structured | 200 queued | PASS (HTTP 200) |
| livestocks | legacy_rejected | 400 rejected, not queued | PASS (HTTP 400) |
| livestocks | structured | 200 queued | PASS (HTTP 200) |

## Cases by Pipeline and Scope

| Pipeline | Scope | Cases | Passed | Median wall s | Max wall s |
| --- | --- | ---: | ---: | ---: | ---: |
| antyodaya | district | 2 | 2 | 2.3 | 2.9 |
| antyodaya | state | 1 | 1 | 84.2 | 84.2 |
| antyodaya | tehsil | 26 | 26 | 0.8 | 2.3 |
| antyodaya | village | 3 | 3 | 1.1 | 1.2 |
| facilities | district | 2 | 2 | 19.8 | 25.2 |
| facilities | state | 1 | 1 | 497.3 | 497.3 |
| facilities | tehsil | 26 | 26 | 3.4 | 13.2 |
| facilities | village | 3 | 3 | 1.8 | 2.4 |
| livestocks | district | 2 | 2 | 1.0 | 1.1 |
| livestocks | state | 1 | 1 | 28.4 | 28.4 |
| livestocks | tehsil | 26 | 26 | 0.5 | 1.2 |
| livestocks | village | 3 | 3 | 0.9 | 1.0 |

## Cache Behaviour

- Repeat requests with `use_pregenerated`: 30, served from cache: 30.

## Report CSV Data-Quality Checks

- CSVs checked: 87
- Admin columns lead the file: 87/87
- Status column right after admin columns: 87/87
- Dataset column contract satisfied: 87/87
- CSVs leaking machine columns (`l2_*`/`l3_*`): 0
- CSVs leaking feature columns (`*_feat_value`): 0
- Runs with undocumented CSV columns in metadata: 0

## Join Coverage (matched villages / admin villages)

- Tehsil-level joins measured: 52; mean 0.830, median 0.880.

Lowest coverage (places to look at first):

- livestocks: Jharkhand / Ramgarh / Patratu -> 0.526
- livestocks: Jharkhand / Ramgarh / Patratu -> 0.526
- livestocks: Jharkhand / Ramgarh / Patratu -> 0.526
- livestocks: Jharkhand / Ramgarh / Patratu -> 0.526
- livestocks: Odisha / Keonjhar (Kendujhar) / Ghatagaon -> 0.624
- livestocks: Odisha / Keonjhar (Kendujhar) / Ghatagaon -> 0.624
- livestocks: Odisha / Sambalpur / Sambalpur -> 0.683
- livestocks: Odisha / Sambalpur / Sambalpur -> 0.683

## Failures

None.

## Artifacts

- Full per-case records: `data/local_pipeline_test_runs/matrix_sample/pipeline_cases.jsonl`
- Case summary CSV: `data/local_pipeline_test_runs/matrix_sample/pipeline_cases_summary.csv`
- Manifest: `data/local_pipeline_test_runs/matrix_sample/run_manifest.json`

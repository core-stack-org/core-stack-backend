# Testing the Local Pipelines

Two complementary suites live in `computing/misc/local_pipeline/tests/`:

1. **The interactive console** (`interactive_cli.py`) — for a human
   exploring, debugging, or demonstrating one request at a time.
2. **The active-locations matrix** (`local_pipeline_active_locations_test.py`)
   — for automated end-to-end and data-quality runs across
   `data/proposed_blocks_active_locations.json`.

Both run from the repo root with the project environment
(`conda activate corestack-backend`). Direct mode needs no services; API
mode expects Django (`python manage.py runserver`) and a Celery worker
(`celery -A nrm_app worker -Q nrm`) running in other terminals.

## Branch Strategy

Test configuration is deliberately branch-pinned: test branches point
`output.root` at `data/tests/outputs/...` and `geoserver_workspace` at
`testworkspace` in the pipeline YAMLs. The intended workflow is one test
branch per concern (core-module testing, pipeline testing) rebased on their
feature branches, merged together into an integration test branch (like
`test/local-pipeline-v3-integration`) for combined end-to-end runs. Before
promoting to staging/main, restore production output roots and workspaces.

## 1. Interactive Console

```bash
python computing/misc/local_pipeline/tests/interactive_cli.py
```

What it gives you, menu-driven:

- **Pick a pipeline** (facilities / antyodaya / livestocks) and a location —
  search the 414 active blocks, take a random one, or type any names.
- **Shape the request**: scope level (tehsil/district/state/village), each
  output flag, GeoServer on/off (defaults to `testworkspace`), overwrite,
  and cache (`use_pregenerated`).
- **Run it in-process** (no server needed) or **through the live API**: it
  POSTs with your JWT (set `LOCAL_PIPELINE_JWT`, base URL via
  `LOCAL_PIPELINE_API_BASE`) and then watches `data/` for the completed
  run's metadata since the task executes on Celery.
- **Inspect the artifacts** without leaving the console: CSV head + numeric
  summary, GPKG layers/columns, run-metadata column dictionary and EDA,
  README text, timings, and validation issues.

Typical uses: verifying one block after a config change, checking what a
CSV will look like before a partner handoff, demonstrating outputs, and
reproducing a failure interactively.

## 2. Active-Locations Matrix

```bash
python computing/misc/local_pipeline/tests/local_pipeline_active_locations_test.py \
    --max-tehsils 10 --district-sample 2 --state-sample 1 --village-sample 3 \
    --admin-resolvable-only --api-smoke --overwrite
```

The runner builds a case matrix and executes each case in-process, writing
per-case JSONL, a CSV summary, and `summary.json` into
`data/local_pipeline_test_runs/<timestamp>/` (or `--output-dir`).

**Case dimensions**

| Dimension | Values | Flag |
| --- | --- | --- |
| Pipeline | facilities, antyodaya, livestocks | `--pipelines` |
| Scope level | tehsil (all sampled), district, state, village | `--district-sample`, `--state-sample`, `--village-sample` |
| Output variant | default bundle, `data_only`, `metadata_only` | `--all-sample`, `--metadata-sample` |
| Cache | fresh run, then `use_pregenerated` repeat | built-in |
| Location sample | random by `--seed` from the active list | `--max-tehsils`, `--only-location` |
| GeoServer | off by default | `--sync-geoserver`, `--test-geoserver-workspace` |

**Checks per case** (recorded under `checks` in the JSONL):

- run status, wall/reported timings, row and match counts;
- every `*_path` in the result exists on disk;
- report CSV contract: the nine admin columns first, the `*_status` column
  right after them, no `l2_*`/`l3_*` machine columns, no `*_feat_value`
  columns; livestock's exact fixed column list; antyodaya's
  clusters-then-values ordering; facilities' category columns present;
- run metadata: `outputs` profiles exist, every CSV column documented with
  a description, EDA present;
- GPKG layers and row counts.

**Special modes**

- `--admin-resolvable-only` first resolves every location against
  `cs_admin_standard.gpkg` and writes `admin_resolution_report.csv` — run it
  with `--max-tehsils 0` for a pure coverage audit of all 414 locations.
- `--api-smoke` calls the three API views in-process (task queuing mocked)
  and asserts the contract: structured bodies queue on the `nrm` queue,
  flat legacy bodies are rejected with 400 and queue nothing.
- `--only-location "State|District|Tehsil"` pins the run to chosen blocks;
  `--fail-fast` stops at the first failure.

## Reading a Run

Start with `summary.json` (counts), then `pipeline_cases_summary.csv` (one
row per case: ok/status/timings/errors), then dig into
`pipeline_cases.jsonl` for full payloads, results, and checks of the cases
you care about. Human-readable reports of past runs are kept in
[test_runs/](test_runs/) alongside this document.

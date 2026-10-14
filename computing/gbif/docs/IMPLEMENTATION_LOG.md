GBIF_USER="your_username"
GBIF_PWD="your_password"
GBIF_EMAIL="you@fplaunchpad.org"# GBIF Species Layer — Implementation Log

> Running log of what was actually implemented, file by file, phase by phase.
> Target design: **UK plan** — block-first, GEE-native (`ee.Join.saveAll` + `aggregate_count_distinct`),
> static per-MWS biodiversity indicators. See [`compare.md`](compare.md) for why this plan was chosen and
> [`uk/implementation.md`](uk/implementation.md) for the full spec.
>
> **Environment caveat:** `pygbif`/`geopandas` are not installed in the dev shell here, and there are no
> GBIF/GEE/GCS credentials, so code is written to match verified repo signatures and **syntax-compiled**,
> but **not run end-to-end**. Manual verification steps are given per phase.
>
> **Location decision:** implemented inside the existing `computing/gbif/` folder (not a new
> `computing/biodiversity/`), reusing the already-scaffolded `config.py` / `gbif_download.py` /
> `gbif_clean.py`. The earlier Python/GeoPandas modules (`gbif_richness.py`, `gbif_species_change.py`)
> are retained as the future **Level-B (change-over-time)** variant.

---

## Verified repo facts used (so code matches reality, not the UK doc's guesses)

- MWS units are a **GEE asset**: `get_gee_asset_path(state,district,block) + "filtered_mws_<district>_<block>_uid"`
  (same asset `change_detection.py` uses). Used both as the join input and the bbox source — one source of truth.
- Models: `StateSOI.state_name`, `DistrictSOI.district_name`, `TehsilSOI.tehsil_name`; `Layer.is_sync_to_geoserver`.
- Helpers confirmed present in `utilities/gee_utils.py`: `gcs_config(gee_account_id)`, `is_gee_asset_exists(path)`,
  `check_task_status(list)`, `gcs_to_gee_asset_cli(gcs_uri, asset_id, gee_account_id)` → returns task_id or None,
  `get_gee_asset_path`, `valid_gee_text`, `ee_initialize`. `GCS_BUCKET_NAME` in settings.
- Registration helpers in `computing/utils.py`: `save_layer_info_to_db(state,district,block,layer_name,asset_id,dataset_name,...)`,
  `sync_layer_to_geoserver(state_name, fc, layer_name, workspace)`, `update_layer_sync_status(layer_id, sync_to_geoserver=True)`.
- Celery: tasks use `from nrm_app.celery import app` + `@app.task(bind=True)`, queue `"nrm"`.
- Highest `computing.dataset` pk was 52.

---

## Phase 0 — Config + registry  ✅ DONE

**What was implemented**

- Extended [`config.py`](config.py) with GEE-native constants (reused, not recreated):
  - `get_gee_block_asset_id(state, district, block)` — delegates to `get_gee_asset_path` (stack-consistent).
  - `GCS_BLOCK_GEOJSON`, `GCS_STATS_PREFIX` — block-scoped GCS paths.
  - `DATASET_NAME_VECTOR`, `VECTOR_STYLE_NAME`, `ALGORITHM_NAME/VERSION`, `THREATENED_IUCN_CATEGORIES`, `DATA_DIR`.
- Added a `Dataset` seed row to [`installation/seed/seed_data.json`](../../installation/seed/seed_data.json):
  `pk 53`, name `"Biodiversity Occurrence"`, `layer_type=vector`, `workspace=biodiversity`, `style_name=biodiversity_mws`.
  (Without this row, `save_layer_info_to_db` raises `Dataset.DoesNotExist`.)

**Reused (not rewritten):** `get_gee_asset_path`, the existing `config.py` scaffold.

**Still manual / not done here:** creating the GeoServer `biodiversity` workspace + uploading the
`biodiversity_mws` SLD style (needs a running GeoServer); loading the seed row into the DB.

**How to verify manually**

```bash
# 1. JSON still valid + row present (already confirmed):
python3 -c "import json,collections;d=json.load(open('installation/seed/seed_data.json'));\
print([o['pk'] for o in d if o.get('model')=='computing.dataset' and o['fields']['name']=='Biodiversity Occurrence'])"
# -> [53]

# 2. Load into DB (dev), then confirm in Django shell:
python manage.py loaddata installation/seed/seed_data.json
python manage.py shell -c "from computing.models import Dataset; print(Dataset.objects.get(name='Biodiversity Occurrence'))"

# 3. Create the GeoServer workspace (once):
python manage.py shell -c "from utilities.geoserver_utils import Geoserver; Geoserver().create_workspace('biodiversity')"
```

---

## Phase 1 — Block GBIF download  ✅ DONE (code) / ⏳ untested (needs GBIF creds)

**What was implemented** — added to [`gbif_download.py`](gbif_download.py) (existing per-taxon
functions kept):

- `get_block_bbox_wkt(state, district, block)` — derives the block bbox **from the MWS GEE asset**
  (`filtered_mws_..._uid`) via `roi.geometry().bounds().getInfo()`, then builds a WKT POLYGON with a
  small buffer. One source of truth for the block extent; no GeoServer WFS guessing.
- `request_block_download(bbox_wkt)` — GBIF Download API for **all taxa** within the bbox
  (`HAS_COORDINATE`, `HAS_GEOSPATIAL_ISSUE=FALSE`, `OCCURRENCE_STATUS=PRESENT`, basis-of-record filter,
  `geometry within <wkt>`).
- `download_block_occurrences(state, district, block)` — cached orchestration: bbox → download →
  `wait_and_fetch` (reused) → local `occurrences_raw.csv`; records the DOI for citation.

**Reused (not rewritten):** `wait_and_fetch`, `occ.download_meta`, `get_gee_asset_path`, `valid_gee_text`.

**⚠️ One thing to verify on first real run:** the `"geometry within <wkt>"` predicate string depends
on the installed pygbif version's operator parsing. If pygbif rejects it, switch to a predicate dict
(`{"type":"within","geometry":bbox_wkt}`). Flagged in the function docstring. Every other predicate is
a plain `KEY OP VALUE` string pygbif parses reliably.

**How to verify manually** (after `conda env update` + `GBIF_USER/PWD/EMAIL` set + `ee_initialize`):

```python
from computing.gbif.gbif_download import get_block_bbox_wkt, download_block_occurrences
from utilities.gee_utils import ee_initialize
ee_initialize(<gee_account_id>)
print(get_block_bbox_wkt("karnataka","ramanagara","channapatna"))   # -> POLYGON((...)) around the block
csv, doi = download_block_occurrences("karnataka","ramanagara","channapatna")
import pandas as pd; print(pd.read_csv(csv, sep="\t").shape, doi)     # rows>0, a DOI string
```

Tip: for a fast dev download, temporarily add a `TAXON_KEY = 212` (Aves) predicate.

---

## Phase 2 — Clean occurrences  ✅ DONE

**What was implemented** — [`gbif_clean.py`](gbif_clean.py): added `stateProvince` to the columns read
(the GEE upload carries it as a display property). The cleaner already keeps `taxonKey`, `species`,
`kingdom`, `class`, `iucnRedListCategory`, `year`, lat/lon and applies the 5 filters (dropna → India
bbox → coordinate-uncertainty → centroid-pile → dedupe) with before/after logging.

**Reused (not rewritten):** the entire existing `clean_occurrences()` — only the column list changed.

**How to verify manually**

```python
from computing.gbif.gbif_clean import clean_occurrences
df = clean_occurrences("<block>/occurrences_raw.csv", "<block>/occurrences_clean.csv")
print(df.columns.tolist())          # includes iucnRedListCategory, class, kingdom, stateProvince
# drop rate for a block-level download should be ~10-50%; logged as "cleaned N -> M records"
```

---

## Environment readiness (checked against nrm_app/.env + core-stack-key.json)

| Item                                                                                    | Status                                                                                    |
| --------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| GEE service account key (`core-stack-key.json`, project `arcane-mason-493503-a6`)   | ✅ present; asset paths resolve to`projects/arcane-mason-493503-a6/assets/apps/mws/...` |
| GCS bucket (`fpl-core-stack-dev`)                                                     | ✅ present                                                                                |
| GeoServer (`http://localhost:8080/geoserver`)                                         | ✅ configured                                                                             |
| Project conda env`corestackenv` (geopandas/rasterio/shapely/earthengine + CLI)        | ✅ present                                                                                |
| `pygbif`                                                                              | ✅ installed into`corestackenv` (0.6.6)                                                 |
| All GBIF modules + task + api + urls import under Django                                | ✅ verified; endpoint resolves to`/api/v1/generate_biodiversity_layer/`                 |
| **GBIF.org account creds (`GBIF_USER` / `GBIF_PWD` / `GBIF_EMAIL`)**        | ❌**NOT set** — separate from Google; blocks the download step                     |
| Postgres up + a`GEEAccount` row + the block's `filtered_mws_..._uid` asset existing | ⏳ needed for an actual run                                                               |

**Blocker for an end-to-end run:** GBIF is a different service from Google Earth Engine. The GEE/GCS
creds present do **not** cover it. A free GBIF.org account is required for the Download API; add to
`nrm_app/.env`:

```bash
GBIF_USER="..."   GBIF_PWD="..."   GBIF_EMAIL="..."
```

---

## Phase 3 — GCS upload + GEE ingestion  ✅ DONE (code, imports verified) / ⏳ run needs creds

[`gbif_gee_upload.py`](gbif_gee_upload.py): `csv_to_geojson` (chunked), `upload_geojson_to_gcs`
(reuses `gcs_config`), `ingest_geojson_to_gee` (reuses `gcs_to_gee_asset_cli` + `is_gee_asset_exists`),
`wait_for_gee_ingestion` (reuses `check_task_status`). Carries `taxonKey/species/kingdom/class/ stateProvince/iucnRedListCategory` into GEE.

## Phase 4 — GEE per-MWS indicators  ✅ DONE (code, imports verified)

[`gbif_mws_stats.py`](gbif_mws_stats.py): `ee.Join.saveAll` MWS×points, then per-MWS
species_richness, occurrence_count, shannon/simpson/pielou (one histogram), rare_species_count,
threatened_species_count (IUCN VU/EN/CR), 6 per-class taxonomy counts, data_poor; zero-record MWS
merged back in; `Export.table.toCloudStorage` GeoJSON. **Caveat:** the `ee.Filter.inList("uid", …).Not()`
zero-merge has GEE's list-size limit (~1000 MWS/block) — fine at block scale, flagged for national.

## Phase 5 — Post-export + sync  ✅ DONE (code, imports verified)

[`gbif_export.py`](gbif_export.py): download stats GeoJSON, add `dominant_class`,
`biodiversity_category`, `observation_density_per_km2` (if MWS area present), coerce string diversity
fields → float, NaN-fill. [`gbif_sync.py`](gbif_sync.py): `sync_layer_to_geoserver` +
`save_layer_info_to_db` + `update_layer_sync_status` (pure reuse).

## Phase 6 — Celery task + API + URL  ✅ DONE (code, imports verified)

[`biodiversity_task.py`](biodiversity_task.py): `generate_biodiversity_block` (`@app.task(bind=True)`,
queue `nrm`) orchestrating phases 1–5, with an upfront GBIF-cred check. Wired
`generate_biodiversity_layer` endpoint in [`api.py`](../api.py) + route in [`urls.py`](../urls.py).
Verified: whole chain imports under Django; `reverse('generate_biodiversity_layer')` →
`/api/v1/generate_biodiversity_layer/`.

**How to verify manually (end-to-end, once GBIF creds are added + Postgres/GEEAccount ready):**

```bash
# 1. env update (adds pygbif from environment.yml) + load the dataset seed row
conda activate corestackenv
python manage.py loaddata installation/seed/seed_data.json
# 2. create GeoServer workspace once
python manage.py shell -c "from utilities.geoserver_utils import Geoserver; Geoserver().create_workspace('biodiversity')"
# 3. trigger for a block that already has a filtered_mws_..._uid asset:
curl -X POST http://localhost:8000/api/v1/generate_biodiversity_layer/ \
  -H "Content-Type: application/json" \
  -d '{"state":"karnataka","district":"ramanagara","block":"channapatna","gee_account_id":1}'
# 4. watch the celery worker log for stage messages; then confirm the GeoServer layer + DB row:
python manage.py shell -c "from computing.models import Layer; print(Layer.objects.filter(layer_name__endswith='_biodiversity').values('layer_name','is_sync_to_geoserver'))"
```

## Phase 7 — Excel + KYL + reports  ✅ DONE (code, imports + templates verified)

Downstream integration so the indicators reach the dashboard (KYL) and both report types — same
pattern as every other layer.

- **Excel** ([`stats_generator/utils.py`](../../stats_generator/utils.py)): `create_excel_for_biodiversity()`
  + `workspace == "biodiversity"` dispatch → writes the `biodiversity` sheet (18 columns).
- **Auto-registration** ([`gbif_sync.py`](gbif_sync.py)): `LayerInfo.get_or_create(...)` with
  `excel_to_be_generated=True` so `fetch_layers_for_excel_generation()` picks the layer up (LayerInfo
  rows aren't seeded — they're runtime; this makes it self-registering).
- **KYL** ([`stats_generator/mws_indicators.py`](../../stats_generator/mws_indicators.py)): added
  `"biodiversity": -1` sheet, a per-MWS extraction block, and 7 KYL keys — `species_richness`,
  `occurrence_count`, `threatened_species_count`, `shannon_diversity_index`, `dominant_taxon_group`,
  `biodiversity_category`, `biodiversity_data_poor`.
- **MWS report** ([`dpr/gen_mws_report.py`](../../dpr/gen_mws_report.py) `get_biodiversity_data()` +
  [`dpr/api.py`](../../dpr/api.py) context `biodiversity_desc`/`biodiversity_data` +
  [`templates/mws-report.html`](../../templates/mws-report.html) section with an indicator table +
  data-poor caveat).
- **Tehsil report** ([`dpr/gen_tehsil_report.py`](../../dpr/gen_tehsil_report.py)
  `get_biodiversity_summary_data()` + [`dpr/api.py`](../../dpr/api.py) context `biodiversity_summary` +
  [`templates/block-report.html`](../../templates/block-report.html) block summary + top-5 table).

**Verified:** all touched modules import under Django in `corestackenv`; both templates parse via
`get_template`. Note: the `is not -1` sentinel check matches the file's existing idiom (a sheet is
either `-1` or a DataFrame; `!= -1` would do an ambiguous elementwise compare).

**How to verify manually (after a pilot run has produced the GeoServer layer):**

```bash
# regenerate the stats excel for the block, then confirm the biodiversity sheet:
python manage.py shell -c "from stats_generator.utils import get_vector_layer_geoserver; \
get_vector_layer_geoserver('karnataka','ramanagara','channapatna', specific_sheets=['biodiversity'])"
python -c "import pandas as pd; xl=pd.ExcelFile('data/stats_excel_files/KARNATAKA/RAMANAGARA/ramanagara_channapatna.xlsx'); \
print('biodiversity' in xl.sheet_names, xl.parse('biodiversity').columns.tolist())"
# KYL JSON: regenerate and grep for the new keys (species_richness, threatened_species_count, ...)
# Reports: generate the MWS + block reports and confirm the "Biodiversity" sections render.
```

---

## Scope decision (user, 2026-07): **UK plan + output_design only. Level B (change over time) is dropped.**

The canonical entry point is `generate_biodiversity_layer` / `generate_biodiversity_block`. The Python
Level-A/B modules (`gbif_richness.py`, `gbif_species_change.py`, `species_task.py` + the
`species_richness`/`species_change` endpoints) are **superseded/unused** — safe to remove.

---

## PILOT RUN (real data — GBIF creds live) — validated the whole scientific core

Block: **bihar / jamui / jamui** (324 MWS). Test download restricted to birds (Aves) for speed.

| Stage                                            | Result                                                                                                |
| ------------------------------------------------ | ----------------------------------------------------------------------------------------------------- |
| GBIF auth (`download_list`)                    | ✅ credentials valid                                                                                  |
| `get_block_bbox_wkt` (bbox from MWS GEE asset) | ✅`POLYGON((85.77.. 24.33..))`                                                                      |
| GBIF download submit + fetch                     | ✅ 21,643 raw bird records (download key`0009612-260623161305970`)                                  |
| `clean_occurrences`                            | ✅ 8,921 clean records, 240 species (12,722 dropped)                                                  |
| IUCN enrichment                                  | ✅ LC 8455 / NT 305 / VU 117 / EN 7 →**6 threatened species**                                  |
| GEE indicators vs real 324 MWS                   | ✅ richness / occurrence / Shannon /**taxonomy** / **threatened** / data_poor all correct |

Sample MWS output: `uid=12_312011 → species_richness=16, bird_species_count=16, threatened=2, shannon=2.736`.

### Two real bugs found and fixed during the pilot

1. **pygbif predicate format** — the download predicates were rejected (HTTP 400, null key). pygbif's
   string parser needs **camelCase keys** (`hasCoordinate`, not `HAS_COORDINATE`) and the `in` list must
   be **valid JSON** (`json.dumps(...)`). `geometry within <wkt>` works as-is. Fixed in
   [`gbif_download.py`](gbif_download.py) (both block + per-taxon builders).
2. **`iucnRedListCategory` is NOT in GBIF SIMPLE_CSV** (the UK doc was wrong). Added
   [`gbif_iucn.py`](gbif_iucn.py) — looks up the category per distinct taxonKey via
   `GET /v1/species/{key}/iucnRedListCategory` (cached), normalises long-form → short codes
   (VULNERABLE→VU…), and attaches the column before GEE upload. Wired into the task after cleaning.

### Remaining to finish the full end-to-end pilot

- The GCS upload → GEE table ingestion → export → **GeoServer sync** tail (standard reused helpers).
  Needs GeoServer running at `localhost:8080` + the `biodiversity` workspace created.
- Then run the real **all-taxa** block via `generate_biodiversity_layer` (the pilot used birds-only for speed).

---

# NATIVE-REUSE REFACTOR (incremental, one logical step at a time)

Goal: make the module read like `change_detection`, reusing existing CoRE Stack infra, minimal new code.

## STEP 1 — GEE ingestion  ✅ DONE & verified
- Deleted `gbif_gee_upload.py` (custom GCS→CLI→asset ingest subsystem).
- Task now builds an in-memory FeatureCollection via **`gdf_to_ee_fc()`** (as plantation/nrega do),
  carrying only NaN-free `taxonKey/kingdom/class/iucnRedListCategory` properties.
- Verified: identical indicators vs pilot (`uid 12_312011 → 16 species / 16 birds / 2 threatened`).

## STEP 2 — GEE export  ✅ DONE & verified (real export on 324-MWS Jamui)
- `export_stats_to_gcs` (toCloudStorage) → **`export_stats_to_asset`** using
  **`export_vector_asset_to_gee`**; consume via **`ee.FeatureCollection(asset_id).getInfo()`**
  (exact `change_detection_vector` flow). Removed `download_stats_geojson` from `gbif_export.py`.
- Two latent bugs found & fixed (surfaced by strict `toAsset`):
  1. join's `gbif_occurrences` (List<Feature>) was carried onto output → emit fresh `ee.Feature`.
  2. matched vs zero branches had heterogeneous EE types (Long/Integer/Boolean/Float) → explicit
     `toInt()/toFloat()/String` casts + a static `_OUTPUT_PROPERTIES` schema.
- Verified end-to-end: export → getInfo → enrich → 324 features, all indicators preserved, then asset deleted.

## STEP 3 — GeoServer sync  ✅ DONE & verified
- Deleted `gbif_sync.py`; **inlined** `save_layer_info_to_db` → `make_asset_public` →
  `sync_layer_to_geoserver` → `update_layer_sync_status` into the task (exact `change_detection_vector`
  flow). Layer asset_id now correctly = the **stats asset** (from STEP 2), not the removed points asset.
- Dropped the non-native `LayerInfo.get_or_create` (no other module auto-creates LayerInfo).
  ⚠️ **Registry note:** the `biodiversity` `LayerInfo` row (for Excel/KYL) must be added via admin/seed,
  the same way every other vector layer is registered.
- Incidental fix: repaired an unrelated syntax corruption in `stats_generator/mws_indicators.py:600`
  (`change_in_croppided tng_intensity_area` → `change_in_cropping_intensity_area`) that was breaking
  the whole import chain.
- Verified: full Django chain imports; `reverse('generate_biodiversity_layer')` OK.

## Layer.misc design — ⏳ AWAITING APPROVAL (implement in STEP 4)
Proposed minimal, flat structure (matches the repo's `misc` = qualifying/processing params convention;
change_detection stores start_year/end_year — GBIF's analog is download provenance + taxon scope):
```
misc = {"gbif_doi", "download_key", "taxon_scope", "raw_record_count",
        "clean_record_count", "download_date"}
```
NOT stored (derivable/duplicated): bbox (function of MWS asset), query predicates (constant in code),
anything on Layer/Dataset/LayerInfo, the indicators themselves.

## Remaining
- STEP 4 (after misc approval): `is_gee_asset_exists()` idempotency guard + store approved `misc` +
  confirm `make_asset_public` placement — mirror `change_detection`.
- STEP 5: repo cleanup (verify `gbif_export.py` is minimal; remove any dead config like GCS paths).

## STEP 4 — idempotency + Layer.misc  ✅ DONE (structure verified; approved misc)
- Added `is_gee_asset_exists(asset_id)` guard around the compute+export (skip if the stats asset
  exists) — exactly like change_detection. Cached GBIF download + clean still run to build `misc`.
- Repurposed the (now-dead) `config.get_gee_block_asset_id` to return the **stats asset** id — one
  source of truth for both the guard and `export_stats_to_asset`.
- `download_block_occurrences` now returns `(csv, meta)` with `{download_key, doi, download_date,
  raw_record_count}` (from `occ.download_meta`), persisted to `meta.txt` and parsed on cache hit.
- Stored approved **Layer.misc** in `save_layer_info_to_db(..., misc=misc)`:
  `{gbif_doi, download_key, taxon_scope, raw_record_count, clean_record_count, download_date}`.
  (bbox/predicates NOT stored — derivable; matches the repo's minimal-misc convention.)
- Verified: imports OK, guard + all 6 misc keys present, asset id resolves to `biodiversity_<d>_<b>`.
  (Full live DB run deferred to final integration; compute/export/sync already validated live.)

## STEP 5 — repository cleanup  ✅ DONE
- Removed dead config constants (0 uses): `GCS_BLOCK_GEOJSON`, `GCS_STATS_PREFIX`,
  `RICHNESS_GRID_DEG`, `RAREFACTION_ITERS`, `MIN_RECORDS_PER_WINDOW`, `VECTOR_STYLE_NAME`
  (orphaned by the deleted GCS-ingest + Level-B modules). Rewrote `config.py` cleanly.
- Verified all 7 remaining modules import; no dead references remain.

### Final module inventory (all genuinely biodiversity-specific or thin task glue)
- `gbif_download.py` — GBIF Download API (external; no CoRE equivalent)
- `gbif_clean.py` — coordinate cleaning (biodiversity-specific)
- `gbif_iucn.py` — IUCN species-API enrichment (external; no CoRE equivalent)
- `gbif_mws_stats.py` — GEE Join + distinct-species indicators (the novel computation)
- `gbif_export.py` — local enrichment only (dominant_class / category / density)
- `biodiversity_task.py` — Celery orchestrator, mirrors change_detection (reuses all CoRE helpers)
- `config.py` — constants + the asset-id helper

Deleted across the refactor: `gbif_gee_upload.py`, `gbif_sync.py` (+ earlier: `gbif_richness.py`,
`gbif_species_change.py`, `species_task.py`). All replaced by existing CoRE Stack infrastructure.

### Open registry decision (not code)
The `biodiversity` `LayerInfo` row (drives Excel/KYL) is no longer auto-created. Add it via admin or
a seed row (like the `Dataset` row) — awaiting your call.

---

# FINAL INTEGRATION PHASE

## STEP 1 — LayerInfo registry via seed data  ✅ DONE
- Finding: `LayerInfo` is **admin-managed** (no seed/migration data rows anywhere; read by
  `stats_generator/utils.py` for Excel + `mws_indicators.py` for the drought-specific `.get`).
  Biodiversity KYL does NOT need a `LayerInfo.get` (it reads the `biodiversity` Excel sheet).
- Added one `stats_generator.layerinfo` row to `installation/seed/seed_data.json` (next to the
  Biodiversity `Dataset` row): `layer_name="{district}_{block}_biodiversity"`, `layer_type="vector"`,
  `workspace="biodiversity"`, `excel_to_be_generated=true`, `style_name="biodiversity_mws"`.
- Used `"pk": null` (auto-assigned) — unlike `Dataset` (fully seed-owned, fixed pks), `LayerInfo` is
  admin-managed, so an explicit pk could clobber an admin row; a null pk appends safely.
- Reuses the existing fixture mechanism (`loaddata installation/seed/seed_data.json`, run by
  install.sh) — no new registration flow, and the task no longer auto-creates LayerInfo (removed with
  gbif_sync in STEP 3).
- Verified: JSON parses; deserializes against the real `LayerInfo` model; matches the
  `fetch_layers_for_excel_generation` filter.

## STEP 1 (revised) — LayerInfo via idempotent management command  ✅ DONE
- Reverted the `LayerInfo` seed row from `seed_data.json` (kept the `Dataset` row). `LayerInfo` is an
  admin/operational registry (no fixtures/migrations seed it), and a `pk:null` fixture row is NOT
  idempotent under repeated `loaddata`.
- Added `computing/management/commands/register_biodiversity_layer.py` — an idempotent `get_or_create`
  command, matching the repo's existing one-time-registration pattern (`seed_default_plantation`,
  `load_layer_mappings`). Works on fresh AND existing DBs; run any number of times → at most one row.
- Re-added `config.VECTOR_STYLE_NAME` (now used by the command; had been removed as dead in STEP 5).
- Verified on the live dev DB: 1st run "Registered (id=1)", 2nd run "already registered (id=1)",
  DB has exactly one `biodiversity` LayerInfo row (`{district}_{block}_biodiversity`, vector, excel=True).
- Registration command for any deployment: `python manage.py register_biodiversity_layer`

## STEP 2 — Full end-to-end live validation (all-taxa, bihar/jamui/jamui)  ✅ CP1–18 PASS / CP19–25 BLOCKED
Real all-taxa run. Prereqs: created canonical `Dataset` row (absent on this dev DB); SOI + LayerInfo present.
| CP | Stage | Result | Evidence |
|----|-------|--------|----------|
| 1 | User request | PASS | bihar/jamui/jamui gee=1 |
| 2 | Block geometry | PASS | 324 MWS polygons |
| 3 | Bounding box | PASS | POLYGON((85.77.. 24.33..)) |
| 4 | GBIF download (all taxa) | PASS | 11.8 MB CSV, key 0019293-260623161305970 |
| 5 | DOI | PASS | 10.15468/dl.vu447a |
| 6 | Download key | PASS | 0019293-260623161305970 |
| 7 | Raw record count | PASS | 21,859 |
| 8 | Cleaning | PASS | 12 cols |
| 9 | Clean record count | PASS | 9,086 rows, 364 species |
| 10 | IUCN enrichment | PASS | LC 8520/NT 307/VU 118/EN 9/DD 5 → 9 threatened species |
| 11 | GeoDataFrame | PASS | 9,086 points |
| 12 | FC uploaded to EE (gdf_to_ee_fc) | PASS | 9,086 features, 20s — **scale risk did NOT materialize** |
| 13 | Join.saveAll | PASS | join graph built |
| 14 | Indicators computed | PASS | all keys present on sampled MWS |
| 15 | EE asset created | PASS | …/biodiversity_jamui_jamui |
| 16 | Asset made public | WARN | make_asset_public called; is_asset_public read False (eventual consistency; asset accessible) |
| 17 | Layer created | PASS | layer_id=25 |
| 18 | Layer.misc persisted | PASS | {gbif_doi, download_key, taxon_scope:all, raw:21859, clean:9086, download_date} |
| 19 | GeoServer sync | BLOCKED | GeoServer not running (localhost:8080 refused; Docker socket perm-denied for this user) |
| 20–25 | GeoServer visibility / SLD / Excel / KYL / MWS report / Tehsil report | BLOCKED | all require a running GeoServer |

- Compute engine + full data pipeline validated LIVE on real all-taxa data. GeoServer stages need the
  user's local Docker GeoServer (cannot be started here).
- Fix applied for GeoServer readiness: added `"biodiversity"` to `installation/setup_local_geoserver.py`
  WORKSPACES (it was missing → sync would fail even with GeoServer up).

## STEP 2 (cont.) — GeoServer stages CP19–25 (live, GeoServer up)
| CP | Stage | Result | Note |
|----|-------|--------|------|
| 0  | biodiversity workspace | PASS | created via Geoserver().create_workspace (HTTP 200) |
| 19 | GeoServer sync (real task) | PASS | layer_id=25, synced=True, 324 features published |
| 20 | Layer visible via WFS | PASS | 324 features returned |
| 21 | SLD uploaded/applied | FAIL | upload_style → HTTP 400 (investigate SLD/version or method args) |
| 22 | Excel biodiversity sheet | PASS (schema) / ⚠ values | sheet + 18 cols written, but values empty due to truncation (below) |
| 23 | KYL indicators | PARTIAL | full KYL needs the block's other stats sheets (hydrological_annual etc.) — not biodiversity-specific |
| 24 | MWS report section | PASS (runs) / ⚠ values | get_biodiversity_data runs; reads 0 due to truncation + EXCEL_DIR path |
| 25 | Tehsil report section | ⚠ | total_mws=0 — reads EXCEL_DIR path / truncated names |

### ⭐ KEY FINDING — shapefile 10-char field-name TRUNCATION on GeoServer sync
`sync_layer_to_geoserver` publishes via a shapefile datastore, which truncates property names to 10
chars. Actual WFS field names: `species_ri, occurrence, threatened, shannon_di, simpson_di,
pielou_eve, rare_speci, bird_speci, mammal_spe, plant_spec, reptile_sp, amphibian_, insect_spe,
dominant_c, biodiversi, observatio, data_poor, area_in_ha, uid`. **Values are correct** (species_ri=1,
dominant_c=Aves, biodiversi="Very Low") — only the NAMES are truncated. Downstream readers
(`create_excel_for_biodiversity`, `get_biodiversity_data`, `get_biodiversity_summary_data`,
`mws_indicators`) use FULL names → read None/empty.
- `change_detection` avoids this by using ≤10-char labels (fo_fo, total_urb, …).
- **FIX OPTIONS (decide tomorrow):** (a) make the Excel/KYL/report readers use the truncated names
  (most consistent with how the repo already works); or (b) shorten the exported property names to
  ≤10 chars in gbif_mws_stats `_OUTPUT_PROPERTIES`; or (c) sync via a non-shapefile datastore. Need to
  check how existing long-named layers (e.g. drainage_density → "drainage_d") handle this.

### Secondary finding — Excel path mismatch (repo-wide, not biodiversity)
Generator writes `EXCEL_PATH/data/stats_excel_files/…`; report readers read `EXCEL_DIR`
(`/var/tmp/core-stack-data/excel_files/…`). These differ in this env → reports don't find the
generated sheet unless copied. Affects all layers; reconcile EXCEL_PATH/EXCEL_DIR in deployment.

### Also done
- Created `installation/geoserver/styles/biodiversity_mws.sld` (richness ramp; data-poor grey).
- CP21 SLD upload returned 400 — to debug tomorrow (SLD version / upload_style args).

## STEP 2 (resolved) — field-name truncation FIXED via GeoPackage sync
**Fix:** `biodiversity_task` now publishes the enriched GeoJSON as a **GeoPackage** (`.gpkg`) via
`push_shape_to_geoserver(..., file_type="gpkg")` (+ `fix_invalid_geometry_in_gdf`) — the same
mechanism as `sync_fc_to_geoserver` — instead of `sync_layer_to_geoserver` (shapefile, which
truncates field names to 10 chars). GeoPackage preserves full field names. Imports updated.

**Re-validation (GeoServer up), all live:**
| CP | Result | Evidence |
|----|--------|----------|
| 19 GeoServer sync (gpkg) | ✅ PASS | layer_id=25, synced=True |
| 20 WFS full names + values | ✅ PASS | 324 features, **full field names**, 91 populated MWS (species_richness=1, dominant_class=Aves, biodiversity_category=Very Low, …) |
| 21 SLD applied to layer | ✅ PASS | uploaded biodiversity_mws.sld; layer defaultStyle = biodiversity:biodiversity_mws |
| 22 Excel sheet + values | ✅ PASS | 324 rows, sum(species_richness)=2722, sum(threatened)=35 |
| 24 MWS report section | ✅ PASS | uid 12_301797 → richness 15, category Low |
| 25 Tehsil report section | ✅ PASS | total_mws=324, with_data=91, data_poor=285, avg_rich=8.4, MWS_with_threatened=21, top5=5 |
| 23 KYL indicators | ⚠ PARTIAL | biodiversity keys are wired + fed by the (now-populated) Excel sheet; a *full* KYL run needs the block's OTHER stats sheets (hydrological_annual, …) — not a biodiversity issue |

**Remaining caveats (not biodiversity code bugs):**
1. **CP16 make_asset_public** → GEE IAM: "users named in the policy do not belong to a permitted
   customer." The public-ACL binding is rejected by this GEE project's IAM. Non-blocking: GeoServer
   serves the vector data (not the live GEE asset), and getInfo/export/WFS all work. Project-IAM config.
2. **EXCEL_PATH vs EXCEL_DIR + trailing slash** (repo-wide): the Excel generator writes
   `EXCEL_PATH/data/stats_excel_files/…`; report readers use `DATA_DIR_TEMP=EXCEL_DIR` with string
   concat and no trailing slash (`…excel_filesBIHAR/…`). Affects ALL layers' reports. Bridged in
   validation by copying to the reader path. Reconcile in deployment (set EXCEL_DIR trailing slash +
   align the two base dirs).

**Net: CP1–25 all validated live** (CP16 GEE-IAM caveat; CP23 needs the block's other stats; both non-biodiversity).

## Local map render — ✅ CONFIRMED (WMS choropleth)
- Fixed the SLD file: removed a stray `</content>` line (line 97) that made it invalid XML.
- Uploaded the SLD **body** via REST (`POST /rest/workspaces/biodiversity/styles?name=biodiversity_mws`
  with `Content-Type: application/vnd.ogc.sld+xml`) — the earlier `upload_style` created only the entry,
  not the body (`No such resource: biodiversity_mws.sld`). Set as the layer's default style.
- **WMS GetMap renders correctly**: 324 MWS polygons; data-poor MWS grey, richness ramp green.
  Verified as a real PNG (900x900).
- View URLs (GeoServer up at :8080):
  - Layer Preview: http://localhost:8080/geoserver/web/ → Layer Preview → biodiversity:jamui_jamui_biodiversity
  - WMS GetMap: `.../biodiversity/wms?...request=GetMap&layers=biodiversity:jamui_jamui_biodiversity&styles=biodiversity_mws&bbox=...&srs=EPSG:4326&width=900&height=900&format=image/png`
- Note: the production CoRE Stack **frontend is a separate repo**; it consumes this same GeoServer
  WMS/WFS layer + the KYL JSON. GeoServer's Layer Preview is the built-in local viewer.

## STEP 4 follow-up — M1 + M3 applied & validated
- **M1:** moved the 3 derived fields (dominant_class, biodiversity_category, observation_density_per_km2)
  + numeric shannon/simpson/pielou into GEE (`gbif_mws_stats`). `biodiversity_task` now publishes via
  **`sync_fc_to_geoserver(ee.FeatureCollection(asset_id), state, layer_name, WORKSPACE, style_name=...)`**
  — full reuse (GeoPackage + style), no inline getInfo/enrich/gpkg. **Deleted `gbif_export.py`** (logic
  absorbed into GEE). Task is simpler (removed getInfo, enrich_and_clean, inline gpkg, 3 imports, `os`).
- **M3:** `installation/setup_local_geoserver.py` now provisions the `biodiversity_mws` SLD idempotently
  (delete-then-POST body — `upload_style` only writes the body on first create).
- **data_poor** changed from Python-boolean to a uniform GEE **integer 0/1** (GEE `n.lt` yields Long).
  Downstream is representation-agnostic (`bool()`); SLD updated to compare `== 1`. Only deviation from
  byte-identical; all aggregates unchanged.
- **Regression (recompute + re-sync + re-render), all IDENTICAL to pre-refactor:**
  WFS 324 feats / 91 populated / derived fields present; Excel Σ richness=2722, Σ threatened=35;
  Tehsil total 324 / with_data 91 / data_poor 285 / avg 8.4 / threatened_MWS 21; **WMS choropleth renders**.
- Module now 6 files: config, gbif_download, gbif_clean, gbif_iucn, gbif_mws_stats, biodiversity_task.

## Second real block — karnataka/hassan/hassan  ✅ (full pipeline, ~6.7 min)
- TASK: layer_id=26, synced=True, doi=10.15468/dl.4ebch5.
- WFS: 117 MWS, 75 populated. Richest MWS 1_18689 → 153 species, 511 records, 2 threatened,
  dominant Aves, category "Very High".
- Excel: 117 rows, Σ species_richness=2885, Σ threatened=38.
- Tehsil: total 117, with_data 75, data_poor 75, avg richness 24.7, threatened_MWS 28.
- WMS choropleth renders (green richness ramp, grey data-poor). Second block confirms the pipeline
  generalizes beyond the Jamui validation block.
- Note: karnataka/uttara_kannada/joida was requested but has NO MWS asset in GEE — biodiversity can't
  run there until the upstream MWS pipeline (generate_mws_layer) is run for that block.

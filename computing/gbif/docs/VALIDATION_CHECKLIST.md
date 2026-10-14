# Biodiversity (GBIF) Module — Validation Checklist

> Official testing checklist for the Biodiversity module. Work top to bottom. Each stage lists its
> **purpose, expected input/output, how to verify, a pass box, failure symptoms, and the files
> involved**. Reference numbers are from a real all-taxa run on **bihar / jamui / jamui** (324 MWS).
> Architecture context: [`PIPELINE.md`](PIPELINE.md). Build history: [`IMPLEMENTATION_LOG.md`](IMPLEMENTATION_LOG.md).

---

## 0. Prerequisites (must all be true before validating)

| # | Prerequisite | How to check / set | ☐ |
|---|---|---|---|
| P1 | Conda env active | `conda activate corestackenv` | ☐ |
| P2 | GBIF account creds in env | `.env` has `GBIF_USER`, `GBIF_PWD`, `GBIF_EMAIL` | ☐ |
| P3 | GEE service account works | `python manage.py shell -c "from utilities.gee_utils import ee_initialize; ee_initialize('1'); import ee; print(ee.Number(1).getInfo())"` → 1 | ☐ |
| P4 | Target block's MWS asset exists in GEE | `filtered_mws_<district>_<block>_uid` under `get_gee_asset_path(...)` | ☐ |
| P5 | SOI rows for state/district/block | `StateSOI/DistrictSOI/TehsilSOI` present | ☐ |
| P6 | `Dataset` "Biodiversity Occurrence" in DB | from seed (`loaddata`) or `Dataset.objects.get_or_create(...)` | ☐ |
| P7 | `LayerInfo` registered | `python manage.py register_biodiversity_layer` (idempotent) | ☐ |
| P8 | GeoServer running + reachable | `curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8080/geoserver/web/` → 200 or 302 | ☐ |
| P9 | GeoServer `biodiversity` workspace + `biodiversity_mws` style | install.sh `geoserver` step, or `python installation/geoserver_style_bundle.py sync ...`; SLD at `installation/geoserver/styles/workspaces/biodiversity/biodiversity_mws.sld` (in `manifest.json`) | ☐ |

**One-shot trigger** (runs CP1–19): `POST /api/v1/generate_biodiversity_layer/` `{state, district, block, gee_account_id}` — or, synchronously for validation:
`python manage.py shell -c "from computing.gbif.biodiversity_task import generate_biodiversity_block as g; print(g('bihar','jamui','jamui',1))"`

---

## Stage checklist (CP1–CP25)

### CP1 — User request received
- **Purpose:** entry point; a location is the only user input.
- **Input:** `{state, district, block, gee_account_id}`.
- **Output:** Celery task queued (`{"Success": "biodiversity task initiated"}`).
- **Verify:** POST the endpoint; expect 200 + task on the `nrm` queue.
- **Failure:** 500 / missing-arg error.
- **Files:** `computing/api.py`, `computing/urls.py`, `computing/gbif/biodiversity_task.py`. ☐

### CP2 — Block geometry fetched
- **Purpose:** load the MWS polygons for the block.
- **Input:** state/district/block.
- **Output:** an `ee.FeatureCollection` of MWS polygons (jamui: **324**).
- **Verify:** `load_mws_featurecollection(...).size().getInfo()` > 0.
- **Failure:** 0 features / asset-not-found → MWS layer not generated for the block (P4).
- **Files:** `gbif_mws_stats.py` (`load_mws_featurecollection`). ☐

### CP3 — Bounding box generated
- **Purpose:** derive the GBIF query extent from the MWS asset.
- **Input:** MWS asset.
- **Output:** WKT `POLYGON((…))` (jamui: `POLYGON((85.77 24.33, …))`).
- **Verify:** `get_block_bbox_wkt(...)` starts with `POLYGON`.
- **Failure:** empty/invalid WKT → MWS geometry issue.
- **Files:** `gbif_download.py` (`get_block_bbox_wkt`). ☐

### CP4 — GBIF download (all taxa)
- **Purpose:** pull all occurrence records in the bbox.
- **Input:** bbox WKT + predicates.
- **Output:** raw `occurrences_raw.csv` (jamui: **11.8 MB, 21,859 rows**), cached under `computing/gbif/_data/<s>/<d>/<b>/`.
- **Verify:** file exists, non-empty; `meta.txt` written.
- **Failure:** GBIF 400 (bad predicate — needs camelCase keys + JSON in-list), auth 401 (P2), or KILLED (rare for block size).
- **Files:** `gbif_download.py` (`request_block_download`, `wait_and_fetch`, `download_block_occurrences`). ☐

### CP5 — DOI obtained
- **Purpose:** citation provenance.
- **Input:** download key.
- **Output:** DOI string (jamui: `10.15468/dl.vu447a`).
- **Verify:** `meta["doi"]` non-null; stored in `Layer.misc.gbif_doi`.
- **Failure:** null DOI (download meta not fetched — non-fatal).
- **Files:** `gbif_download.py`. ☐

### CP6 — Download key obtained
- **Purpose:** re-fetch/debug handle.
- **Output:** key like `0019293-260623161305970`.
- **Verify:** `meta["download_key"]` set; in `Layer.misc.download_key`.
- **Failure:** missing key.
- **Files:** `gbif_download.py`. ☐

### CP7 — Raw record count
- **Purpose:** data-volume/QA baseline.
- **Output:** integer (jamui: 21,859).
- **Verify:** `meta["raw_record_count"]` == CSV row count.
- **Failure:** mismatch / 0 records (empty block → all MWS `data_poor`).
- **Files:** `gbif_download.py`. ☐

### CP8 — Cleaning completed
- **Purpose:** remove bad/imprecise coordinates.
- **Input:** raw CSV.
- **Output:** clean `DataFrame`.
- **Verify:** `clean_occurrences(raw_csv, None)` returns a df; log shows "cleaned N -> M".
- **Failure:** exception; drop-rate outside ~10–60%.
- **Files:** `gbif_clean.py`. ☐

### CP9 — Clean record count
- **Output:** rows + distinct species (jamui: **9,086 rows, 364 species**).
- **Verify:** `len(df)`, `df.taxonKey.nunique()`.
- **Failure:** 0 rows (over-aggressive filters).
- **Files:** `gbif_clean.py`. ☐

### CP10 — IUCN enrichment
- **Purpose:** add per-species IUCN category (not in SIMPLE_CSV).
- **Input:** clean df.
- **Output:** `iucnRedListCategory` column (jamui: LC 8520 / NT 307 / VU 118 / EN 9 / DD 5 → **9 threatened species**).
- **Verify:** column present; VU/EN/CR count plausible; cache file `_cache/iucn_by_taxonkey.json` grows.
- **Failure:** all empty (species-API unreachable) → threatened count 0.
- **Files:** `gbif_iucn.py`. ☐

### CP11 — GeoDataFrame created
- **Purpose:** attach point geometry for GEE.
- **Output:** `GeoDataFrame` of points (jamui: 9,086, `Point`).
- **Verify:** `gdf.geometry.geom_type` == Point; only needed props carried (`taxonKey/kingdom/class/iucnRedListCategory`).
- **Failure:** NaN in props (must be `fillna("")`).
- **Files:** `biodiversity_task.py`. ☐

### CP12 — FeatureCollection uploaded to Earth Engine
- **Purpose:** get points into GEE for the join.
- **Output:** `ee.FeatureCollection` (jamui: 9,086 features, ~20 s).
- **Verify:** `points_to_ee_fc(gdf, state, district, block)[0].size().getInfo()` == clean count.
- **Failure:** `Request payload size exceeds the limit: 10485760 bytes` — only possible if `config.POINTS_INLINE_LIMIT`
  was raised; above that limit the points are exported in chunks to `biodiversity_points_<district>_<block>_partN` assets.
- **Files:** `biodiversity_task.py`, `gbif_mws_stats.py` (`points_to_ee_fc`), `utilities/gee_utils.py` (`gdf_to_ee_fc`). ☐

### CP13 — Join.saveAll completed
- **Purpose:** assign each point to its MWS.
- **Verify:** join graph builds; downstream stats compute.
- **Failure:** join error (geometry/CRS mismatch).
- **Files:** `gbif_mws_stats.py` (`compute_mws_biodiversity`). ☐

### CP14 — Indicators computed
- **Purpose:** per-MWS metrics.
- **Output:** FC with all indicator keys per MWS.
- **Verify:** sample a populated MWS via getInfo; keys present: `species_richness, occurrence_count, shannon/simpson/pielou, rare_species_count, threatened_species_count, <taxon>_species_count, data_poor`.
- **Failure:** missing keys; `Type<Feature>`/`List<Feature>` export error (fixed via fresh `ee.Feature` + uniform casts + static `_OUTPUT_PROPERTIES`).
- **Files:** `gbif_mws_stats.py`. ☐

### CP15 — Earth Engine asset created
- **Purpose:** persist the per-MWS stats table.
- **Output:** asset `…/biodiversity_<district>_<block>`.
- **Verify:** `is_gee_asset_exists(config.get_gee_block_asset_id(...))` == True after `check_task_status`.
- **Failure:** export FAILED (schema error — see CP14).
- **Files:** `gbif_mws_stats.py` (`export_stats_to_asset`), `biodiversity_task.py`. ☐

### CP16 — Asset made public
- **Purpose:** asset accessibility.
- **Verify:** `make_asset_public(asset_id)` runs.
- **Known caveat:** on some GEE projects the public-ACL binding is rejected (*"users … do not belong to a permitted customer"*) → **WARN, non-blocking** (GeoServer serves the vector data; getInfo/export/WFS still work). Project-IAM config.
- **Files:** `biodiversity_task.py`, `utilities/gee_utils.py` (`make_asset_public`). ☐

### CP17 — Layer created (DB)
- **Purpose:** register the layer.
- **Output:** a `Layer` row (jamui: id 25).
- **Verify:** `save_layer_info_to_db(...)` returns a layer_id; row exists.
- **Failure:** `Dataset.DoesNotExist` (P6) or SOI lookup fail (P5).
- **Files:** `biodiversity_task.py`, `computing/utils.py` (`save_layer_info_to_db`). ☐

### CP18 — Layer.misc persisted
- **Purpose:** provenance.
- **Output:** `{gbif_doi, download_key, taxon_scope:"all", raw_record_count, clean_record_count, download_date}`.
- **Verify:** `Layer.objects.get(id=…).misc` has the keys.
- **Failure:** empty misc.
- **Files:** `biodiversity_task.py`. ☐

### CP19 — GeoServer sync
- **Purpose:** publish the per-MWS vector layer.
- **Input:** enriched GeoJSON.
- **Output:** GeoServer layer `biodiversity:<district>_<block>_biodiversity` (**GeoPackage**, not shapefile).
- **Verify:** task returns `synced=True`; `update_layer_sync_status` sets the flag.
- **Failure:** GeoServer down (P8) / workspace missing (P9). **Must be GeoPackage** — shapefile truncates field names to 10 chars.
- **Files:** `biodiversity_task.py`, `computing/utils.py` (`push_shape_to_geoserver`, `fix_invalid_geometry_in_gdf`). ☐

### CP20 — Layer visible via WFS (full names + values)
- **Purpose:** confirm the published layer is queryable with correct fields.
- **Verify:** `GET …/biodiversity/ows?…GetFeature&typeName=biodiversity:<layer>&outputFormat=application/json` → feature count == MWS count (324); property names are **full** (`species_richness`, not `species_ri`); ≥1 MWS has `occurrence_count>0` (jamui: 91 populated).
- **Failure:** truncated names (shapefile — see CP19) / 0 features / empty values.
- **Files:** (GeoServer) — produced by CP19. ☐

### CP21 — SLD applied
- **Purpose:** choropleth styling.
- **Verify:** upload `biodiversity_mws.sld` (skip if exists); `publish_style`; `GET /rest/layers/biodiversity:<layer>.json` → `defaultStyle.name` == `biodiversity:biodiversity_mws`.
- **Failure:** upload 400 (SLD/version); 500 "already exists" (idempotency — treat as OK, then just `publish_style`). Requires full field names (CP20).
- **Files:** `installation/geoserver/styles/biodiversity_mws.sld`, `utilities/geoserver_utils.py`. ☐

### CP22 — Excel generated
- **Purpose:** the `biodiversity` sheet (bridge to KYL/reports).
- **Verify:** `get_vector_layer_geoserver(state,district,block, specific_sheets=["biodiversity"])`; open `EXCEL_PATH/data/stats_excel_files/<S>/<D>/<d>_<b>.xlsx` → sheet `biodiversity`, 18 cols, **values populated** (jamui: Σ species_richness=2722).
- **Failure:** empty values → field-name truncation (CP19) not fixed.
- **Files:** `stats_generator/utils.py` (`create_excel_for_biodiversity`). ☐

### CP23 — KYL indicators
- **Purpose:** dashboard filter keys.
- **Verify:** regenerate KYL; keys present per MWS: `species_richness, occurrence_count, threatened_species_count, shannon_diversity_index, dominant_taxon_group, biodiversity_category, biodiversity_data_poor`.
- **Known caveat:** a **full** KYL run needs the block's *other* stats sheets (`hydrological_annual`, …) — if absent, KYL fails before biodiversity for reasons unrelated to this module. Validate the biodiversity keys once the block's full stats exist.
- **Files:** `stats_generator/mws_indicators.py`. ☐

### CP24 — MWS report section
- **Purpose:** per-watershed narrative + table.
- **Verify:** `get_biodiversity_data(state,district,block,uid)` → non-empty desc + `{species_richness, threatened_species_count, biodiversity_category, …}` (jamui uid → richness 15, category Low).
- **Failure:** empty (Excel not at reader path — see EXCEL_DIR caveat below).
- **Files:** `dpr/gen_mws_report.py`, `dpr/api.py`, `templates/mws-report.html`. ☐

### CP25 — Tehsil report section
- **Purpose:** block rollup.
- **Verify:** `get_biodiversity_summary_data(state,district,block)` → `total_mws>0` (jamui: total 324, with_data 91, data_poor 285, avg 8.4, threatened_MWS 21, top5).
- **Failure:** `total_mws=0` → Excel not at reader path (EXCEL_DIR caveat).
- **Files:** `dpr/gen_tehsil_report.py`, `dpr/api.py`, `templates/block-report.html`. ☐

---

## Known gotchas (discovered during validation — check these first if a stage fails)

1. **Field-name truncation (CP20/22/24/25 empty values):** GeoServer **must** publish via **GeoPackage**, not shapefile. Shapefile truncates names to 10 chars; the task uses `push_shape_to_geoserver(file_type="gpkg")`. If values are empty, confirm the gpkg path is in effect.
2. **`make_asset_public` IAM (CP16 WARN):** rejected on some GEE projects; non-blocking.
3. **EXCEL_PATH vs EXCEL_DIR (CP24/25 empty):** the generator writes `EXCEL_PATH/data/stats_excel_files/…`; report readers use `DATA_DIR_TEMP=EXCEL_DIR` via string concat with **no trailing slash** (`…excel_filesBIHAR/…`). Repo-wide (all layers). Ensure `EXCEL_DIR` ends with `/` and resolves to the generator's directory.
4. **KYL needs full block stats (CP23):** biodiversity keys are wired, but a full KYL run requires the block's other stats sheets.
5. **pygbif predicates (CP4):** camelCase keys + JSON `in`-list + `geometry within <wkt>`.
6. **Large all-taxa blocks (CP12):** above `config.POINTS_INLINE_LIMIT` points the upload is chunked into intermediate GEE assets automatically (validated on guindy, 56,809 points).

## Quick reproduce (one block, end to end)
```bash
conda activate corestackenv
# P6/P7:
python manage.py shell -c "from computing.models import Dataset; Dataset.objects.get_or_create(name='Biodiversity Occurrence', defaults={'layer_type':'vector','workspace':'biodiversity','style_name':'biodiversity_mws'})"
python manage.py register_biodiversity_layer
# P8/P9: start GeoServer, then:
curl -s -u admin:geoserver -H 'Content-Type: application/json' -d '{"workspace":{"name":"biodiversity"}}' http://localhost:8080/geoserver/rest/workspaces
python installation/geoserver_style_bundle.py sync --url http://localhost:8080/geoserver --username admin --password geoserver
# CP1–19:
python manage.py shell -c "from computing.gbif.biodiversity_task import generate_biodiversity_block as g; print(g('bihar','jamui','jamui',1))"
# CP20 (WFS):
curl -s "http://localhost:8080/geoserver/biodiversity/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=biodiversity:jamui_jamui_biodiversity&outputFormat=application/json&maxFeatures=1"
# CP22 (Excel):
python manage.py shell -c "from stats_generator.utils import get_vector_layer_geoserver as x; x('bihar','jamui','jamui', specific_sheets=['biodiversity'])"
```
</content>

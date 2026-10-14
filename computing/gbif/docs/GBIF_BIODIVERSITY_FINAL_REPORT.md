# GBIF Biodiversity Module — Final Report

> Capstone summary of the Biodiversity (GBIF) module: what it delivers, its architecture, what was
> validated (live, on real data), how to deploy it, the design decisions, and the known caveats.
> Companion docs: [`PIPELINE.md`](PIPELINE.md) (deep architecture), [`REVIEW.md`](REVIEW.md)
> (architecture review), [`VALIDATION_CHECKLIST.md`](VALIDATION_CHECKLIST.md) (test checklist),
> [`IMPLEMENTATION_LOG.md`](IMPLEMENTATION_LOG.md) (build history), [`compare.md`](compare.md) +
> [`Species-Plan.md`](Species-Plan.md) (decision record), [`uk/`](uk/) (original spec).

---

## 1. Executive summary

The module adds a **per-micro-watershed biodiversity layer** to CoRE Stack. A user picks a location
(state → district → block); the system downloads every GBIF species-occurrence record in that block,
cleans it, adds IUCN threat status, and — in Google Earth Engine — assigns each record to its
micro-watershed and computes **~16 biodiversity indicators per MWS**. The result is published as a
GeoServer vector layer (map + attributes), an Excel sheet, KYL dashboard filters, and MWS/tehsil
report sections — using the **same infrastructure every other CoRE Stack layer uses**.

**Status: feature-complete and validated end-to-end on a real all-taxa block** (bihar/jamui/jamui,
324 MWS). The choropleth renders live in GeoServer. Remaining items are low-value polish and two
deployment-config concerns that affect all layers (not this module).

---

## 2. Architecture (in brief — see PIPELINE.md for depth)

```
User(state/district/block) → API → Celery task
  → GBIF download (block bbox)        [gbif_download]
  → clean coordinates                 [gbif_clean]
  → IUCN enrichment (species API)     [gbif_iucn]
  → points → ee.FeatureCollection     [gdf_to_ee_fc]
  → Join.saveAll + per-MWS indicators [gbif_mws_stats]  (GEE, server-side)
  → export to GEE asset               [export_vector_asset_to_gee]
  → register Layer(+misc) + publish   [save_layer_info_to_db + sync_fc_to_geoserver]
  → GeoServer vector layer  →  Excel  →  KYL  →  MWS/Tehsil reports
```

Key choices: **block-first** (small, testable, independent), **GEE-native** compute (the MWS polygons
already live in GEE; the point-in-polygon join runs server-side and preserves species identity —
which rasterization would destroy), and **full reuse** of the CoRE Stack export → GeoServer → Excel →
report chain.

---

## 3. File inventory

| File | Responsibility | Native? |
|---|---|---|
| `computing/gbif/config.py` | Tunables + `get_gee_block_asset_id` | biodiversity-specific config |
| `computing/gbif/gbif_download.py` | Block bbox (from MWS asset) + GBIF Download API + provenance | external service (GBIF) |
| `computing/gbif/gbif_clean.py` | 5-filter coordinate cleaning | biodiversity-specific |
| `computing/gbif/gbif_iucn.py` | Per-species IUCN category lookup (cached) | external service (IUCN) |
| `computing/gbif/gbif_mws_stats.py` | GEE `Join.saveAll` + all indicators (incl. derived) + export | the novel computation |
| `computing/gbif/biodiversity_task.py` | Celery orchestrator (mirrors `change_detection`) | thin glue |
| `computing/management/commands/register_biodiversity_layer.py` | Idempotent `LayerInfo` registration | mirrors `seed_default_plantation` |
| `installation/geoserver/styles/biodiversity_mws.sld` | Richness choropleth style | asset |

**Edits to existing files (extensions, not new subsystems):** `computing/api.py` + `urls.py` (endpoint),
`stats_generator/utils.py` (`create_excel_for_biodiversity`), `stats_generator/mws_indicators.py` (KYL keys),
`dpr/gen_mws_report.py` + `dpr/gen_tehsil_report.py` + `dpr/api.py` + `templates/*.html` (report sections),
`installation/seed/seed_data.json` (`Dataset` row), `installation/geoserver/styles/` + `manifest.json` (bundled SLD) + `install.sh` workspace list,
`installation/install.sh` + `INSTALLATION.md` (register step).

**Reuses (no reimplementation):** `ee_initialize, get_gee_asset_path, valid_gee_text, gdf_to_ee_fc,
export_vector_asset_to_gee, check_task_status, is_gee_asset_exists, make_asset_public,
save_layer_info_to_db, update_layer_sync_status, sync_fc_to_geoserver, Geoserver.upload_style`.
**No new DB model** (`Layer`/`Dataset`/`LayerInfo`/`Layer.misc` cover it).

---

## 4. Indicators produced (per MWS)

`species_richness, occurrence_count, shannon_diversity_index, simpson_diversity_index, pielou_evenness,
rare_species_count, threatened_species_count (IUCN VU/EN/CR), bird/mammal/plant/reptile/amphibian/insect_species_count,
dominant_class, biodiversity_category, observation_density_per_km2, data_poor`. Plus `Layer.misc`
provenance: `gbif_doi, download_key, taxon_scope, raw_record_count, clean_record_count, download_date`.

Design guardrail: `occurrence_count` and `data_poor` always travel with richness, because GBIF is
opportunistic — "low richness" often means "under-surveyed", not "low biodiversity".

---

## 5. What was validated (live, real data — bihar/jamui/jamui)

| Stage group | Result |
|---|---|
| Download → clean → IUCN | 21,859 raw → 9,086 clean, **364 species, 9 threatened** (VU/EN) |
| Points → GEE → Join → indicators | 9,086-pt FC in ~20 s; 324 MWS computed; all indicators correct |
| Export → asset → register | GEE asset created; `Layer` id=25 with full `misc` |
| GeoServer sync (GeoPackage) | 324 features, **full field names**, 91 populated MWS |
| SLD | `biodiversity_mws` provisioned + applied; **WMS choropleth renders** (grey data-poor, green richness) |
| Excel | `biodiversity` sheet, 18 cols, **Σ richness=2722, Σ threatened=35** |
| Tehsil report | total 324, with_data 91, data_poor 285, avg 8.4, threatened_MWS 21 |
| MWS report | per-watershed narrative + indicators (e.g. richness 15, category Low) |

Full stage-by-stage evidence: [`VALIDATION_CHECKLIST.md`](VALIDATION_CHECKLIST.md) and
[`IMPLEMENTATION_LOG.md`](IMPLEMENTATION_LOG.md).

---

## 6. Deployment runbook

```bash
conda activate corestackenv

# 1. GBIF account credentials in nrm_app/.env
#    GBIF_USER=...  GBIF_PWD=...  GBIF_EMAIL=...

# 2. Registry (fresh install runs these via install.sh automatically; on an existing DB run manually):
python manage.py loaddata installation/seed/seed_data.json      # Dataset "Biodiversity Occurrence"
python manage.py register_biodiversity_layer                    # LayerInfo (idempotent)

# 3. GeoServer up, then provision workspace + style (install.sh's geoserver step does both):
curl -s -u admin:geoserver -H 'Content-Type: application/json' -d '{"workspace":{"name":"biodiversity"}}' http://localhost:8080/geoserver/rest/workspaces
python installation/geoserver_style_bundle.py sync --url http://localhost:8080/geoserver --username admin --password geoserver

# 4. Generate the layer for a block (MWS asset must exist for that block):
curl -X POST http://localhost:8080/api/v1/generate_biodiversity_layer/ \
  -H "Content-Type: application/json" \
  -d '{"state":"bihar","district":"jamui","block":"jamui","gee_account_id":1}'
#   (or synchronously: manage.py shell -c "from computing.gbif.biodiversity_task import generate_biodiversity_block as g; g('bihar','jamui','jamui',1)")

# 5. Downstream (same endpoints as other layers): stats Excel → KYL → reports.
# View: GeoServer Layer Preview → biodiversity:<district>_<block>_biodiversity
```

---

## 7. Known caveats (all non-blocking; none are defects in this module)
1. **`make_asset_public`** — this GEE project's IAM rejects the public binding (WARN). GeoServer serves
   the vector data, so it doesn't matter for the layer.
2. **`EXCEL_PATH` vs `EXCEL_DIR`** — the Excel generator and report readers use different base dirs, and
   `DATA_DIR_TEMP` string-concat lacks a trailing slash. Repo-wide (all layers). Reconcile in deployment.
3. **`data_poor` is integer 0/1** (GEE-native), not boolean; downstream uses `bool()`, so semantics are
   identical; the SLD compares `== 1`.
4. **Full KYL** needs the block's other stats sheets (`hydrological_annual`, …) — not a biodiversity issue.
5. **Points upload scale** — Earth Engine rejects requests over 10 MB. Blocks above
   `config.POINTS_INLINE_LIMIT` cleaned points are exported in chunks to intermediate GEE assets and merged
   server-side (`gbif_mws_stats.points_to_ee_fc`); smaller blocks go inline via `gdf_to_ee_fc`.

---

## 8. Key design decisions (defensible)
- **No rasterization** — would destroy species identity (richness = distinct `taxonKey`); the join preserves it.
- **Block-first + GEE-native** — small/testable, reuses the MWS asset + the whole export/publish chain.
- **No `GBIFBlockDownload` model** — GEE layers use `Layer` + `is_gee_asset_exists`, not a status model.
- **GeoPackage sync** (not shapefile) — shapefile truncates field names to 10 chars.
- **IUCN enrichment** — the category is per-species (species API), not in the occurrence CSV.
- **All-taxa** — richness + per-class taxonomy only make sense over all taxa.
- **`Layer.misc` for provenance** — the repo's slot for qualifying parameters (like change_detection's years).

---

## 9. Optional future polish (from REVIEW.md, all Low)
Trim dead `_USECOLS` columns (L1); rename `get_gee_block_asset_id` (L2); read `misc` on idempotent skip (L3);
log/skip `make_asset_public` (L4). None blocking.

## 10. Not in scope (deferred by design)
Change-over-time (Level B / temporal), `dominant_species` name, threatened-species name list, endemic/invasive
flags, habitat-stratified diversity, national rollout. See `uk/output_design.md` §10 for the roadmap.

---

**Conclusion.** The Biodiversity module is complete, validated on real data end-to-end (data → GEE →
GeoServer map → Excel → KYL → reports), and built as a native extension of CoRE Stack with minimal new
code and no new abstractions. It is ready for use pending the standard deployment steps in §6.
</content>

# GBIF Biodiversity — Gap Analysis vs UK Plan & Next Plans

> Deep comparison of what's **implemented** against [`uk/implementation.md`](uk/implementation.md)
> (the 11-stage pipeline spec) and [`uk/output_design.md`](uk/output_design.md) (the indicator set),
> followed by a **prioritized plan of what's left**. Scope: **UK plan + output_design only — Level B
> (change detection) is dropped** per the latest decision.
>
> Status legend: ✅ done & validated · 🟡 done, differs from doc (intentional) · ⚠️ partial · ❌ not done.

---

## 1. `implementation.md` — stage-by-stage status

| Stage | Spec | Status | Notes |
| --- | --- | --- | --- |
| 0 | Registry & config | ⚠️ | `config.py` + `Dataset` seed row (pk 53) done. **GeoServer `biodiversity` workspace + `biodiversity_mws` SLD style NOT created** (needs running GeoServer). `pygbif` added to `environment.yml` (repo uses conda, not requirements.txt); creds in `.env` done. |
| 1 | Block GBIF download | 🟡 | Done **and improved**: bbox derived from the **MWS GEE asset** (one source of truth) instead of the doc's GeoServer WFS call. Predicate-format bug found + fixed (camelCase keys + JSON `in`-list). Validated on real data. |
| 2 | Cleaning | ✅ | Reused existing 5-filter `clean_occurrences`. Validated (21,643→8,921 on pilot). |
| 3 | GCS upload | ✅ (code) | `gbif_gee_upload.csv_to_geojson` + `upload_geojson_to_gcs`. Not yet run against GCS. |
| 4 | GEE ingestion | ✅ (code) | `ingest_geojson_to_gee` + `wait_for_gee_ingestion` (reuses `gcs_to_gee_asset_cli`). Not yet run. |
| 4a | Pan-India raster | ❌ (correct) | Doc defers to v3. Not needed for MWS stats. Intentionally skipped. |
| 5 | GEE per-MWS stats | 🟡 ✅ | Done **and expanded**: doc's minimal 4 indicators → I compute the **full output_design set** (richness, occurrence, Shannon, Simpson, Pielou, rare, threatened, 6 taxonomy counts, data_poor). **Validated on the real 324-MWS Jamui asset.** |
| 6 | Post-export processing | 🟡 | Done + better: doc set `dominant_taxon_group="Unknown"` placeholder; I actually compute `dominant_class` + `biodiversity_category` + `observation_density` + NaN-fill (`gbif_export.py`). |
| 7 | GeoServer sync | ✅ (code) | `gbif_sync.py` (`sync_layer_to_geoserver` + `save_layer_info_to_db` + `update_layer_sync_status`) + auto-registers `LayerInfo`. Not yet run. |
| 8 | Excel sheet | 🟡 ✅ | `create_excel_for_biodiversity` writes **18 columns** (superset of the doc's 6 — includes all output_design indicators). Import-verified. |
| 9 | KYL filters | ⚠️ | **7 keys added** (`species_richness`, `occurrence_count`, `threatened_species_count`, `shannon_diversity_index`, `dominant_taxon_group`, `biodiversity_category`, `biodiversity_data_poor`). output_design's KYL section wants **~19** (all taxonomy + simpson/pielou/rare/observation_density). **Gap: ~12 more keys.** |
| 10 | MWS report section | 🟡 | `get_biodiversity_data` + template section done. **Missing: GBIF DOI citation** (doc pulls it from the `GBIFBlockDownload` model — not built; DOI currently only in a `meta.txt`). |
| 11 | Tehsil report section | 🟡 | `get_biodiversity_summary_data` + template done (summary + top-5). Doc's shape was `get_biodiversity_pattern_data` (summary + per-MWS patterns) — functionally equivalent, slightly different keys. |
| — | **`GBIFBlockDownload` model** | ❌ | **Not built.** No per-stage status tracking, no idempotency check, no DB-stored DOI. This is the biggest structural gap vs the doc. |
| — | Celery task | 🟡 | `generate_biodiversity_block` done, but **without** the model's status writes and the "already computed → skip" idempotency guard. |
| — | API endpoint | 🟡 | `generate_biodiversity_layer` done; uses the repo's standard `@api_view/@schema` decorators (not the doc's `@api_security_check`) — consistent with `change_detection`. |
| — | Management command | ❌ | `generate_gbif_block` CLI command not built (useful for dev/debug + national loop). |

---

## 2. `output_design.md` — indicator status

| Indicator | Version | Status | Where |
| --- | --- | --- | --- |
| species_richness | V1 | ✅ | GEE, validated |
| occurrence_count | V1 | ✅ | GEE, validated |
| shannon_diversity_index | V1 | ✅ | GEE, validated |
| data_poor | V1 | ✅ | GEE, validated |
| threatened_species_count | V1 | ✅ | via **IUCN enrichment** (`gbif_iucn.py`) — doc's "it's in SIMPLE_CSV" was **wrong**; fixed. Validated (6 in Jamui). |
| simpson_diversity_index | V1 | ✅ | GEE |
| pielou_evenness | V1 | ✅ | GEE |
| rare_species_count | V1 | ✅ | GEE |
| bird/mammal/plant/reptile/amphibian/insect_species_count | V1 | ✅ | GEE, validated (bird=16 on all-bird pilot) |
| dominant_class | V1 | ✅ | post-export |
| biodiversity_category | V1 | ✅ | post-export |
| observation_density_per_km2 | V1 | ⚠️ | computed **if** the MWS feature carries an area property; else `null`. Needs confirming the MWS asset has `area_in_ha`. |
| other_species_count | V1 | ❌ | not computed (richness − Σ taxonomy groups). Minor, post-export add. |
| dominant_species (name) | V2 | ❌ (correct) | doc defers to V2. |
| threatened species **names** list | V2 | ❌ (correct) | doc defers to V2. |
| temporal / endemic / invasive / habitat-stratified | V3+ | ❌ (correct) | out of scope. |

**Indicator verdict:** all **V1 indicators are implemented and computing correctly** except
`other_species_count` (minor) and `observation_density` (data-availability dependent). V2/V3 correctly deferred.

---

## 3. Intentional deviations (done differently, on purpose)

1. **bbox from GEE MWS asset**, not GeoServer WFS — one source of truth, matches `change_detection.py`.
2. **More indicators than implementation.md's Stage 5** — implemented the full `output_design` set, not the minimal 4.
3. **`dominant_class` actually computed**, not the doc's `"Unknown"` placeholder.
4. **Excel = 18 columns** (superset) so all indicators are available downstream.
5. **No `computing/biodiversity/` folder** — implemented in existing `computing/gbif/` reusing scaffolding.

---

## 4. Prioritized next plans

### P0 — required to be genuinely "done" end-to-end
1. **Run the full pipeline once, real, all-taxa** for one block via `generate_biodiversity_layer`
   (pilot so far was birds-only + the GEE compute tested in isolation). Needs:
   - GeoServer running at `localhost:8080`.
   - Create workspace `biodiversity` + upload `biodiversity_mws` SLD (Stage 0 leftover).
   - Verify the GCS→ingest→join→export→GeoServer tail actually completes.
2. **`GBIFBlockDownload` model** (implementation.md core): status per stage + `doi` + task ids +
   idempotency ("already READY → skip"). Wire status writes into `generate_biodiversity_block`.
   Add the migration. *This is the main structural gap.*

### P1 — output_design compliance
3. **Complete the KYL key set** (~12 more keys) in `mws_indicators.py` so the dashboard can filter on
   all indicators (taxonomy counts, simpson, pielou, rare, observation_density), matching
   output_design §5. The values already exist in the Excel sheet — just extract + append.
4. **GBIF DOI in the MWS report** (citation/provenance) — surface from the `GBIFBlockDownload` record
   (after P0#2) or from the cached `meta.txt` in the interim.
5. **`other_species_count`** — trivial post-export addition.

### P2 — nice to have / operational
6. **Management command** `generate_gbif_block` for CLI runs + national iteration.
7. **STAC `layer_mapping.csv`** entry + `load_layer_mappings` (catalog/STAC visibility).
8. **Confirm `observation_density`** by checking the MWS asset's area property key.
9. **Cleanup**: remove the superseded Level-B / Python modules (`gbif_species_change.py`,
   `gbif_richness.py`, `species_task.py`, the `species_richness`/`species_change` endpoints + routes)
   so `generate_biodiversity_layer` is the single entry point.

---

## 5. Manual verification checklist (for you, before moving forward)

- [ ] Read this file + [`IMPLEMENTATION_LOG.md`](IMPLEMENTATION_LOG.md) (per-phase detail + pilot results).
- [ ] Confirm the scope call: **UK plan Level-A only, no change detection** (already applied).
- [ ] Decide P0#2: do we need the `GBIFBlockDownload` status model now, or is `Layer`+`LayerInfo` enough for v1?
- [ ] Decide P1#3: does the frontend need all ~19 KYL keys, or are the 7 headline keys enough for v1?
- [ ] Confirm GeoServer is up locally so I can finish the P0#1 real run.
- [ ] Decide P2#9: remove the Level-B leftovers now, or keep them dormant?

Once you've reviewed, tell me which P-items to execute and I'll proceed.
</content>

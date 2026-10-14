# Biodiversity (GBIF) Module — Architecture Review

> Final architecture review of the module against the CoRE Stack conventions and `change_detection`.
> Reviewed after the M1/M3 consolidation. **Verdict: production-ready** — the code is native, reuses
> the standard helpers, introduces no new models/abstractions, and is validated end-to-end. Remaining
> items are low-value polish and two environment/config concerns outside the module.

## What the review changed (applied)
| Item | Action taken | Result |
|---|---|---|
| **M1** — bespoke GeoServer/GeoPackage code in the task | Moved the 3 derived fields (`dominant_class`, `biodiversity_category`, `observation_density_per_km2`) + numeric diversity indices into GEE (`gbif_mws_stats`); task now calls **`sync_fc_to_geoserver(...)`** in full (GeoPackage + style). **Deleted `gbif_export.py`.** | Full reuse; task simpler; **output values identical** (regression: Σ richness=2722, Σ threatened=35, tehsil 324/91/285/8.4/21; WMS choropleth unchanged) |
| **M3** — SLD body never provisioned (`upload_style` only writes body on first create) | Added idempotent SLD provisioning to `installation/setup_local_geoserver.py` (delete-then-POST) | Style provisions reliably; `create_styles` verified `OK` |
| (fallout) `data_poor` type | GEE `n.lt` yields `Long`; standardized to uniform integer `0/1`; SLD compares `== 1` | Renders correctly; downstream `bool()` unaffected |

## Remaining findings (all Low — not blocking)
| # | Issue | Severity | Recommended Fix | Reason |
|---|---|---|---|---|
| L1 | Dead columns read by `gbif_clean._USECOLS` (`stateProvince`, `year`, `eventDate`, `iucnRedListCategory`) | Low | Trim `_USECOLS` to used columns | Leftovers from dropped Level-B / pre-IUCN design; harmless (`usecols` lambda tolerates missing) |
| L2 | `config.get_gee_block_asset_id` name is generic (returns the *biodiversity* asset) | Low | Rename to `get_biodiversity_asset_id` | Clarity only |
| L3 | Idempotent skip still runs download+clean+IUCN to compute `clean_record_count` for `misc` | Low | Read existing `Layer.misc` on skip | Minor; download is cached so cost is small |
| L4 | `make_asset_public` failure swallowed (GEE IAM rejects it here) | Low | Optional: log a warning; or skip for biodiversity (GeoServer serves the data, not the live asset) | Matches `change_detection` (keep for consistency) |
| L5 | `data_poor` is int `0/1` rather than boolean | Low | Leave as-is (downstream uses `bool()`) | GEE-native; SLD + reports + KYL all handle it |

## Environment / config (repo-wide — not this module)
| Item | Impact | Fix |
|---|---|---|
| `EXCEL_PATH` (generator) vs `EXCEL_DIR` (report readers) diverge; `DATA_DIR_TEMP` string-concat lacks a trailing slash (`…excel_filesBIHAR/…`) | Reports read a different path than the generator writes — affects **all** layers | Set `EXCEL_DIR` with a trailing slash and align it with `EXCEL_PATH/data/stats_excel_files` |
| GEE project IAM rejects the `make_asset_public` binding | `make_asset_public` WARN (non-blocking) | Project-level IAM setting |
| Full KYL run needs the block's other stats sheets | KYL fails before biodiversity if the block has no `hydrological_annual`, etc. | Generate the block's full stats before KYL (not a biodiversity concern) |

## Verified clean (no action)
- **No unused imports** (AST scan) · **no dead config constants** · **no new models** (`GBIFBlockDownload` avoided).
- **Reuses**: `ee_initialize, get_gee_asset_path, valid_gee_text, gdf_to_ee_fc, export_vector_asset_to_gee, check_task_status, is_gee_asset_exists, make_asset_public, save_layer_info_to_db, update_layer_sync_status, sync_fc_to_geoserver, Geoserver.upload_style`.
- **Task mirrors `change_detection`**: idempotency guard → export-to-asset → register → sync.
- **6 modules, all genuinely biodiversity-specific**: `config, gbif_download, gbif_clean, gbif_iucn, gbif_mws_stats, biodiversity_task` (+ `register_biodiversity_layer` command).
- **No bespoke GeoServer / GCS / model code remains.**

## Recommendation
Ship it. The module is a natural extension of the repo. L1–L5 are optional polish; the two environment
items (EXCEL_DIR path, GEE IAM) are deployment-config concerns that affect all layers, not defects in
this module.
</content>

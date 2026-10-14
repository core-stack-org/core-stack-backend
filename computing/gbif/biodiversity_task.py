"""
Phase 6 — Celery orchestrator for the block-first, GEE-native biodiversity layer (UK plan).

Runs the whole pipeline for one block:
  1. download GBIF for the block bbox   (gbif_download)
  2. clean + IUCN enrichment            (gbif_clean, gbif_iucn)
  3. points -> GEE FeatureCollection     (inline, or chunked GEE assets for large blocks)
  4. per-MWS indicators in GEE -> GCS    (gbif_mws_stats)
  5. register + publish to GeoServer      (sync_fc_to_geoserver — GeoPackage + style)

Follows the repo task pattern used by get_change_detection (@app.task(bind=True), queue="nrm").
The same task runs for one block or, iterated, for national coverage.
"""

import logging

import ee
import geopandas as gpd
from nrm_app.celery import app
from utilities.gee_utils import (
    ee_initialize,
    check_task_status,
    make_asset_public,
    is_gee_asset_exists,
    valid_gee_text,
)
from computing.utils import (
    save_layer_info_to_db,
    update_layer_sync_status,
    sync_fc_to_geoserver,
)

from . import config
from .gbif_download import download_block_occurrences
from .gbif_clean import clean_occurrences
from .gbif_iucn import enrich_with_iucn
from .gbif_mws_stats import (
    load_mws_featurecollection,
    points_to_ee_fc,
    delete_points_assets,
    compute_mws_biodiversity,
    export_stats_to_asset,
)

logger = logging.getLogger(__name__)


@app.task(bind=True)
def generate_biodiversity_block(self, state, district, block, gee_account_id):
    """Complete per-block biodiversity pipeline. See module docstring for stages."""
    for var in ("GBIF_USER", "GBIF_PWD", "GBIF_EMAIL"):
        if not getattr(config, var):
            raise RuntimeError(f"Missing {var}. Add GBIF account credentials to the environment.")

    ee_initialize(gee_account_id)

    # 1. download (cached) — provenance kept for Layer.misc
    raw_csv, meta = download_block_occurrences(state, district, block)

    # 2. clean + IUCN enrichment (iucnRedListCategory is not in SIMPLE_CSV; looked up per species)
    df = clean_occurrences(raw_csv, None)
    df = enrich_with_iucn(df)
    df = df.dropna(subset=["decimalLatitude", "decimalLongitude", "taxonKey"])

    asset_id = config.get_gee_block_asset_id(state, district, block)

    # 3-4. compute per-MWS indicators in GEE and persist to the asset.
    #      Idempotent: skip the compute+export if the asset already exists (as change_detection does).
    if not is_gee_asset_exists(asset_id):
        # cleaned points -> GEE FeatureCollection (inline for small blocks, chunked assets above
        # config.POINTS_INLINE_LIMIT because GEE caps a request at 10 MB);
        # carry only the properties the indicators use, NaN-free, so the FC serializes cleanly.
        props = df[["taxonKey", "kingdom", "class", "iucnRedListCategory"]].copy()
        props["taxonKey"] = props["taxonKey"].astype("int64").astype(str)
        props = props.fillna("")
        gdf = gpd.GeoDataFrame(
            props,
            geometry=gpd.points_from_xy(df["decimalLongitude"], df["decimalLatitude"]),
            crs="EPSG:4326",
        )
        gbif_fc, part_ids = points_to_ee_fc(gdf, state, district, block)
        mws_fc = load_mws_featurecollection(state, district, block)
        stats_fc = compute_mws_biodiversity(gbif_fc, mws_fc)  # join points to MWS, compute indicators
        task_id, asset_id = export_stats_to_asset(state, district, block, stats_fc)
        check_task_status([task_id])
        if not is_gee_asset_exists(asset_id):
            raise RuntimeError(f"Biodiversity stats export did not produce {asset_id}")
        delete_points_assets(part_ids)

    # 5. register in DB + publish to GeoServer via the shared helper.
    #    sync_fc_to_geoserver reads the asset, writes a GeoPackage (full field names, unlike a
    #    shapefile), publishes the layer, and applies the biodiversity_mws style.
    layer_name = (
        f"{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}_biodiversity"
    )
    # GBIF provenance that cannot be derived from Layer/Dataset/LayerInfo (matches the repo's
    # convention of storing only qualifying parameters in Layer.misc, e.g. change_detection's years).
    misc = {
        "gbif_doi": meta.get("doi"),
        "download_key": meta.get("download_key"),
        "taxon_scope": "all",
        "raw_record_count": meta.get("raw_record_count"),
        "clean_record_count": int(len(df)),
        "download_date": meta.get("download_date"),
    }
    layer_id = save_layer_info_to_db(  # Layer row + provenance in Layer.misc
        state,
        district,
        block,
        layer_name=layer_name,
        asset_id=asset_id,
        dataset_name=config.DATASET_NAME_VECTOR,
        algorithm=config.ALGORITHM_NAME,
        algorithm_version=config.ALGORITHM_VERSION,
        misc=misc,
    )
    make_asset_public(asset_id)
    res = sync_fc_to_geoserver(  # GeoPackage -> GeoServer (WMS/WFS) with the biodiversity_mws style
        ee.FeatureCollection(asset_id),
        state,
        layer_name,
        config.WORKSPACE,
        style_name=config.VECTOR_STYLE_NAME,
    )
    synced = bool(res) and res != "No features in FeatureCollection"
    if synced and layer_id:
        update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)

    return (
        f"biodiversity done for {district}_{block}: "
        f"layer_id={layer_id} synced={synced} doi={meta.get('doi')}"
    )

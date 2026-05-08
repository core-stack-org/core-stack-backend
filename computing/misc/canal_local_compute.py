import os
import json
import datetime
import pandas as pd
import geopandas as gpd
from nrm_app.celery import app
from utilities.gee_utils import valid_gee_text
from computing.local_compute_helper import (
    PROJECT_ROOT,
    PRECOMPUTED_TEHSIL_WATERSHED_DIR,
    build_output_vector_path,
    get_watershed_areas_in_hectares,
    load_precomputed_watersheds,
    read_validated_vector_file,
    validate_geometry,
    write_vector_output,
)
from computing.utils import (
    push_shape_to_geoserver,
    save_layer_info_to_db,
    update_layer_sync_status,
)

CANAL_VECTOR_PATH = PROJECT_ROOT / "data/canal/Canal_pan_india.geojson"
LOCAL_OUTPUT_BASE_DIR = PROJECT_ROOT / "data/canal/canal_local"
GEOSERVER_WORKSPACE = "canal"


def _compute_canal_properties_for_watersheds(watersheds_gdf, canals_gdf):
    """
    Mirroring the logic of canal_layer.py (GEE):
    1. Clips canals to ROI boundaries.
    2. Separates matched canals (inside watersheds) and gap canals (outside watersheds but inside ROI).
    3. Produces a Line layer with MWS UID and area attached to each segment.
    """
    # 0. Ensure same CRS for all spatial operations (using WGS84 for GeoServer compatibility)
    target_crs = "EPSG:4326"
    watersheds_gdf = validate_geometry(watersheds_gdf).to_crs(target_crs)
    canals_gdf = validate_geometry(canals_gdf).to_crs(target_crs)

    # ── Outer dissolved boundary of the entire ROI ─────────────────
    outer_boundary = watersheds_gdf.geometry.unary_union

    # ── Step 1: Filter canals that touch the outer boundary ────────
    canals_in_roi = canals_gdf[canals_gdf.intersects(outer_boundary)].copy()

    if canals_in_roi.empty:
        print("No canals found within the outer boundary.")
        return canals_in_roi

    # ── Step 2: For each canal, collect every watershed it touches ──
    # Using inner join to find matched canals
    matched_joined = gpd.sjoin(
        canals_in_roi,
        watersheds_gdf[["uid", "area_in_ha", "geometry"]],
        how="inner",
        predicate="intersects",
    )

    # ── Step 3: Identify Gap Canals (no watershed match) ───────────
    # A canal is a "gap" if it hits the outer boundary but no specific watershed
    matched_indices = matched_joined.index.unique()
    gap_canals = canals_in_roi.loc[~canals_in_roi.index.isin(matched_indices)].copy()

    result_segments = []

    # ── Step 4: Expand matched canals → clip to individual watersheds ──
    if not matched_joined.empty:
        print(f"Clipping {len(matched_joined)} matched canal segments...")

        def clip_matched(row):
            canal_geom = row.geometry
            # index_right is the index of the intersecting watershed in watersheds_gdf
            watershed_geom = watersheds_gdf.at[row.index_right, "geometry"]
            row.geometry = canal_geom.intersection(watershed_geom)
            return row

        matched_fc = matched_joined.apply(clip_matched, axis=1)
        result_segments.append(matched_fc)

    # ── Step 5: Handle gap canals → clip to outer ROI boundary ─────
    if not gap_canals.empty:
        print(f"Clipping {len(gap_canals)} gap canal segments...")
        gap_canals["uid"] = ""
        gap_canals["area_in_ha"] = ""

        def clip_gap(row):
            row.geometry = row.geometry.intersection(outer_boundary)
            return row

        gap_fc = gap_canals.apply(clip_gap, axis=1)
        result_segments.append(gap_fc)

    if not result_segments:
        # Return empty GDF with correct columns
        return gpd.GeoDataFrame(columns=canals_gdf.columns, crs=target_crs)

    # ── Step 6: Merge and Clean ───────────────────────────────────
    # Use pandas concat then cast to GeoDataFrame
    final_df = pd.concat(result_segments, ignore_index=True)
    final_gdf = gpd.GeoDataFrame(final_df, crs=target_crs)

    # Filter out empty or non-line geometries
    final_gdf = final_gdf[final_gdf.geometry.type.isin(["LineString", "MultiLineString"])]
    final_gdf = final_gdf[~final_gdf.geometry.is_empty]

    # Drop index_right if exists
    if "index_right" in final_gdf.columns:
        final_gdf = final_gdf.drop(columns=["index_right"])

    return final_gdf


def run_canal_vector_local(
    state=None,
    district=None,
    block=None,
    asset_suffix=None,
    roi=None,
    canal_vector_path=CANAL_VECTOR_PATH,
    precomputed_roi_dir=None,
    push_to_geoserver=True,
    sync_layer_metadata=True,
):
    """
    Orchestrates the local canal vector generation.
    """
    if state and district and block:
        layer_name = (
            f"{valid_gee_text(str(district).strip().lower())}_"
            f"{valid_gee_text(str(block).strip().lower())}_canal_vector"
        )
        watersheds_gdf, watershed_source = load_precomputed_watersheds(
            state=state,
            district=district,
            block=block,
            precomputed_roi_dir=precomputed_roi_dir,
        )
        print(f"Watershed boundary source: {watershed_source}")
    else:
        if not roi or not asset_suffix:
            raise ValueError(
                "For non state/district/block runs, both `roi` and `asset_suffix` are required."
            )
        layer_name = f"{asset_suffix}_canal_vector".lower()
        watersheds_gdf = read_validated_vector_file(
            roi,
            f"ROI file has no valid geometries: {roi}",
        )
        print(f"ROI source: {roi}")

    if not os.path.exists(canal_vector_path):
        raise FileNotFoundError(f"Canal source file not found: {canal_vector_path}")

    print(f"Loading canal source: {canal_vector_path}")
    canals_gdf = read_validated_vector_file(
        canal_vector_path,
        f"Canal source file has no valid geometries: {canal_vector_path}",
    )

    result_gdf = _compute_canal_properties_for_watersheds(
        watersheds_gdf=watersheds_gdf,
        canals_gdf=canals_gdf,
    )

    output_path = build_output_vector_path(
        layer_name=layer_name,
        state=state,
        district=district,
        block=block,
        output_base_dir=LOCAL_OUTPUT_BASE_DIR,
    )

    asset_id = write_vector_output(
        gdf=result_gdf,
        output_path=output_path,
        layer_name=layer_name,
    )
    print(f"Saved local canal vector: {asset_id}")

    if push_to_geoserver:
        geoserver_response = push_shape_to_geoserver(
            os.path.splitext(asset_id)[0],
            workspace=GEOSERVER_WORKSPACE,
            layer_name=layer_name,
            file_type="gpkg",
        )
        print(f"GeoServer response: {geoserver_response}")

    if sync_layer_metadata and state and district and block:
        layer_id = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=layer_name,
            asset_id=asset_id,
            dataset_name="Canal Vector",
        )
        if layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("Sync to GeoServer flag updated for canal vector")

    return True


@app.task(bind=True)
def canal_vector(
    self,
    state=None,
    district=None,
    block=None,
    asset_suffix=None,
    roi=None,
    asset_folder_list=None,
    app_type="MWS",
    gee_account_id=None,
):
    """
    Celery task for local canal vector generation.
    Matches signature of canal_layer.py
    """
    _ = self, asset_folder_list, app_type, gee_account_id
    return run_canal_vector_local(
        state=state,
        district=district,
        block=block,
        asset_suffix=asset_suffix,
        roi=roi,
        push_to_geoserver=True,
        sync_layer_metadata=True,
    )

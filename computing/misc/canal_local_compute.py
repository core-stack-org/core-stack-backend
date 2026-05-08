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
    fix_invalid_geometry_in_gdf,
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
    target_crs = "EPSG:4326"

    # Ensure CRS is set and aligned
    if watersheds_gdf.crs is None:
        watersheds_gdf.set_crs(target_crs, inplace=True)
    if canals_gdf.crs is None:
        canals_gdf.set_crs(target_crs, inplace=True)

    watersheds_gdf = validate_geometry(watersheds_gdf).to_crs(target_crs)
    canals_gdf = validate_geometry(canals_gdf).to_crs(target_crs)

    # CRITICAL: Reset index to ensure unique mapping for intersection
    watersheds_gdf = watersheds_gdf.reset_index(drop=True)

    # ── Outer dissolved boundary of the entire ROI ─────────────────
    outer_boundary = watersheds_gdf.geometry.unary_union

    # ── Step 1: Filter canals that touch the outer boundary ────────
    canals_in_roi = canals_gdf[canals_gdf.intersects(outer_boundary)].copy()

    if canals_in_roi.empty:
        print("No canals found within the outer boundary.")
        return canals_in_roi

    # ── Step 2: For each canal, collect every watershed it touches ──
    matched_joined = gpd.sjoin(
        canals_in_roi,
        watersheds_gdf[["uid", "area_in_ha", "geometry"]],
        how="inner",
        predicate="intersects",
    )

    # ── Step 3: Identify Gap Canals (no watershed match) ───────────
    matched_indices = matched_joined.index.unique()
    gap_canals = canals_in_roi.loc[~canals_in_roi.index.isin(matched_indices)].copy()

    result_segments = []

    # ── Step 4: Expand matched canals → clip to individual watersheds ──
    if not matched_joined.empty:
        print(f"Clipping {len(matched_joined)} matched canal segments...")

        def clip_matched(row):
            # Using the unique index from watersheds_gdf to get geometry
            watershed_geom = watersheds_gdf.geometry.iloc[row.index_right]
            return row.geometry.intersection(watershed_geom)

        matched_joined["geometry"] = matched_joined.apply(clip_matched, axis=1)
        # Keep only LineStrings
        matched_joined = matched_joined[
            matched_joined.geometry.type.isin(["LineString", "MultiLineString"])
        ]
        result_segments.append(matched_joined)

    # ── Step 5: Handle gap canals → clip to outer ROI boundary ─────
    if not gap_canals.empty:
        print(f"Clipping {len(gap_canals)} gap canal segments...")
        gap_canals["uid"] = ""
        gap_canals["area_in_ha"] = ""

        def clip_gap(row):
            return row.geometry.intersection(outer_boundary)

        gap_canals["geometry"] = gap_canals.apply(clip_gap, axis=1)
        gap_canals = gap_canals[
            gap_canals.geometry.type.isin(["LineString", "MultiLineString"])
        ]
        result_segments.append(gap_canals)

    if not result_segments:
        return gpd.GeoDataFrame(columns=canals_gdf.columns, crs=target_crs)

    # ── Step 6: Merge, Clean and Fix Geometries ─────────────────────
    final_gdf = gpd.GeoDataFrame(pd.concat(result_segments, ignore_index=True), crs=target_crs)
    
    # Cast to string to ensure database/geoserver compatibility
    final_gdf["uid"] = final_gdf["uid"].astype(str)
    final_gdf["area_in_ha"] = final_gdf["area_in_ha"].astype(str)

    # Filter out empty
    final_gdf = final_gdf[~final_gdf.geometry.is_empty]
    
    # Clean geometries using the same helper as GEE pipeline
    final_gdf = fix_invalid_geometry_in_gdf(final_gdf)

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

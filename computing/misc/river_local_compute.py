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

RIVER_VECTOR_PATH = PROJECT_ROOT / "data/river/River_pan_india.geojson"
LOCAL_OUTPUT_BASE_DIR = PROJECT_ROOT / "data/river/river_local"
GEOSERVER_WORKSPACE = "river"

def _compute_river_properties_for_watersheds(watersheds_gdf, rivers_gdf):
    """
    Mirroring the logic of river_layer.py (GEE):
    1. Clips rivers to ROI boundaries.
    2. Separates matched rivers (inside watersheds) and gap rivers (outside watersheds but inside ROI).
    3. Produces a Line layer with MWS UID and area attached to each segment.
    """
    watersheds_gdf = validate_geometry(watersheds_gdf)
    rivers_gdf = validate_geometry(rivers_gdf)

    # CRITICAL: Reset index to ensure unique mapping for intersection
    watersheds_gdf = watersheds_gdf.reset_index(drop=True)

    # ── Outer dissolved boundary of the entire ROI ─────────────────
    outer_boundary = watersheds_gdf.geometry.unary_union

    # ── Step 1: Filter rivers that touch the outer boundary ────────
    rivers_in_roi = rivers_gdf[rivers_gdf.intersects(outer_boundary)].copy()

    if rivers_in_roi.empty:
        print("No rivers found within the outer boundary.")
        return rivers_in_roi

    # ── Step 2: For each river, collect every watershed it touches ──
    # Using inner join to find matched rivers
    matched_joined = gpd.sjoin(
        rivers_in_roi,
        watersheds_gdf[["uid", "area_in_ha", "geometry"]],
        how="inner",
        predicate="intersects",
    )

    # ── Step 3: Identify Gap Rivers (no watershed match) ───────────
    matched_indices = matched_joined.index.unique()
    gap_rivers = rivers_in_roi.loc[~rivers_in_roi.index.isin(matched_indices)].copy()

    result_segments = []

    # ── Step 4: Expand matched rivers → clip to individual watersheds ──
    if not matched_joined.empty:
        print(f"Clipping {len(matched_joined)} matched river segments...")

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

    # ── Step 5: Handle gap rivers → clip to outer ROI boundary ─────
    if not gap_rivers.empty:
        print(f"Clipping {len(gap_rivers)} gap river segments...")
        gap_rivers["uid"] = ""
        gap_rivers["area_in_ha"] = ""

        def clip_gap(row):
            return row.geometry.intersection(outer_boundary)

        gap_rivers["geometry"] = gap_rivers.apply(clip_gap, axis=1)
        gap_rivers = gap_rivers[
            gap_rivers.geometry.type.isin(["LineString", "MultiLineString"])
        ]
        result_segments.append(gap_rivers)

    if not result_segments:
        return gpd.GeoDataFrame(columns=rivers_gdf.columns, crs=rivers_gdf.crs)

    # ── Step 6: Merge, Clean and Fix Geometries ─────────────────────
    final_gdf = gpd.GeoDataFrame(pd.concat(result_segments, ignore_index=True), crs=rivers_gdf.crs)
    
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


    return final_gdf


def run_river_vector_local(
    state=None,
    district=None,
    block=None,
    asset_suffix=None,
    roi=None,
    river_vector_path=RIVER_VECTOR_PATH,
    precomputed_roi_dir=None,
    push_to_geoserver=True,
    sync_layer_metadata=True,
):
    """
    Orchestrates the local river vector generation.
    """
    if state and district and block:
        layer_name = (
            f"{valid_gee_text(str(district).strip().lower())}_"
            f"{valid_gee_text(str(block).strip().lower())}_river_vector"
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
        layer_name = f"{asset_suffix}_river_vector".lower()
        watersheds_gdf = read_validated_vector_file(
            roi,
            f"ROI file has no valid geometries: {roi}",
        )
        print(f"ROI source: {roi}")

    if not os.path.exists(river_vector_path):
        raise FileNotFoundError(f"River source file not found: {river_vector_path}")

    print(f"Loading river source: {river_vector_path}")
    rivers_gdf = read_validated_vector_file(
        river_vector_path,
        f"River source file has no valid geometries: {river_vector_path}",
    )

    result_gdf = _compute_river_properties_for_watersheds(
        watersheds_gdf=watersheds_gdf,
        rivers_gdf=rivers_gdf,
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
    print(f"Saved local river vector: {asset_id}")

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
            dataset_name="River Vector",
        )
        if layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("Sync to GeoServer flag updated for river vector")

    return True


@app.task(bind=True)
def river_vector(
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
    Celery task for local river vector generation.
    Matches signature of river_layer.py
    """
    _ = self, asset_folder_list, app_type, gee_account_id
    return run_river_vector_local(
        state=state,
        district=district,
        block=block,
        asset_suffix=asset_suffix,
        roi=roi,
        push_to_geoserver=True,
        sync_layer_metadata=True,
    )

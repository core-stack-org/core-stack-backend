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
    Clips rivers to ROI boundaries.
    - Matched rivers  → clipped to each intersecting watershed, uid + area_in_ha attached
    - Gap rivers      → clipped to outer dissolved boundary, uid = "", area_in_ha = ""
    - Empty/invalid geometries are dropped throughout
    """
    watersheds_gdf = validate_geometry(watersheds_gdf).reset_index(drop=True)
    rivers_gdf = validate_geometry(rivers_gdf).reset_index(drop=True)

    outer_boundary = watersheds_gdf.geometry.unary_union

    # ── Step 1: Coarse filter ─────────────────────────────────────────────
    rivers_in_roi = rivers_gdf[rivers_gdf.intersects(outer_boundary)].copy()

    if rivers_in_roi.empty:
        print("No rivers found within the outer boundary.")
        return gpd.GeoDataFrame(columns=rivers_gdf.columns, crs=rivers_gdf.crs)

    print(f"Rivers within outer boundary: {len(rivers_in_roi)}")

    # ── Step 2: Spatial join – river (left) × watershed (right) ──────────
    # Keep original river index so we can identify gap rivers later
    joined = gpd.sjoin(
        rivers_in_roi,
        watersheds_gdf[["uid", "area_in_ha", "geometry"]].reset_index(
            names="ws_iloc"  # positional index of watershed row
        ),
        how="inner",
        predicate="intersects",
    )

    matched_river_indices = joined.index.unique()
    gap_rivers = rivers_in_roi.loc[
        ~rivers_in_roi.index.isin(matched_river_indices)
    ].copy()

    print(f"Matched river-watershed pairs : {len(joined)}")
    print(f"Gap rivers (no watershed hit) : {len(gap_rivers)}")

    result_segments = []

    # ── Step 3: Clip each matched river to its specific watershed ─────────
    if not joined.empty:

        def clip_to_watershed(row):
            """
            row.ws_iloc is the positional index of the matched watershed row.
            Using iloc here is correct and safe — ws_iloc was set from
            reset_index() so it maps 1-to-1 with watersheds_gdf.iloc positions.
            """
            try:
                ws_geom = watersheds_gdf.geometry.iloc[int(row["ws_iloc"])]
                clipped = row.geometry.intersection(ws_geom)
                if clipped.is_empty or not clipped.is_valid:
                    return None
                return clipped
            except Exception as e:
                print(f"Clip error for river index {row.name}: {e}")
                return None

        joined = joined.copy()
        joined["geometry"] = joined.apply(clip_to_watershed, axis=1)

        # Drop rows where clipping failed or produced empty geometry
        joined = joined[joined["geometry"].notna()]
        joined = joined[~joined["geometry"].is_empty]
        joined = joined[joined["geometry"].is_valid]

        # Keep only line geometries (intersection may produce points at edges)
        joined = joined[
            joined.geometry.geom_type.isin(["LineString", "MultiLineString"])
        ]

        if not joined.empty:
            result_segments.append(joined)

    # ── Step 4: Clip gap rivers to outer boundary ─────────────────────────
    if not gap_rivers.empty:

        def clip_to_outer(row):
            try:
                clipped = row.geometry.intersection(outer_boundary)
                if clipped.is_empty or not clipped.is_valid:
                    return None
                return clipped
            except Exception as e:
                print(f"Gap clip error for river index {row.name}: {e}")
                return None

        gap_rivers = gap_rivers.copy()
        gap_rivers["uid"] = ""
        gap_rivers["area_in_ha"] = ""
        gap_rivers["geometry"] = gap_rivers.apply(clip_to_outer, axis=1)

        gap_rivers = gap_rivers[gap_rivers["geometry"].notna()]
        gap_rivers = gap_rivers[~gap_rivers["geometry"].is_empty]
        gap_rivers = gap_rivers[gap_rivers["geometry"].is_valid]
        gap_rivers = gap_rivers[
            gap_rivers.geometry.geom_type.isin(["LineString", "MultiLineString"])
        ]

        if not gap_rivers.empty:
            result_segments.append(gap_rivers)

    # ── Step 5: Merge all segments ────────────────────────────────────────
    if not result_segments:
        print("No valid river segments after clipping.")
        return gpd.GeoDataFrame(columns=rivers_gdf.columns, crs=rivers_gdf.crs)

    final_gdf = gpd.GeoDataFrame(
        pd.concat(result_segments, ignore_index=True),
        crs=rivers_gdf.crs,
    )

    # ── Step 6: Final cleanup ─────────────────────────────────────────────
    # Cast to string for GeoServer/DB compatibility
    final_gdf["uid"] = final_gdf["uid"].astype(str)
    final_gdf["area_in_ha"] = final_gdf["area_in_ha"].astype(str)

    # Drop helper columns
    for col in ["index_right", "ws_iloc"]:
        if col in final_gdf.columns:
            final_gdf = final_gdf.drop(columns=[col])

    # Drop empty / invalid geometries (final safety net)
    final_gdf = final_gdf[~final_gdf.geometry.is_empty]
    final_gdf = final_gdf[final_gdf.geometry.is_valid]
    final_gdf = final_gdf[final_gdf.geometry.notna()]

    # Fix any remaining geometry issues
    final_gdf = fix_invalid_geometry_in_gdf(final_gdf)

    # Drop anything that still has a null/empty bbox
    final_gdf = final_gdf[
        final_gdf.geometry.apply(
            lambda g: g is not None
            and not g.is_empty
            and g.bounds != (0.0, 0.0, 0.0, 0.0)
            and g.bounds[0] <= g.bounds[2]  # minX <= maxX
            and g.bounds[1] <= g.bounds[3]  # minY <= maxY
        )
    ]

    print(f"Final valid river segments    : {len(final_gdf)}")
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

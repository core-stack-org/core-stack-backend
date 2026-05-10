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

    watersheds_gdf = validate_geometry(watersheds_gdf).reset_index(drop=True)
    rivers_gdf = validate_geometry(rivers_gdf).reset_index(drop=True)

    outer_boundary = watersheds_gdf.geometry.unary_union

    # ── Step 1: Coarse filter ─────────────────────────────────────────────
    rivers_in_roi = rivers_gdf[rivers_gdf.intersects(outer_boundary)].copy()

    if rivers_in_roi.empty:
        print("No rivers found within the outer boundary.")
        return gpd.GeoDataFrame(columns=rivers_gdf.columns, crs=rivers_gdf.crs)

    print(f"Rivers within outer boundary: {len(rivers_in_roi)}")

    # ── Step 2: Spatial join ──────────────────────────────────────────────
    # Watersheds get a named integer index column for safe lookup after join
    watersheds_indexed = watersheds_gdf[["uid", "area_in_ha", "geometry"]].copy()
    watersheds_indexed.index.name = "ws_idx"

    joined = gpd.sjoin(
        rivers_in_roi,
        watersheds_indexed,
        how="inner",
        predicate="intersects",
    )
    # index_right now holds the watersheds_gdf integer index (0-based)
    # because we reset_index(drop=True) above — iloc and loc are equivalent

    matched_river_indices = joined.index.unique()
    gap_rivers = rivers_in_roi.loc[
        ~rivers_in_roi.index.isin(matched_river_indices)
    ].copy()

    print(f"Matched river-watershed pairs : {len(joined)}")
    print(f"Gap rivers (no watershed hit) : {len(gap_rivers)}")

    # ── DEBUG: print first few rows to verify index_right ────────────────
    print("joined columns:", joined.columns.tolist())
    print("joined index_right sample:\n", joined["index_right"].head())
    print("watersheds_gdf index sample:\n", watersheds_gdf.index[:5].tolist())

    result_segments = []

    # ── Step 3: Clip matched rivers to their watershed ────────────────────
    if not joined.empty:
        clipped_rows = []

        for idx, row in joined.iterrows():
            try:
                ws_idx = int(row["index_right"])  # integer positional index
                ws_geom = watersheds_gdf.loc[ws_idx, "geometry"]
                river_geom = row.geometry

                # ── DEBUG ──
                print(
                    f"  River idx={idx} ws_idx={ws_idx} "
                    f"river_type={river_geom.geom_type} "
                    f"river_valid={river_geom.is_valid} "
                    f"ws_valid={ws_geom.is_valid}"
                )

                # Fix geometries before intersection
                if not river_geom.is_valid:
                    river_geom = river_geom.buffer(0)
                if not ws_geom.is_valid:
                    ws_geom = ws_geom.buffer(0)

                clipped = river_geom.intersection(ws_geom)

                print(
                    f"    clipped type={clipped.geom_type} "
                    f"empty={clipped.is_empty} "
                    f"valid={clipped.is_valid} "
                    f"bounds={clipped.bounds}"
                )

                if clipped is None or clipped.is_empty or not clipped.is_valid:
                    print(f"    → DROPPED (empty/invalid)")
                    continue

                # Extract only line parts (intersection may add points)
                if clipped.geom_type == "GeometryCollection":
                    from shapely.ops import unary_union

                    lines = [
                        g
                        for g in clipped.geoms
                        if g.geom_type in ("LineString", "MultiLineString")
                    ]
                    if not lines:
                        print(f"    → DROPPED (no lines in GeometryCollection)")
                        continue
                    clipped = unary_union(lines)

                if clipped.geom_type not in ("LineString", "MultiLineString"):
                    print(f"    → DROPPED (not a line: {clipped.geom_type})")
                    continue

                new_row = row.copy()
                new_row["geometry"] = clipped
                clipped_rows.append(new_row)
                print(f"    → KEPT")

            except Exception as e:
                print(f"  Clip error river idx={idx}: {e}")
                continue

        if clipped_rows:
            matched_fc = gpd.GeoDataFrame(clipped_rows, crs=rivers_gdf.crs)
            result_segments.append(matched_fc)
            print(f"Valid matched segments kept: {len(clipped_rows)}")
        else:
            print("No matched segments survived clipping.")

    # ── Step 4: Gap rivers ────────────────────────────────────────────────
    if not gap_rivers.empty:
        clipped_gaps = []

        for idx, row in gap_rivers.iterrows():
            try:
                clipped = row.geometry.intersection(outer_boundary)

                if clipped is None or clipped.is_empty or not clipped.is_valid:
                    continue

                if clipped.geom_type == "GeometryCollection":
                    from shapely.ops import unary_union

                    lines = [
                        g
                        for g in clipped.geoms
                        if g.geom_type in ("LineString", "MultiLineString")
                    ]
                    if not lines:
                        continue
                    clipped = unary_union(lines)

                if clipped.geom_type not in ("LineString", "MultiLineString"):
                    continue

                new_row = row.copy()
                new_row["geometry"] = clipped
                new_row["uid"] = ""
                new_row["area_in_ha"] = ""
                clipped_gaps.append(new_row)

            except Exception as e:
                print(f"  Gap clip error river idx={idx}: {e}")
                continue

        if clipped_gaps:
            gap_fc = gpd.GeoDataFrame(clipped_gaps, crs=rivers_gdf.crs)
            result_segments.append(gap_fc)
            print(f"Valid gap segments kept: {len(clipped_gaps)}")

    # ── Step 5: Merge ─────────────────────────────────────────────────────
    if not result_segments:
        print("No valid river segments after clipping.")
        return gpd.GeoDataFrame(columns=rivers_gdf.columns, crs=rivers_gdf.crs)

    final_gdf = gpd.GeoDataFrame(
        pd.concat(result_segments, ignore_index=True),
        crs=rivers_gdf.crs,
    )

    # ── Step 6: Final cleanup ─────────────────────────────────────────────
    final_gdf["uid"] = final_gdf["uid"].astype(str)
    final_gdf["area_in_ha"] = final_gdf["area_in_ha"].astype(str)

    for col in ["index_right", "ws_idx"]:
        if col in final_gdf.columns:
            final_gdf = final_gdf.drop(columns=[col])

    final_gdf = final_gdf[~final_gdf.geometry.is_empty]
    final_gdf = final_gdf[final_gdf.geometry.is_valid]
    final_gdf = final_gdf[final_gdf.geometry.notna()]
    final_gdf = fix_invalid_geometry_in_gdf(final_gdf)

    # Bbox sanity check
    final_gdf = final_gdf[
        final_gdf.geometry.apply(
            lambda g: g is not None
            and not g.is_empty
            and g.bounds[0] <= g.bounds[2]
            and g.bounds[1] <= g.bounds[3]
        )
    ]

    print(f"Final valid river segments: {len(final_gdf)}")
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

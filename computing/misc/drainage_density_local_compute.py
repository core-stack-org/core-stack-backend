import os

import geopandas as gpd

from nrm_app.celery import app
from utilities.gee_utils import valid_gee_text
from utilities.constants import DRAINAGE_DENSITY_OUTPUT
from computing.utils import (
    push_shape_to_geoserver,
    save_layer_info_to_db,
    update_layer_sync_status,
)
from computing.local_compute_helper import (
    build_output_vector_path,
    load_precomputed_watersheds,
    read_validated_vector_file,
    validate_geometry,
    write_vector_output,
    PROJECT_ROOT,
)

DRAINAGE_DENSITY_VECTOR_PATH = (
    PROJECT_ROOT / "data/drainage_density/drainage_density_pan_india.geojson"
)
GEOSERVER_WORKSPACE = "drainage_density"


def _compute_drainage_density_for_watersheds(watersheds_gdf, drainage_density_gdf):
    """
    Clips pan-India drainage density polygons to watershed boundaries.
    One feature per (drainage_density × watershed) intersection,
    with uid and area_in_ha from the matched watershed attached.
    Mirrors canal local compute logic exactly.
    """
    watersheds_gdf = validate_geometry(watersheds_gdf).reset_index(drop=True)
    drainage_density_gdf = validate_geometry(drainage_density_gdf).reset_index(
        drop=True
    )

    outer_boundary = watersheds_gdf.geometry.unary_union

    # ── Step 1: Coarse filter to study area ──────────────────────────────
    dd_in_roi = drainage_density_gdf[
        drainage_density_gdf.intersects(outer_boundary)
    ].copy()

    if dd_in_roi.empty:
        print("No drainage density features found within the outer boundary.")
        return gpd.GeoDataFrame(
            columns=drainage_density_gdf.columns,
            crs=drainage_density_gdf.crs,
        )

    print(f"Drainage density features within boundary: {len(dd_in_roi)}")

    # ── Step 2: Spatial join – dd (left) × watershed (right) ─────────────
    joined = gpd.sjoin(
        dd_in_roi,
        watersheds_gdf[["uid", "area_in_ha", "geometry"]].reset_index(names="ws_iloc"),
        how="inner",
        predicate="intersects",
    )

    print(f"Matched dd-watershed pairs: {len(joined)}")

    if joined.empty:
        print("No matched drainage density segments.")
        return gpd.GeoDataFrame(
            columns=drainage_density_gdf.columns,
            crs=drainage_density_gdf.crs,
        )

    # ── Step 3: Clip each dd feature to its matched watershed ────────────
    clipped_rows = []

    for idx, row in joined.iterrows():
        try:
            ws_geom = watersheds_gdf.geometry.iloc[int(row["ws_iloc"])]
            dd_geom = row.geometry

            if not dd_geom.is_valid:
                dd_geom = dd_geom.buffer(0)
            if not ws_geom.is_valid:
                ws_geom = ws_geom.buffer(0)

            clipped = dd_geom.intersection(ws_geom)

            if clipped is None or clipped.is_empty or not clipped.is_valid:
                continue

            new_row = row.copy()
            new_row["geometry"] = clipped
            clipped_rows.append(new_row)

        except Exception as e:
            print(f"Clip error for dd idx={idx}: {e}")
            continue

    if not clipped_rows:
        print("No valid drainage density segments after clipping.")
        return gpd.GeoDataFrame(
            columns=drainage_density_gdf.columns,
            crs=drainage_density_gdf.crs,
        )

    # ── Step 4: Build final GeoDataFrame ─────────────────────────────────
    final_gdf = gpd.GeoDataFrame(clipped_rows, crs=drainage_density_gdf.crs)

    final_gdf["uid"] = final_gdf["uid"].astype(str)
    final_gdf["area_in_ha"] = final_gdf["area_in_ha"].astype(str)

    for col in ["index_right", "ws_iloc"]:
        if col in final_gdf.columns:
            final_gdf = final_gdf.drop(columns=[col])

    final_gdf = final_gdf[~final_gdf.geometry.is_empty]
    final_gdf = final_gdf[final_gdf.geometry.is_valid]
    final_gdf = final_gdf[final_gdf.geometry.notna()]

    print(f"Final drainage density segments: {len(final_gdf)}")
    return final_gdf


def run_drainage_density_local(
    state=None,
    district=None,
    block=None,
    asset_suffix=None,
    roi=None,
    drainage_density_vector_path=DRAINAGE_DENSITY_VECTOR_PATH,
    precomputed_roi_dir=None,
    push_to_geoserver=True,
    sync_layer_metadata=True,
):
    """
    Orchestrates local drainage density generation.
    Mirrors canal local compute structure exactly.
    """
    if state and district and block:
        layer_name = (
            f"{valid_gee_text(str(district).strip().lower())}_"
            f"{valid_gee_text(str(block).strip().lower())}_"
            f"drainage_density_vector"
        )
        watersheds_gdf, watershed_source = load_precomputed_watersheds(
            state=state,
            district=district,
            block=block,
            precomputed_roi_dir=precomputed_roi_dir,
        )
        print(f"Watershed source: {watershed_source}")

    else:
        if not roi or not asset_suffix:
            raise ValueError(
                "For non state/district/block runs, "
                "both `roi` and `asset_suffix` are required."
            )
        layer_name = f"{asset_suffix}_drainage_density_vector".lower()
        watersheds_gdf = read_validated_vector_file(
            roi,
            f"ROI file has no valid geometries: {roi}",
        )
        print(f"ROI source: {roi}")

    if not os.path.exists(drainage_density_vector_path):
        raise FileNotFoundError(
            f"Drainage density source file not found: {drainage_density_vector_path}"
        )

    print(f"Loading drainage density source: {drainage_density_vector_path}")
    drainage_density_gdf = read_validated_vector_file(
        drainage_density_vector_path,
        f"Drainage density source file has no valid geometries: {drainage_density_vector_path}",
    )

    # ── Compute ───────────────────────────────────────────────────────────
    result_gdf = _compute_drainage_density_for_watersheds(
        watersheds_gdf=watersheds_gdf,
        drainage_density_gdf=drainage_density_gdf,
    )

    # ── Save output ───────────────────────────────────────────────────────
    output_path = build_output_vector_path(
        layer_name=layer_name,
        state=state,
        district=district,
        block=block,
        output_base_dir=DRAINAGE_DENSITY_OUTPUT,
    )

    asset_id = write_vector_output(
        gdf=result_gdf,
        output_path=output_path,
        layer_name=layer_name,
    )
    print(f"Saved local drainage density vector: {asset_id}")

    # ── Push to GeoServer ─────────────────────────────────────────────────
    if push_to_geoserver:
        geoserver_response = push_shape_to_geoserver(
            os.path.splitext(asset_id)[0],
            workspace=GEOSERVER_WORKSPACE,
            layer_name=layer_name,
            file_type="gpkg",
        )
        print(f"GeoServer response: {geoserver_response}")

    # ── Sync layer metadata ───────────────────────────────────────────────
    if sync_layer_metadata and state and district and block:
        layer_id = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=layer_name,
            asset_id=asset_id,
            dataset_name="Drainage Density",
        )
        if layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("Sync to GeoServer flag updated for drainage density")

    return True


@app.task(bind=True)
def drainage_density(
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
    Celery task for local drainage density vector generation.
    Matches signature of the GEE drainage_density task.
    """
    _ = self, asset_folder_list, app_type, gee_account_id
    return run_drainage_density_local(
        state=state,
        district=district,
        block=block,
        asset_suffix=asset_suffix,
        roi=roi,
        push_to_geoserver=True,
        sync_layer_metadata=True,
    )

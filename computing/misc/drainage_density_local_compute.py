import os
from pathlib import Path

import geopandas as gpd
from shapely.geometry import box

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

# Pan-India drainage LINES dataset (has ORDER column, stream order 1-11)
DRAINAGE_LINES_GPKG_PATH = (
    PROJECT_ROOT / "data/drainage_density/drainage_density_pan_india.gpkg"
)
GEOSERVER_WORKSPACE = "drainage_density"

# Influence factors for stream orders 1 to 11
INFLUENCE_FACTORS = [
    60 / 385,
    55 / 385,
    50 / 385,
    45 / 385,
    40 / 385,
    35 / 385,
    30 / 385,
    25 / 385,
    20 / 385,
    15 / 385,
    10 / 385,
]


def _load_drainage_lines_for_roi(watersheds_gdf):
    """
    Loads only drainage line features overlapping the study area bbox.
    GPKG spatial index ensures only relevant tiles are read.
    """
    bounds = watersheds_gdf.geometry.total_bounds
    bbox_geom = box(*bounds)

    print(f"Loading drainage lines (bbox indexed read): {DRAINAGE_LINES_GPKG_PATH}")
    print(f"Study area bounds: {bounds}")

    lines_gdf = gpd.read_file(DRAINAGE_LINES_GPKG_PATH, bbox=bbox_geom)

    print(f"Loaded {len(lines_gdf)} drainage line features within bounding box")
    return lines_gdf


def _compute_drainage_density(watersheds_gdf, drainage_lines_gdf):
    """
    Mirrors original generate_vector() DD logic exactly:

    For each watershed:
      1. Clip drainage lines to watershed boundary
      2. For each stream order 1-11, calculate:
           - total line length in km
           - weighted drainage density using influence factor
      3. Sum all stream order DDs → total DD
      4. Store DD, DD_stream (per order density), str_len_km (per order length)

    Output: watersheds GeoDataFrame with DD columns added.
    """
    # Reproject to metric CRS for accurate length/area calculation
    # (same as original: crs=7755 is India-specific metric projection)
    drainage_lines_gdf = drainage_lines_gdf.to_crs(crs=7755)
    watersheds_gdf = watersheds_gdf.to_crs(crs=7755)

    watersheds_gdf["DD"] = None
    watersheds_gdf["DD_stream"] = None
    watersheds_gdf["str_len_km"] = None

    for index, watershed in watersheds_gdf.iterrows():

        # Clip drainage lines to this watershed boundary
        clipped_lines = gpd.clip(drainage_lines_gdf, watershed.geometry)

        # Area in km² (area_in_ha / 100)
        area_km2 = watershed["area_in_ha"] / 100

        stream_length = {}
        stream_drainage_density = {}

        for stream_order, influence_factor in zip(range(1, 12), INFLUENCE_FACTORS):

            # Filter lines for this stream order
            order_lines = clipped_lines[clipped_lines["ORDER"] == stream_order]

            # Total length in km
            total_length_km = order_lines.geometry.length.sum() / 1000

            # Weighted drainage density for this stream order
            dd = (
                total_length_km * influence_factor * 100 / area_km2 if area_km2 else 0.0
            )

            stream_length[stream_order] = total_length_km
            stream_drainage_density[stream_order] = dd

        watersheds_gdf.at[index, "DD"] = sum(stream_drainage_density.values())
        watersheds_gdf.at[index, "DD_stream"] = str(
            [float(v) for v in stream_drainage_density.values()]
        )
        watersheds_gdf.at[index, "str_len_km"] = str(
            [float(v) for v in stream_length.values()]
        )

    # Restore geographic CRS
    watersheds_gdf = watersheds_gdf.to_crs(crs=4326)
    watersheds_gdf["DD"] = watersheds_gdf["DD"].astype(float)

    return watersheds_gdf


def run_drainage_density_local(
    state=None,
    district=None,
    block=None,
    asset_suffix=None,
    roi=None,
    precomputed_roi_dir=None,
    push_to_geoserver=True,
    sync_layer_metadata=True,
):
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

    # ── Load drainage lines for study area ────────────────────────────────
    drainage_lines_gdf = _load_drainage_lines_for_roi(watersheds_gdf)

    # ── Compute DD per watershed ──────────────────────────────────────────
    print("Computing drainage density per watershed...")
    result_gdf = _compute_drainage_density(
        watersheds_gdf=watersheds_gdf,
        drainage_lines_gdf=drainage_lines_gdf,
    )

    print(f"Total watersheds processed: {len(result_gdf)}")

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

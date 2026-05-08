import os
import json
import datetime
import pandas as pd
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


def _build_canal_properties(canal_row):
    """
    Extracts canal properties from a GeoPandas row and formats them for JSON.
    Mirroring build_canal_json from canal_layer.py
    """

    def get_val(key):
        val = canal_row.get(key)
        if pd.isna(val):
            return None
        return val

    properties = {
        "canname": get_val("canname"),
        "cancode": get_val("cancode"),
        "prjname": get_val("prjname"),
        "prjcode": get_val("prjcode"),
        "state": get_val("state"),
        "cn_purp": get_val("cn_purp"),
        "cn_ss": get_val("cn_ss"),
        "cn_st": get_val("cn_st"),
        "cn_type": get_val("cn_type"),
        "status_yr": get_val("status_yr"),
        "can_type": get_val("can_type"),
        "objectid": get_val("objectid"),
        "st_length": get_val("st_length")
        or get_val("st_length_")
        or get_val("st_length("),
    }

    # Ensure all values are JSON serializable
    for k, v in properties.items():
        if isinstance(v, (pd.Timestamp, datetime.date, datetime.datetime)):
            properties[k] = v.isoformat()

    return properties


def _compute_canal_properties_for_watersheds(watersheds_gdf, canals_gdf):
    """
    Performs spatial intersection between ROI (watersheds) and Canals,
    aggregating canal info into a 'misc' JSON column.
    GEE equivalent: Step 1-6 in canal_layer.py
    """
    watersheds_gdf = validate_geometry(watersheds_gdf)
    canals_gdf = validate_geometry(canals_gdf)

    watersheds_result = watersheds_gdf.copy()
    watersheds_result["area_in_ha"] = get_watershed_areas_in_hectares(
        watersheds_result
    ).astype(float)

    # Use the original datasets for intersection
    watersheds_projected = watersheds_result
    canals_projected = canals_gdf

    computed_rows = []
    total = len(watersheds_projected)

    print(f"Starting canal intersection for {total} watersheds...")

    for index, watershed_idx in enumerate(watersheds_projected.index, start=1):
        watershed_geometry = watersheds_projected.at[watershed_idx, "geometry"]
        uid = watersheds_result.at[watershed_idx, "uid"]
        area_in_ha = watersheds_result.at[watershed_idx, "area_in_ha"]

        if watershed_geometry is None or watershed_geometry.is_empty:
            computed_rows.append(
                {
                    "uid": uid,
                    "area_in_ha": area_in_ha,
                    "canal_available": False,
                    "misc": "[]",
                }
            )
            continue

        # Spatial query to find intersecting canals
        intersecting_canals = canals_projected.loc[
            canals_projected.intersects(watershed_geometry)
        ]

        if not intersecting_canals.empty:
            canal_list = []
            for _, canal_row in intersecting_canals.iterrows():
                canal_list.append(_build_canal_properties(canal_row))

            computed_rows.append(
                {
                    "uid": uid,
                    "area_in_ha": area_in_ha,
                    "canal_available": True,
                    "misc": json.dumps(canal_list),
                }
            )
        else:
            computed_rows.append(
                {
                    "uid": uid,
                    "area_in_ha": area_in_ha,
                    "canal_available": False,
                    "misc": "[]",
                }
            )

        if index % 200 == 0 or index == total:
            print(f"Computed canal properties for {index}/{total} watersheds")

    computed_df = pd.DataFrame(computed_rows)

    # Merge computed properties back to the result GDF
    # First drop any existing columns that we're about to add to avoid duplicates
    cols_to_drop = ["canal_available", "misc"]
    watersheds_result = watersheds_result.drop(
        columns=[c for c in cols_to_drop if c in watersheds_result.columns]
    )

    watersheds_result = watersheds_result.merge(
        computed_df[["uid", "canal_available", "misc"]], on="uid", how="left"
    )

    return watersheds_result


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

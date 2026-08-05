import os
import boto3
import geopandas as gpd
import pandas as pd
import numpy as np
from io import BytesIO

from nrm_app.celery import app
from utilities.gee_utils import valid_gee_text
from nrm_app.settings import NREGA_BUCKET, S3_ACCESS_KEY, S3_SECRET_KEY
from computing.utils import (
    push_shape_to_geoserver,
    save_layer_info_to_db,
    update_layer_sync_status,
)
from computing.local_compute_helper import (
    load_precomputed_watersheds,
    read_validated_vector_file,
    write_vector_output,
    build_output_vector_path,
    validate_geometry,
)
from computing.config_loader import NREGA_LOCAL_OUTPUT

GEOSERVER_WORKSPACE = "nrega_assets"


def _compute_nrega_for_watersheds(watersheds_gdf, nrega_gdf):
    if nrega_gdf.empty:
        return nrega_gdf

    watersheds_gdf = validate_geometry(watersheds_gdf)
    nrega_gdf = validate_geometry(nrega_gdf)

    if watersheds_gdf.crs and nrega_gdf.crs and watersheds_gdf.crs != nrega_gdf.crs:
        nrega_gdf = nrega_gdf.to_crs(watersheds_gdf.crs)

    outer_boundary = watersheds_gdf.geometry.unary_union
    
    nrega_in_roi = nrega_gdf[nrega_gdf.intersects(outer_boundary)].copy()

    # Clean column names
    cleaned_columns = []
    for i, col in enumerate(nrega_in_roi.columns):
        if not str(col).strip():
            cleaned_columns.append(f"col_{i}")
        else:
            cleaned = str(col).strip().replace(" ", "_").replace(".", "_")
            cleaned_columns.append(cleaned)
    nrega_in_roi.columns = cleaned_columns

    nrega_in_roi = nrega_in_roi.replace({np.nan: None})

    for col in nrega_in_roi.columns:
        if col != "geometry":
            nrega_in_roi[col] = nrega_in_roi[col].map(
                lambda value: value.isoformat()
                if isinstance(value, pd.Timestamp)
                else value
            )

    return nrega_in_roi


@app.task(bind=True)
def generate_nrega_data_local(
    self,
    state=None,
    district=None,
    block=None,
    asset_suffix=None,
    roi_path=None,
    gee_account_id=None,
    precomputed_roi_dir=None,
    push_to_geoserver=True,
    sync_layer_metadata=True,
):
    if state and district and block:
        layer_name = f"nrega_{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}"
        watersheds_gdf, watershed_source = load_precomputed_watersheds(
            state=state,
            district=district,
            block=block,
            precomputed_roi_dir=precomputed_roi_dir,
        )
        print(f"Watershed boundary source: {watershed_source}")
    else:
        if not roi_path or not asset_suffix:
            raise ValueError("ROI path and asset_suffix are required for custom runs.")
        layer_name = f"{valid_gee_text(asset_suffix).lower()}_nrega"
        watersheds_gdf = read_validated_vector_file(roi_path, f"Invalid ROI file: {roi_path}")
        print(f"ROI source: {roi_path}")

    s3 = boto3.resource(
        "s3",
        region_name="ap-south-1",
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
    )

    print("Fetching NREGA data from S3...")
    # It fetches district level geojson from S3
    if state and district:
        key = f"{valid_gee_text(state).upper()}/{valid_gee_text(district).upper()}.geojson"
    else:
        # Fallback or error if custom ROI does not provide state/district mapping to S3
        print("Warning: Custom ROI run without state/district cannot fetch from S3 reliably.")
        return False
        
    try:
        file_obj = s3.Object(NREGA_BUCKET, key).get()
        nrega_gdf = gpd.read_file(BytesIO(file_obj["Body"].read()))
    except Exception as e:
        print("Error while reading NREGA file from S3:", e)
        return False

    print(f"Loaded {len(nrega_gdf)} NREGA features from S3")

    result_gdf = _compute_nrega_for_watersheds(
        watersheds_gdf=watersheds_gdf,
        nrega_gdf=nrega_gdf,
    )
    print(f"Final valid NREGA features after spatial filter: {len(result_gdf)}")

    output_path = build_output_vector_path(
        layer_name=layer_name,
        state=state,
        district=district,
        block=block,
        output_base_dir=NREGA_LOCAL_OUTPUT,
    )

    asset_id = write_vector_output(
        gdf=result_gdf,
        output_path=output_path,
        layer_name=layer_name,
    )
    print(f"Saved local NREGA vector: {asset_id}")

    layer_at_geoserver = False

    if push_to_geoserver:
        geoserver_response = push_shape_to_geoserver(
            os.path.splitext(asset_id)[0],
            workspace=GEOSERVER_WORKSPACE,
            layer_name=layer_name,
            file_type="gpkg",
        )
        print(f"GeoServer response: {geoserver_response}")
        if geoserver_response and geoserver_response.get("status_code") in (200, 201):
            layer_at_geoserver = True

    if sync_layer_metadata and state and district and block:
        layer_id = save_layer_info_to_db(
            state=state,
            district=district,
            block=block,
            layer_name=layer_name,
            asset_id=asset_id,
            dataset_name="NREGA Assets",
            misc={"is_generated_locally": True},
        )
        if layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)
            print("Sync to GeoServer flag updated for NREGA vector")

    return layer_at_geoserver

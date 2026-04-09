import os
import geopandas as gpd
import pandas as pd
from nrm_app.celery import app
from computing.utils import (
    push_shape_to_geoserver,
    save_layer_info_to_db,
    get_directory_size,
    update_layer_sync_status,
)
from utilities.constants import (
    NREGA_ASSETS_OUTPUT_DIR,
)
import boto3
from io import BytesIO
from nrm_app.settings import NREGA_BUCKET, S3_ACCESS_KEY, S3_SECRET_KEY
from utilities.gee_utils import (
    gdf_to_ee_fc,
    export_vector_asset_to_gee,
    check_task_status,
    valid_gee_text,
    get_gee_asset_path,
    upload_shp_to_gee,
    is_gee_asset_exists,
    ee_initialize,
    make_asset_public,
)
import ee
import numpy as np
import shutil

from computing.STAC_specs import generate_STAC_layerwise


def export_shp_to_gee(district, block, layer_path, asset_id, gee_account_id):
    print("Inside export shp to gee")
    layer_name = (
        "nrega_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
    )
    upload_shp_to_gee(layer_path, layer_name, asset_id, gee_account_id)


@app.task(bind=True)
def clip_nrega_district_block(self, state, district, block, gee_account_id):
    print("Start nrega asset clipping")
    ee_initialize(gee_account_id)
    s3 = boto3.resource(
        "s3",
        region_name="ap-south-1",
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
    )

    key = f"{valid_gee_text(state).upper()}/{valid_gee_text(district).upper()}.geojson"
    layer_at_geoserver = False

    try:
        file_obj = s3.Object(NREGA_BUCKET, key).get()
        gdf = gpd.read_file(BytesIO(file_obj["Body"].read()))
    except Exception as e:
        print("Error while reading file from S3:", e)
        return layer_at_geoserver

    # Ensure CRS
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")

    admin_description = (
        "admin_boundary_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
    )

    admin_asset_id = get_gee_asset_path(state, district, block) + admin_description

    geojson_dict = ee.FeatureCollection(admin_asset_id).getInfo()
    boundary_gdf = gpd.GeoDataFrame.from_features(
        geojson_dict["features"], crs="EPSG:4326"
    )

    # Merge to single geometry
    boundary_geom = boundary_gdf.union_all()

    # BBOX filter (very fast)
    minx, miny, maxx, maxy = boundary_geom.bounds
    gdf = gdf.cx[minx:maxx, miny:maxy]

    # Point-in-polygon check (fast & correct)
    clipped = gdf[gdf.geometry.within(boundary_geom)].copy()
    clipped = clipped.reset_index(drop=True)

    # ── 6. Prepare Data ───────────────────────────────────────────────────────
    block_metadata_df = clipped.copy()

    # if block_metadata_df.crs is None:
    #     block_metadata_df = block_metadata_df.set_crs("EPSG:4326")

    # # Remove unwanted columns
    # block_metadata_df = block_metadata_df.loc[
    #     :, ~block_metadata_df.columns.str.contains("^Unnamed")
    # ]

    # Clean column names
    cleaned_columns = []
    for i, col in enumerate(block_metadata_df.columns):
        if not str(col).strip():
            cleaned_columns.append(f"col_{i}")
        else:
            cleaned = str(col).strip().replace(" ", "_").replace(".", "_")
            cleaned_columns.append(cleaned)
    block_metadata_df.columns = cleaned_columns

    # Replace NaN
    block_metadata_df = block_metadata_df.replace({np.nan: None})

    # Convert datetime columns
    for col in block_metadata_df.columns:
        if col != "geometry":
            if pd.api.types.is_datetime64_any_dtype(block_metadata_df[col]):
                block_metadata_df[col] = (
                    block_metadata_df[col].astype(str).replace("NaT", None)
                )

    # Save Shapefile
    nrega_folder_name = (
        f"{'_'.join(valid_gee_text(district).split())}"
        f"_{'_'.join(valid_gee_text(block).split())}"
    )

    output_dir = os.path.join(
        NREGA_ASSETS_OUTPUT_DIR, valid_gee_text(state), nrega_folder_name
    )

    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)

    os.makedirs(output_dir, exist_ok=True)
    shp_path = os.path.join(output_dir, f"{nrega_folder_name}.shp")
    block_metadata_df.to_file(shp_path, driver="ESRI Shapefile", encoding="UTF-8")
    print(f"Shapefile saved: {shp_path}")

    nrega_description = (
        "nrega_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
    )

    nrega_asset_id = get_gee_asset_path(state, district, block) + nrega_description

    file_size_mb = get_directory_size(output_dir) / (1024 * 1024)
    if not is_gee_asset_exists(nrega_asset_id):

        if file_size_mb > 10:
            export_shp_to_gee(district, block, shp_path, nrega_asset_id, gee_account_id)
        else:
            fc = gdf_to_ee_fc(block_metadata_df)
            task_id = export_vector_asset_to_gee(fc, nrega_description, nrega_asset_id)

            if task_id:
                check_task_status([task_id])

    if is_gee_asset_exists(nrega_asset_id):

        layer_id = save_layer_info_to_db(
            state,
            district,
            block,
            layer_name=f"{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}",
            asset_id=nrega_asset_id,
            dataset_name="NREGA Assets",
        )

        make_asset_public(nrega_asset_id)

        res = push_shape_to_geoserver(output_dir, workspace="nrega_assets")

        if res["status_code"] == 201 and layer_id:

            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)

            layer_STAC_generated = generate_STAC_layerwise.generate_vector_stac(
                state=state,
                district=district,
                block=block,
                layer_name="nrega_vector",
            )

            update_layer_sync_status(
                layer_id=layer_id,
                is_stac_specs_generated=layer_STAC_generated,
            )

            layer_at_geoserver = True
            print("nrega data sync to geoserver")

    layer_at_geoserver = True
    return layer_at_geoserver

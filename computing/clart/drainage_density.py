import ee
import json
import geopandas as gpd
import os
from utilities.gee_utils import (
    ee_initialize,
    check_task_status,
    valid_gee_text,
    get_gee_asset_path,
    is_gee_asset_exists,
    upload_tif_to_gcs,
    upload_tif_from_gcs_to_gee,
    sync_vector_to_gcs,
    get_geojson_from_gcs,
    export_vector_asset_to_gee,
    make_asset_public,
)
from utilities.constants import DRAINAGE_LINES_OUTPUT, DRAINAGE_DENSITY_OUTPUT
from nrm_app.celery import app
from .rasterize_vector import rasterize_vector
from computing.utils import save_layer_info_to_db


@app.task(bind=True)
def drainage_density(self, state, district, block):
    asset_id = (
        get_gee_asset_path(state, district, block)
        + "drainage_density_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
    )

    if not is_gee_asset_exists(asset_id):
        input_path = generate_vector(state, district, block)
        input_path = os.path.join(
            input_path,
            f"{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}.shp",
        )
        attribute_column = "DD"

        output_file_name = (
            f"{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}.tif"
        )

        output_raster_path = os.path.join(
            DRAINAGE_DENSITY_OUTPUT,
            f"{'_'.join(state.split())}/{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}/{output_file_name}",
        )

        rasterize_vector(input_path, output_raster_path, attribute_column)

        gcs_path = upload_tif_to_gcs(output_file_name, output_raster_path)

        task_id = upload_tif_from_gcs_to_gee(gcs_path, asset_id, 30)

        task_list = check_task_status([task_id])
        print("drainage_density task list ", task_list)
        if is_gee_asset_exists(asset_id):
            save_layer_info_to_db(
                state,
                district,
                block,
                layer_name=f"drainage_density_{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}",
                asset_id=asset_id,
                dataset_name="Drainage Density",
            )
            print("saved drainage density info at the gee level...")
            make_asset_public(asset_id)


def generate_vector(state, district, block):
    ee_initialize()
    mws = ee.FeatureCollection(
        get_gee_asset_path(state, district, block)
        + "filtered_mws_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
        + "_uid"
    ).getInfo()

    if isinstance(mws, str):
        mws = json.loads(mws)

    watersheds = gpd.GeoDataFrame.from_features(mws)
    watersheds.set_crs("EPSG:4326", inplace=True)

    drainage_lines = ee.FeatureCollection(
        get_gee_asset_path(state, district, block)
        + f"drainage_lines_{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}"
    )

    try:
        drainage_lines = drainage_lines.getInfo()
    except Exception as e:
        print("Exception in getInfo()", e)
        layer_name = f"drainage_lines_{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}"
        task_id = sync_vector_to_gcs(drainage_lines, layer_name, "GeoJSON")
        check_task_status([task_id])

        drainage_lines = get_geojson_from_gcs(layer_name)

    if isinstance(drainage_lines, str):
        drainage_lines = json.loads(drainage_lines)

    drainage_lines = gpd.GeoDataFrame.from_features(drainage_lines)
    drainage_lines.set_crs("EPSG:4326", inplace=True)

    # drainage_line_path = os.path.join(
    #     DRAINAGE_LINES_OUTPUT,
    #     f"""{'_'.join(state.split())}/{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}/{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}.shp""",
    # )

    # Define influence factors for stream orders 1 to 11
    influence_factors = [
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

    # Load drainage lines shapefile
    # drainage_lines = gpd.read_file(drainage_line_path)

    # changing CRS for length calculation
    drainage_lines = drainage_lines.to_crs(crs=7755)
    watersheds = watersheds.to_crs(crs=7755)
    watersheds["DD"] = None
    watersheds["DD_stream"] = None
    watersheds["str_len_km"] = None

    # Iterate over each watershed
    for index, watershed in watersheds.iterrows():
        # Filter drainage lines within the current watershed
        clipped_drainage_lines = gpd.clip(drainage_lines, watershed.geometry)

        stream_length = {}
        stream_drainage_density = {}

        # Calculate the total area of the current watershed
        area = watershed["area_in_ha"] / 100

        # Iterate over stream orders and calculate drainage density
        for stream_order, influence_factor in zip(range(1, 12), influence_factors):
            # Filter drainage lines for the current stream order
            stream_order_lines = clipped_drainage_lines[
                clipped_drainage_lines["ORDER"] == stream_order
            ]

            # Calculate the sum of lengths for the current stream order
            total_length_stream_order = stream_order_lines.geometry.length.sum() / 1000

            # Calculate drainage density for the current stream order
            drainage_density = total_length_stream_order * influence_factor * 100 / area

            stream_length[stream_order] = total_length_stream_order
            stream_drainage_density[stream_order] = drainage_density

        # Create new columns in the 'watersheds' GeoDataFrame
        watersheds.at[index, "DD"] = sum(stream_drainage_density.values())
        watersheds.at[index, "DD_stream"] = stream_drainage_density
        watersheds.at[index, "str_len_km"] = stream_length

    # Restoring the original CRS
    watersheds = watersheds.to_crs(crs=4326)

    watersheds["DD"] = watersheds["DD"].astype(float)
    watersheds["DD_stream"] = watersheds.apply(
        lambda row: convert_dict_to_list(row, "DD_stream"), axis=1
    )

    watersheds["str_len_km"] = watersheds.apply(
        lambda row: convert_dict_to_list(row, "str_len_km"), axis=1
    )

    output_path = os.path.join(DRAINAGE_DENSITY_OUTPUT, state.replace(" ", "_"))
    if not os.path.exists(output_path):
        os.mkdir(output_path)
    output_path = os.path.join(
        str(output_path),
        f"{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}",
    )
    # Save the results to a new shapefile
    watersheds.to_file(output_path, driver="ESRI Shapefile", encoding="UTF-8")

    # watersheds = watersheds.to_crs("EPSG:4326")
    # fc = gdf_to_ee_fc(watersheds)
    #
    # try:
    #     # Export an ee.FeatureCollection as an Earth Engine asset.
    #     mws_task = export_vector_asset_to_gee(
    #         fc,
    #         "drainage_density_vector_"
    #         + valid_gee_text(district.lower())
    #         + "_"
    #         + valid_gee_text(block.lower()),
    #         get_gee_asset_path(state, district, block)
    #         + "drainage_density_vector_"
    #         + valid_gee_text(district.lower())
    #         + "_"
    #         + valid_gee_text(block.lower())
    #     )
    #     print("Successfully started the drainage_density")
    #     # return [mws_task]
    # except Exception as e:
    #     print(f"Error occurred in running drainage_density task: {e}")
    return output_path


def convert_dict_to_list(row, key):
    if isinstance(row[key], dict):
        # Convert dictionary values to a list and then to string or just take specific values you need
        values = [float(v) for v in row[key].values()]
        return str(values)  # or return values if you want a list
    return row[key]

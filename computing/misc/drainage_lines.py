import geopandas as gpd
import os
import pandas as pd
from computing.utils import push_shape_to_geoserver
from utilities.gee_utils import ee_initialize, valid_gee_text
from utilities.constants import (
    DRAINAGE_LINES_OUTPUT,
    DRAINAGE_LINES_SHAPEFILES,
    BASIN_BOUNDARIES,
    ADMIN_BOUNDARY_OUTPUT_DIR,
)
from nrm_app.celery import app


@app.task(bind=True)
def clip_drainage_lines(
    self,
    state,
    district,
    block,
):
    # Load the input file
    input_file_path = os.path.join(
        ADMIN_BOUNDARY_OUTPUT_DIR,
        f"""{valid_gee_text(state.lower())}/{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}/{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}.shp""",
    )
    print(input_file_path)
    # Load input basin boundary shapefile
    basin_boundary = gpd.read_file(BASIN_BOUNDARIES)
    basin_boundary = basin_boundary.to_crs("EPSG:4326")

    # Load the input file
    input_file = gpd.read_file(input_file_path)
    input_file = input_file.to_crs("EPSG:4326")
    print(input_file)
    # Create an empty GeoDataFrame to store the result
    result_gdf = gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
    # Iterate over each basin in the input shapefile
    for index, basin in basin_boundary.iterrows():
        # if not basin.geometry.is_valid:
        #     basin.geometry = basin.geometry.buffer(0)  # Attempt to fix invalid geometries

        clipped_basin = gpd.clip(input_file, basin.geometry)

        # Check if the input file intersects with this basin
        if (
            not clipped_basin.empty
            and not (clipped_basin.geometry.type == "Point").any()
        ):
            # Load drainage lines shapefile for this basin
            drainage_lines_path = os.path.join(
                DRAINAGE_LINES_SHAPEFILES, f"{basin['ba_name']}_dl_so.shp"
            )
            drainage_lines = gpd.read_file(drainage_lines_path)
            drainage_lines = drainage_lines.to_crs("EPSG:4326")

            # Clip drainage lines based on the input shapefile boundary
            clipped_lines = gpd.clip(drainage_lines, input_file)
            if (
                not clipped_lines.empty
                and not (clipped_lines.geometry.type == "Point").any()
            ):
                result_gdf = gpd.GeoDataFrame(
                    pd.concat([result_gdf, clipped_lines], ignore_index=True),
                    crs=result_gdf.crs,
                )

    state_dir = os.path.join(DRAINAGE_LINES_OUTPUT, state.replace(" ", "_"))
    if not os.path.exists(state_dir):
        os.mkdir(state_dir)

    output_shapefile_path = os.path.join(
        str(state_dir),
        f"{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}",
    )

    if not result_gdf.empty:
        int_columns = ["fid", "cat", "type_code", "network", "ORDER"]
        result_gdf[int_columns] = result_gdf[int_columns].astype(int)

        # Save the final result to a new shapefile
        result_gdf.to_file(
            output_shapefile_path, driver="ESRI Shapefile", encoding="UTF-8"
        )
        print("Clipping process completed for:", output_shapefile_path)
        return push_shape_to_geoserver(output_shapefile_path, workspace="drainage")
    else:
        print("No valid geometries were processed for:", output_shapefile_path)

import geopandas as gpd
import os
import pandas as pd
from shapely.validation import make_valid
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
    try:
        # Load the input file
        input_file_path = os.path.join(
            ADMIN_BOUNDARY_OUTPUT_DIR,
            f"""{valid_gee_text(state.lower())}/{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}/{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}.shp""",
        )
        print(f"Loading input file from: {input_file_path}")
        
        # Load input basin boundary shapefile
        basin_boundary = gpd.read_file(BASIN_BOUNDARIES)
        basin_boundary = basin_boundary.to_crs("EPSG:4326")

        # Load the admin boundary file
        input_file = gpd.read_file(input_file_path)
        input_file = input_file.to_crs("EPSG:4326")
        
        # Make geometries valid
        input_file['geometry'] = input_file.geometry.apply(make_valid)
        basin_boundary['geometry'] = basin_boundary.geometry.apply(make_valid)

        # Create an empty GeoDataFrame to store the result
        result_gdf = gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
        
        # Iterate over each basin in the input shapefile
        for index, basin in basin_boundary.iterrows():
            try:
                # Clip admin boundary to current basin
                clipped_basin = gpd.clip(input_file, basin.geometry)

                # Check if the clipped result is valid
                if clipped_basin.empty or (clipped_basin.geometry.type == "Point").any():
                    continue

                # Load drainage lines shapefile for this basin
                drainage_lines_path = os.path.join(
                    DRAINAGE_LINES_SHAPEFILES, f"{basin['ba_name']}_dl_so.shp"
                )
                if not os.path.exists(drainage_lines_path):
                    print(f"Drainage lines file not found: {drainage_lines_path}")
                    continue
                    
                drainage_lines = gpd.read_file(drainage_lines_path)
                drainage_lines = drainage_lines.to_crs("EPSG:4326")
                drainage_lines['geometry'] = drainage_lines.geometry.apply(make_valid)

                # Clip drainage lines based on the admin boundary
                clipped_lines = gpd.clip(drainage_lines, input_file)
                
                if clipped_lines.empty or (clipped_lines.geometry.type == "Point").any():
                    continue

                # Append to results
                result_gdf = gpd.GeoDataFrame(
                    pd.concat([result_gdf, clipped_lines], ignore_index=True),
                    crs=result_gdf.crs,
                )

            except Exception as e:
                print(f"Error processing basin {basin['ba_name']}: {str(e)}")
                continue

        # Prepare output directory
        state_dir = os.path.join(DRAINAGE_LINES_OUTPUT, state.replace(" ", "_"))
        os.makedirs(state_dir, exist_ok=True)

        output_shapefile_path = os.path.join(
            str(state_dir),
            f"{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}",
        )

        if not result_gdf.empty:
            # Convert specified columns to integers
            int_columns = ["fid", "cat", "type_code", "network", "ORDER"]
            for col in int_columns:
                if col in result_gdf.columns:
                    result_gdf[col] = result_gdf[col].astype(int)

            # Save the final result
            result_gdf.to_file(
                output_shapefile_path, driver="ESRI Shapefile", encoding="UTF-8"
            )
            print("Clipping process completed successfully for:", output_shapefile_path)
            return push_shape_to_geoserver(output_shapefile_path, workspace="drainage")
        else:
            print("No valid geometries were processed for:", output_shapefile_path)
            return None

    except Exception as e:
        print(f"Error in clip_drainage_lines: {str(e)}")
        raise
import os
import geopandas as gpd
import pandas as pd
from nrm_app.celery import app
from shapely import wkt
from shapely.geometry import Point
from computing.utils import push_shape_to_geoserver
from utilities.constants import (
    ADMIN_BOUNDARY_INPUT_DIR,
    NREGA_ASSETS_INPUT_DIR,
    NREGA_ASSETS_OUTPUT_DIR,
)
from computing.views import create_generated_layer
from geoadmin.models import State, District, Block
from unidecode import unidecode


@app.task(bind=True)
def clip_nrega_district_block(self, state_name, district_name, block_name, user):
    print("inside clip")
    formatted_state_name = state_name.title()
    if " " in formatted_state_name:
        formatted_state_name = formatted_state_name.replace(" ", "_")
    if " " in district_name:
        district_name = district_name.replace(" ", "_")
    if " " in block_name:
        block_name = block_name.replace(" ", "_")
    district_shape_file_metadata_df = pd.read_csv(
        os.path.join(
            NREGA_ASSETS_INPUT_DIR,
            f"{formatted_state_name.upper()}/{district_name.upper()}.csv",
        )
    )


    district_shape_file_metadata_df["Geometry"] = [
        f"{Point(xy)}"
        for xy in zip(
            district_shape_file_metadata_df["lon"],
            district_shape_file_metadata_df["lat"],
        )
    ]
    district_shape_file_metadata_df["Geometry"] = district_shape_file_metadata_df[
        "Geometry"
    ].apply(wkt.loads)

    soi = gpd.read_file(ADMIN_BOUNDARY_INPUT_DIR + "/soi_tehsil.geojson")

    soi = soi[(soi["STATE"].str.lower() == state_name)]
    soi = soi[(soi["District"].str.lower() == district_name)]
    soi = soi[(soi["TEHSIL"].str.lower() == block_name)]

    # geojson_path = os.path.join(
    # ADMIN_BOUNDARY_INPUT_DIR, "soi_tehsil.geojson",
    # )
    # state_bounds = gpd.read_file(geojson_path)
    # print("state_bounds", state_bounds)
    #
    # district_bounds = state_bounds[
    #     state_bounds["District"].str.lower() == district_name.lower()
    # ]
    #
    # district_bounds = district_bounds[
    #     district_bounds["TEHSIL"].str.lower() == block_name.lower()
    # ]

    soi = soi.dissolve()

    block_bounds = soi.geometry.iloc[0] if not soi.empty else None
    gdf = gpd.GeoDataFrame(district_shape_file_metadata_df, geometry="Geometry")

    block_metadata_df = gdf[gdf.geometry.within(block_bounds)] if block_bounds else gdf
    columns_for_unicode = ["Work Name", "Panchayat"]
    def apply_unidecode_to_columns(df, cols):
        df[cols] = df[cols].applymap(lambda x: unidecode(x) if isinstance(x, str) else x)
        return df

    block_metadata_df = apply_unidecode_to_columns(block_metadata_df, columns_for_unicode)
    block_metadata_df.crs = "EPSG:4326"

    path = os.path.join(
        NREGA_ASSETS_OUTPUT_DIR,
        f"""{"_".join(district_name.split())}_{"_".join(block_name.split())}""",
    )

    block_metadata_df.to_file(path, driver="ESRI Shapefile", encoding="UTF-8")

    '''
        Creating entry in db for the generated layer
    '''
    state = State.objects.get(state_name=state_name)
    district = District.objects.get(district_name=district_name, state_id=state.state_census_code)
    block = Block.objects.get(block_name=block_name, district_id=district.id)

    nrega_layer_data = {
        'layer_name': f"{district_name}_{block_name}",
        'layer_type': 'vector',
        'state': state,
        'district': district,
        'block': block,
        'gee_path': '',
        'workspace': 'nrega_assets',
        'algorithm': 'bhuvan_nrega_data',
        'version': '1',
        'style_name': '',
    }

    try:
        new_layer = create_generated_layer(nrega_layer_data, user)
        print(f"✅ Entry created for NREGA layer: {new_layer.layer_name}")
    except Exception as e:
        print(f"Exception while creating entry in layer table: {str(e)}")


    return push_shape_to_geoserver(path, workspace="nrega_assets")

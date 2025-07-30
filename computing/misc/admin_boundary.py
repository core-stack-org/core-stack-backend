import os
import ee
from nrm_app.celery import app
import geopandas as gpd
from geojson import Feature, FeatureCollection
from shapely import wkt
from shapely.geometry import mapping
from computing.utils import (
    generate_shape_files,
    push_shape_to_geoserver,
)
from utilities.gee_utils import (
    ee_initialize,
    check_task_status,
    valid_gee_text,
    get_gee_asset_path,
    geojson_to_ee_featurecollection,
    is_gee_asset_exists,
    create_gee_directory,
    upload_shp_to_gee,
    export_vector_asset_to_gee,
    make_asset_public,
)
from utilities.constants import ADMIN_BOUNDARY_INPUT_DIR, ADMIN_BOUNDARY_OUTPUT_DIR
from computing.utils import save_layer_info_to_db


@app.task(bind=True)
def generate_tehsil_shape_file_data(self, state, district, block):
    ee_initialize()
    description = (
        "admin_boundary_"
        + valid_gee_text(district.lower())
        + "_"
        + valid_gee_text(block.lower())
    )
    asset_id = get_gee_asset_path(state, district, block) + description

    collection, state_dir = clip_block_from_admin_boundary(state, district, block)

    if not is_gee_asset_exists(asset_id):
        task_id = sync_admin_boundary_to_ee(
            collection, description, state, district, block
        )

        task_id_list = check_task_status([task_id]) if task_id else []
        print("task_id", task_id_list)

    if is_gee_asset_exists(asset_id):
        save_layer_info_to_db(
            state,
            district,
            block,
            layer_name=f"{district.title()}_{block.title()}",
            asset_id=asset_id,
            dataset_name="Admin Boundary",
        )
        make_asset_public(asset_id)

    # Generate shape files and sync to geoserver
    shp_path = sync_admin_boundry_to_geoserver(
        collection, state_dir, district, block, state, asset_id
    )

    if not is_gee_asset_exists(asset_id):
        layer_name = (
            "admin_boundary_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
        )
        layer_path = os.path.splitext(shp_path)[0] + "/" + shp_path.split("/")[-1]
        upload_shp_to_gee(layer_path, layer_name, asset_id)
        make_asset_public(asset_id)


def sync_admin_boundry_to_geoserver(
    collection, state_dir, district, block, state, asset_id
):
    path = os.path.join(
        str(state_dir),
        f"{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}",
    )
    # Write the feature collection into json file
    with open(path + ".json", "w") as f:
        try:
            f.write(f"{collection}")
        except Exception as e:
            print(e)
    path = generate_shape_files(path)
    res = push_shape_to_geoserver(path, workspace="panchayat_boundaries")
    if res["status_code"] == 201:
        save_layer_info_to_db(
            state,
            district,
            block,
            layer_name=f"{district.title()}_{block.title()}",
            asset_id=asset_id,
            dataset_name="Admin Boundary",
            sync_to_geoserver=True,
        )
    return path


def sync_admin_boundary_to_ee(collection, description, state, district, block):
    create_gee_directory(state, district, block)

    fc = geojson_to_ee_featurecollection(collection)
    try:
        # Export an ee.FeatureCollection as an Earth Engine asset.
        task = export_vector_asset_to_gee(
            fc, description, get_gee_asset_path(state, district, block) + description
        )
        return task
    except Exception as e:
        print(f"Error occurred in running admin_boundary_task: {e}")
        return None


def clip_block_from_admin_boundary(state, district, block):
    census_2011 = gpd.read_file(
        ADMIN_BOUNDARY_INPUT_DIR
        + "/"
        + state.replace(" ", "_")
        + "/"
        + district.replace(" ", "_")
        + ".geojson"
    )
    print("census_2011", census_2011)
    cols = list(census_2011.columns)
    if "TEHSIL" in cols:
        admin_boundary_data = census_2011[(census_2011["TEHSIL"].str.lower() == block)]
    else:
        soi = gpd.read_file(ADMIN_BOUNDARY_INPUT_DIR + "/soi_tehsil.geojson")

        soi = soi[(soi["STATE"].str.lower() == state)]
        soi = soi[(soi["District"].str.lower() == district)]
        soi = soi[(soi["TEHSIL"].str.lower() == block)]
        soi.rename(
            columns={"STATE": "state_name", "District": "district_name"}, inplace=True
        )
        print("soi", soi)

        census_2011["area"] = census_2011.geometry.area
        # Ensure both GeoDataFrames are in the same coordinate reference system (CRS)
        if soi.crs != census_2011.crs:
            census_2011 = census_2011.to_crs(soi.crs)

        # Perform the intersection
        admin_boundary_data = gpd.overlay(soi, census_2011, how="intersection")
        # # Calculate areas
        # intersection["int_area"] = intersection.geometry.area
        # # Calculate overlap percentage
        # intersection["overlap_pct"] = intersection["int_area"] / intersection["area"]
        #
        # # Filter blocks with more than 80% overlap
        # filtered_blocks = intersection[intersection["overlap_pct"] > 0.8]

    features = []
    for index, row in admin_boundary_data.iterrows():
        features.append(
            Feature(
                geometry=mapping(row["geometry"]),
                properties={
                    "vill_ID": row["pc11_village_id"],
                    "vill_name": row["NAME"],
                    "block_cen": row["pc11_subdistrict_id"],
                    # "block": row["subdistrict"],
                    "tehsil": row["TEHSIL"],
                    "dist_cen": row["pc11_district_id"],
                    "district": row["district_name"],
                    "state_cen": row["pc11_state_id"],
                    "state": row["state_name"],
                    "ADI_2001": row["ADI_2001"],
                    "ADI_2011": row["ADI_2011"],
                    "ADI_2019": row["ADI_2019"],
                    "No_HH": row["No_HH"],
                    "TOT_P": row["TOT_P"],
                    "TOT_M": row["TOT_M"],
                    "TOT_F": row["TOT_F"],
                    "P_SC": row["P_SC"],
                    "M_SC": row["M_SC"],
                    "F_SC": row["F_SC"],
                    "P_ST": row["P_ST"],
                    "M_ST": row["M_ST"],
                    "F_ST": row["F_ST"],
                    "P_LIT": row["P_LIT"],
                    "M_LIT": row["M_LIT"],
                    "F_LIT": row["F_LIT"],
                    "P_ILL": row["P_ILL"],
                    "M_ILL": row["M_ILL"],
                    "F_ILL": row["F_ILL"],
                    "BF_2001": row["BF_2001"],
                    "FC_2001": row["FC_2001"],
                    "MSW_2001": row["MSW_2001"],
                    "ASSET_2001": row["ASSET_2001"],
                    "BF_2011": row["BF_2011"],
                    "FC_2011": row["FC_2011"],
                    "MSW_2011": row["MSW_2011"],
                    "ASSET_2011": row["ASSET_2011"],
                    "BF_2019": row["BF_2019"],
                    "FC_2019": row["FC_2019"],
                    "MSW_2019": row["MSW_2019"],
                    "ASSET_2019": row["ASSET_2019"],
                },
            )
        )

    # Create the directory for state if doesn't exist already
    state_dir = os.path.join(ADMIN_BOUNDARY_OUTPUT_DIR, state.replace(" ", "_"))
    if not os.path.exists(state_dir):
        os.mkdir(state_dir)

    # Creating the feature collection out of the features list built in the previous cell
    collection = FeatureCollection(features)
    return collection, state_dir

import os
import ee
from nrm_app.celery import app
import geopandas as gpd
from utilities.layer_generation_logging import (
    log_task_failure,
    log_task_step,
    task_location_context,
)
from geojson import Feature, FeatureCollection
from shapely.geometry import mapping
from computing.utils import (
    generate_shape_files,
    push_shape_to_geoserver,
)
from utilities.gee_utils import (
    ee_initialize,
    valid_gee_text,
    get_gee_asset_path,
    is_gee_asset_exists,
    create_gee_directory,
    upload_shp_to_gee,
    make_asset_public,
)
from utilities.constants import (
    ADMIN_BOUNDARY_INPUT_DIR,
    ADMIN_BOUNDARY_OUTPUT_DIR,
    SOI_TEHSIL,
)
from computing.utils import save_layer_info_to_db, update_layer_sync_status

from computing.STAC_specs import generate_STAC_layerwise


TASK_NAME = "generate_tehsil_shape_file_data"


@app.task(bind=True)
def generate_tehsil_shape_file_data(self, state, district, block, gee_account_id):
    """
    It will generate Admin boundary of given location as tehsil levels
    """
    ctx = task_location_context(
        state=state,
        district=district,
        block=block,
        gee_account_id=gee_account_id,
    )
    log_task_step(TASK_NAME, "start", **ctx)
    try:
        log_task_step(TASK_NAME, "ee_initialize", **ctx)
        ee_initialize(gee_account_id)

        description = (
            "admin_boundary_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
        )
        asset_id = get_gee_asset_path(state, district, block) + description
        log_task_step(TASK_NAME, "asset_id_resolved", asset_id=asset_id, **ctx)

        log_task_step(TASK_NAME, "clip_block_from_admin_boundary", **ctx)
        collection, state_dir = clip_block_from_admin_boundary(state, district, block)
        feature_count = len(collection.get("features", []))
        log_task_step(
            TASK_NAME,
            "clip_complete",
            feature_count=feature_count,
            state_dir=str(state_dir),
            **ctx,
        )
        if feature_count == 0:
            raise ValueError(
                f"No admin boundary features for state={state!r} "
                f"district={district!r} block={block!r}. "
                "Check census/SOI inputs and spelling."
            )

        layer_id = None
        log_task_step(TASK_NAME, "create_shp_files", **ctx)
        shp_path = create_shp_files(collection, state_dir, district, block, layer_id)
        log_task_step(TASK_NAME, "shapefile_created", shp_path=shp_path, **ctx)

        log_task_step(TASK_NAME, "create_gee_directory", **ctx)
        create_gee_directory(state, district, block)

        if not is_gee_asset_exists(asset_id):
            layer_name = (
                "admin_boundary_"
                + valid_gee_text(district.lower())
                + "_"
                + valid_gee_text(block.lower())
            )
            layer_path = os.path.splitext(shp_path)[0] + "/" + shp_path.split("/")[-1]
            log_task_step(
                TASK_NAME,
                "upload_shp_to_gee",
                layer_path=layer_path,
                layer_name=layer_name,
                asset_id=asset_id,
                **ctx,
            )
            upload_shp_to_gee(layer_path, layer_name, asset_id)

        if is_gee_asset_exists(asset_id):
            log_task_step(TASK_NAME, "make_asset_public", asset_id=asset_id, **ctx)
            make_asset_public(asset_id)
            layer_id = save_layer_info_to_db(
                state,
                district,
                block,
                layer_name=f"{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}",
                asset_id=asset_id,
                dataset_name="Admin Boundary",
            )
            log_task_step(TASK_NAME, "layer_saved_to_db", layer_id=layer_id, **ctx)

        log_task_step(
            TASK_NAME,
            "push_shape_to_geoserver",
            shp_path=shp_path,
            workspace="panchayat_boundaries",
            **ctx,
        )
        res = push_shape_to_geoserver(shp_path, workspace="panchayat_boundaries")
        layer_at_geoserver = False
        log_task_step(
            TASK_NAME,
            "geoserver_response",
            status_code=res.get("status_code"),
            response=res,
            **ctx,
        )
        if res["status_code"] == 201 and layer_id:
            update_layer_sync_status(layer_id=layer_id, sync_to_geoserver=True)

            layer_STAC_generated = generate_STAC_layerwise.generate_vector_stac(
                state=state,
                district=district,
                block=block,
                layer_name="admin_boundaries_vector",
            )
            update_layer_sync_status(
                layer_id=layer_id, is_stac_specs_generated=layer_STAC_generated
            )
            layer_at_geoserver = True

        log_task_step(TASK_NAME, "complete", layer_at_geoserver=layer_at_geoserver, **ctx)
        return layer_at_geoserver
    except Exception as exc:
        log_task_failure(TASK_NAME, exc, **ctx)
        raise


def create_shp_files(collection, state_dir, district, block, layer_id):
    print("sync_admin_boundry_to_geoserver")
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
    return path


def clip_block_from_admin_boundary(state, district, block):
    census_path = (
        f"{ADMIN_BOUNDARY_INPUT_DIR}/{state.replace(' ', '_')}/"
        f"{district.replace(' ', '_')}.geojson"
    )
    log_task_step(
        TASK_NAME,
        "load_census_geojson",
        census_path=census_path,
        **task_location_context(state=state, district=district, block=block),
    )
    census_2011 = None
    try:
        census_2011 = gpd.read_file(census_path)
        log_task_step(
            TASK_NAME,
            "census_loaded",
            row_count=len(census_2011),
            columns=list(census_2011.columns),
        )
    except Exception as e:
        log_task_step(TASK_NAME, "census_not_available", census_path=census_path, error=str(e))

    admin_boundary_data = None
    features = []

    if census_2011 is not None and "TEHSIL" in list(census_2011.columns):
        admin_boundary_data = census_2011[(census_2011["TEHSIL"].str.lower() == block)]
        log_task_step(
            TASK_NAME,
            "filter_census_tehsil",
            matched_rows=len(admin_boundary_data),
        )
    else:
        log_task_step(TASK_NAME, "load_soi_tehsil", soi_path=SOI_TEHSIL)
        soi = gpd.read_file(SOI_TEHSIL)

        soi = soi[(soi["STATE"].str.lower() == state)]
        soi = soi[(soi["District"].str.lower() == district)]
        soi = soi[(soi["TEHSIL"].str.lower() == block)]
        log_task_step(TASK_NAME, "filter_soi_tehsil", matched_rows=len(soi))
        if len(soi) == 0:
            raise ValueError(
                f"No SOI tehsil match for state={state!r} district={district!r} "
                f"block={block!r}"
            )
        soi.rename(
            columns={"STATE": "state_name", "District": "district_name"}, inplace=True
        )
        print("soi", soi)

        if census_2011 is not None:
            census_2011["area"] = census_2011.geometry.area
            # Ensure both GeoDataFrames are in the same coordinate reference system (CRS)
            if soi.crs != census_2011.crs:
                census_2011 = census_2011.to_crs(soi.crs)

            # Perform the intersection
            admin_boundary_data = gpd.overlay(soi, census_2011, how="intersection")
        else:
            tehsil_boundary = soi.iloc[0]
            features.append(
                Feature(
                    geometry=mapping(tehsil_boundary["geometry"]),
                    properties={
                        "tehsil": tehsil_boundary["TEHSIL"],
                        "district": tehsil_boundary["district_name"],
                        "state": tehsil_boundary["state_name"],
                    },
                )
            )

    if admin_boundary_data is not None:
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

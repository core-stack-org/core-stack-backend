import json
from datetime import datetime

import ee

from computing.utils import (
    sync_fc_to_geoserver,
    calculate_precipitation_season,
    get_season_key,
    get_agri_year_key,
    update_dashboard_geojson,
    sync_project_fc_to_geoserver,
)
from constants.pan_india_path import CATCHMENT_AREA, STREAM_ORDER_RASTER
from geoadmin.models import (
    District,
    State_Disritct_Block_Properties,
    StateSOI,
    DistrictSOI,
    TehsilSOI,
)
from nrm_app.celery import app
from projects.models import Project
from utilities.constants import GEE_PATHS
from utilities.gee_utils import (
    ee_initialize,
    valid_gee_text,
    get_gee_dir_path,
    is_gee_asset_exists,
    export_vector_asset_to_gee,
    make_asset_public,
    check_task_status,
)

from waterrejuvenation.utils import calculate_zoi_area, wait_for_task_completion


def generate_zoi1(
    state=None,
    district=None,
    block=None,
    roi=None,
    asset_suffix=None,
    asset_folder_list=None,
    app_type="MWS",
    gee_account_id=None,
):
    print("insdie zoi")
    ee_initialize(gee_account_id)
    description = "swb4_" + asset_suffix
    asset_id = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + description
    )

    roi = ee.FeatureCollection(asset_id)
    description_zoi = "zoi_" + asset_suffix
    asset_id_zoi = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + description_zoi
    )

    zoi_fc = roi.map(compute_zoi)
    zoi_fc = ee.FeatureCollection(zoi_fc)
    zoi_rings = zoi_fc.filter(ee.Filter.gt("zoi_wb", 0)).map(create_ring)
    if not is_gee_asset_exists(asset_id_zoi):
        zoi_task = export_vector_asset_to_gee(zoi_rings, description_zoi, asset_id_zoi)
        check_task_status([zoi_task])
        make_asset_public(asset_id_zoi)


def compute_zoi(feature):

    area_of_wb = ee.Number(feature.get("area_ored"))  # assumes area field exists

    # logistic_weight
    def logistic_weight(x, x0=0.2, k=50):
        return ee.Number(1).divide(
            ee.Number(1).add((ee.Number(-k).multiply(x.subtract(x0))).exp())
        )

    # y_small_bodies
    def y_small_bodies(area):
        return ee.Number(126.84).multiply(area.add(0.05).log()).add(383.57)

    # y_large_bodies
    def y_large_bodies(area):
        return ee.Number(140).multiply(area.add(0.05).log()).add(500)

    s = logistic_weight(area_of_wb)

    zoi = (
        (ee.Number(1).subtract(s))
        .multiply(y_small_bodies(area_of_wb))
        .add(s.multiply(y_large_bodies(area_of_wb)).round())
    )

    return feature.set("zoi_wb", zoi)


def create_ring(feature):
    geom = feature.geometry()  # can be point or polygon
    zoi = ee.Number(feature.get("zoi_wb"))
    uid = feature.get("UID")

    # Make circle buffer from centroid
    centroid = geom.centroid()
    circle = centroid.buffer(zoi)

    zoi_area = calculate_zoi_area(zoi)

    return ee.Feature(circle).set(
        {
            "zoi": zoi,
            "UID": uid,
            "zoi_area": zoi_area,
        }
    )


@app.task(bind=True)
def generate_zoi_ci(
    self,
    state=None,
    district=None,
    block=None,
    zoi_roi=None,
    asset_suffix=None,
    asset_folder_list=None,
    app_type="MWS",
    gee_account_id=None,
):
    from computing.cropping_intensity.cropping_intensity import (
        generate_cropping_intensity,
    )

    asset_folder_list = [state, district, block]
    asset_suffix = (
        valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
    )
    description_zoi = "zoi_" + asset_suffix
    asset_id_zoi = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + description_zoi
    )

    description_ci = "zoi_cropping_intensity_" + asset_suffix
    asset_id_ci = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + description_ci
    )
    asset_folder_list = [state, district, block]

    generate_cropping_intensity.delay(
        roi_path=asset_id_zoi,
        asset_folder_list=asset_folder_list,
        asset_suffix=asset_suffix,
        app_type="MWS",
        start_year=2017,
        end_year=2023,
        gee_account_id=gee_account_id,
        asset_id_ci=asset_id_ci,
    )


@app.task(bind=True)
def get_ndvi_for_zoi(
    self,
    state=None,
    district=None,
    block=None,
    zoi_roi=None,
    asset_suffix=None,
    asset_folder_list=None,
    app_type="MWS",
    gee_account_id=None,
):
    ee_initialize(gee_account_id)
    from waterrejuvenation.utils import get_ndvi_data

    asset_folder_list = [state, district, block]
    asset_suffix = (
        valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
    )
    description_zoi = "zoi_" + asset_suffix
    asset_id_zoi = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
        )
        + description_zoi
    )
    description_ndvi = "ndvi_" + asset_suffix
    ndvi_asset_path = (
        get_gee_dir_path(
            asset_folder_list, asset_path=GEE_PATHS["MWS"]["GEE_ASSET_PATH"]
        )
        + description_ndvi
    )

    zoi_collections = ee.FeatureCollection(asset_id_zoi)
    fc = get_ndvi_data(zoi_collections, 2017, 2024, description_ndvi, ndvi_asset_path)
    task = ee.batch.Export.table.toAsset(
        collection=fc, description="export_ndvi_waterrej_task", assetId=ndvi_asset_path
    )
    task.start()
    wait_for_task_completion(task)
    return ndvi_asset_path


@app.task(bind=True)
def GenerateWaterBalanceGeoJson(
    self,
    state=None,
    district=None,
    block=None,
    app_type="MWS",
    gee_account_id=1,
    proj_id=None,
):
    ee_initialize(gee_account_id)

    if state and district and block:
        asset_suffix = (
            valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
        )
        asset_folder_list = [state, district, block]
        description_catchment_so = f"swb4_" + asset_suffix
        asset_id_swb4 = (
            get_gee_dir_path(
                asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
            )
            + description_catchment_so
        )
        workspace_name = "waterbodies_geojson"
        layer_name = f"{state}_{district}_{block}_wb"

    else:
        proj_obj = Project.objects.get(pk=proj_id)
        asset_folder = [proj_obj.name]
        asset_suffix_so = f"swb4_{proj_obj.name}_{proj_obj.id}"
        asset_id_swb4 = (
            get_gee_dir_path(
                asset_folder, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
            )
            + asset_suffix_so
        )
        worspace_name = proj_obj.app_tyoe
        layer_name = f"{proj_obj.name}_{proj_obj.id}_wb"
    fc = ee.FeatureCollection(asset_id_swb4)
    # season_fc = calculate_precipitation_season(
    #     mws_geojson_op, draught_asset_id, proj_obj.id
    # )
    if proj_id:
        proj_obj = Project.objects.get(pk=proj_id)
        update_dashboard_geojson(
            proj_id=proj_obj.id, layer_name=layer_name, workspace_name=workspace_name
        )
        sync_project_fc_to_geoserver(fc, proj_obj.name, layer_name, "waterrrej")

    else:
        update_dashboard_geojson(state, district, block, layer_name, workspace_name)
        sync_fc_to_geoserver(fc, state, layer_name, "waterrej")


@app.task(bind=True)
def GenerateZoiGeoJson(
    self,
    state=None,
    district=None,
    block=None,
    proj_id=None,
    app_type="MWS",
    gee_account_id=1,
):
    ee_initialize(gee_account_id)
    if state and district and block:
        asset_suffix = (
            valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
        )
        asset_folder_list = [state, district, block]

        description_zoi = "zoi_ring_" + asset_suffix
        asset_id_zoi = (
            get_gee_dir_path(
                asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
            )
            + description_zoi
        )
        description_ci = "zoi_cropping_intensity_" + asset_suffix
        asset_id_ci = (
            get_gee_dir_path(
                asset_folder_list, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
            )
            + description_ci
        )
        description_ndvi = "zoi_ndvi_" + asset_suffix
        asset_id_ndmi = (
            get_gee_dir_path(
                asset_folder_list, asset_path=GEE_PATHS["MWS"]["GEE_ASSET_PATH"]
            )
            + description_ndvi
        )
        layer_name = f"{state}_{district}_{block}_zoi"
    else:
        proj_obj = Project.objcets.get(pk=proj_id)
        asset_folder = [proj_obj]
        asset_suffix_zoi = f"zoi_ring_"
        asset_id_zoi = (
            get_gee_dir_path(
                asset_folder, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
            )
            + asset_suffix_zoi
        )
        asset_suffix_zoi_ci = f"zoi_cropping_intensity_{proj_obj.name}_{proj_obj.id}"
        asset_id_ci = (
            get_gee_dir_path(
                asset_folder, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
            )
            + asset_suffix_zoi_ci
        )
        description_ndvi = f"zoi_ndvi_{proj_obj.name}_{proj_obj.id}"
        asset_id_ndmi = (
            get_gee_dir_path(
                asset_folder, asset_path=GEE_PATHS["MWS"]["GEE_ASSET_PATH"]
            )
            + asset_suffix_zoi_ci
        )
        layer_name = f"{proj_obj.organization.name}_{proj_obj.name}_mws"
    zoi_fc = ee.FeatureCollection(asset_id_zoi)
    ci_fc = ee.FeatureCollection(asset_id_ci)
    ndmi_fc = ee.FeatureCollection(asset_id_ndmi)
    # Optional: ensure unique UIDs in secondary collections
    ci_fc = ci_fc.distinct("UID")
    ndmi_fc = ndmi_fc.distinct("UID")

    # --- Join ZOI with CI ---
    uid_filter = ee.Filter.equals(leftField="UID", rightField="UID")

    # This keeps all ZOI features and attaches the matching CI feature as a property
    join_ci = ee.Join.saveFirst("ci_match").apply(zoi_fc, ci_fc, uid_filter)

    def merge_ci_props(feature):
        ci_match = ee.Feature(feature.get("ci_match"))
        # merge CI properties if match exists
        return ee.Feature(feature).copyProperties(ci_match, exclude=["system:index"])

    zoi_ci_merged = join_ci.map(merge_ci_props)

    # --- Join with NDMI ---
    join_ndmi = ee.Join.saveFirst("ndmi_match").apply(
        zoi_ci_merged, ndmi_fc, uid_filter
    )

    def merge_ndmi_props(feature):
        ndmi_match = ee.Feature(feature.get("ndmi_match"))
        return ee.Feature(feature).copyProperties(ndmi_match, exclude=["system:index"])

    merged_fc = join_ndmi.map(merge_ndmi_props)

    workspace_name = "waterrej"
    if proj_id:
        update_dashboard_geojson(
            proj_id=proj_obj.id, layer_name=layer_name, workspace_name=workspace_name
        )
        sync_project_fc_to_geoserver(merged_fc, proj_obj.name, layer_name, "waterrrej")

    else:
        update_dashboard_geojson(state, district, block, layer_name, workspace_name)
        sync_fc_to_geoserver(merged_fc, state, layer_name, "waterrej")


@app.task(bind=True)
def GenerateMWSGeoJson(
    self,
    state=None,
    district=None,
    block=None,
    proj_id=None,
    app_type="MWS",
    gee_account_id=1,
    start_year=2017,
    end_year=2022,
):
    import geemap, json
    from datetime import datetime
    import ee

    ee_initialize(gee_account_id)
    if state and district and block:

        asset_suffix = (
            valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
        )
        asset_folder_list = [state, district, block]
        description_precip = "Prec_fortnight_" + asset_suffix
        description_drought = f"drought_{asset_suffix}_{start_year}_{end_year}"

        asset_id_precip = (
            get_gee_dir_path(asset_folder_list, GEE_PATHS[app_type]["GEE_ASSET_PATH"])
            + description_precip
        )
        asset_id_drought = (
            get_gee_dir_path(asset_folder_list, GEE_PATHS[app_type]["GEE_ASSET_PATH"])
            + description_drought
        )
        layer_name = f"{state}_{district}_{block}_mws"
    else:
        proj_obj = Project.objcts.get(pk=proj_id)
        asset_suffix_prec = f"precipitation_forthnight_{proj_obj.name}_{proj_obj.id}"
        asset_suffix_draught = f"draught_{proj_obj.name}_{proj_obj.id}"
        asset_folder_list = [proj_obj.name]
        asset_id_precip = (
            get_gee_dir_path(asset_folder_list, GEE_PATHS[app_type]["GEE_ASSET_PATH"])
            + asset_suffix_prec
        )
        asset_id_drought = (
            get_gee_dir_path(asset_folder_list, GEE_PATHS[app_type]["GEE_ASSET_PATH"])
            + asset_suffix_draught
        )
        layer_name = f"{proj_obj.organization.name}_{proj_obj.name}_mws"

    precip = ee.FeatureCollection(asset_id_precip)
    drought_fc = ee.FeatureCollection(asset_id_drought)

    # --- Convert precipitation to GeoDataFrame and rebuild with aggregated props ---
    gdf = geemap.ee_to_gdf(precip)

    mws_geojson_op = f"data/fc_to_shape/{state}/{layer_name}.geojson"
    gdf.to_file(mws_geojson_op, driver="GeoJSON")

    with open(mws_geojson_op, "r") as f:
        feature_collection = json.load(f)

    features_ee = []

    for feature in feature_collection["features"]:
        original_props = feature["properties"]
        new_props = {}

        if "uid" in original_props:
            new_props["uid"] = original_props["uid"]

        agri_year_totals = {}
        for key, val in original_props.items():
            try:
                date = datetime.strptime(key, "%Y-%m-%d")
                season_key = get_season_key(date)
                if not season_key:
                    continue

                agri_key = get_agri_year_key(season_key)
                if not agri_key:
                    continue

                agri_start = int(agri_key.split("-")[0])
                if not (start_year <= agri_start <= end_year):
                    continue

                season = season_key.split("_")[0]
                full_key = f"{season}_{agri_key}"
                agri_year_totals[full_key] = agri_year_totals.get(full_key, 0) + float(
                    val
                )
            except Exception:
                continue

        for agri_key, total in agri_year_totals.items():
            new_props[f"precipitation_{agri_key}"] = total

        geom_ee = ee.Geometry(feature["geometry"])
        feature_ee = ee.Feature(geom_ee, new_props)
        features_ee.append(feature_ee)

    # --- Create precipitation FeatureCollection ---
    precip_fc = ee.FeatureCollection(features_ee)

    # --- Merge drought properties into precipitation using UID ---
    uid_filter = ee.Filter.equals(leftField="uid", rightField="uid")
    joined = ee.Join.saveFirst("drought_data").apply(precip_fc, drought_fc, uid_filter)

    def merge_drought_props(f):
        f = ee.Feature(f)
        drought_match = ee.Feature(f.get("drought_data"))

        # Merge drought properties
        merged = f.copyProperties(drought_match, exclude=["system:index"])

        # Get all property names except the join key (server-side safe)
        prop_names = ee.List(merged.propertyNames()).remove("drought_data")

        # Build a clean feature manually (copying all properties but using same geometry)
        return ee.Feature(merged.geometry(), merged.toDictionary(prop_names))

    merged_fc = joined.map(merge_drought_props)
    workspace_name = "waterrej"
    print(layer_name)
    # --- Push to GeoServer (or save) ---

    if proj_id:
        update_dashboard_geojson(
            proj_id=proj_obj.id, layer_name=layer_name, workspace_name=workspace_name
        )
        sync_project_fc_to_geoserver(merged_fc, proj_obj.name, layer_name, "waterrrej")

    else:
        update_dashboard_geojson(state, district, block, layer_name, workspace_name)
        sync_fc_to_geoserver(merged_fc, state, layer_name, "waterrej")

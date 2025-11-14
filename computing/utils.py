import os

import geopandas as gpd
import fiona
import copy

from computing.models import Layer, Dataset
from geoadmin.models import (
    TehsilSOI,
    DistrictSOI,
    StateSOI,
    State_Disritct_Block_Properties,
)
from projects.models import Project
from utilities.gee_utils import (
    ee_initialize,
    sync_vector_to_gcs,
    check_task_status,
    get_geojson_from_gcs,
    is_gee_asset_exists,
    valid_gee_text,
    get_gee_asset_path,
    get_gee_dir_path,
    is_asset_public,
)
from utilities.geoserver_utils import Geoserver
import shutil
from utilities.constants import (
    ADMIN_BOUNDARY_OUTPUT_DIR,
    SHAPEFILE_DIR,
    GEE_HELPER_PATH,
    GEE_ASSET_PATH,
    GEE_PATHS,
)
import ee
import json
from shapely.geometry import shape
from shapely.validation import explain_validity
import zipfile
from datetime import datetime, timedelta


def generate_shape_files(path):
    gdf = gpd.read_file(path + ".json")
    os.remove(path + ".json")

    gdf.to_file(
        path,
        driver="ESRI Shapefile",
    )
    return path


def convert_to_zip(dir_name, file_type):
    if file_type == "gpkg":
        with zipfile.ZipFile(dir_name + ".zip", "w", zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(dir_name + ".gpkg", arcname=os.path.basename(dir_name + ".gpkg"))
        return dir_name + ".zip"
    else:
        return shutil.make_archive(dir_name, "zip", dir_name + "/")


def push_shape_to_geoserver(path, store_name=None, workspace=None, file_type="shp"):
    geo = Geoserver()

    zip_path = convert_to_zip(path, file_type)

    response = geo.create_shp_datastore(
        path=zip_path,
        store_name=store_name,
        workspace=workspace,
        file_extension=file_type,
    )
    # if response["status_code"] in [200, 201, 202]:
    #     os.remove(zip_path)
    #     shutil.rmtree(shape_path_dir)
    return response["response_text"]


def kml_to_geojson(state_name, district_name, block_name, kml_path):
    fiona.drvsupport.supported_drivers["kml"] = (
        "rw"  # enable KML support which is disabled by default
    )
    fiona.drvsupport.supported_drivers["KML"] = (
        "rw"  # enable KML support which is disabled by default
    )
    gdf = gpd.read_file(kml_path)
    geometry_types = gdf.geometry.geometry.type.unique()
    state_dir = os.path.join(ADMIN_BOUNDARY_OUTPUT_DIR, state_name)

    for gtype in geometry_types:
        df = gdf.loc[gdf.geometry.geometry.type == gtype]
        path = os.path.join(state_dir, f"{district_name}_{block_name}_{gtype}")
        df.to_file(path + ".json", driver="GeoJSON")
        generate_shape_files(path)
        push_shape_to_geoserver(path, workspace="test_workspace")


def convert_kml_to_shapefile(kml_path, output_dir, shapefile_name):
    if not os.path.exists(output_dir + "/" + shapefile_name):
        os.makedirs(output_dir + "/" + shapefile_name)

    shapefile_path = os.path.join(
        output_dir + "/" + shapefile_name, shapefile_name + ".shp"
    )
    print("path path", shapefile_path)
    cmd = f"ogr2ogr -f 'ESRI Shapefile' {shapefile_path} {kml_path}"  # output.shp input.kml
    os.system(command=cmd)

    return output_dir + "/" + shapefile_name


def kml_to_shp(state_name, district_name, block_name, kml_path):
    shapefile_name = f"{district_name}_{block_name}"
    shapefile_layer_path = convert_kml_to_shapefile(
        kml_path, SHAPEFILE_DIR, shapefile_name
    )

    push_shape_to_geoserver(shapefile_layer_path, workspace="customkml")

    # os.remove(kml_path)
    # shutil.rmtree(shapefile_layer_path)
    os.remove(shapefile_layer_path + ".zip")


def sync_layer_to_geoserver(state_name, fc, layer_name, workspace):
    state_dir = os.path.join("data/fc_to_shape", state_name)
    if not os.path.exists(state_dir):
        os.mkdir(state_dir)
    path = os.path.join(state_dir, f"{layer_name}")
    # Write the feature collection into json file
    with open(path + ".json", "w") as f:
        try:
            f.write(f"{json.dumps(fc)}")
        except Exception as e:
            print(e)

    path = generate_shape_files(path)
    return push_shape_to_geoserver(path, workspace=workspace)


def sync_fc_to_geoserver(fc, state_name, layer_name, workspace):
    try:
        geojson_fc = fc.getInfo()
    except Exception as e:
        print("Exception in getInfo()", e)
        task_id = sync_vector_to_gcs(fc, layer_name, "GeoJSON")
        check_task_status([task_id])

        geojson_fc = get_geojson_from_gcs(layer_name)

    if len(geojson_fc["features"]) > 0:
        state_dir = os.path.join("data/fc_to_shape", state_name)
        if not os.path.exists(state_dir):
            os.mkdir(state_dir)
        path = os.path.join(state_dir, f"{layer_name}")

        # Convert to GeoDataFrame
        gdf = gpd.GeoDataFrame.from_features(geojson_fc["features"])

        # Set CRS (Earth Engine uses EPSG:4326 by default)
        gdf.crs = "EPSG:4326"

        gdf = fix_invalid_geometry_in_gdf(gdf)

        # Save as GeoPackage
        gdf.to_file(path + ".gpkg", driver="GPKG")

        return push_shape_to_geoserver(path, workspace=workspace, file_type="gpkg")
        # new_fc = {"features": geojson_fc["features"], "type": geojson_fc["type"]}
        #
        # state_dir = os.path.join("data/fc_to_shape", state_name)
        # if not os.path.exists(state_dir):
        #     os.mkdir(state_dir)
        # path = os.path.join(state_dir, f"{layer_name}")
        # # Write the feature collection into json file
        # with open(path + ".json", "w") as f:
        #     try:
        #         f.write(f"{json.dumps(new_fc)}")
        #     except Exception as e:
        #         print(e)
        #
        # path = generate_shape_files(path)
        # return push_shape_to_geoserver(path, workspace=workspace)


def sync_project_fc_to_geoserver(fc, project_name, layer_name, workspace):
    print("inside")
    try:
        geojson_fc = fc.getInfo()
    except Exception as e:
        print("Exception in getInfo()", e)
        task_id = sync_vector_to_gcs(fc, layer_name, "GeoJSON")
        check_task_status([task_id])

        geojson_fc = get_geojson_from_gcs(layer_name)

    if len(geojson_fc["features"]) > 0:
        state_dir = os.path.join("data/fc_to_shape", project_name)
        if not os.path.exists(state_dir):
            os.mkdir(state_dir)
        path = os.path.join(state_dir, f"{layer_name}")

        # Convert to GeoDataFrame
        gdf = gpd.GeoDataFrame.from_features(geojson_fc["features"])

        # Set CRS (Earth Engine uses EPSG:4326 by default)
        gdf.crs = "EPSG:4326"

        gdf = fix_invalid_geometry_in_gdf(gdf)

        # Save as GeoPackage
        gdf.to_file(path + ".gpkg", driver="GPKG")
        print("pushed to geoserver")
        return push_shape_to_geoserver(path, workspace=workspace, file_type="gpkg")
    else:
        print("no features found")
        return


def to_camelcase(text):
    words = text.split()
    camelcase = words[0].lower()
    for word in words[1:]:
        camelcase += word.capitalize()
    return camelcase


def create_chunk(aoi, description, chunk_size):
    size = aoi.size().getInfo()
    parts = size // chunk_size
    # task_ids = []
    rois = []
    descs = []
    for part in range(parts + 1):
        start = part * chunk_size
        end = start + chunk_size
        block_name_for_parts = description + "_" + str(start) + "-" + str(end)
        roi = ee.FeatureCollection(aoi.toList(aoi.size()).slice(start, end))
        if roi.size().getInfo() > 0:
            descs.append(block_name_for_parts)
            rois.append(roi)

    return rois, descs


def merge_chunks(
    aoi,
    folder_list,
    description,
    chunk_size,
    chunk_asset_path=GEE_HELPER_PATH,
    merge_asset_path=GEE_ASSET_PATH,
):
    print("Merge Chunk task initiated")
    ee_initialize()
    size = aoi.size().getInfo()
    parts = size // chunk_size
    assets = []
    for part in range(parts + 1):
        start = part * chunk_size
        end = start + chunk_size
        block_name_for_parts = description + "_" + str(start) + "-" + str(end)
        src_asset_id = (
            get_gee_dir_path(folder_list, chunk_asset_path) + block_name_for_parts
        )
        if is_gee_asset_exists(src_asset_id):
            assets.append(ee.FeatureCollection(src_asset_id))

    asset = ee.FeatureCollection(assets).flatten()

    asset_id = get_gee_dir_path(folder_list, merge_asset_path) + description
    try:
        # Export an ee.FeatureCollection as an Earth Engine asset.
        task = ee.batch.Export.table.toAsset(
            **{
                "collection": asset,
                "description": description,
                "assetId": asset_id,
            }
        )

        task.start()
        print("Successfully started the merge chunk", task.status())
        return task.status()["id"]
    except Exception as e:
        print(f"Error occurred in running merge task: {e}")


def fix_invalid_geometry_in_gdf(gdf):
    invalid = gdf[~gdf.is_valid]
    if not invalid.empty:
        print("Invalid geometries found:")
        for idx, geom in invalid.geometry.items():
            print(f"Index {idx}: {explain_validity(geom)}")
            gdf.loc[idx, "geometry"] = gdf.loc[idx, "geometry"].buffer(0)

    return gdf


def get_season_key(date):
    """Return season key like 'rabi_2017-2018' based on Indian cropping seasons."""
    month = date.month
    year = date.year
    next_year = year + 1

    if month in [1, 2]:
        return f"rabi_{year - 1}-{year}"  # Jan–Feb → Rabi of previous year
    elif month in [11, 12]:
        return f"rabi_{year}-{next_year}"  # Nov–Dec → Rabi starting this year
    elif month in [3, 4, 5, 6]:
        return f"zaid_{year}-{next_year}"
    elif month in [7, 8, 9, 10]:
        return f"kharif_{year}-{next_year}"
    else:
        return None


def get_agri_year_key(season_key):
    """Convert a season key to agricultural year key (e.g., rabi_2017-2018 → 2017-2018)."""
    season, years = season_key.split("_")
    start_year, end_year = map(int, years.split("-"))

    if season in ["kharif", "rabi"]:
        return f"{start_year}-{end_year}"
    elif season == "zaid":
        return f"{start_year - 1}-{start_year}"  # Zaid 2018-2019 → Agri year 2017-2018
    else:
        return None


def calculate_precipitation_season(
    geojson_filepath, draught_asset_id, proj_id, start_year=2017, end_year=2024
):
    proj_obj = Project.objects.get(pk=proj_id)
    # Load the GeoJSON file
    with open(geojson_filepath, "r") as f:
        feature_collection = json.load(f)

    features_ee = []

    for feature in feature_collection["features"]:
        original_props = feature["properties"]
        new_props = {}

        # Copy UID if available
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

                # Filter by agri year range
                agri_start = int(agri_key.split("-")[0])
                if not (start_year <= agri_start <= end_year):
                    continue

                season = season_key.split("_")[0]  # e.g., 'kharif'
                full_key = f"{season}_{agri_key}"  # e.g., '2017-2018_kharif'

                agri_year_totals[full_key] = agri_year_totals.get(full_key, 0) + float(
                    val
                )

            except Exception:
                continue  # Skip bad keys/values

        # Add precipitation totals per agri year and season
        for agri_key, total in agri_year_totals.items():
            new_props[f"precipitation_{agri_key}"] = total

        # Optional debug
        print(new_props)

        # Create Earth Engine Feature
        geom_ee = ee.Geometry(feature["geometry"])
        feature_ee = ee.Feature(geom_ee, new_props)
        features_ee.append(feature_ee)
    mws_fc = ee.FeatureCollection(features_ee)
    draught_fc = ee.FeatureCollection(draught_asset_id)

    # Define spatial filter (intersects)
    spatial_filter = ee.Filter.intersects(
        leftField=".geo",  # geometry field of left collection
        rightField=".geo",
        maxError=1,
    )

    # Define the join
    join = ee.Join.inner()

    # Apply the join
    joined = join.apply(mws_fc, draught_fc, spatial_filter)

    # Convert joined result into a proper FeatureCollection
    # by merging properties from both
    def merge_features(feature):
        left = ee.Feature(feature.get("primary"))
        right = ee.Feature(feature.get("secondary"))
        return left.copyProperties(right)

    final_fc = ee.FeatureCollection(joined.map(merge_features))
    layer_name = "WaterRejapp_mws_" + str(proj_obj.name) + "_" + str(proj_obj.id)
    sync_project_fc_to_geoserver(final_fc, proj_obj.name, layer_name, "waterrej")


def generate_geojson_with_ci_and_ndvi(zoi_asset, ci_asset, ndvi_asset, proj_id):
    # Load project
    proj_obj = Project.objects.get(pk=proj_id)

    # Build CI and NDVI asset paths
    asset_path_ci = (
        get_gee_dir_path(
            [proj_obj.name], asset_path=GEE_PATHS["WATER_REJ"]["GEE_ASSET_PATH"]
        )
        + ci_asset
    )

    asset_path_ndvi = (
        get_gee_dir_path(
            [proj_obj.name], asset_path=GEE_PATHS["WATER_REJ"]["GEE_ASSET_PATH"]
        )
        + ndvi_asset
    )

    # Load FeatureCollections
    zoi = ee.FeatureCollection(zoi_asset)
    ci = ee.FeatureCollection(asset_path_ci)
    ndvi = ee.FeatureCollection(asset_path_ndvi)

    # -------------------------
    # STEP 1: Join ZOI with Cropping Intensity
    # -------------------------
    join = ee.Join.inner()
    filter = ee.Filter.intersects(leftField=".geo", rightField=".geo")
    zoi_ci_joined = join.apply(zoi, ci, filter)

    def merge_zoi_ci(pair):
        zoi_feat = ee.Feature(pair.get("primary"))
        ci_feat = ee.Feature(pair.get("secondary"))
        merged_props = zoi_feat.toDictionary().combine(ci_feat.toDictionary(), True)
        return ee.Feature(zoi_feat.geometry(), merged_props)

    zoi_with_ci = ee.FeatureCollection(zoi_ci_joined.map(merge_zoi_ci))

    # -------------------------
    # STEP 2: Join ZOI+CI with NDVI
    # -------------------------
    zoi_ndvi_joined = join.apply(zoi_with_ci, ndvi, filter)

    def merge_zoi_ci_ndvi(pair):
        ci_feat = ee.Feature(pair.get("primary"))
        ndvi_feat = ee.Feature(pair.get("secondary"))
        merged_props = ci_feat.toDictionary().combine(ndvi_feat.toDictionary(), True)
        return ee.Feature(ci_feat.geometry(), merged_props)

    final_merged = ee.FeatureCollection(zoi_ndvi_joined.map(merge_zoi_ci_ndvi))

    # -------------------------
    # STEP 3: Export or Push to GeoServer
    # -------------------------
    layer_name = f"WaterRejapp_zoi_{proj_obj.name}_{proj_obj.id}"
    sync_project_fc_to_geoserver(final_merged, proj_obj.name, layer_name, "waterrej")


def get_directory_size(path):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            if os.path.isfile(file_path):
                total_size += os.path.getsize(file_path)
    return total_size


def generate_geojson_with_ci_ndvi_ndmi(
    zoi_asset, ci_asset, ndvi_asset, ndmi_asset, proj_id
):

    # Load project
    proj_obj = Project.objects.get(pk=proj_id)

    # Build asset paths
    asset_path_ci = (
        get_gee_dir_path([proj_obj.name], GEE_PATHS["WATER_REJ"]["GEE_ASSET_PATH"])
        + ci_asset
    )
    asset_path_ndvi = (
        get_gee_dir_path([proj_obj.name], GEE_PATHS["WATER_REJ"]["GEE_ASSET_PATH"])
        + ndvi_asset
    )
    # asset_path_ndmi = get_gee_dir_path([proj_obj.name], GEE_PATHS['WATER_REJ']["GEE_ASSET_PATH"]) + ndmi_asset

    # Load FeatureCollections

    zoi = ee.FeatureCollection(zoi_asset)
    print("Number of features zoi:", zoi.size().getInfo())

    ci = ee.FeatureCollection(asset_path_ci)
    print("Number of features zoi:", ci.size().getInfo())
    ndvi = ee.FeatureCollection(asset_path_ndvi)
    print("Number of features zoi:", ndvi.size().getInfo())
    ndmi = ee.FeatureCollection(ndmi_asset)
    print("Number of features zoi:", ndmi.size().getInfo())

    # -------------------------
    # STEP 1: Join ZOI with CI
    # -------------------------
    join = ee.Join.inner()
    filter = ee.Filter.intersects(leftField=".geo", rightField=".geo")
    zoi_ci_joined = join.apply(zoi, ci, filter)

    def merge_zoi_ci(pair):
        zoi_feat = ee.Feature(pair.get("primary"))
        ci_feat = ee.Feature(pair.get("secondary"))
        merged_props = zoi_feat.toDictionary().combine(ci_feat.toDictionary(), True)
        return ee.Feature(zoi_feat.geometry(), merged_props)  # ✅ keep ZOI geom

    zoi_with_ci = ee.FeatureCollection(zoi_ci_joined.map(merge_zoi_ci))

    # -------------------------
    # STEP 2: Join with NDVI
    # -------------------------
    zoi_ndvi_joined = join.apply(zoi_with_ci, ndvi, filter)

    def merge_zoi_ci_ndvi(pair):
        prev_feat = ee.Feature(pair.get("primary"))
        ndvi_feat = ee.Feature(pair.get("secondary"))
        merged_props = prev_feat.toDictionary().combine(ndvi_feat.toDictionary(), True)
        return ee.Feature(prev_feat.geometry(), merged_props)  # ✅ still ZOI geom

    zoi_ci_ndvi = ee.FeatureCollection(zoi_ndvi_joined.map(merge_zoi_ci_ndvi))

    # -------------------------
    # STEP 3: Join with NDMI
    # -------------------------
    zoi_ndmi_joined = join.apply(zoi_ci_ndvi, ndmi, filter)

    def merge_zoi_ci_ndvi_ndmi(pair):
        prev_feat = ee.Feature(pair.get("primary"))
        ndmi_feat = ee.Feature(pair.get("secondary"))
        merged_props = prev_feat.toDictionary().combine(ndmi_feat.toDictionary(), True)
        return ee.Feature(prev_feat.geometry(), merged_props)  # ✅ keep ZOI geom

    final_merged = ee.FeatureCollection(zoi_ndmi_joined.map(merge_zoi_ci_ndvi_ndmi))

    # -------------------------
    # STEP 4: Export or Push to GeoServer
    # -------------------------
    layer_name = f"WaterRejapp_zoi_{proj_obj.name}_{proj_obj.id}"
    print(layer_name)
    sync_project_fc_to_geoserver(final_merged, proj_obj.name, layer_name, "waterrej")


def generate_geojson_with_ci_ndvi(zoi_asset, ci_asset, ndvi_asset, proj_id):
    # Load project
    proj_obj = Project.objects.get(pk=proj_id)

    # Initialize Earth Engine
    ee_initialize(4)

    # Load FeatureCollections
    zoi = ee.FeatureCollection(zoi_asset)
    ci = ee.FeatureCollection(ci_asset)
    ndvi = ee.FeatureCollection(ndvi_asset)

    print("ZOI:", zoi.size().getInfo())
    print("CI:", ci.size().getInfo())
    print("NDVI:", ndvi.size().getInfo())

    # Common join logic on UID
    join = ee.Join.inner()
    uid_filter = ee.Filter.equals(leftField="UID", rightField="UID")

    # --- Join ZOI + CI ---
    zoi_ci_joined = join.apply(zoi, ci, uid_filter)

    def merge_zoi_ci(pair):
        zoi_feat = ee.Feature(pair.get("primary"))
        ci_feat = ee.Feature(pair.get("secondary"))
        merged_props = zoi_feat.toDictionary().combine(ci_feat.toDictionary(), True)
        # Keep ZOI geometry only
        return ee.Feature(zoi_feat.geometry(), merged_props)

    zoi_with_ci = ee.FeatureCollection(zoi_ci_joined.map(merge_zoi_ci))

    # --- Join with NDVI ---
    zoi_ndvi_joined = join.apply(zoi_with_ci, ndvi, uid_filter)

    def merge_with_ndvi(pair):
        base_feat = ee.Feature(pair.get("primary"))
        ndvi_feat = ee.Feature(pair.get("secondary"))
        merged_props = base_feat.toDictionary().combine(ndvi_feat.toDictionary(), True)
        # Always retain ZOI geometry
        return ee.Feature(base_feat.geometry(), merged_props)

    merged_final = ee.FeatureCollection(zoi_ndvi_joined.map(merge_with_ndvi))

    # --- Ensure ZOI geometry retained in all features ---
    merged_final = merged_final.map(
        lambda f: ee.Feature(
            f.setGeometry(
                ee.Feature(
                    zoi.filter(ee.Filter.eq("UID", f.get("UID"))).first()
                ).geometry()
            )
        )
    )

    layer_name = f"WaterRejapp_zoi_{proj_obj.name}_{proj_obj.id}"
    print(layer_name)

    sync_project_fc_to_geoserver(merged_final, proj_obj.name, layer_name, "waterrej")


def save_layer_info_to_db(
    state,
    district,
    block,
    layer_name,
    asset_id,
    dataset_name,
    sync_to_geoserver=False,
    layer_version="1.0",
    algorithm=None,
    algorithm_version="1.0",
    misc=None,
    is_override=False,
):
    print("inside the save_layer_info_to_db function")

    dataset = Dataset.objects.get(name=dataset_name)

    try:
        state_obj = StateSOI.objects.get(state_name__iexact=state)
        district_obj = DistrictSOI.objects.get(
            district_name__iexact=district, state=state_obj
        )
        block_obj = TehsilSOI.objects.get(
            tehsil_name__iexact=block, district=district_obj
        )
    except Exception as e:
        print("Error fetching in state district block:", e)
        return

    is_public = is_asset_public(asset_id)

    # Check if there’s an existing layer
    existing_layer = (
        Layer.objects.filter(
            dataset=dataset,
            layer_name=layer_name,
            state=state_obj,
            district=district_obj,
            block=block_obj,
        )
        .order_by("-layer_version")
        .first()
    )

    if existing_layer:
        if existing_layer.algorithm_version != algorithm_version:
            # Algorithm version changed --> create new record with incremented layer_version
            new_layer_version = str(float(existing_layer.layer_version) + 1)
            print(
                f"Algorithm version changed. Creating new layer version: {new_layer_version}"
            )
            layer_obj = Layer.objects.create(
                dataset=dataset,
                layer_name=layer_name,
                state=state_obj,
                district=district_obj,
                block=block_obj,
                layer_version=new_layer_version,
                algorithm=algorithm,
                algorithm_version=algorithm_version,
                is_sync_to_geoserver=sync_to_geoserver,
                is_public_gee_asset=is_public,
                is_override=is_override,
                misc=misc,
                gee_asset_path=asset_id,
            )
        else:
            # Algorithm version is same --> update existing layer
            print("Algorithm version same. Updating existing layer.")
            for field, value in {
                "algorithm": algorithm,
                "algorithm_version": algorithm_version,
                "is_sync_to_geoserver": sync_to_geoserver,
                "is_public_gee_asset": is_public,
                "is_override": is_override,
                "misc": misc,
                "gee_asset_path": asset_id,
            }.items():
                setattr(existing_layer, field, value)
            existing_layer.save()
            layer_obj = existing_layer
    else:
        # No existing record --> create a new one
        print("No existing layer found. Creating new one.")
        layer_obj = Layer.objects.create(
            dataset=dataset,
            layer_name=layer_name,
            state=state_obj,
            district=district_obj,
            block=block_obj,
            layer_version=layer_version,
            algorithm=algorithm,
            algorithm_version=algorithm_version,
            is_sync_to_geoserver=sync_to_geoserver,
            is_public_gee_asset=is_public,
            is_override=is_override,
            misc=misc,
            gee_asset_path=asset_id,
        )

    print(f"Saved layer info (id={layer_obj.id}, version={layer_obj.layer_version})")
    return layer_obj.id


def update_layer_sync_status(layer_id, sync_to_geoserver=True):
    try:
        updated_count = Layer.objects.filter(id=layer_id).update(
            is_sync_to_geoserver=sync_to_geoserver
        )

        if updated_count > 0:
            print(
                f"Updated sync status to {sync_to_geoserver} for layer ID: {layer_id}"
            )
            return True
        else:
            print(f"Layer with ID {layer_id} not found")
            return False

    except Exception as e:
        print(f"Error updating layer sync status: {e}")
        return False


def get_existing_end_year(dataset_name, layer_name):
    """fetch objects from db on the basis of dataset name and layer_name"""
    dataset = Dataset.objects.get(name=dataset_name)
    layer_obj = Layer.objects.get(dataset=dataset, layer_name=layer_name)
    existing_end_date = layer_obj.misc["end_year"]
    print("existing_end_date", existing_end_date)
    return existing_end_date


def get_layer_object(state, district, block, layer_name, dataset_name):
    state_obj = StateSOI.objects.get(state_name__iexact=state)
    district_obj = DistrictSOI.objects.get(
        district_name__iexact=district, state=state_obj
    )
    block_obj = TehsilSOI.objects.get(tehsil_name__iexact=block, district=district_obj)
    layer_obj = Layer.objects.get(
        state=state_obj,
        district=district_obj,
        block=block_obj,
        layer_name=layer_name,
        dataset__name=dataset_name,
    )
    return layer_obj


def update_dashboard_geojson(state, district, block, layer_name, workspace_name):
    print(f"🔄 Updating GeoJSON for {state}, {district}, {block}")

    # Get related objects
    state_obj = StateSOI.objects.get(state_name=state)
    district_obj = DistrictSOI.objects.get(district_name=district)
    tehsil_obj = TehsilSOI.objects.get(tehsil_name=block)  # fixed typo

    # Get or create main record
    obj, created = State_Disritct_Block_Properties.objects.get_or_create(
        state=state_obj, district=district_obj, tehsil=tehsil_obj
    )

    # Map suffix to json_key
    suffix_to_key = {
        "wb": "wb_geojson",
        "zoi": "zoi_geojson",
        "mws": "mws_geojson",
    }

    # Detect which key this layer corresponds to
    json_key = None
    for suffix, key in suffix_to_key.items():
        if layer_name == f"{state}_{district}_{block}_{suffix}":
            json_key = key
            break

    if not json_key:
        print(f"⚠️ Layer name {layer_name} did not match any known type.")
        return

    # Construct GeoServer URL
    waterrej_url = (
        f"https://geoserver.core-stack.org:8443/geoserver/waterrej/ows?"
        f"service=WFS&version=1.0.0&request=GetFeature&typeName={workspace_name}:{layer_name}"
        f"&outputFormat=application%2Fjson"
    )

    # Load existing dashboard_geojson or create new
    misc = obj.dashboard_geojson or {}

    # Ensure waterrej section exists
    if "waterrej" not in misc:
        misc["waterrej"] = {}

    # Update or add this specific json_key
    misc["waterrej"][json_key] = waterrej_url

    # Save the updated JSON field
    obj.dashboard_geojson = misc
    obj.save()

    print(f"✅ Added/Updated {json_key} for {state}, {district}, {block}")

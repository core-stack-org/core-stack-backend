import csv
import os
import sys
import time
from celery import shared_task


from computing.lulc.lulc_v3 import clip_lulc_v3
from computing.misc.catchment_area import (
    generate_catchment_area_singleflow,
)
from computing.misc.stream_order import generate_stream_order
from computing.mws.precipitation import precipitation
from computing.terrain_descriptor.terrain_raster_fabdem import (
    generate_terrain_raster_clip,
)
from computing.utils import (
    sync_project_fc_to_geoserver,
    calculate_precipitation_season,
    sync_fc_to_geoserver,
)
from computing.water_rejuvenation.water_rejuventation import (
    find_watersheds_for_point_with_buffer,
)
from computing.zoi_layers.zoi import generate_zoi
from projects.models import Project

from utilities.constants import SITE_DATA_PATH, GEE_PATHS
from utilities.gee_utils import (
    ee_initialize,
    gdf_to_ee_fc,
    get_gee_dir_path,
    make_asset_public,
    is_gee_asset_exists,
    valid_gee_text,
)
import ee
import logging
from datetime import datetime
import geemap


from waterrejuvenation.utils import (
    wait_for_task_completion,
    delete_asset_on_GEE,
    find_nearest_water_pixel,
)
from computing.surface_water_bodies.swb import generate_swb_layer

from shapely.geometry import Point
import geopandas as gpd
from computing.drought.drought import calculate_drought
from computing.misc.drainage_lines import clip_drainage_lines

# logger object for writing logs to file
logger = logging.getLogger(__name__)

# task to take file obj and process all desilting points shared
import math
import pandas as pd


def is_nan(value):
    return (
        value is None
        or (isinstance(value, float) and math.isnan(value))
        or pd.isna(value)
    )


@shared_task
def Upload_Desilting_Points(
    file_obj_id=None,
    is_closest_wp=True,
    is_lulc_required=True,
    gee_account_id=None,
    is_processing_required=True,
    is_force_regeneration=True,
):
    import pandas as pd
    import requests
    from django.conf import settings as django_settings
    from .models import WaterbodiesFileUploadLog, WaterbodiesDesiltingLog

    def normalize_str(val, default="unknown"):
        """Normalize Excel string-ish fields.

        - Blank/NaN -> "unknown"
        - Non-blank -> trimmed string
        """
        if pd.isna(val):
            return default
        if isinstance(val, str):
            val = val.strip()
            return val if val else default
        return str(val) if val is not None else default

    def normalize_float(val):
        """Normalize Excel numeric fields (lat/lon). Blank/NaN -> None."""
        if pd.isna(val):
            return None
        if isinstance(val, str) and val.strip() == "":
            return None
        try:
            return float(val)
        except (TypeError, ValueError):
            return None

    def parse_admin_details_by_latlon(lat, lon):
        """Call GeoServer API to fetch State/District/Tehsil for a lat/lon."""
        # Cache to avoid repeated calls for identical coordinates.
        cache_key = (round(lat, 4), round(lon, 4))
        if cache_key in admin_cache:
            return admin_cache[cache_key]

        api_key = os.getenv(
            "ADMIN_DETAILS_BY_LATLON_API_KEY",
            os.getenv(
                "GET_ADMIN_DETAILS_BY_LATLON_API_KEY",
                os.getenv("GEO_SERVER_PUBLIC_API_KEY") or "",
            ),
        )
        if not api_key:
            admin_cache[cache_key] = {"State": "unknown", "District": "unknown", "Tehsil": "unknown"}
            return admin_cache[cache_key]

        url = f"{django_settings.BASE_URL}api/v1/get_admin_details_by_latlon/"
        headers = {"X-API-Key": api_key}
        params = {"latitude": lat, "longitude": lon}
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=30)
            if resp.status_code != 200:
                admin_cache[cache_key] = {"State": "unknown", "District": "unknown", "Tehsil": "unknown"}
                return admin_cache[cache_key]

            data = resp.json() if resp.content else {}
            result = {
                "State": data.get("State") or "unknown",
                "District": data.get("District") or "unknown",
                "Tehsil": data.get("Tehsil") or "unknown",
            }
            admin_cache[cache_key] = result
            return result
        except Exception as e:
            logger.warning(f"Admin lookup failed for ({lat}, {lon}): {e}")
            admin_cache[cache_key] = {
                "State": "unknown",
                "District": "unknown",
                "Tehsil": "unknown",
            }
            return admin_cache[cache_key]

    ee_initialize(gee_account_id)

    wb_obj = WaterbodiesFileUploadLog.objects.get(pk=file_obj_id)
    proj_obj = Project.objects.get(pk=wb_obj.project_id)
    if is_force_regeneration:
        wdsl_log = WaterbodiesDesiltingLog.objects.filter(project_id=wb_obj.project_id)
        wdsl_log.delete()

    if wb_obj.process:
        logger.warning("File already processed. Skipping.")
        return

    df = pd.read_excel(wb_obj.file)
    merged_features = []
    admin_cache = {}

    for index, row in df.iterrows():
        print(row)
        # -----------------------------
        # Create DB row FIRST (lossless)
        # -----------------------------
        dsilting_obj_log = WaterbodiesDesiltingLog.objects.create(
            # For user-facing text fields: blank -> "unknown"
            name_of_ngo=normalize_str(row.get("Name of NGO")),
            State=normalize_str(row.get("State")),
            District=normalize_str(row.get("District")),
            Taluka=normalize_str(row.get("Taluka")),
            Village=normalize_str(row.get("Village")),
            waterbody_name=normalize_str(row.get("Name of the waterbody ")),
            # For numeric fields: blank -> None (so we can validate/skip)
            lat=normalize_float(row.get("Latitude")),
            lon=normalize_float(row.get("Longitude")),
            slit_excavated=normalize_str(row.get("Silt Excavated as per App")),
            intervention_year=normalize_str(row.get("Intervention_year")),
            excel_hash=wb_obj.excel_hash,
            project=proj_obj,
            process=False,
        )

        # -----------------------------
        # Validate lat / lon
        # -----------------------------
        if dsilting_obj_log.lat is None or dsilting_obj_log.lon is None:
            print("inside none conditom")
            dsilting_obj_log.failure_reason = "Latitude or Longitude missing"
            dsilting_obj_log.save(update_fields=["failure_reason"])
            continue

        # -----------------------------
        # Find nearest water pixel
        # -----------------------------
        if is_closest_wp:
            print("inside closest wp")
            try:
                result_dict = find_nearest_water_pixel(
                    dsilting_obj_log.lat, dsilting_obj_log.lon, 1500
                )
                print(result_dict)
            except Exception as e:
                print(e)
                dsilting_obj_log.failure_reason = f"GEE error: {str(e)}"
                dsilting_obj_log.save(update_fields=["failure_reason"])
                continue
        else:
            result_dict = {
                "success": True,
                "latitude": dsilting_obj_log.lat,
                "longitude": dsilting_obj_log.lon,
                "distance_m": 0,
            }

        if not result_dict.get("success"):
            dsilting_obj_log.failure_reason = "No water pixel found within 1500m"
            dsilting_obj_log.save(update_fields=["failure_reason"])
            continue

        closest_lat = result_dict.get("latitude")
        print(f"------{closest_lat}----------")
        closest_lon = result_dict.get("longitude")
        print(f"------{closest_lon}----------")
        distance = result_dict.get("distance_m")
        print(f"------{distance}----------")

        if closest_lat is None or closest_lon is None:
            dsilting_obj_log.failure_reason = "Closest water pixel invalid"
            dsilting_obj_log.save(update_fields=["failure_reason"])
            continue

        # -----------------------------
        # SUCCESS CASE
        # -----------------------------
        dsilting_obj_log.closest_wb_lat = closest_lat
        dsilting_obj_log.closest_wb_long = closest_lon
        dsilting_obj_log.distance_closest_wb_pixel = distance
        dsilting_obj_log.process = True
        dsilting_obj_log.failure_reason = None

        # If admin fields were blank in Excel, fill using GeoServer API.
        # We use the nearest water pixel coords to ensure consistency.
        if (
            dsilting_obj_log.State == "unknown"
            or dsilting_obj_log.District == "unknown"
            or dsilting_obj_log.Taluka == "unknown"
        ):
            admin = parse_admin_details_by_latlon(closest_lat, closest_lon)
            dsilting_obj_log.State = admin.get("State") or "unknown"
            dsilting_obj_log.District = admin.get("District") or "unknown"
            # API returns "Tehsil"; DB column is "Taluka"
            dsilting_obj_log.Taluka = admin.get("Tehsil") or "unknown"

        dsilting_obj_log.save()

        try:
            watershed_fc, buffer = find_watersheds_for_point_with_buffer(
                closest_lat, closest_lon
            )
            print("---------")
            print(watershed_fc, buffer)
            merged_features.append(watershed_fc)
        except Exception as e:
            logger.info(f"Watershed failure for row {index}: {e}")

    # -----------------------------
    # Post processing (LULC)
    # -----------------------------
    print(merged_features)

    intersecting_mws_asset = (
        ee.FeatureCollection(merged_features).flatten().distinct("uid")
    )

    if is_processing_required:
        Generate_lulc_mws(
            intersecting_mws_asset=intersecting_mws_asset,
            is_lulc_required=is_lulc_required,
            gee_account_id=gee_account_id,
            proj_id=proj_obj.id,
        )

    wb_obj.process = True
    wb_obj.save(update_fields=["process"])


def Generate_lulc_mws(
    intersecting_mws_asset=None,
    is_lulc_required=True,
    gee_account_id=None,
    proj_id=None,
):
    proj_obj = Project.objects.get(pk=proj_id)
    asset_suffix = f"{proj_obj.name}_{proj_obj.id}".lower()
    asset_folder = [proj_obj.name.lower()]
    description = "mws_" + asset_suffix
    mws_asset_id = (
        get_gee_dir_path(
            asset_folder, asset_path=GEE_PATHS["WATERBODY"]["GEE_ASSET_PATH"]
        )
        + description
    )
    filter_mws_task = ee.batch.Export.table.toAsset(
        collection=intersecting_mws_asset,
        description="water_rej_app_mws_tasks",
        assetId=mws_asset_id,
    )
    try:
        filter_mws_task.start()
        logger.info("MWS task started for given lat long")
        wait_for_task_completion(filter_mws_task)
        logger.info("MWS task completed")

        logger.info(f"is lulc required: {is_lulc_required}")
        make_asset_public(mws_asset_id)
        if is_lulc_required:
            clip_lulc_v3(
                start_year=2017,
                end_year=2024,
                gee_account_id=gee_account_id,
                roi_path=mws_asset_id,
                asset_folder=asset_folder,
                asset_suffix=f"{proj_obj.name}_{proj_obj.id}".lower(),
                app_type="WATERBODY",
                force_regenerate=True,
            )
            logger.info("luc Task finished for lulc")
    except Exception as e:
        logger.error(f"Error in Generating Lulc and mws layer: {str(e)}")
    Generate_water_balance_indicator(
        mws_asset_id, proj_id=proj_obj.id, gee_account_id=gee_account_id
    )
    asset_suffix_swb3 = f"swb3_{proj_obj.name}+{proj_obj.id}"
    asset_id_swb = (
        get_gee_dir_path(
            [proj_obj.name], asset_path=GEE_PATHS["WATERBODY"]["GEE_ASSET_PATH"]
        )
        + asset_suffix_swb3
    )
    BuildMWSLayer(
        gee_account_id=gee_account_id, proj_id=proj_obj.id, app_type="WATERBODY"
    )
    asset_suffix_wb = f"waterbodies_{asset_suffix}".lower()
    asset_id_wb = (
        get_gee_dir_path(
            asset_folder, asset_path=GEE_PATHS["WATERBODY"]["GEE_ASSET_PATH"]
        )
        + asset_suffix_wb
    )
    Genereate_zoi_and_zoi_indicator(
        roi=asset_id_wb,
        proj_id=proj_obj.id,
        gee_project_id=gee_account_id,
        asset_suffix=asset_suffix,
        asset_folder=asset_folder,
        app_type="WATERBODY",
    )


@shared_task()
def Generate_water_balance_indicator(mws_asset_id, proj_id, gee_account_id=None):

    print(f"project id {gee_account_id}")
    proj_obj = Project.objects.get(pk=proj_id)
    logger.info("Generating SWB layer for given lat long")
    asset_folder = [str(proj_obj.name).lower()]
    asset_suffix = f"{proj_obj.name}_{proj_obj.id}".lower()
    clip_drainage_lines(
        roi_path=mws_asset_id,
        asset_suffix=asset_suffix,
        asset_folder=asset_folder,
        gee_account_id=gee_account_id,
        proj_id=proj_obj.id,
        app_type="WATERDBOY",
    )

    generate_catchment_area_singleflow(
        roi_path=mws_asset_id,
        asset_suffix=asset_suffix,
        asset_folder=asset_folder,
        gee_account_id=gee_account_id,
        proj_id=proj_obj.id,
        app_type="WATERBODY",
    )

    generate_stream_order(
        roi_path=mws_asset_id,
        asset_suffix=asset_suffix,
        asset_folder=asset_folder,
        gee_account_id=gee_account_id,
        proj_id=proj_obj.id,
        app_type="WATERBODY",
    )
    asset_id_swb1 = (
        get_gee_dir_path(
            asset_folder, asset_path=GEE_PATHS["WATERBODY"]["GEE_ASSET_PATH"]
        )
        + f"swb1_{asset_suffix}"
    )
    asset_id_swb2 = (
        get_gee_dir_path(
            asset_folder, asset_path=GEE_PATHS["WATERBODY"]["GEE_ASSET_PATH"]
        )
        + f"swb2_{asset_suffix}"
    )

    delete_asset_on_GEE(asset_id_swb1)
    delete_asset_on_GEE(asset_id_swb2)
    generate_swb_layer(
        roi_path=mws_asset_id,
        asset_suffix=asset_suffix,
        asset_folder_list=asset_folder,
        app_type="WATERBODY",
        start_year="2017",
        end_year="2024",
        is_all_classes=True,
        gee_account_id=gee_account_id,
    )

    logger.info("SWB layer Generation successfull")
    make_asset_public(asset_id_swb2)
    asset_suffix_prec = (
        f"precipitation_forthnight_{proj_obj.name}_{proj_obj.id}".lower()
    )

    asset_id_prec = (
        get_gee_dir_path(
            asset_folder, asset_path=GEE_PATHS["WATERBODY"]["GEE_ASSET_PATH"]
        )
        + asset_suffix_prec
    )
    roi = ee.FeatureCollection(mws_asset_id)
    sys.setrecursionlimit(6000)
    precipitation(
        roi=roi,
        asset_suffix=asset_suffix,
        asset_folder_list=asset_folder,
        app_type="WATERBODY",
        start_date="2017-06-30",
        end_date="2025-07-1",
        is_annual=False,
    )
    make_asset_public(asset_id_prec)

    result_d = calculate_drought(
        roi_path=mws_asset_id,
        asset_suffix=asset_suffix,
        asset_folder_list=asset_folder,
        app_type="WATERBODY",
        start_year=2017,
        end_year=2024,
        gee_account_id=gee_account_id,
        state=proj_obj.state_soi.state_name,
    )
    dst_filename = "drought_" + asset_suffix + "_" + str(2017) + "_" + str(2022)
    draught_asset_id = (
        get_gee_dir_path(
            asset_folder, asset_path=GEE_PATHS["WATERBODY"]["GEE_ASSET_PATH"]
        )
        + dst_filename
    )

    BuildDesiltingLayer(proj_obj.id, gee_account_id)
    BuildWaterBodyLayer(
        proj_id=proj_obj.id,
        app_type="WATERBODY",
        gee_account_id=gee_account_id,
        asset_suffix=asset_suffix,
        asset_folder=asset_folder,
    )

    generate_terrain_raster_clip(
        asset_suffix=asset_suffix,
        asset_folder=[proj_obj.name],
        app_type="WATERBODY",
        roi=mws_asset_id,
        gee_account_id=gee_account_id,
    )


@shared_task()
def Genereate_zoi_and_zoi_indicator(
    state=None,
    district=None,
    block=None,
    proj_id=None,
    gee_project_id=None,
    app_type=None,
    asset_suffix=None,
    asset_folder=None,
    roi=None,
):
    print(f"roi: {roi}")
    ee_initialize(gee_project_id)
    if proj_id:
        proj_obj = Project.objects.get(pk=proj_id)
        asset_suffix = f"{proj_obj.name}_{proj_obj.id}".lower()
        asset_folder = [proj_obj.name.lower()]

    generate_zoi(
        state=None,
        district=None,
        block=None,
        roi=roi,
        asset_suffix=asset_suffix,
        asset_folder_list=asset_folder,
        app_type=app_type,
        gee_account_id=gee_project_id,
        proj_id=proj_id,
    )


@shared_task()
def BuildDesiltingLayer(
    project_id, asset_suffix=None, asset_folder=None, gee_account_id=None
):
    from waterrejuvenation.models import WaterbodiesDesiltingLog

    # ee_initialize(gee_account_id)  # Uncomment if needed

    instance = Project.objects.get(pk=project_id)
    data = WaterbodiesDesiltingLog.objects.filter(
        project_id=project_id, closest_wb_lat__isnull=False, process=True
    )

    # Asset paths
    asset_folder = [instance.name]
    asset_suffix_desilt = f"Desilt_layer_{instance.name}_{instance.id}".lower()
    asset_id_desilt = (
        get_gee_dir_path(
            asset_folder, asset_path=GEE_PATHS["WATERBODY"]["GEE_ASSET_PATH"]
        )
        + asset_suffix_desilt
    )

    delete_asset_on_GEE(asset_id_desilt)

    # File paths
    project_id = instance.id
    org_name = instance.organization.name
    app_type = instance.app_type
    project_name = instance.name
    filename = f"{org_name}_{app_type}_{project_id}_{project_name}_{int(datetime.now().timestamp())}.csv"
    directory = f"{org_name}/{app_type}/{project_id}_{project_name}"
    full_path = os.path.join(SITE_DATA_PATH, directory)
    file_path = os.path.join(full_path, filename)
    os.makedirs(full_path, exist_ok=True)

    # Write CSV
    csv_columns = [
        "desilt_id",
        "latitude",
        "longitude",
        "desiltingpoint_lat",
        "desiltingpoint_lon",
        "Village",
        "distance_from_desilting_point",
        "name_of_ngo",
        "State",
        "District",
        "Taluka",
        "waterbody_name",
        "slit_excavated",
        "intervention_year",
    ]

    with open(file_path, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(csv_columns)
        for loc in data:
            writer.writerow(
                [
                    loc.id,
                    loc.closest_wb_lat,
                    loc.closest_wb_long,
                    loc.lat,
                    loc.lon,
                    loc.Village,
                    loc.distance_closest_wb_pixel,
                    loc.name_of_ngo,
                    loc.State,
                    loc.District,
                    loc.Taluka,
                    loc.waterbody_name,
                    loc.slit_excavated,
                    loc.intervention_year,
                ]
            )

    # Read CSV into DataFrame
    df = pd.read_csv(file_path)

    # --- FIX: handle numeric vs string columns ---
    numeric_cols = [
        "latitude",
        "longitude",
        "desiltingpoint_lat",
        "desiltingpoint_lon",
        "distance_from_desilting_point",
        "slit_excavated",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(
            df[col], errors="coerce"
        )  # keeps numbers, NaN if invalid

    # Drop rows without geometry
    df = df.dropna(subset=["latitude", "longitude"])

    # Fill missing numeric values with 0 (optional)
    df[numeric_cols] = df[numeric_cols].fillna(0)

    # Fill missing string columns with "N/A"
    string_cols = [c for c in df.columns if c not in numeric_cols]
    df[string_cols] = df[string_cols].fillna("N/A").replace(r"^\s*$", "N/A", regex=True)

    # Create GeoDataFrame
    geometry = [Point(xy) for xy in zip(df["longitude"], df["latitude"])]
    gdf = gpd.GeoDataFrame(df, geometry=geometry)
    gdf.set_crs("EPSG:4326", inplace=True)

    # Convert to GEE FeatureCollection
    fc = gdf_to_ee_fc(gdf)

    # Delete previous asset if exists
    delete_asset_on_GEE(asset_id_desilt)

    # Export to GEE
    task = ee.batch.Export.table.toAsset(
        collection=fc, description=asset_suffix_desilt, assetId=asset_id_desilt
    )
    task.start()
    wait_for_task_completion(task)

    return {"status": "success", "asset_id": asset_id_desilt}


def BuildMWSLayer(
    gee_account_id=None,
    state=None,
    proj_id=None,
    app_type="MWS",
    block=None,
    district=None,
    drought_asset_override=None,  # optional: full path to drought asset if you want to override default
    export_year_range=(2017, 2022),  # for naming drought asset
):
    """
    Full BuildMWSLayer: builds final MWS waterbody FC, joins drought properties (flat, prefixed),
    exports merged FeatureCollection to a GEE asset, and syncs to GeoServer.

    Returns:
        dict: {
            "status": "SUCCESS" | "FAILED",
            "asset_id": asset_id_wb_mws (str),
            "export_task_id": <task id or None>,
            "feature_count": <int or None>,
            "message": <string>
        }
    """

    try:
        # initialize GEE
        ee_initialize(gee_account_id)

        # -------------------------
        # Build asset suffix & paths
        # -------------------------
        if proj_id:
            instance = Project.objects.get(pk=proj_id)
            asset_folder = [instance.name.lower()]
            asset_suffix = f"{instance.name}_{instance.id}".lower()
            mws_geojson_op = f"data/fc_to_shape/{instance.name}/{asset_suffix}"
        else:
            if not (state and district and block):
                raise ValueError(
                    "state, district and block required when proj_id is not provided"
                )
            asset_suffix = (
                valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
            )
            asset_folder = [state, district, block]
            mws_geojson_op = f"data/fc_to_shape/{state}/{asset_suffix}"

        # -------------------------
        # Load precipitation FC
        # -------------------------
        asset_suffix_prec = f"Prec_fortnight_{asset_suffix}"
        asset_id_prec = (
            get_gee_dir_path(
                asset_folder, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
            )
            + asset_suffix_prec
        )
        precip = ee.FeatureCollection(asset_id_prec)

        # If precip empty -> fail early
        if precip.size().getInfo() == 0:
            msg = f"Precipitation feature collection empty: {asset_id_prec}"
            logger.warning(msg)
            return {
                "status": "FAILED",
                "message": msg,
                "asset_id": None,
                "export_task_id": None,
                "feature_count": 0,
            }

        # convert to geodataframe for local processing (as in your flow)
        gdf = geemap.ee_to_gdf(precip)

        # -------------------------
        # Drought asset id (default naming)
        # -------------------------
        if drought_asset_override:
            draught_asset_id = drought_asset_override
        else:
            start_y, end_y = export_year_range
            dst_filename = f"drought_{asset_suffix}"
            draught_asset_id = (
                get_gee_dir_path(
                    asset_folder, asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"]
                )
                + dst_filename
            )

        # -------------------------
        # Save GDF to GeoJSON for custom processing
        # -------------------------
        # Ensure parent dir exists on disk (optional, your environment may handle)
        try:
            gdf.to_file(mws_geojson_op, driver="GeoJSON")
        except Exception as e:
            logger.exception("Failed to write GeoJSON to %s: %s", mws_geojson_op, e)
            return {
                "status": "FAILED",
                "message": f"Error writing GeoJSON: {e}",
                "asset_id": None,
                "export_task_id": None,
                "feature_count": None,
            }

        # -------------------------
        # Create final_fc using your domain function
        # -------------------------
        final_fc = calculate_precipitation_season(
            mws_geojson_op, draught_asset_id=draught_asset_id
        )
        final_fc = ee.FeatureCollection(final_fc)

        # quick check
        try:
            final_count = final_fc.size().getInfo()
        except Exception:
            final_count = None

        if final_count == 0:
            msg = "final_fc is empty after calculate_precipitation_season"
            logger.warning(msg)
            return {
                "status": "FAILED",
                "message": msg,
                "asset_id": None,
                "export_task_id": None,
                "feature_count": 0,
            }

        # ------------------------------------------------
        # JOIN AND FLATTEN DROUGHT PROPERTIES (prefixed)
        # ------------------------------------------------
        drought_fc = ee.FeatureCollection(draught_asset_id)

        # It's possible drought asset does not exist or is empty - handle gracefully
        try:
            drought_count = drought_fc.size().getInfo()
        except Exception:
            drought_count = 0

        if drought_count == 0:
            # No drought data - keep final_fc as-is (but ensure no non-exportable complex properties)
            logger.info(
                "Drought FC not found or empty (%s). Skipping join.", draught_asset_id
            )
            merged_fc = final_fc.map(
                lambda f: ee.Feature(f).select(ee.List(ee.Feature(f).propertyNames()))
            )  # ensure properties are primitives
        else:
            # Use saveFirst to avoid List<Feature> problem
            join = ee.Join.saveFirst("match")
            ffilter = ee.Filter.equals(leftField="uid", rightField="uid")
            joined = join.apply(
                primary=final_fc, secondary=drought_fc, condition=ffilter
            )

            def dedupe_by_uid(fc, uid_field="uid"):
                uids = fc.aggregate_array(uid_field).distinct()
                return ee.FeatureCollection(
                    uids.map(
                        lambda u: ee.Feature(
                            fc.filter(ee.Filter.eq(uid_field, u)).first()
                        )
                    )
                )

            # Map function to flatten the match's properties prefixed with 'drought_'
            def _flatten_match(feat):
                feat = ee.Feature(feat)

                # copy_props will only be executed if feat.get('match') is truthy (exists)
                def copy_props(_):
                    match = ee.Feature(
                        feat.get("match")
                    )  # safe because only called when match exists
                    match_props = match.propertyNames()

                    def _setter(prop, acc):
                        acc = ee.Feature(acc)
                        prop = ee.String(prop)
                        val = match.get(prop)
                        new_name = ee.String("drought_").cat(prop)
                        return acc.set(new_name, val)

                    merged = ee.Feature(match_props.iterate(_setter, feat))
                    # remove the temporary 'match' property so exports won't fail
                    merged = ee.Feature(merged).select(
                        ee.List(merged.propertyNames()).remove("match")
                    )
                    return merged

                # If no match, just remove 'match' (if present) and return original feature
                def remove_match(_):
                    return ee.Feature(feat).select(
                        ee.List(feat.propertyNames()).remove("match")
                    )

                # ee.Algorithms.If will evaluate the server-side truthiness of feat.get('match')
                result = ee.Algorithms.If(
                    feat.get("match"), copy_props(None), remove_match(None)
                )
                return ee.Feature(result)

            # Use it as before:
            merged_fc = ee.FeatureCollection(joined.map(_flatten_match))

        # -------------------------
        # Prepare export asset id (waterbodies)
        # -------------------------
        asset_suffix_wb = f"waterbodies_mws_{asset_suffix}".lower()
        asset_id_wb_mws = (
            get_gee_dir_path(
                asset_folder, asset_path=GEE_PATHS["WATERBODY"]["GEE_ASSET_PATH"]
            )
            + asset_suffix_wb
        )

        # -------------------------
        # Export merged FC to GEE asset
        # -------------------------
        # Earth Engine can reject very large table exports with:
        # "Request payload size exceeds the limit" (10MB payload limit).
        # Strategy:
        # 1) Try exporting the full table.
        # 2) If it fails with that specific payload-size error, retry using chunk exports.
        # IMPORTANT:
        # Calling merged_fc.size().getInfo() can itself fail with the same
        # "Request payload size exceeds the limit" error for very large tables.
        # We therefore reuse `final_count` computed earlier for the base table,
        # and if it's missing we will assume an upper bound only when retrying.
        merged_count = final_count

        # Aggressive fallback chunk size to avoid EE 10MB payload limit.
        # Slower, but safest for very heavy per-feature payloads.
        chunk_size = 1
        chunk_asset_ids = []

        def _export_table(collection, description, asset_id):
            """
            Start an EE table export and recover once from request_id collision.
            """
            try_descriptions = [description, f"{description}_{int(time.time())}"]
            last_err = None
            for idx, desc in enumerate(try_descriptions):
                try:
                    export_task = ee.batch.Export.table.toAsset(
                        collection=collection,
                        description=desc,
                        assetId=asset_id,
                    )
                    export_task.start()
                    return export_task
                except ee.EEException as err:
                    last_err = err
                    if (
                        "A different Operation was already started with the given request_id"
                        in str(err)
                        and idx < len(try_descriptions) - 1
                    ):
                        # Retry immediately with a unique description so EE creates
                        # a fresh operation/request context.
                        time.sleep(2)
                        continue
                    raise
            raise last_err

        export_task = None
        try:
            if is_gee_asset_exists(asset_id_wb_mws):
                delete_asset_on_GEE(asset_id_wb_mws)
            export_task = _export_table(merged_fc, asset_suffix_wb, asset_id_wb_mws)
            logger.info(
                "Started export task %s -> %s", export_task.id, asset_id_wb_mws
            )
            wait_for_task_completion(export_task)
            merged_fc = ee.FeatureCollection(asset_id_wb_mws)
        except ee.EEException as ee_err:
            msg = str(ee_err)
            if "Request payload size exceeds the limit" not in msg:
                raise

            # Retry with chunk exports
            logger.warning(
                "Export payload too large. Retrying with chunk exports (chunk_size=%s). Error=%s",
                chunk_size,
                msg,
            )
            if merged_count is None:
                # Fallback: pick a conservative upper bound to avoid calling
                # merged_fc.size().getInfo() (which triggers payload errors).
                assumed_max_features = chunk_size * 50  # 1000 features
                merged_count = assumed_max_features
                logger.warning(
                    "merged_count unknown; assuming max features=%s (chunk_size=%s, chunks=%s).",
                    merged_count,
                    chunk_size,
                    50,
                )

            # Export each chunk (reuse existing chunks if present).
            # Use local GDF chunking so each chunk builds a smaller EE graph.
            local_chunk_size = 50
            for start in range(0, len(gdf), local_chunk_size):
                end = min(start + local_chunk_size, len(gdf))
                chunk_asset_suffix = f"{asset_suffix_wb}_chunk_{start}-{end}"
                chunk_asset_id = (
                    get_gee_dir_path(
                        asset_folder,
                        asset_path=GEE_PATHS["WATERBODY"]["GEE_ASSET_PATH"],
                    )
                    + chunk_asset_suffix
                )

                if is_gee_asset_exists(chunk_asset_id):
                    chunk_asset_ids.append(chunk_asset_id)
                    continue

                chunk_geojson_op = f"{mws_geojson_op}_chunk_{start}_{end}.geojson"
                chunk_gdf = gdf.iloc[start:end].copy()
                chunk_gdf.to_file(chunk_geojson_op, driver="GeoJSON")

                primary_chunk_fc = ee.FeatureCollection(
                    calculate_precipitation_season(
                        chunk_geojson_op, draught_asset_id=draught_asset_id
                    )
                )

                if drought_count == 0:
                    chunk_fc = primary_chunk_fc.map(
                        lambda f: ee.Feature(f).select(
                            ee.List(ee.Feature(f).propertyNames())
                        )
                    )
                else:
                    join = ee.Join.saveFirst("match")
                    ffilter = ee.Filter.equals(leftField="uid", rightField="uid")
                    joined = join.apply(
                        primary=primary_chunk_fc,
                        secondary=drought_fc,
                        condition=ffilter,
                    )

                    def _flatten_match(feat):
                        feat = ee.Feature(feat)

                        def copy_props(_):
                            match = ee.Feature(feat.get("match"))
                            match_props = match.propertyNames()

                            def _setter(prop, acc):
                                acc = ee.Feature(acc)
                                prop = ee.String(prop)
                                val = match.get(prop)
                                new_name = ee.String("drought_").cat(prop)
                                return acc.set(new_name, val)

                            merged = ee.Feature(match_props.iterate(_setter, feat))
                            merged = ee.Feature(merged).select(
                                ee.List(merged.propertyNames()).remove("match")
                            )
                            return merged

                        def remove_match(_):
                            return ee.Feature(feat).select(
                                ee.List(feat.propertyNames()).remove("match")
                            )

                        result = ee.Algorithms.If(
                            feat.get("match"), copy_props(None), remove_match(None)
                        )
                        return ee.Feature(result)

                    chunk_fc = ee.FeatureCollection(joined.map(_flatten_match))

                delete_asset_on_GEE(chunk_asset_id)
                chunk_task = _export_table(chunk_fc, chunk_asset_suffix, chunk_asset_id)
                logger.info(
                    "Started chunk export %s -> %s",
                    chunk_task.id,
                    chunk_asset_id,
                )
                wait_for_task_completion(chunk_task)
                chunk_asset_ids.append(chunk_asset_id)

            # Merge chunk assets back into a single final asset.
            merged_fc_final = None
            for asset_id in chunk_asset_ids:
                part_fc = ee.FeatureCollection(asset_id)
                merged_fc_final = (
                    part_fc
                    if merged_fc_final is None
                    else merged_fc_final.merge(part_fc)
                )

            if is_gee_asset_exists(asset_id_wb_mws):
                delete_asset_on_GEE(asset_id_wb_mws)
            export_task = _export_table(
                merged_fc_final, asset_suffix_wb, asset_id_wb_mws
            )
            logger.info(
                "Started final chunk-merged export -> %s", asset_id_wb_mws
            )
            wait_for_task_completion(export_task)
            merged_fc = ee.FeatureCollection(asset_id_wb_mws)

        # Avoid calling .size().getInfo() here; it can fail with the same
        # payload-size limitations for very large tables.
        exported_count = None

        # -------------------------
        # Push to GeoServer
        # -------------------------
        layer_name = (
            asset_suffix_wb  # same as f"waterbodies_mws_{asset_suffix}".lower()
        )

        if proj_id:
            proj_obj = Project.objects.get(pk=proj_id)
            sync_project_fc_to_geoserver(merged_fc, proj_obj.name, layer_name, "mws")
        else:
            sync_fc_to_geoserver(merged_fc, state, layer_name, "mws")

        return {
            "status": "SUCCESS",
            "asset_id": asset_id_wb_mws,
            "export_task_id": (
                export_task.id if export_task and hasattr(export_task, "id") else None
            ),
            "feature_count": exported_count,
            "message": f"Exported and synced layer {layer_name}",
        }

    except ee.EEException as ee_err:
        logger.exception("EarthEngine error in BuildMWSLayer: %s", ee_err)
        return {
            "status": "FAILED",
            "message": f"EE error: {ee_err}",
            "asset_id": None,
            "export_task_id": None,
            "feature_count": None,
        }
    except Exception as e:
        logger.exception("Unexpected error in BuildMWSLayer: %s", e)
        return {
            "status": "FAILED",
            "message": str(e),
            "asset_id": None,
            "export_task_id": None,
            "feature_count": None,
        }


@shared_task()
def BuildWaterBodyLayer(
    gee_account_id=None,
    asset_folder=None,
    asset_suffix=None,
    app_type=None,
    proj_id=None,
):
    ee_initialize(gee_account_id)

    proj_obj = Project.objects.get(pk=proj_id)

    # ------------------------------------------------------------------
    # Waterbody polygons
    # ------------------------------------------------------------------
    wb_description = "swb3_" + asset_suffix
    waterbody_asset_id = (
        get_gee_dir_path(
            asset_folder,
            asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"],
        )
        + wb_description
    )
    waterbodies = ee.FeatureCollection(waterbody_asset_id)

    # ------------------------------------------------------------------
    # Desilting points
    # ------------------------------------------------------------------
    desilt_suffix = f"desilt_layer_{asset_suffix}".lower()
    desilt_asset_id = (
        get_gee_dir_path(
            asset_folder,
            asset_path=GEE_PATHS["WATERBODY"]["GEE_ASSET_PATH"],
        )
        + desilt_suffix
    )
    desilting_points = ee.FeatureCollection(desilt_asset_id)

    MAX_DISTANCE = 100  # meters

    # ------------------------------------------------------------------
    # Point → Polygon matching logic
    # ------------------------------------------------------------------
    def attach_polygon_to_point(point):
        pt_geom = point.geometry()

        intersecting = waterbodies.filterBounds(pt_geom)
        nearby = waterbodies.filterBounds(pt_geom.buffer(MAX_DISTANCE))

        intersect_size = intersecting.size()
        nearby_size = nearby.size()

        has_match = intersect_size.gt(0).Or(nearby_size.gt(0))

        matched_polygon = ee.Algorithms.If(
            intersect_size.gt(0),
            intersecting.first(),
            ee.Algorithms.If(nearby_size.gt(0), nearby.first(), None),
        )

        match_type = ee.Algorithms.If(
            intersect_size.gt(0),
            "intersect",
            ee.Algorithms.If(nearby_size.gt(0), "near", "none"),
        )

        return ee.Feature(
            ee.Algorithms.If(
                has_match,
                #  MATCH FOUND → duplicate polygon geometry
                ee.Feature(matched_polygon)
                .copyProperties(point)
                .set("matched", True)
                .set("match_type", match_type),
                #  NO MATCH → keep original POINT geometry
                ee.Feature(pt_geom)
                .copyProperties(point)
                .set("matched", False)
                .set("match_type", "none"),
            )
        )

    # ------------------------------------------------------------------
    # Apply matching
    # ------------------------------------------------------------------
    exploded = desilting_points.map(attach_polygon_to_point)

    matched_fc = exploded.filter(ee.Filter.eq("matched", True))
    unmatched_fc = exploded.filter(ee.Filter.eq("matched", False))

    # ------------------------------------------------------------------
    # EXPORT 1: MATCHED POLYGONS (GeoServer / GeoJSON)
    # ------------------------------------------------------------------
    matched_asset_suffix = f"waterbodies_{asset_suffix}".lower()
    matched_asset_id = (
        get_gee_dir_path(
            asset_folder,
            asset_path=GEE_PATHS["WATERBODY"]["GEE_ASSET_PATH"],
        )
        + matched_asset_suffix
    )

    delete_asset_on_GEE(matched_asset_id)

    export_matched = ee.batch.Export.table.toAsset(
        collection=matched_fc,
        description=f"water_rej_desilting_{proj_obj.id}",
        assetId=matched_asset_id,
    )

    export_matched.start()
    wait_for_task_completion(export_matched)

    # ------------------------------------------------------------------
    # EXPORT 2: UNMATCHED POINTS (INTERNAL – DB UPDATE ONLY)
    # ------------------------------------------------------------------
    unmatched_asset_suffix = f"desilt_unmatched_{asset_suffix}".lower()
    unmatched_asset_id = (
        get_gee_dir_path(
            asset_folder,
            asset_path=GEE_PATHS["WATERBODY"]["GEE_ASSET_PATH"],
        )
        + unmatched_asset_suffix
    )

    delete_asset_on_GEE(unmatched_asset_id)

    export_unmatched = ee.batch.Export.table.toAsset(
        collection=unmatched_fc,
        description=f"water_rej_desilting_unmatched_{proj_obj.id}",
        assetId=unmatched_asset_id,
    )

    export_unmatched.start()
    wait_for_task_completion(export_unmatched)

    # ------------------------------------------------------------------
    # Publish matched layer to GeoServer
    # ------------------------------------------------------------------
    layer_name = f"waterbodies_{proj_obj.name}_{proj_obj.id}".lower()
    sync_project_fc_to_geoserver(
        matched_fc,
        proj_obj.name,
        layer_name,
        "swb",
    )

    # ------------------------------------------------------------------
    # Update Django DB for unmatched points
    # ------------------------------------------------------------------
    from .models import WaterbodiesDesiltingLog

    try:
        unmatched_info = ee.FeatureCollection(unmatched_asset_id).getInfo()
        unmatched_ids = []
        for feature in unmatched_info["features"]:
            desilting_id = feature["properties"]["desilt_id"]
            if desilting_id:
                unmatched_ids.append(desilting_id)

        if unmatched_ids:
            WaterbodiesDesiltingLog.objects.filter(id__in=unmatched_ids).update(
                process=False, failure_reason="No waterbody found within 100m"
            )

    except Exception as e:
        print("No Umnatch point found")

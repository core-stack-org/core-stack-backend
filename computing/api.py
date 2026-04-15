import os
import requests
from nrm_app.settings import BASE_DIR, LOCAL_COMPUTE_API_URL
from rest_framework.decorators import api_view, parser_classes, schema
from rest_framework.response import Response
from rest_framework import status
from rest_framework.parsers import MultiPartParser, FormParser
import time
from computing.change_detection.change_detection_vector import (
    vectorise_change_detection,
)
from .lulc.lulc_vector import vectorise_lulc
from .lulc.river_basin_lulc.lulc_v2_river_basin import lulc_river_basin_v2
from .lulc.river_basin_lulc.lulc_v3_river_basin_using_v2 import lulc_river_basin_v3
from .lulc.tehsil_level.lulc_v2 import generate_lulc_v2_tehsil
from .lulc.tehsil_level.lulc_v3 import generate_lulc_v3_tehsil
from .lulc.v4.lulc_v4 import generate_lulc_v4
from .misc.ndvi_time_series import ndvi_timeseries
from .misc.restoration_opportunity import generate_restoration_opportunity
from .misc.stream_order import generate_stream_order
from .mws.generate_hydrology import generate_hydrology
from .utils import (
    Geoserver,
    kml_to_shp,
)
from utilities.gee_utils import download_gee_layer, check_gee_task_status
from django.core.files.storage import FileSystemStorage
from utilities.constants import KML_PATH
from .mws.mws import mws_layer
from .cropping_intensity.cropping_intensity import generate_cropping_intensity
from .surface_water_bodies.swb import generate_swb_layer
from .drought.drought import calculate_drought
from .terrain_descriptor.terrain_clusters import generate_terrain_clusters
from .terrain_descriptor.terrain_raster_fabdem import generate_terrain_raster_clip
from computing.misc.drainage_lines import clip_drainage_lines
from .lulc_X_terrain.lulc_on_slope_cluster import lulc_on_slope_cluster
from .lulc_X_terrain.lulc_on_plain_cluster import lulc_on_plain_cluster
from .clart.clart import generate_clart_layer
from .misc.admin_boundary import generate_tehsil_shape_file_data
from .misc.nrega import clip_nrega_district_block
from computing.change_detection.change_detection import get_change_detection
from .lulc.lulc_v3 import clip_lulc_v3
from .crop_grid.crop_grid import create_crop_grids
from .tree_health.ccd import tree_health_ccd_raster
from .tree_health.canopy_height import tree_health_ch_raster
from .tree_health.overall_change import tree_health_overall_change_raster
from .drought.drought_causality import drought_causality
from .tree_health.overall_change_vector import tree_health_overall_change_vector
from .tree_health.canopy_height_vector import tree_health_ch_vector
from .tree_health.ccd_vector import tree_health_ccd_vector
from .plantation.site_suitability import site_suitability
from .misc.aquifer_vector import generate_aquifer_vector
from .misc.soge_vector import generate_soge_vector
from .clart.fes_clart_to_geoserver import generate_fes_clart_layer
from .surface_water_bodies.merge_swb_ponds import merge_swb_ponds
from utilities.auth_check_decorator import api_security_check
from computing.layer_dependency.layer_generation_in_order import layer_generate_map
from .views import layer_status, get_layers_of_workspace
from .misc.lcw_conflict import generate_lcw_conflict_data
from .misc.agroecological_space import generate_agroecological_data
from .misc.factory_csr import generate_factory_csr_data
from .misc.green_credit import generate_green_credit_data
from .misc.mining_data import generate_mining_data
from .misc.slope_percentage import generate_slope_percentage_data
from .misc.naturaldepression import generate_natural_depression_data
from .misc.distancetonearestdrainage import generate_distance_to_nearest_drainage_line
from .misc.catchment_area import generate_catchment_area_singleflow
from .zoi_layers.zoi import generate_zoi
from .mws.mws_connectivity import generate_mws_connectivity_data
from .mws.mws_centroid import generate_mws_centroid_data
from .misc.facilities_proximity import generate_facilities_proximity_task

# Admin Boundary
@api_security_check(allowed_methods="POST")
@schema(None)
def generate_admin_boundary(request):
    print("Inside generate_block_layer API.")
    start_time = time.time()
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        gee_account_id = request.data.get("gee_account_id")
        execution_id = request.data.get("execution_id", "local")

        # SYNC — direct call, no Celery
        asset_id = generate_tehsil_shape_file_data(
            state=state,
            district=district,
            block=block,
            gee_account_id = gee_account_id
        )

        # TODO: Replace hardcoded stac_spec with build_stac_spec() call
        execution_time = time.time() - start_time
        return Response({
            "status": "success",
            "message": "Admin boundary completed",
            "execution_id": execution_id,
            "node_type": "Admin_Boundary",
            "asset_ids": [asset_id],
            "hosting_platform": "GEE",
            "stac_spec": {},
            "execution_time": execution_time,
        })
    except Exception as e:
        print("Exception in generate_block_layer api :: ", e)
        return Response(
            {"status": "failed", "message": str(e), "node_type": "Admin_Boundary"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
    
# NREGA
@api_security_check(allowed_methods="POST")
@schema(None)
def generate_nrega_layer(request):
    print("Inside generate_nrega_layer API.")
    start_time = time.time()
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        gee_account_id = request.data.get("gee_account_id")
        execution_id = request.data.get("execution_id")


        asset_id = clip_nrega_district_block(state, district, block, gee_account_id)
        execution_time = time.time() - start_time

        return Response(
            {
                "status": "success",
                "message": "NREGA Clip Completed",
                "execution_id": execution_id,
                "node_type": "NREGA_Clip",
                "asset_ids": [asset_id],
                "hosting_platform": "GEE",
                "stac_spec": {},
                "execution_time": execution_time,
            },
            status=status.HTTP_200_OK,
        )
    except Exception as e:
        print("Exception in generate_nrega_layer api :: ", e)
        return Response({"Exception": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# Drainage Lines
@api_view(["POST"])
@schema(None)
def generate_drainage_layer(request):
    print("Inside generate_drainage_layer API.")
    start_time = time.time()
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        gee_account_id = request.data.get("gee_account_id")
        execution_id = request.data.get("execution_id", "local")

        asset_id  = clip_drainage_lines(
                state = state,
                district = district,
                block = block,
                gee_account_id = gee_account_id,
        )
        execution_time = time.time() - start_time
        return Response({
            "status": "success",
            "message": "Drainage Lines completed",
            "execution_id": execution_id,
            "node_type": "Drainage_Lines",
            "asset_ids": [asset_id],
            "hosting_platform": "GEE",
            "stac_spec": {},
            "execution_time": execution_time,
        }, status=status.HTTP_200_OK)

    except Exception as e:
        print("Exception in generate_drainage_layer api :: ", e)
        return Response({
            "status": "failed",
            "message": str(e),
            "execution_id": request.data.get("execution_id", "local"),
            "node_type": "Drainage_Lines",
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
@schema(None)
def create_workspace(request):
    print("Inside create_workspace API.")
    try:
        workspace = request.data.get("workspace_name")
        print("workspace :: ", workspace)
        geo = Geoserver()
        response = geo.create_workspace(workspace)
        print(response)
        return Response({"Success": response}, status=status.HTTP_201_CREATED)
    except Exception as e:
        print("Exception in create_workspace api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
@schema(None)
def delete_layer(request):
    print("Inside delete_layer API.")
    try:
        workspace = request.data.get("workspace")
        layer_name = request.data.get("layer_name")
        geo = Geoserver()
        response = geo.delete_layer(layer_name, workspace)
        print(response)
        return Response({"Success": response}, status=status.HTTP_200_OK)
    except Exception as e:
        print("Exception in delete_layer api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
@schema(None)
def upload_kml(request):
    print("Inside upload_kml API.")
    try:
        req_body = request.POST.dict()
        state = req_body.get("state").lower()
        district = req_body.get("district").lower()
        block = req_body.get("block").lower()
        kml_file = request.FILES["file"]

        fs = FileSystemStorage(KML_PATH)
        filename = fs.save(kml_file.name, kml_file)

        kml_to_shp(state, district, block, KML_PATH + filename)

        return Response(
            {"Success": "Successfully uploaded"}, status=status.HTTP_201_CREATED
        )
    except Exception as e:
        print("Exception in upload_kml api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# MWS Layer
@api_security_check(allowed_methods="POST")
@schema(None)
def generate_mws_layer(request):
    print("Inside generate_mws_layer")
    start_time = time.time()
    try:
        state = request.data.get("state")
        district = request.data.get("district")
        block = request.data.get("block")
        gee_account_id = request.data.get("gee_account_id")
        execution_id = request.data.get("execution_id", "local")

        # SYNC — direct call, no Celery
        asset_id = mws_layer(
            state=state,
            district=district,
            block=block,
            gee_account_id = gee_account_id
        )

        # TODO: Replace hardcoded stac_spec with build_stac_spec() call
        execution_time = time.time() - start_time
        return Response({
            "status": "success",
            "message": "MWS layer completed",
            "execution_id": execution_id,
            "node_type": "MWS_Layer",
            "asset_ids": [asset_id],
            "hosting_platform": "GEE",
            "stac_spec": {},
            "execution_time": execution_time,
        })
    except Exception as e:
        print("Exception in generate_mws_layer api :: ", e)
        return Response(
            {"status": "failed", "message": str(e), "node_type": "MWS_Layer"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
# Fortnightly Hydrology
@api_security_check(allowed_methods="POST")
@schema(None)
def generate_fortnightly_hydrology(request):
    print("Inside generate_fortnightly_hydrology")
    start_time = time.time()
    try:
        state = request.data.get("state")
        district = request.data.get("district")
        block = request.data.get("block")
        start_year = int(request.data.get("start_year"))
        end_year = int(request.data.get("end_year"))
        gee_account_id = request.data.get("gee_account_id")
        execution_id = request.data.get("execution_id", "local")

        asset_id = generate_hydrology(
            state=state,
            district=district,
            block=block,
            start_year=start_year,
            end_year=end_year,
            gee_account_id=gee_account_id,
            is_annual=False,
        )

        execution_time = time.time() - start_time
        return Response({
            "status": "success",
            "message": "Fortnightly Hydrology completed",
            "execution_id": execution_id,
            "node_type": "Hydrology",
            "asset_ids": [asset_id],
            "hosting_platform": "GEE",
            "stac_spec": {},
            "execution_time": execution_time,
        }, status=status.HTTP_200_OK)

    except Exception as e:
        print("Exception in generate_fortnightly_hydrology api :: ", e)
        return Response({
            "status": "failed",
            "message": str(e),
            "execution_id": request.data.get("execution_id", "local"),
            "node_type": "Hydrology",
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


# Annual Hydrology
@api_view(["POST"])
@schema(None)
def generate_annual_hydrology(request):
    print("Inside generate_annual_hydrology")
    start_time = time.time()
    try:
        state = request.data.get("state")
        district = request.data.get("district")
        block = request.data.get("block")
        start_year = int(request.data.get("start_year"))
        end_year = int(request.data.get("end_year"))
        gee_account_id = request.data.get("gee_account_id")
        execution_id = request.data.get("execution_id", "local")

        asset_id = generate_hydrology(
            state=state,
            district=district,
            block=block,
            start_year=start_year,
            end_year=end_year,
            gee_account_id=gee_account_id,
            is_annual=True,
        )

        execution_time = time.time() - start_time
        return Response({
            "status": "success",
            "message": "Annual Hydrology completed",
            "execution_id": execution_id,
            "node_type": "Hydrology_Annual",
            "asset_ids": [asset_id],
            "hosting_platform": "GEE",
            "stac_spec": {},
            "execution_time": execution_time,
        }, status=status.HTTP_200_OK)

    except Exception as e:
        print("Exception in generate_annual_hydrology api :: ", e)
        return Response({
            "status": "failed",
            "message": str(e),
            "execution_id": request.data.get("execution_id", "local"),
            "node_type": "Hydrology_Annual",
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@api_view(["POST"])
@schema(None)
def lulc_for_tehsil(request):
    print("Inside lulc_v3 api.")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        start_year = request.data.get("start_year")
        end_year = request.data.get("end_year")
        gee_account_id = request.data.get("gee_account_id")
        version = request.data.get("version")
        if version == "v2":
            generate_lulc_v2_tehsil.apply_async(
                args=[state, district, block, start_year, end_year, gee_account_id],
                queue="nrm",
            )
            return Response(
                {"Success": "generate_lulc_v2_tehsil task initiated"},
                status=status.HTTP_200_OK,
            )
        else:
            generate_lulc_v3_tehsil.apply_async(
                args=[state, district, block, start_year, end_year, gee_account_id],
                queue="nrm",
            )
            return Response(
                {"Success": "generate_lulc_v3_tehsil task initiated"},
                status=status.HTTP_200_OK,
            )
    except Exception as e:
        print("Exception in lulc_for_tehsil api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
@schema(None)
def lulc_v2_river_basin(request):
    """
        To generate LULC v2 layers on river basin.
    Args:
        request:
            basin_object_id: object id of river basin (from "projects/corestack-datasets/assets/datasets/CGWB_basin" dataset)
            start_year: start year for layer generation
            end_year: end year for layer generation
    Returns:
        Response: Success/Exception
    """
    print("Inside lulc_v2_river_basin")
    try:
        basin_object_id = request.data.get("basin_object_id")
        start_year = request.data.get("start_year")
        end_year = request.data.get("end_year")
        lulc_river_basin_v2.apply_async(
            args=[basin_object_id, start_year, end_year], queue="nrm"
        )
        return Response({"Success": "lulc_v2_river_basin"}, status=status.HTTP_200_OK)
    except Exception as e:
        print("Exception in lulc_v2_river_basin api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
@schema(None)
def lulc_v3_river_basin(request):
    """
        To generate LULC v3 layers on river basin.
    Args:
        request:
            basin_object_id: object id of river basin (from "projects/corestack-datasets/assets/datasets/CGWB_basin" dataset)
            start_year: start year for layer generation
            end_year: end year for layer generation
    Returns:
        Response: Success/Exception
    """
    print("Inside lulc_v3_river_basin")
    try:
        basin_object_id = request.data.get("basin_object_id")
        start_year = request.data.get("start_year")
        end_year = request.data.get("end_year")
        lulc_river_basin_v3.apply_async(
            args=[basin_object_id, start_year, end_year], queue="nrm"
        )
        return Response({"Success": "lulc_v3_river_basin"}, status=status.HTTP_200_OK)
    except Exception as e:
        print("Exception in lulc_v3_river_basin api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


# LULC Raster Algo
@api_view(["POST"])
@schema(None)
def lulc_v3(request):
    print("Inside lulc_v3 api.")
    from datetime import datetime as dt
    start_time = time.time()
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        start_year = int(request.data.get("start_year"))
        end_year = int(request.data.get("end_year"))
        execution_id = request.data.get("execution_id", "local")
        

        # SYNC — direct call, no Celery
        asset_ids = clip_lulc_v3(
            state=state,
            district=district,
            block=block,
            start_year=start_year,
            end_year=end_year,
        )

        execution_time = time.time() - start_time

        # TODO: Replace hardcoded stac_spec with build_stac_spec() call
        # that computes actual geometry and bbox from asset_id.
        # Pattern: stac_spec = build_stac_spec(asset_ids, node_type, execution_id, ...)
        return Response({
            "status": "success",
            "message": f"LULC v3 completed - {len(asset_ids)} assets created",
            "execution_id": execution_id,
            "node_type": "LULC_Algorithm",
            "asset_ids": asset_ids,
            "hosting_platform": "GEE",
            "stac_spec": {
                "stac_version": "1.0.0",
                "type": "Feature",
                "id": f"{state}_{district}_{block}_lulc_{execution_id[:8]}",
                "properties": {
                    "datetime": dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "title": f"LULC v3 for {block}, {district}, {state}",
                    "stacd:algorithm": "LULC_Algorithm",
                    "stacd:execution_id": execution_id,
                    "stacd:state": state,
                    "stacd:district": district,
                    "stacd:block": block,
                    "stacd:start_year": str(start_year),
                    "stacd:end_year": str(end_year),
                    "stacd:hosting_platform": "GEE",
                },
                "assets": {
                    asset_id: {
                        "href": f"https://code.earthengine.google.com/?asset={asset_id}",
                        "type": "image/tiff",
                        "roles": ["data"],
                        "gee:asset_id": asset_id,
                    }
                    for asset_id in asset_ids
                },
            },
            "execution_time": execution_time,
        }, status=status.HTTP_200_OK)

    except Exception as e:
        print("Exception in lulc_v3 api :: ", e)
        return Response({
            "status": "failed",
            "message": str(e),
            "execution_id": request.data.get("execution_id", "local"),
            "node_type": "LULC_Algorithm",
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# LULC Vectorization Algo
@api_view(["POST"])
@schema(None)
def lulc_vector(request):
    print("Inside lulc_vector")
    from datetime import datetime as dt
    start_time = time.time()
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        start_year = int(request.data.get("start_year"))
        end_year = int(request.data.get("end_year"))
        execution_id = request.data.get("execution_id", "local")
        gee_account_id = request.data.get("gee_account_id")

        # SYNC — direct call, no Celery
        asset_id = vectorise_lulc(
            state=state,
            district=district,
            block=block,
            start_year=start_year,
            end_year=end_year,
            gee_account_id=gee_account_id
        )

        execution_time = time.time() - start_time

        # TODO: Replace hardcoded stac_spec with build_stac_spec() call
        # that computes actual geometry and bbox from asset_id.
        # Pattern: stac_spec = build_stac_spec(asset_ids, node_type, execution_id, ...)
        return Response({
            "status": "success",
            "message": "LULC vectorization completed",
            "execution_id": execution_id,
            "node_type": "LULC_Vectorization",
            "asset_ids": [asset_id],
            "hosting_platform": "GEE",
            "stac_spec": {
                "stac_version": "1.0.0",
                "type": "Feature",
                "id": f"{state}_{district}_{block}_lulc_vector_{execution_id[:8]}",
                "properties": {
                    "datetime": dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "title": f"LULC Vector for {block}, {district}, {state}",
                    "stacd:algorithm": "LULC_Vectorization",
                    "stacd:execution_id": execution_id,
                    "stacd:state": state,
                    "stacd:district": district,
                    "stacd:block": block,
                    "stacd:start_year": str(start_year),
                    "stacd:end_year": str(end_year),
                    "stacd:hosting_platform": "GEE",
                },
                "assets": {
                    asset_id: {
                        "href": f"https://code.earthengine.google.com/?asset={asset_id}",
                        "type": "application/geo+json",
                        "roles": ["data"],
                        "gee:asset_id": asset_id,
                    }
                },
            },
            "execution_time": execution_time,
        }, status=status.HTTP_200_OK)

    except Exception as e:
        print("Exception in lulc_vector api :: ", e)
        return Response({
            "status": "failed",
            "message": str(e),
            "execution_id": request.data.get("execution_id", "local"),
            "node_type": "LULC_Vectorization",
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(["POST"])
@schema(None)
def lulc_v4(request):
    print("Inside lulc_time_series")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        start_year = request.data.get("start_year")
        end_year = request.data.get("end_year")
        gee_account_id = request.data.get("gee_account_id")
        generate_lulc_v4.apply_async(
            args=[state, district, block, start_year, end_year, gee_account_id],
            queue="nrm",
        )
        return Response(
            {"Success": "lulc_time_series task initiated"},
            status=status.HTTP_200_OK,
        )
    except Exception as e:
        print("Exception in lulc_time_series api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
@schema(None)
def get_gee_layer(request):
    print("Inside get_gee_layer")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        response = download_gee_layer(state, district, block)

        return Response({"Success": response}, status=status.HTTP_200_OK)
    except Exception as e:
        print("Exception in get_gee_layer api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# Cropping Intensity
@api_view(["POST"])
@schema(None)
def generate_ci_layer(request):
    print("Inside generate_cropping_intensity_layer")
    start_time = time.time()
    try:
        state = request.data.get("state")
        district = request.data.get("district")
        block = request.data.get("block")
        start_year = request.data.get("start_year")
        end_year = request.data.get("end_year")
        gee_account_id = request.data.get("gee_account_id")
        execution_id = request.data.get("execution_id", "local")

        asset_id = generate_cropping_intensity(
            state=state,
            district=district,
            block=block,
            start_year=start_year,
            end_year=end_year,
            gee_account_id=gee_account_id,
        )

        execution_time = time.time() - start_time
        return Response({
            "status": "success",
            "message": "Cropping Intensity completed",
            "execution_id": execution_id,
            "node_type": "Cropping_Intensity",
            "asset_ids": [asset_id],
            "hosting_platform": "GEE",
            "stac_spec": {},
            "execution_time": execution_time,
        }, status=status.HTTP_200_OK)

    except Exception as e:
        print("Exception in generate_cropping_intensity_layer api :: ", e)
        return Response({
            "status": "failed",
            "message": str(e),
            "execution_id": request.data.get("execution_id", "local"),
            "node_type": "Cropping_Intensity",
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# SWB Layer
@api_view(["POST"])
@schema(None)
def generate_swb(request):
    print("Inside generate_swf")
    start_time = time.time()
    try:
        state = request.data.get("state")
        district = request.data.get("district")
        block = request.data.get("block")
        start_year = request.data.get("start_year")
        end_year = request.data.get("end_year")
        gee_account_id = request.data.get("gee_account_id")
        execution_id = request.data.get("execution_id", "local")

        asset_id = generate_swb_layer(
            state=state,
            district=district,
            block=block,
            start_year=start_year,
            end_year=end_year,
            gee_account_id=gee_account_id,
        )

        execution_time = time.time() - start_time
        return Response({
            "status": "success",
            "message": "SWB Layer completed",
            "execution_id": execution_id,
            "node_type": "SWB_Layer",
            "asset_ids": [asset_id],
            "hosting_platform": "GEE",
            "stac_spec": {},
            "execution_time": execution_time,
        }, status=status.HTTP_200_OK)

    except Exception as e:
        print("Exception in generate_swf api :: ", e)
        return Response({
            "status": "failed",
            "message": str(e),
            "execution_id": request.data.get("execution_id", "local"),
            "node_type": "SWB_Layer",
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# Drought
@api_view(["POST"])
@schema(None)
def generate_drought_layer(request):
    print("Inside generate_drought_layer")
    start_time = time.time()
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        start_year = request.data.get("start_year")
        end_year = request.data.get("end_year")
        gee_account_id = request.data.get("gee_account_id")
        execution_id = request.data.get("execution_id", "local")

        asset_id = calculate_drought(
            state=state,
            district=district,
            block=block,
            start_year=start_year,
            end_year=end_year,
            gee_account_id=gee_account_id,
        )

        execution_time = time.time() - start_time
        return Response({
            "status": "success",
            "message": "Drought layer generated successfully",
            "execution_id": execution_id,
            "node_type": "Drought",
            "asset_ids": [asset_id],
            "hosting_platform": "GEE",
            "stac_spec": {},
            "execution_time": execution_time,
        }, status=status.HTTP_200_OK)

    except Exception as e:
        print("Exception in generate_drought_layer api :: ", e)
        return Response({
            "status": "failed",
            "message": str(e),
            "execution_id": request.data.get("execution_id", "local"),
            "node_type": "Drought",
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
# Terrain vectorization algo
@api_view(["POST"])
@schema(None)
def generate_terrain_descriptor(request):
    print("Inside generate_terrain_descriptor")
    from datetime import datetime as dt
    start_time = time.time()
    try:
        state = request.data.get("state")
        district = request.data.get("district")
        block = request.data.get("block")
        gee_account_id = request.data.get("gee_account_id")
        execution_id = request.data.get("execution_id", "local")

        # SYNC — direct call, no Celery
        asset_id = generate_terrain_clusters(
            state=state,
            district=district,
            block=block,
            gee_account_id = gee_account_id
        )

        execution_time = time.time() - start_time

        # TODO: Replace hardcoded stac_spec with build_stac_spec() call
        # that computes actual geometry and bbox from asset_id.
        # Pattern: stac_spec = build_stac_spec(asset_ids, node_type, execution_id, ...)
        return Response({
            "status": "success",
            "message": "Terrain clusters completed",
            "execution_id": execution_id,
            "node_type": "Terrain_Vectorization",
            "asset_ids": [asset_id],
            "hosting_platform": "GEE",
            "stac_spec": {
                "stac_version": "1.0.0",
                "type": "Feature",
                "id": f"{state}_{district}_{block}_terrain_clusters_{execution_id[:8]}",
                "properties": {
                    "datetime": dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "title": f"Terrain Clusters for {block}, {district}, {state}",
                    "stacd:algorithm": "Terrain_Vectorization",
                    "stacd:execution_id": execution_id,
                    "stacd:state": state,
                    "stacd:district": district,
                    "stacd:block": block,
                    "stacd:hosting_platform": "GEE",
                },
                "assets": {
                    asset_id: {
                        "href": f"https://code.earthengine.google.com/?asset={asset_id}",
                        "type": "application/geo+json",
                        "roles": ["data"],
                        "gee:asset_id": asset_id,
                    }
                },
            },
            "execution_time": execution_time,
        }, status=status.HTTP_200_OK)

    except Exception as e:
        print("Exception in generate_terrain_descriptor api :: ", e)
        return Response({
            "status": "failed",
            "message": str(e),
            "execution_id": request.data.get("execution_id", "local"),
            "node_type": "Terrain_Vectorization",
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# Terrain Raster Algo
@api_view(["POST"])
@schema(None)
def generate_terrain_raster(request):
    print("Inside generate_terrain_raster")
    from datetime import datetime as dt
    start_time = time.time()
    try:
        state = request.data.get("state")
        district = request.data.get("district")
        block = request.data.get("block")
        gee_account_id = request.data.get("gee_account_id")

        execution_id = request.data.get("execution_id", "local")

        # SYNC — direct call, no Celery
        asset_id = generate_terrain_raster_clip(
            state=state,
            district=district,
            block=block,
            gee_account_id = gee_account_id

        )

        # TODO: Replace hardcoded stac_spec dict with a call to build_stac_spec()
        # utility function that computes actual geometry and bbox from the asset_id.
        # Pattern: stac_spec = build_stac_spec(asset_ids, node_type, execution_id, ...)

        execution_time = time.time() - start_time
        return Response({
            "status": "success",
            "message": "Terrain raster completed",
            "execution_id": execution_id,
            "node_type": "Terrain_Algorithm",
            "asset_ids": [asset_id],
            "hosting_platform": "GEE",
            "stac_spec": {
                "stac_version": "1.0.0",
                "type": "Feature",
                "id": f"{state}_{district}_{block}_terrain_{execution_id[:8]}",
                "properties": {
                    "datetime": dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "title": f"Terrain Raster for {block}, {district}, {state}",
                    "stacd:algorithm": "Terrain_Algorithm",
                    "stacd:execution_id": execution_id,
                    "stacd:state": state,
                    "stacd:district": district,
                    "stacd:block": block,
                    "stacd:hosting_platform": "GEE",
                },
                "assets": {
                    asset_id: {
                        "href": f"https://code.earthengine.google.com/?asset={asset_id}",
                        "type": "image/tiff",
                        "roles": ["data"],
                        "gee:asset_id": asset_id,
                    }
                },
            },
            "execution_time": execution_time,
        }, status=status.HTTP_200_OK)

    except Exception as e:
        print("Exception in generate_terrain_raster api :: ", e)
        return Response({
            "status": "failed",
            "message": str(e),
            "execution_id": request.data.get("execution_id", "local"),
            "node_type": "Terrain_Algorithm",
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# TerrainXLulc slope vectorization
@api_view(["POST"])
@schema(None)
def terrain_lulc_slope_cluster(request):
    print("Inside terrain_lulc_slope_cluster")
    from datetime import datetime as dt
    start_time = time.time()
    try:
        state = request.data.get("state")
        district = request.data.get("district")
        block = request.data.get("block")
        start_year = int(request.data.get("start_year"))
        end_year = int(request.data.get("end_year"))
        gee_account_id = request.data.get("gee_account_id")
        execution_id = request.data.get("execution_id", "local")
    

        # SYNC — direct call, no Celery
        asset_id = lulc_on_slope_cluster(
            state=state, district=district, block=block,
            start_year=start_year, end_year=end_year,
            gee_account_id = gee_account_id
        )

        execution_time = time.time() - start_time

        # TODO: Replace hardcoded stac_spec with build_stac_spec() call
        return Response({
            "status": "success",
            "message": "LULC x Terrain slope clustering completed",
            "execution_id": execution_id,
            "node_type": "Terrain_LULC_Slope",
            "asset_ids": [asset_id],
            "hosting_platform": "GEE",
            "stac_spec": {
                "stac_version": "1.0.0",
                "type": "Feature",
                "id": f"{state}_{district}_{block}_lulc_slope_{execution_id[:8]}",
                "properties": {
                    "datetime": dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "title": f"LULC x Terrain Slope for {block}, {district}, {state}",
                    "stacd:algorithm": "Terrain_LULC_Slope",
                    "stacd:execution_id": execution_id,
                    "stacd:state": state,
                    "stacd:district": district,
                    "stacd:block": block,
                    "stacd:start_year": str(start_year),
                    "stacd:end_year": str(end_year),
                    "stacd:hosting_platform": "GEE",
                },
                "assets": {
                    asset_id: {
                        "href": f"https://code.earthengine.google.com/?asset={asset_id}",
                        "type": "application/geo+json",
                        "roles": ["data"],
                        "gee:asset_id": asset_id,
                    }
                },
            },
            "execution_time": execution_time,
        }, status=status.HTTP_200_OK)

    except Exception as e:
        print("Exception in terrain_lulc_slope_cluster api :: ", e)
        return Response({
            "status": "failed",
            "message": str(e),
            "execution_id": request.data.get("execution_id", "local"),
            "node_type": "Terrain_LULC_Slope",
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# TerrainXLulc plain vectorization
@api_view(["POST"])
@schema(None)
def terrain_lulc_plain_cluster(request):
    print("Inside terrain_lulc_plain_cluster")
    from datetime import datetime as dt
    start_time = time.time()
    try:
        state = request.data.get("state")
        district = request.data.get("district")
        block = request.data.get("block")
        start_year = int(request.data.get("start_year"))
        end_year = int(request.data.get("end_year"))
        gee_account_id = request.data.get("gee_account_id")
        execution_id = request.data.get("execution_id", "local")


        # SYNC — direct call, no Celery
        asset_id = lulc_on_plain_cluster(
            state=state, district=district, block=block,
            start_year=start_year, end_year=end_year,
            gee_account_id = gee_account_id
        )

        execution_time = time.time() - start_time

        # TODO: Replace hardcoded stac_spec with build_stac_spec() call
        return Response({
            "status": "success",
            "message": "LULC x Terrain plain clustering completed",
            "execution_id": execution_id,
            "node_type": "Terrain_LULC_Plain",
            "asset_ids": [asset_id],
            "hosting_platform": "GEE",
            "stac_spec": {
                "stac_version": "1.0.0",
                "type": "Feature",
                "id": f"{state}_{district}_{block}_lulc_plain_{execution_id[:8]}",
                "properties": {
                    "datetime": dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "title": f"LULC x Terrain Plain for {block}, {district}, {state}",
                    "stacd:algorithm": "Terrain_LULC_Plain",
                    "stacd:execution_id": execution_id,
                    "stacd:state": state,
                    "stacd:district": district,
                    "stacd:block": block,
                    "stacd:start_year": str(start_year),
                    "stacd:end_year": str(end_year),
                    "stacd:hosting_platform": "GEE",
                },
                "assets": {
                    asset_id: {
                        "href": f"https://code.earthengine.google.com/?asset={asset_id}",
                        "type": "application/geo+json",
                        "roles": ["data"],
                        "gee:asset_id": asset_id,
                    }
                },
            },
            "execution_time": execution_time,
        }, status=status.HTTP_200_OK)

    except Exception as e:
        print("Exception in terrain_lulc_plain_cluster api :: ", e)
        return Response({
            "status": "failed",
            "message": str(e),
            "execution_id": request.data.get("execution_id", "local"),
            "node_type": "Terrain_LULC_Plain",
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(["POST"])
@schema(None)
def generate_clart(request):
    print("Inside generate_clart")
    start_time = time.time()
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        gee_account_id = request.data.get("gee_account_id")
        execution_id = request.data.get("execution_id", "local")

        asset_id = generate_clart_layer(
            state=state,
            district=district,
            block=block,
            gee_account_id=gee_account_id,
        )

        execution_time = time.time() - start_time
        return Response({
            "status": "success",
            "message": "CLART Layer completed",
            "execution_id": execution_id,
            "node_type": "CLART_Layer",
            "asset_ids": [asset_id],
            "hosting_platform": "GEE",
            "stac_spec": {},
            "execution_time": execution_time,
        }, status=status.HTTP_200_OK)

    except Exception as e:
        print("Exception in generate_clart api :: ", e)
        return Response({
            "status": "failed",
            "message": str(e),
            "execution_id": request.data.get("execution_id", "local"),
            "node_type": "CLART_Layer",
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# Change Detection
@api_view(["POST"])
@schema(None)
def change_detection(request):
    print("Inside change_detection")
    start_time = time.time()
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        start_year = request.data.get("start_year")
        end_year = request.data.get("end_year")
        gee_account_id = request.data.get("gee_account_id")
        execution_id = request.data.get("execution_id", "local")

        asset_id = get_change_detection(
            state=state,
            district=district,
            block=block,
            start_year=start_year,
            end_year=end_year,
            gee_account_id=gee_account_id,
        )

        execution_time = time.time() - start_time
        return Response({
            "status": "success",
            "message": "Change Detection completed",
            "execution_id": execution_id,
            "node_type": "Change_Detection",
            "asset_ids": [asset_id],
            "hosting_platform": "GEE",
            "stac_spec": {},
            "execution_time": execution_time,
        }, status=status.HTTP_200_OK)

    except Exception as e:
        print("Exception in change_detection api :: ", e)
        return Response({
            "status": "failed",
            "message": str(e),
            "execution_id": request.data.get("execution_id", "local"),
            "node_type": "Change_Detection",
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# Change Detection Vector
@api_view(["POST"])
@schema(None)
def change_detection_vector(request):
    print("Inside change_detection_vector")
    start_time = time.time()
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        start_year = request.data.get("start_year")
        end_year = request.data.get("end_year")
        gee_account_id = request.data.get("gee_account_id")
        execution_id = request.data.get("execution_id", "local")

        asset_id = vectorise_change_detection(
            state=state,
            district=district,
            block=block,
            start_year=start_year,
            end_year=end_year,
            gee_account_id=gee_account_id,
        )

        execution_time = time.time() - start_time
        return Response({
            "status": "success",
            "message": "Change Detection Vector completed",
            "execution_id": execution_id,
            "node_type": "Change_Detection_Vector",
            "asset_ids": [asset_id],
            "hosting_platform": "GEE",
            "stac_spec": {},
            "execution_time": execution_time,
        }, status=status.HTTP_200_OK)

    except Exception as e:
        print("Exception in change_detection_vector api :: ", e)
        return Response({
            "status": "failed",
            "message": str(e),
            "execution_id": request.data.get("execution_id", "local"),
            "node_type": "Change_Detection_Vector",
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
# Crop Grids    
@api_view(["POST"])
@schema(None)
def crop_grid(request):
    print("Inside crop_grid api")
    start_time = time.time()
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        gee_account_id = request.data.get("gee_account_id")
        execution_id = request.data.get("execution_id", "local")

        asset_id = create_crop_grids(
            state=state,
            district=district,
            block=block,
            gee_account_id=gee_account_id,
        )

        execution_time = time.time() - start_time
        return Response({
            "status": "success",
            "message": "Crop Grid completed",
            "execution_id": execution_id,
            "node_type": "Crop_Grid",
            "asset_ids": [asset_id],
            "hosting_platform": "GeoServer",
            "stac_spec": {},
            "execution_time": execution_time,
        }, status=status.HTTP_200_OK)

    except Exception as e:
        print("Exception in crop_grid api :: ", e)
        return Response({
            "status": "failed",
            "message": str(e),
            "execution_id": request.data.get("execution_id", "local"),
            "node_type": "Crop_Grid",
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
# Drought Causality
@api_view(["POST"])
@schema(None)
def mws_drought_causality(request):
    print("Inside Drought Causality API")
    start_time = time.time()
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        start_year = request.data.get("start_year")
        end_year = request.data.get("end_year")
        gee_account_id = request.data.get("gee_account_id")
        execution_id = request.data.get("execution_id", "local")

        geo_filename = drought_causality(
            state=state,
            district=district,
            block=block,
            start_year=start_year,
            end_year=end_year,
            gee_account_id=gee_account_id,
        )

        execution_time = time.time() - start_time
        return Response({
            "status": "success",
            "message": "Drought Causality completed",
            "execution_id": execution_id,
            "node_type": "Drought_Causality",
            "asset_ids": [geo_filename],
            "hosting_platform": "GeoServer",
            "stac_spec": {},
            "execution_time": execution_time,
        }, status=status.HTTP_200_OK)

    except Exception as e:
        print("Exception in Drought Causality api :: ", e)
        return Response({
            "status": "failed",
            "message": str(e),
            "execution_id": request.data.get("execution_id", "local"),
            "node_type": "Drought_Causality",
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# Tree Health Raster
@api_view(["POST"])
@schema(None)
def tree_health_raster(request):
    print("Inside tree_health_change API")
    start_time = time.time()
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        start_year = request.data.get("start_year")
        end_year = request.data.get("end_year")
        gee_account_id = request.data.get("gee_account_id")
        execution_id = request.data.get("execution_id", "local")

        ccd_asset_ids = tree_health_ccd_raster(
            state=state, district=district, block=block,
            start_year=start_year, end_year=end_year,
            gee_account_id=gee_account_id,
        )
        ch_asset_ids = tree_health_ch_raster(
            state=state, district=district, block=block,
            start_year=start_year, end_year=end_year,
            gee_account_id=gee_account_id,
        )
        overall_asset_id = tree_health_overall_change_raster(
            state=state, district=district, block=block,
            start_year=start_year, end_year=end_year,
            gee_account_id=gee_account_id,
        )

        all_asset_ids = ccd_asset_ids + ch_asset_ids + [overall_asset_id]

        execution_time = time.time() - start_time
        return Response({
            "status": "success",
            "message": "Tree Health Rasters completed",
            "execution_id": execution_id,
            "node_type": "TreeHealth_Rasters",
            "asset_ids": all_asset_ids,
            "hosting_platform": "GEE",
            "stac_spec": {},
            "execution_time": execution_time,
        }, status=status.HTTP_200_OK)

    except Exception as e:
        print("Exception in tree_health_raster api :: ", e)
        return Response({
            "status": "failed",
            "message": str(e),
            "execution_id": request.data.get("execution_id", "local"),
            "node_type": "TreeHealth_Rasters",
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# Tree Health Vector
@api_security_check(allowed_methods="POST")
@schema(None)
def tree_health_vector(request):
    print("Inside Overall_change_vector")
    start_time = time.time()
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        start_year = request.data.get("start_year")
        end_year = request.data.get("end_year")
        gee_account_id = request.data.get("gee_account_id")
        execution_id = request.data.get("execution_id", "local")

        ch_asset_id = tree_health_ch_vector(
            state=state, district=district, block=block,
            start_year=start_year, end_year=end_year,
            gee_account_id=gee_account_id,
        )
        ccd_asset_id = tree_health_ccd_vector(
            state=state, district=district, block=block,
            start_year=start_year, end_year=end_year,
            gee_account_id=gee_account_id,
        )
        overall_asset_id = tree_health_overall_change_vector(
            state=state, district=district, block=block,
            gee_account_id=gee_account_id,
        )

        execution_time = time.time() - start_time
        return Response({
            "status": "success",
            "message": "Tree Health Vectors completed",
            "execution_id": execution_id,
            "node_type": "TreeHealth_Vectors",
            "asset_ids": [ch_asset_id, ccd_asset_id, overall_asset_id],
            "hosting_platform": "GEE",
            "stac_spec": {},
            "execution_time": execution_time,
        }, status=status.HTTP_200_OK)

    except Exception as e:
        print("Exception in Overall_change_vector api :: ", e)
        return Response({
            "status": "failed",
            "message": str(e),
            "execution_id": request.data.get("execution_id", "local"),
            "node_type": "TreeHealth_Vectors",
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@api_view(["POST"])
@schema(None)
def gee_task_status(request):
    print("Inside gee_task_status API.")
    try:
        task_id = request.data.get("task_id")
        response = check_gee_task_status(task_id)
        return Response({"Response": response}, status=status.HTTP_200_OK)
    except Exception as e:
        print("Exception in gee_task_status api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# Stream Order
@api_security_check(allowed_methods="POST")
@schema(None)
def stream_order(request):
    print("Inside stream_order_vector api")
    start_time = time.time()
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        gee_account_id = request.data.get("gee_account_id")
        execution_id = request.data.get("execution_id", "local")

        asset_id = generate_stream_order(
            state=state,
            district=district,
            block=block,
            gee_account_id=gee_account_id,
        )

        execution_time = time.time() - start_time
        return Response({
            "status": "success",
            "message": "Stream Order completed",
            "execution_id": execution_id,
            "node_type": "Stream_Order",
            "asset_ids": [asset_id],
            "hosting_platform": "GEE",
            "stac_spec": {},
            "execution_time": execution_time,
        }, status=status.HTTP_200_OK)

    except Exception as e:
        print("Exception in stream_order api :: ", e)
        return Response({
            "status": "failed",
            "message": str(e),
            "execution_id": request.data.get("execution_id", "local"),
            "node_type": "Stream_Order",
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# Restoration Opportunity
@api_view(["POST"])
@schema(None)
def restoration_opportunity(request):
    print("Inside restoration_opportunity api")
    start_time = time.time()
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        gee_account_id = request.data.get("gee_account_id")
        execution_id = request.data.get("execution_id", "local")

        asset_id = generate_restoration_opportunity(
            state=state,
            district=district,
            block=block,
            gee_account_id=gee_account_id,
        )

        execution_time = time.time() - start_time
        return Response({
            "status": "success",
            "message": "Restoration Opportunity completed",
            "execution_id": execution_id,
            "node_type": "Restoration_Opportunity",
            "asset_ids": [asset_id],
            "hosting_platform": "GEE",
            "stac_spec": {},
            "execution_time": execution_time,
        }, status=status.HTTP_200_OK)

    except Exception as e:
        print("Exception in restoration_opportunity api :: ", e)
        return Response({
            "status": "failed",
            "message": str(e),
            "execution_id": request.data.get("execution_id", "local"),
            "node_type": "Restoration_Opportunity",
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# Site Suitability
@api_view(["POST"])
@schema(None)
def plantation_site_suitability(request):
    print("Inside plantation_site_suitability API")
    start_time = time.time()
    try:
        project_id = request.data.get("project_id")
        state = request.data.get("state").lower() if request.data.get("state") else None
        district = request.data.get("district").lower() if request.data.get("district") else None
        block = request.data.get("block").lower() if request.data.get("block") else None
        start_year = request.data.get("start_year")
        end_year = request.data.get("end_year")
        gee_account_id = request.data.get("gee_account_id") if request.data.get("gee_account_id") else None
        execution_id = request.data.get("execution_id", "local")

        asset_id = site_suitability(
            project_id,
            start_year,
            end_year,
            state=state,
            district=district,
            block=block,
            gee_account_id=gee_account_id,
        )

        execution_time = time.time() - start_time
        return Response({
            "status": "success",
            "message": "Site Suitability completed",
            "execution_id": execution_id,
            "node_type": "Site_Suitability",
            "asset_ids": [asset_id],
            "hosting_platform": "GEE",
            "stac_spec": {},
            "execution_time": execution_time,
        }, status=status.HTTP_200_OK)

    except Exception as e:
        print("Exception in Plantation_site_suitability api :: ", e)
        return Response({
            "status": "failed",
            "message": str(e),
            "execution_id": request.data.get("execution_id", "local"),
            "node_type": "Site_Suitability",
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
# Aquifer Vector
@api_view(["POST"])
@schema(None)
def aquifer_vector(request):
    print("Inside Aquifer vector layer api")
    start_time = time.time()
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        gee_account_id = request.data.get("gee_account_id")
        execution_id = request.data.get("execution_id", "local")

        asset_id = generate_aquifer_vector(
            state=state,
            district=district,
            block=block,
            gee_account_id=gee_account_id,
        )

        execution_time = time.time() - start_time
        return Response({
            "status": "success",
            "message": "Aquifer Vector completed",
            "execution_id": execution_id,
            "node_type": "Aquifer_Vector",
            "asset_ids": [asset_id],
            "hosting_platform": "GEE",
            "stac_spec": {},
            "execution_time": execution_time,
        }, status=status.HTTP_200_OK)

    except Exception as e:
        print("Exception in aquifer vector api :: ", e)
        return Response({
            "status": "failed",
            "message": str(e),
            "execution_id": request.data.get("execution_id", "local"),
            "node_type": "Aquifer_Vector",
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# SOGE Vector
@api_view(["POST"])
@schema(None)
def soge_vector(request):
    print("Inside soge vector layer api")
    start_time = time.time()
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        gee_account_id = request.data.get("gee_account_id")
        execution_id = request.data.get("execution_id", "local")

        asset_id = generate_soge_vector(
            state=state,
            district=district,
            block=block,
            gee_account_id=gee_account_id,
        )

        execution_time = time.time() - start_time
        return Response({
            "status": "success",
            "message": "SOGE Vector completed",
            "execution_id": execution_id,
            "node_type": "SOGE_Vector",
            "asset_ids": [asset_id],
            "hosting_platform": "GEE",
            "stac_spec": {},
            "execution_time": execution_time,
        }, status=status.HTTP_200_OK)

    except Exception as e:
        print("Exception in SOGE vector api :: ", e)
        return Response({
            "status": "failed",
            "message": str(e),
            "execution_id": request.data.get("execution_id", "local"),
            "node_type": "SOGE_Vector",
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
@api_view(["POST"])
@schema(None)
@parser_classes([MultiPartParser, FormParser])
def fes_clart_upload_layer(request):
    try:
        print("Inside upload_fes_clart_layer API")
        state = request.data.get("state", "").lower()
        district = request.data.get("district", "").lower()
        block = request.data.get("block", "").lower()
        gee_account_id = request.data.get("gee_account_id").lower()
        uploaded_file = request.FILES.get("clart_file")

        if not uploaded_file:
            return Response(
                {"error": "No file provided"}, status=status.HTTP_400_BAD_REQUEST
            )

        # Save file to temp location
        file_extension = os.path.splitext(uploaded_file.name)[1]
        filename = f'{district.strip().replace(" ", "_")}_{block.strip().replace(" ", "_")}_clart_fes{file_extension}'

        temp_upload_dir = os.path.join(
            BASE_DIR,
            "data",
            "fes_clart_file",
            state.strip().replace(" ", "_"),
            district.strip().replace(" ", "_"),
        )
        os.makedirs(temp_upload_dir, exist_ok=True)
        file_path = os.path.join(temp_upload_dir, filename)

        with open(file_path, "wb+") as destination:
            for chunk in uploaded_file.chunks():
                destination.write(chunk)

        # Pass file path to the task
        generate_fes_clart_layer.apply_async(
            args=[state, district, block, file_path, gee_account_id],
            queue="nrm",
        )

        return Response(
            {"success": "Fes clart task Initiated"}, status=status.HTTP_200_OK
        )

    except Exception as e:
        print("Exception in clart upload_geoserver_layer API:", e)
        return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
@schema(None)
def swb_pond_merging(request):
    print("Inside merge_swb_ponds API.")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        gee_account_id = request.data.get("gee_account_id")
        merge_swb_ponds.apply_async(
            args=[state, district, block, gee_account_id], queue="nrm"
        )
        return Response(
            {"Success": "Successfully initiated"}, status=status.HTTP_200_OK
        )
    except Exception as e:
        print("Exception in merge_swb_ponds api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
@schema(None)
def lulc_farm_boundary(request):
    print("Inside lulc_farm_boundary api")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()

        headers = {"Content-Type": "application/json"}
        payload = {"state": state, "district": district, "block": block}

        response = requests.post(
            LOCAL_COMPUTE_API_URL + "farm-boundary/",
            headers=headers,
            json=payload,
        )
        response.raise_for_status()
        data = response.json()
        print(data)

        return Response({"Success": "lulc_farm_boundary task initiated"}, status=200)

    except requests.exceptions.HTTPError as e:
        return Response(
            {
                "error": "External API returned an error",
                "details": str(e),
                "status_code": e.response.status_code,
                "url": e.response.url,
                "response_text": e.response.text,
            },
            status=status.HTTP_502_BAD_GATEWAY,
        )
    except requests.exceptions.RequestException as e:
        return Response(
            {"error": "Request to external API failed", "details": str(e)}, status=502
        )
    except Exception as e:
        return Response({"error": "Unhandled error", "details": str(e)}, status=500)


@api_view(["POST"])
@schema(None)
def ponds_compute(request):
    print("Inside ponds_compute api")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()

        headers = {"Content-Type": "application/json"}
        payload = {"state": state, "district": district, "block": block}

        response = requests.post(
            LOCAL_COMPUTE_API_URL + "ponds/",
            headers=headers,
            json=payload,
        )
        response.raise_for_status()
        data = response.json()
        print(data)

        return Response({"Success": "ponds_compute task initiated"}, status=200)

    except requests.exceptions.HTTPError as e:
        return Response(
            {
                "error": "External API returned an error",
                "details": str(e),
                "status_code": e.response.status_code,
                "url": e.response.url,
                "response_text": e.response.text,
            },
            status=status.HTTP_502_BAD_GATEWAY,
        )
    except requests.exceptions.RequestException as e:
        return Response(
            {"error": "Request to external API failed", "details": str(e)}, status=502
        )
    except Exception as e:
        return Response({"error": "Unhandled error", "details": str(e)}, status=500)


@api_view(["POST"])
@schema(None)
def wells_compute(request):
    print("Inside wells_compute api")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()

        headers = {"Content-Type": "application/json"}
        payload = {"state": state, "district": district, "block": block}

        response = requests.post(
            LOCAL_COMPUTE_API_URL + "wells/",
            headers=headers,
            json=payload,
        )
        response.raise_for_status()
        data = response.json()
        print(data)

        return Response({"Success": "wells_compute task initiated"}, status=200)

    except requests.exceptions.HTTPError as e:
        return Response(
            {
                "error": "External API returned an error",
                "details": str(e),
                "status_code": e.response.status_code,
                "url": e.response.url,
                "response_text": e.response.text,
            },
            status=status.HTTP_502_BAD_GATEWAY,
        )
    except requests.exceptions.RequestException as e:
        return Response(
            {"error": "Request to external API failed", "details": str(e)}, status=502
        )
    except Exception as e:
        return Response({"error": "Unhandled error", "details": str(e)}, status=500)


@api_view(["POST"])
@schema(None)
def generate_layer_in_order(request):
    print("inside generate_layer_order_first")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        map_order = request.data.get("map")
        gee_account_id = request.data.get("gee_account_id")
        start_year = request.data.get("start_year")
        end_year = request.data.get("end_year")
        start_year = int(start_year) if start_year is not None else None
        end_year = int(end_year) if end_year is not None else None
        layer_generate_map.apply_async(
            kwargs={
                "state": state,
                "district": district,
                "block": block,
                "map_order": map_order,
                "gee_account_id": gee_account_id,
                "start_year": start_year,
                "end_year": end_year,
            },
            queue="nrm",
        )
        return Response(
            {"Success": "Successfully initiated"}, status=status.HTTP_200_OK
        )
    except Exception as e:
        print("Exception in generate_layer_order_first api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
@schema(None)
def layer_status_dashboard(request):
    print("inside layer_staus_dashboard")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        result = layer_status(state, district, block)
        return Response(
            {"result": result},
            status=status.HTTP_200_OK,
        )
    except Exception as e:
        print("Exception in layer_staus_dashboard api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
@schema(None)
def generate_lcw(request):
    print("Inside generate_lcw_conflict_data API.")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        gee_account_id = request.data.get("gee_account_id")
        generate_lcw_conflict_data.apply_async(
            args=[state, district, block, gee_account_id], queue="nrm"
        )
        return Response(
            {"Success": "Successfully initiated"}, status=status.HTTP_200_OK
        )
    except Exception as e:
        print("Exception in generate_lcw_conflict_data api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# Agro Ecological
@api_view(["POST"])
@schema(None)
def generate_agroecological(request):
    print("Inside generate_agroecological_data API.")
    start_time = time.time()
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        gee_account_id = request.data.get("gee_account_id")
        execution_id = request.data.get("execution_id", "local")

        asset_id = generate_agroecological_data(state, district, block, gee_account_id)

        execution_time = time.time() - start_time
        return Response({
            "status": "success",
            "message": "Agroecological Layer completed",
            "execution_id": execution_id,
            "node_type": "Agroecological",
            "asset_ids": [asset_id],
            "hosting_platform": "GEE",
            "stac_spec": {},
            "execution_time": execution_time,
        }, status=status.HTTP_200_OK)

    except Exception as e:
        print("Exception in generate_agroecological_data api :: ", e)
        return Response({
            "status": "failed",
            "message": str(e),
            "execution_id": request.data.get("execution_id", "local"),
            "node_type": "Agroecological",
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# Factory CSR
@api_view(["POST"])
@schema(None)
def generate_factory_csr(request):
    print("Inside generate_factory_csr_to_gee API.")
    start_time = time.time()
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        gee_account_id = request.data.get("gee_account_id")
        execution_id = request.data.get("execution_id", "local")

        asset_id = generate_factory_csr_data(state, district, block, gee_account_id)

        execution_time = time.time() - start_time
        return Response({
            "status": "success",
            "message": "Factory CSR Layer completed",
            "execution_id": execution_id,
            "node_type": "Factory_CSR",
            "asset_ids": [asset_id],
            "hosting_platform": "GEE",
            "stac_spec": {},
            "execution_time": execution_time,
        }, status=status.HTTP_200_OK)

    except Exception as e:
        print("Exception in generate_factory_csr_to_gee api :: ", e)
        return Response({
            "status": "failed",
            "message": str(e),
            "execution_id": request.data.get("execution_id", "local"),
            "node_type": "Factory_CSR",
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
@schema(None)
def generate_green_credit(request):
    print("Inside generate_green_credit_to_gee API.")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        gee_account_id = request.data.get("gee_account_id")
        generate_green_credit_data.apply_async(
            args=[state, district, block, gee_account_id], queue="nrm"
        )
        return Response(
            {"Success": "Successfully initiated"}, status=status.HTTP_200_OK
        )
    except Exception as e:
        print("Exception in generate_green_credit_to_gee api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# Mining
@api_view(["POST"])
@schema(None)
def generate_mining(request):
    print("Inside generate_mining_to_gee API.")
    start_time = time.time()
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        gee_account_id = request.data.get("gee_account_id")
        execution_id = request.data.get("execution_id", "local")

        asset_id = generate_mining_data(state, district, block, gee_account_id)

        execution_time = time.time() - start_time
        return Response({
            "status": "success",
            "message": "Mining Layer completed",
            "execution_id": execution_id,
            "node_type": "Mining",
            "asset_ids": [asset_id],
            "hosting_platform": "GEE",
            "stac_spec": {},
            "execution_time": execution_time,
        }, status=status.HTTP_200_OK)

    except Exception as e:
        print("Exception in generate_mining_to_gee api :: ", e)
        return Response({
            "status": "failed",
            "message": str(e),
            "execution_id": request.data.get("execution_id", "local"),
            "node_type": "Mining",
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["GET"])
@schema(None)
def get_layers_for_workspace(request):
    print("inside get_layers_of_workspace API")
    try:
        workspace = request.query_params.get("workspace").lower()
        result = get_layers_of_workspace(workspace)
        return Response({"result": result}, status=status.HTTP_200_OK)
    except Exception as e:
        print("Exception in get_layers_for_workspace api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# Natural Depression
@api_view(["POST"])
@schema(None)
def generate_natural_depression(request):
    print("Inside generate_natural_depression_to_gee API.")
    start_time = time.time()
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        gee_account_id = request.data.get("gee_account_id")
        execution_id = request.data.get("execution_id", "local")

        asset_id = generate_natural_depression_data(
            state=state,
            district=district,
            block=block,
            gee_account_id=gee_account_id,
        )

        execution_time = time.time() - start_time
        return Response({
            "status": "success",
            "message": "Natural Depression completed",
            "execution_id": execution_id,
            "node_type": "Natural_Depression",
            "asset_ids": [asset_id],
            "hosting_platform": "GEE",
            "stac_spec": {},
            "execution_time": execution_time,
        }, status=status.HTTP_200_OK)

    except Exception as e:
        print("Exception in generate_natural_depression_to_gee api :: ", e)
        return Response({
            "status": "failed",
            "message": str(e),
            "execution_id": request.data.get("execution_id", "local"),
            "node_type": "Natural_Depression",
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# Distance to drainage
@api_view(["POST"])
@schema(None)
def generate_distance_nearest_upstream_DL(request):
    print("Inside generate_distance_nearest_upstream_DL_to_gee API.")
    start_time = time.time()
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        gee_account_id = request.data.get("gee_account_id")
        execution_id = request.data.get("execution_id", "local")

        asset_id = generate_distance_to_nearest_drainage_line(
            state=state,
            district=district,
            block=block,
            gee_account_id=gee_account_id,
        )

        execution_time = time.time() - start_time
        return Response({
            "status": "success",
            "message": "Distance to Nearest Drainage completed",
            "execution_id": execution_id,
            "node_type": "Dist_to_Drainage",
            "asset_ids": [asset_id],
            "hosting_platform": "GEE",
            "stac_spec": {},
            "execution_time": execution_time,
        }, status=status.HTTP_200_OK)

    except Exception as e:
        print("Exception in generate_distance_nearest_upstream_DL_to_gee api :: ", e)
        return Response({
            "status": "failed",
            "message": str(e),
            "execution_id": request.data.get("execution_id", "local"),
            "node_type": "Dist_to_Drainage",
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
# Catchment Area
@api_security_check(allowed_methods="POST")
@schema(None)
def generate_catchment_area_SF(request):
    print("Inside generate_catchment_area_SF API.")
    start_time = time.time()
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        gee_account_id = request.data.get("gee_account_id")
        execution_id = request.data.get("execution_id", "local")

        asset_id = generate_catchment_area_singleflow(
            state=state,
            district=district,
            block=block,
            gee_account_id=gee_account_id,
        )

        execution_time = time.time() - start_time
        return Response({
            "status": "success",
            "message": "Catchment Area completed",
            "execution_id": execution_id,
            "node_type": "Catchment_Area",
            "asset_ids": [asset_id],
            "hosting_platform": "GEE",
            "stac_spec": {},
            "execution_time": execution_time,
        }, status=status.HTTP_200_OK)

    except Exception as e:
        print("Exception in generate_catchment_area_SF api :: ", e)
        return Response({
            "status": "failed",
            "message": str(e),
            "execution_id": request.data.get("execution_id", "local"),
            "node_type": "Catchment_Area",
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# Slope Percentage
@api_view(["POST"])
@schema(None)
def generate_slope_percentage(request):
    print("Inside generate_slope_percentage_to_gee API.")
    start_time = time.time()
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        gee_account_id = request.data.get("gee_account_id")
        execution_id = request.data.get("execution_id", "local")

        asset_id = generate_slope_percentage_data(
            state=state,
            district=district,
            block=block,
            gee_account_id=gee_account_id,
        )

        execution_time = time.time() - start_time
        return Response({
            "status": "success",
            "message": "Slope Percentage completed",
            "execution_id": execution_id,
            "node_type": "Slope_Percentage",
            "asset_ids": [asset_id],
            "hosting_platform": "GEE",
            "stac_spec": {},
            "execution_time": execution_time,
        }, status=status.HTTP_200_OK)

    except Exception as e:
        print("Exception in generate_slope_percentage_to_gee api :: ", e)
        return Response({
            "status": "failed",
            "message": str(e),
            "execution_id": request.data.get("execution_id", "local"),
            "node_type": "Slope_Percentage",
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
# NDVI_Timeseries
@api_view(["POST"])
@schema(None)
def generate_ndvi_timeseries(request):
    print("Inside generate_ndvi_timeseries API.")
    start_time = time.time()
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        start_year = request.data.get("start_year")
        end_year = request.data.get("end_year")
        gee_account_id = request.data.get("gee_account_id")
        execution_id = request.data.get("execution_id", "local")

        asset_id = ndvi_timeseries(
            state=state,
            district=district,
            block=block,
            start_year=start_year,
            end_year=end_year,
            gee_account_id=gee_account_id,
        )

        execution_time = time.time() - start_time
        return Response({
            "status": "success",
            "message": "NDVI Timeseries completed",
            "execution_id": execution_id,
            "node_type": "NDVI_Timeseries",
            "asset_ids": [asset_id],
            "hosting_platform": "GeoServer",
            "stac_spec": {},
            "execution_time": execution_time,
        }, status=status.HTTP_200_OK)

    except Exception as e:
        print("Exception in generate_ndvi_timeseries api :: ", e)
        return Response({
            "status": "failed",
            "message": str(e),
            "execution_id": request.data.get("execution_id", "local"),
            "node_type": "NDVI_Timeseries",
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(["POST"])
@schema(None)
def generate_zoi_to_gee(request):
    print("Inside generate zoi layers")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        gee_account_id = request.data.get("gee_account_id")
        generate_zoi.apply_async(
            kwargs={
                "state": state,
                "district": district,
                "block": block,
                "gee_account_id": gee_account_id,
            },
            queue="waterbody",
        )

        return Response(
            {"Success": "Successfully initiated"}, status=status.HTTP_200_OK
        )
    except Exception as e:
        print("Exception in generate_mining_to_gee api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# MWS Connectivity
@api_view(["POST"])
@schema(None)
def generate_mws_connectivity(request):
    print("Inside generate_mws_connectivity_to_gee API.")
    start_time = time.time()
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        gee_account_id = request.data.get("gee_account_id")
        execution_id = request.data.get("execution_id", "local")

        asset_id = generate_mws_connectivity_data(
            state=state,
            district=district,
            block=block,
            gee_account_id=gee_account_id,
        )

        execution_time = time.time() - start_time
        return Response({
            "status": "success",
            "message": "MWS Connectivity completed",
            "execution_id": execution_id,
            "node_type": "MWS_Connectivity",
            "asset_ids": [asset_id],
            "hosting_platform": "GEE",
            "stac_spec": {},
            "execution_time": execution_time,
        }, status=status.HTTP_200_OK)

    except Exception as e:
        print("Exception in generate_mws_connectivity_to_gee api :: ", e)
        return Response({
            "status": "failed",
            "message": str(e),
            "execution_id": request.data.get("execution_id", "local"),
            "node_type": "MWS_Connectivity",
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# MWS Centroid
@api_view(["POST"])
@schema(None)
def generate_mws_centroid(request):
    print("Inside generate_mws_centroid API.")
    start_time = time.time()
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        gee_account_id = request.data.get("gee_account_id")
        execution_id = request.data.get("execution_id", "local")

        asset_id = generate_mws_centroid_data(state, district, block, gee_account_id)

        execution_time = time.time() - start_time
        return Response({
            "status": "success",
            "message": "MWS Centroid completed",
            "execution_id": execution_id,
            "node_type": "MWS_Centroid",
            "asset_ids": [asset_id],
            "hosting_platform": "GEE",
            "stac_spec": {},
            "execution_time": execution_time,
        }, status=status.HTTP_200_OK)

    except Exception as e:
        print("Exception in generate_mws_centroid api :: ", e)
        return Response({
            "status": "failed",
            "message": str(e),
            "execution_id": request.data.get("execution_id", "local"),
            "node_type": "MWS_Centroid",
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
@schema(None)
def generate_facilities_proximity(request):
    print("Inside generate_facilities_proximity API.")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        gee_account_id = request.data.get("gee_account_id")
        generate_facilities_proximity_task.apply_async(
            args=[state, district, block, gee_account_id], queue="nrm"
        )
        return Response(
            {"Success": "Successfully initiated"}, status=status.HTTP_200_OK
        )
    except Exception as e:
        print("Exception in generate_facilities_proximity api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

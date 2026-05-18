import json
import os
import inspect
from datetime import datetime
import requests
from nrm_app.settings import DATA_DIR, LOCAL_COMPUTE_API_URL
from rest_framework.decorators import (
    api_view,
    authentication_classes,
    parser_classes,
    permission_classes,
    schema,
)
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework import status
from rest_framework.parsers import MultiPartParser, FormParser

from computing.change_detection.change_detection_vector import (
    vectorise_change_detection,
)
from .utils import (
    save_layer_info_to_db,
    update_layer_sync_status,
)
from django.conf import settings
from computing.STAC_specs.stac_collection import sanitize_text, STACConfig
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
from .views import layer_status, get_layers_of_workspace, check_missing_layers
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
from .STAC_specs.stac_collection import _make_celery_task as _make_stac_task
from utilities.layer_generation_mode import sync_layer_generation_if_enabled
from utilities.layer_generation_logging import (
    layer_api_error_response,
    layer_generation_api_logging,
)
from computing import layer_asset_ids as layer_assets
from utilities.constants import GEE_PATHS
from utilities.gee_utils import get_gee_dir_path, valid_gee_text
from gee_computing.models import GEEAccount


def _build_mws_asset_id(state, district, block, description):
    return (
        get_gee_dir_path([state, district, block], asset_path=GEE_PATHS["MWS"]["GEE_ASSET_PATH"])
        + description
    )


def _build_lulc_v3_asset_id(state, district, block, year):
    description = (
        f"{valid_gee_text(district.lower())}_{valid_gee_text(block.lower())}"
        f"_{year}-07-01_{year + 1}-06-30_LULCmap_10m"
    )
    return _build_mws_asset_id(state, district, block, description)


def _tehsil_suffix(district, block):
    return layer_assets.tehsil_suffix(district, block)


def _task_started_response(message, task=None, asset_id=None, asset_ids=None):
    payload = {
        "status": "initiated",
        "Success": message,
        "message": message,
    }
    if task is not None and getattr(task, "id", None):
        payload["task_id"] = task.id
    if asset_id is not None:
        payload["asset_id"] = asset_id
    if asset_ids is not None:
        payload["asset_ids"] = asset_ids
        if asset_id is None and len(asset_ids) == 1:
            payload["asset_id"] = asset_ids[0]
        elif asset_id is None and asset_ids:
            payload["asset_id"] = asset_ids[0]
    return Response(payload, status=status.HTTP_200_OK)


@api_security_check(allowed_methods="POST")
@schema(None)
def generate_admin_boundary(request):
    print("Inside generate_block_layer API.")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        gee_account_id = request.data.get("gee_account_id")
        if gee_account_id in (None, ""):
            return Response(
                {"error": "gee_account_id is required."},
                status=status.HTTP_400_BAD_REQUEST,
            )
        try:
            gee_account_id = int(gee_account_id)
        except (TypeError, ValueError):
            return Response(
                {"error": "gee_account_id must be an integer."},
                status=status.HTTP_400_BAD_REQUEST,
            )
        if not GEEAccount.objects.filter(pk=gee_account_id).exists():
            return Response(
                {"error": f"GEEAccount with id={gee_account_id} was not found."},
                status=status.HTTP_400_BAD_REQUEST,
            )
        task = generate_tehsil_shape_file_data.apply_async(
            args=[state, district, block, gee_account_id], queue="nrm"
        )
        asset_id = layer_assets.admin_boundary_asset_id(state, district, block)
        return _task_started_response("Successfully initiated", task=task, asset_id=asset_id)
    except Exception as e:
        return layer_api_error_response("generate_admin_boundary", e, request=request)


@api_security_check(allowed_methods="POST")
@schema(None)
def generate_nrega_layer(request):
    print("Inside generate_nrega_layer API.")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        gee_account_id = request.data.get("gee_account_id")
        task = clip_nrega_district_block.apply_async(
            args=[state, district, block, gee_account_id], queue="nrm"
        )
        asset_id = layer_assets.nrega_asset_id(state, district, block)
        return _task_started_response("Successfully initiated", task=task, asset_id=asset_id)
    except Exception as e:
        return layer_api_error_response("generate_nrega_layer", e, request=request)


@api_view(["POST"])
@schema(None)
def generate_drainage_layer(request):
    print("Inside generate_drainage_layer API.")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        gee_account_id = request.data.get("gee_account_id")
        task = clip_drainage_lines.apply_async(
            kwargs={
                "state": state,
                "district": district,
                "block": block,
                "gee_account_id": gee_account_id,
            },
            queue="nrm",
        )
        asset_id = layer_assets.drainage_lines_asset_id(state, district, block)
        return _task_started_response("Successfully initiated", task=task, asset_id=asset_id)
    except Exception as e:
        return layer_api_error_response("generate_drainage_layer", e, request=request)


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
        return layer_api_error_response("create_workspace", e, request=request)


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
        return layer_api_error_response("delete_layer", e, request=request)


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
        return layer_api_error_response("upload_kml", e, request=request)


@api_security_check(allowed_methods="POST")
@schema(None)
def generate_mws_layer(request):
    print("Inside generate_mws_layer")
    try:
        state = request.data.get("state")
        district = request.data.get("district")
        block = request.data.get("block")
        gee_account_id = request.data.get("gee_account_id")
        task = mws_layer.apply_async(
            args=[state, district, block, gee_account_id], queue="nrm"
        )
        asset_id = _build_mws_asset_id(
            state,
            district,
            block,
            "filtered_mws_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
            + "_uid",
        )
        return _task_started_response("Successfully initiated", task=task, asset_id=asset_id)
    except Exception as e:
        return layer_api_error_response("generate_mws_layer", e, request=request)


@api_security_check(allowed_methods="POST")
@schema(None)
def generate_fortnightly_hydrology(request):
    print("Inside generate_fortnightly_hydrology")
    try:
        state = request.data.get("state")
        district = request.data.get("district")
        block = request.data.get("block")
        start_year = int(request.data.get("start_year"))
        end_year = int(request.data.get("end_year"))
        gee_account_id = request.data.get("gee_account_id")
        task = generate_hydrology.apply_async(
            kwargs={
                "state": state,
                "district": district,
                "block": block,
                "start_year": start_year,
                "end_year": end_year,
                "gee_account_id": gee_account_id,
                "is_annual": False,
            },
            queue="nrm",
        )
        asset_ids = layer_assets.hydrology_asset_ids(
            state, district, block, is_annual=False
        )
        return _task_started_response(
            "Successfully initiated",
            task=task,
            asset_ids=asset_ids,
        )
    except Exception as e:
        return layer_api_error_response("generate_fortnightly_hydrology", e, request=request)


@api_view(["POST"])
@schema(None)
def generate_annual_hydrology(request):
    print("Inside generate_annual_hydrology")
    try:
        state = request.data.get("state")
        district = request.data.get("district")
        block = request.data.get("block")
        start_year = int(request.data.get("start_year"))
        end_year = int(request.data.get("end_year"))
        gee_account_id = request.data.get("gee_account_id")
        task = generate_hydrology.apply_async(
            kwargs={
                "state": state,
                "district": district,
                "block": block,
                "start_year": start_year,
                "end_year": end_year,
                "is_annual": True,
                "gee_account_id": gee_account_id,
            },
            queue="nrm",
        )
        asset_ids = layer_assets.hydrology_asset_ids(
            state, district, block, is_annual=True
        )
        return _task_started_response(
            "Successfully initiated",
            task=task,
            asset_ids=asset_ids,
        )
    except Exception as e:
        return layer_api_error_response("generate_annual_hydrology", e, request=request)


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
        end_year = int(end_year)
        if version == "v2":
            task = generate_lulc_v2_tehsil.apply_async(
                args=[state, district, block, start_year, end_year, gee_account_id],
                queue="nrm",
            )
            asset_id = layer_assets.lulc_tehsil_asset_id(
                state, district, block, end_year, version="v2"
            )
            return _task_started_response(
                "generate_lulc_v2_tehsil task initiated", task=task, asset_id=asset_id
            )
        task = generate_lulc_v3_tehsil.apply_async(
            args=[state, district, block, start_year, end_year, gee_account_id],
            queue="nrm",
        )
        asset_id = layer_assets.lulc_tehsil_asset_id(
            state, district, block, end_year, version="v3"
        )
        return _task_started_response(
            "generate_lulc_v3_tehsil task initiated", task=task, asset_id=asset_id
        )
    except Exception as e:
        return layer_api_error_response("lulc_for_tehsil", e, request=request)


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
        return layer_api_error_response("lulc_v2_river_basin", e, request=request)


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
        gee_account_id = request.data.get("gee_account_id")
        lulc_river_basin_v3.apply_async(
            args=[basin_object_id, start_year, end_year, gee_account_id], queue="nrm"
        )
        return Response({"Success": "lulc_v3_river_basin"}, status=status.HTTP_200_OK)
    except Exception as e:
        return layer_api_error_response("lulc_v3_river_basin", e, request=request)


@api_view(["POST"])
@schema(None)
def lulc_v3(request):
    print("Inside lulc_v3 api.")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        start_year = int(request.data.get("start_year"))
        end_year = int(request.data.get("end_year"))
        gee_account_id = request.data.get("gee_account_id")
        asset_id = _build_lulc_v3_asset_id(state, district, block, end_year)

        if bool(getattr(settings, "LAYER_GENERATION_SYNC_MODE", False)):
            task_result = clip_lulc_v3.apply(
                args=[state, district, block, start_year, end_year, gee_account_id]
            )
            if task_result.failed():
                return Response(
                    {"error": str(task_result.result), "asset_id": asset_id},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR,
                )
            return Response(
                {"Success": "LULC v3 completed", "asset_id": asset_id},
                status=status.HTTP_200_OK,
            )

        task = clip_lulc_v3.apply_async(
            args=[state, district, block, start_year, end_year, gee_account_id],
            queue="nrm",
        )
        return _task_started_response("LULC v3 task initiated", task=task, asset_id=asset_id)
    except Exception as e:
        return layer_api_error_response("lulc_v3", e, request=request)


@api_view(["POST"])
@schema(None)
def lulc_vector(request):
    print("Inside lulc_vector")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        start_year = request.data.get("start_year")
        end_year = request.data.get("end_year")
        gee_account_id = request.data.get("gee_account_id")
        task = vectorise_lulc.apply_async(
            args=[state, district, block, start_year, end_year, gee_account_id],
            queue="nrm",
        )
        asset_id = layer_assets.lulc_vector_asset_id(state, district, block)
        return _task_started_response(
            "lulc_vector task initiated", task=task, asset_id=asset_id
        )
    except Exception as e:
        return layer_api_error_response("lulc_vector", e, request=request)


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
        task = generate_lulc_v4.apply_async(
            args=[state, district, block, start_year, end_year, gee_account_id],
            queue="nrm",
        )
        asset_id = layer_assets.lulc_v4_asset_id(state, district, block)
        return _task_started_response(
            "lulc_time_series task initiated", task=task, asset_id=asset_id
        )
    except Exception as e:
        return layer_api_error_response("lulc_v4", e, request=request)


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
        return layer_api_error_response("get_gee_layer", e, request=request)


@api_view(["POST"])
@schema(None)
def generate_ci_layer(request):
    print("Inside generate_cropping_intensity_layer")
    try:
        state = request.data.get("state")
        district = request.data.get("district")
        block = request.data.get("block")
        start_year = request.data.get("start_year")
        end_year = request.data.get("end_year")
        gee_account_id = request.data.get("gee_account_id")
        task = generate_cropping_intensity.apply_async(
            kwargs={
                "state": state,
                "district": district,
                "block": block,
                "start_year": start_year,
                "end_year": end_year,
                "gee_account_id": gee_account_id,
            },
            queue="nrm",
        )
        asset_suffix = valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
        asset_id = _build_mws_asset_id(
            state, district, block, "cropping_intensity_" + asset_suffix
        )
        return _task_started_response(
            "Cropping Intensity task initiated", task=task, asset_id=asset_id
        )
    except Exception as e:
        return layer_api_error_response("generate_ci_layer", e, request=request)


@api_view(["POST"])
@schema(None)
def generate_swb(request):
    print("Inside generate swb api")
    print(request.data)

    try:
        state = request.data.get("state") or request.data.get("State")
        district = request.data.get("district") or request.data.get("District")
        block = request.data.get("block") or request.data.get("Block")
        start_year = request.data.get("start_year")
        end_year = request.data.get("end_year")
        gee_account_id = request.data.get("gee_account_id") or request.data.get(
            "gee_account_d"
        )

        missing = []
        if not state:
            missing.append("state")
        if not district:
            missing.append("district")
        if not block:
            missing.append("block")
        if not gee_account_id:
            missing.append("gee_account_id")
        if missing:
            return Response(
                {
                    "error": f"Missing required fields: {', '.join(missing)}",
                    "hint": "Use keys state, district, block, gee_account_id in request body.",
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        state = state.lower()
        district = district.lower()
        block = block.lower()
        task_result = generate_swb_layer.apply(
            kwargs={
                "state": state,
                "district": district,
                "block": block,
                "start_year": start_year,
                "end_year": end_year,
                "gee_account_id": gee_account_id,
            }
        )
        asset_suffix = valid_gee_text(district) + "_" + valid_gee_text(block)
        expected_asset_id = (
            get_gee_dir_path(
                [state, district, block], asset_path=GEE_PATHS["MWS"]["GEE_ASSET_PATH"]
            )
            + "swb3_"
            + asset_suffix
        )
        if task_result.failed():
            return Response(
                {"error": str(task_result.result)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
        if not task_result.result:
            return Response(
                {"error": "SWB generation failed", "asset_id": expected_asset_id},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
        return Response(
            {"Success": "Generate swb completed", "asset_id": expected_asset_id},
            status=status.HTTP_200_OK,
        )
    except Exception as e:
        return layer_api_error_response("generate_swb", e, request=request)


@api_view(["POST"])
@schema(None)
def generate_drought_layer(request):
    print("Inside generate_drought_layer")
    try:
        state = request.data.get("state")
        district = request.data.get("district")
        block = request.data.get("block")
        start_year = request.data.get("start_year")
        end_year = request.data.get("end_year")
        gee_account_id = request.data.get("gee_account_id")
        task = calculate_drought.apply_async(
            kwargs={
                "state": state,
                "district": district,
                "block": block,
                "start_year": start_year,
                "end_year": end_year,
                "gee_account_id": gee_account_id,
            },
            queue="nrm",
        )
        asset_suffix = valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
        asset_id = _build_mws_asset_id(state, district, block, "drought_" + asset_suffix)
        return _task_started_response(
            "generate_drought_layer task initiated", task=task, asset_id=asset_id
        )
    except Exception as e:
        return layer_api_error_response("generate_drought_layer", e, request=request)


@api_view(["POST"])
@schema(None)
def generate_terrain_descriptor(request):
    print("Inside generate_terrain_descriptor")
    try:
        state = request.data.get("state")
        district = request.data.get("district")
        block = request.data.get("block")
        gee_account_id = request.data.get("gee_account_id")
        task = generate_terrain_clusters.apply_async(
            args=[state, district, block, gee_account_id], queue="nrm"
        )
        asset_id = layer_assets.mws_asset_id(
            state,
            district,
            block,
            _tehsil_suffix(district, block) + "_terrain_clusters",
        )
        return _task_started_response(
            "generate_terrain_descriptor task initiated", task=task, asset_id=asset_id
        )
    except Exception as e:
        return layer_api_error_response("generate_terrain_descriptor", e, request=request)


@api_view(["POST"])
@schema(None)
def generate_terrain_raster(request):
    print("Inside generate_terrain_raster")
    try:
        state = request.data.get("state")
        district = request.data.get("district")
        block = request.data.get("block")
        gee_account_id = request.data.get("gee_account_id")
        task = generate_terrain_raster_clip.apply_async(
            kwargs={
                "state": state,
                "district": district,
                "block": block,
                "gee_account_id": gee_account_id,
            },
            queue="nrm",
        )
        asset_id = layer_assets.mws_asset_id(
            state, district, block, _tehsil_suffix(district, block)
        )
        return _task_started_response(
            "generate_terrain_raster task initiated",
            task=task,
            asset_id=asset_id,
        )
    except Exception as e:
        return layer_api_error_response("generate_terrain_raster", e, request=request)


@api_view(["POST"])
@schema(None)
def terrain_lulc_slope_cluster(request):
    print("Inside terrain_lulc_slope_cluster")
    try:
        state = request.data.get("state")
        district = request.data.get("district")
        block = request.data.get("block")
        start_year = request.data.get("start_year")
        end_year = request.data.get("end_year")
        gee_account_id = request.data.get("gee_account_id")
        task = lulc_on_slope_cluster.apply_async(
            args=[state, district, block, start_year, end_year, gee_account_id],
            queue="nrm",
        )
        asset_id = layer_assets.mws_asset_id(
            state,
            district,
            block,
            _tehsil_suffix(district, block) + "_lulcXslopes_clusters",
        )
        return _task_started_response(
            "terrain_lulc_slope_cluster task initiated", task=task, asset_id=asset_id
        )
    except Exception as e:
        return layer_api_error_response("terrain_lulc_slope_cluster", e, request=request)


@api_view(["POST"])
@schema(None)
def terrain_lulc_plain_cluster(request):
    print("Inside terrain_lulc_plain_cluster")
    try:
        state = request.data.get("state")
        district = request.data.get("district")
        block = request.data.get("block")
        start_year = request.data.get("start_year")
        end_year = request.data.get("end_year")
        gee_account_id = request.data.get("gee_account_id")
        task = lulc_on_plain_cluster.apply_async(
            args=[state, district, block, start_year, end_year, gee_account_id],
            queue="nrm",
        )
        asset_id = layer_assets.mws_asset_id(
            state,
            district,
            block,
            _tehsil_suffix(district, block) + "_lulcXplains_clusters",
        )
        return _task_started_response(
            "terrain_lulc_plain_cluster task initiated", task=task, asset_id=asset_id
        )
    except Exception as e:
        return layer_api_error_response("terrain_lulc_plain_cluster", e, request=request)


@api_view(["POST"])
@schema(None)
def generate_clart(request):
    print("Inside generate_clart")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        gee_account_id = request.data.get("gee_account_id")
        task = generate_clart_layer.apply_async(
            args=[state, district, block, gee_account_id], queue="nrm"
        )
        asset_id = layer_assets.mws_asset_id(
            state, district, block, "clart_" + _tehsil_suffix(district, block)
        )
        return _task_started_response(
            "generate_clart task initiated", task=task, asset_id=asset_id
        )
    except Exception as e:
        return layer_api_error_response("generate_clart", e, request=request)


@api_view(["POST"])
@schema(None)
def change_detection(request):
    print("Inside change_detection")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        start_year = request.data.get("start_year")
        end_year = request.data.get("end_year")
        gee_account_id = request.data.get("gee_account_id")
        task = get_change_detection.apply_async(
            args=[state, district, block, start_year, end_year, gee_account_id],
            queue="nrm",
        )
        asset_ids = layer_assets.change_detection_asset_ids(
            state, district, block, int(start_year), int(end_year)
        )
        return _task_started_response(
            "change_detection task initiated", task=task, asset_ids=asset_ids
        )
    except Exception as e:
        return layer_api_error_response("change_detection", e, request=request)


@api_view(["POST"])
@schema(None)
def change_detection_vector(request):
    print("Inside change_detection_vector")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        start_year = request.data.get("start_year")
        end_year = request.data.get("end_year")
        gee_account_id = request.data.get("gee_account_id")
        task = vectorise_change_detection.apply_async(
            args=[state, district, block, start_year, end_year, gee_account_id],
            queue="nrm",
        )
        asset_ids = layer_assets.change_detection_vector_asset_ids(
            state, district, block, int(start_year), int(end_year)
        )
        return _task_started_response(
            "change_detection_vector task initiated", task=task, asset_ids=asset_ids
        )
    except Exception as e:
        return layer_api_error_response("change_detection_vector", e, request=request)


@api_view(["POST"])
@schema(None)
def crop_grid(request):
    print("Inside crop_grid api")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        gee_account_id = request.data.get("gee_account_id")
        task = create_crop_grids.apply_async(
            args=[state, district, block, gee_account_id], queue="nrm"
        )
        asset_id = layer_assets.mws_asset_id(
            state,
            district,
            block,
            "crop_grid_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower() + "_with_uid_16ha"),
        )
        return _task_started_response(
            "crop_grid task initiated", task=task, asset_id=asset_id
        )
    except Exception as e:
        return layer_api_error_response("crop_grid", e, request=request)


@api_view(["POST"])
@schema(None)
def mws_drought_causality(request):
    print("Inside Drought Causality API")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        start_year = request.data.get("start_year")
        end_year = request.data.get("end_year")
        gee_account_id = request.data.get("gee_account_id")
        task = drought_causality.apply_async(
            args=[state, district, block, start_year, end_year, gee_account_id],
            queue="nrm",
        )
        asset_id = layer_assets.drought_causality_asset_id(
            state, district, block, int(end_year)
        )
        return _task_started_response(
            "Drought Causality task initiated", task=task, asset_id=asset_id
        )
    except Exception as e:
        return layer_api_error_response("mws_drought_causality", e, request=request)


@api_view(["POST"])
@schema(None)
def tree_health_raster(request):
    print("Inside tree_health_change API")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        start_year = request.data.get("start_year")
        end_year = request.data.get("end_year")
        gee_account_id = request.data.get("gee_account_id")
        tree_health_ccd_raster.apply_async(
            kwargs={
                "state": state,
                "district": district,
                "block": block,
                "start_year": start_year,
                "end_year": end_year,
                "gee_account_id": gee_account_id,
            },
            queue="nrm",
        )
        tree_health_ch_raster.apply_async(
            kwargs={
                "state": state,
                "district": district,
                "block": block,
                "start_year": start_year,
                "end_year": end_year,
                "gee_account_id": gee_account_id,
            },
            queue="nrm",
        )
        tree_health_overall_change_raster.apply_async(
            kwargs={
                "state": state,
                "district": district,
                "block": block,
                "start_year": start_year,
                "end_year": end_year,
                "gee_account_id": gee_account_id,
            },
            queue="nrm",
        )
        asset_ids = layer_assets.tree_health_raster_asset_ids(
            state, district, block, start_year, end_year
        )
        return _task_started_response(
            "tree_health task initiated", asset_ids=asset_ids
        )
    except Exception as e:
        return layer_api_error_response("tree_health_raster", e, request=request)


@api_security_check(allowed_methods="POST")
@schema(None)
def tree_health_vector(request):
    print("Inside Overall_change_vector")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        start_year = request.data.get("start_year")
        end_year = request.data.get("end_year")
        gee_account_id = request.data.get("gee_account_id")

        tree_health_ch_vector.apply_async(
            kwargs={
                "state": state,
                "district": district,
                "block": block,
                "start_year": start_year,
                "end_year": end_year,
                "gee_account_id": gee_account_id,
            },
            queue="nrm",
        )

        tree_health_ccd_vector.apply_async(
            kwargs={
                "state": state,
                "district": district,
                "block": block,
                "start_year": start_year,
                "end_year": end_year,
                "gee_account_id": gee_account_id,
            },
            queue="nrm",
        )

        tree_health_overall_change_vector.apply_async(
            kwargs={
                "state": state,
                "district": district,
                "block": block,
                "gee_account_id": gee_account_id,
            },
            queue="nrm",
        )
        asset_ids = layer_assets.tree_health_vector_asset_ids(
            state, district, block, int(start_year), int(end_year)
        )
        return _task_started_response(
            "Overall_change_vector task initiated", asset_ids=asset_ids
        )
    except Exception as e:
        return layer_api_error_response("tree_health_vector", e, request=request)


@api_view(["POST"])
@schema(None)
def gee_task_status(request):
    print("Inside gee_task_status API.")
    try:
        task_id = request.data.get("task_id")
        response = check_gee_task_status(task_id)
        return Response({"Response": response}, status=status.HTTP_200_OK)
    except Exception as e:
        return layer_api_error_response("gee_task_status", e, request=request)


@api_view(["POST"])
@schema(None)
def stream_order(request):
    print("Inside stream_order_vector api")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        gee_account_id = request.data.get("gee_account_id")
        task = generate_stream_order.apply_async(
            args=[state, district, block, gee_account_id], queue="nrm"
        )
        description = "stream_order_" + valid_gee_text(district) + "_" + valid_gee_text(block)
        asset_id = _build_mws_asset_id(state, district, block, description + "_vector")
        return _task_started_response(
            "stream_order_vector task initiated", task=task, asset_id=asset_id
        )
    except Exception as e:
        return layer_api_error_response("stream_order", e, request=request)


@api_view(["POST"])
@schema(None)
def restoration_opportunity(request):
    print("Inside restoration_opportunity api")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        gee_account_id = request.data.get("gee_account_id")
        task = generate_restoration_opportunity.apply_async(
            args=[state, district, block, gee_account_id], queue="nrm"
        )
        description = "restoration_" + valid_gee_text(district) + "_" + valid_gee_text(block)
        asset_id = _build_mws_asset_id(state, district, block, description + "_vector")
        return _task_started_response(
            "restoration_opportunity task initiated", task=task, asset_id=asset_id
        )
    except Exception as e:
        return layer_api_error_response("restoration_opportunity", e, request=request)


@api_view(["POST"])
@schema(None)
def plantation_site_suitability(request):
    print("Inside plantation_site_suitability API")
    try:
        project_id = request.data.get("project_id")
        state = request.data.get("state").lower() if request.data.get("state") else None
        district = (
            request.data.get("district").lower()
            if request.data.get("district")
            else None
        )
        block = request.data.get("block").lower() if request.data.get("block") else None
        start_year = request.data.get("start_year")
        end_year = request.data.get("end_year")
        gee_account_id = (
            request.data.get("gee_account_id")
            if request.data.get("gee_account_id")
            else None
        )
        task_result = site_suitability.apply(
            args=[
                project_id,
                start_year,
                end_year,
                state,
                district,
                block,
                gee_account_id,
            ]
        )

        if task_result.failed():
            return Response(
                {"error": str(task_result.result)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        return Response(
            {"Success": "Plantation_site_suitability completed"},
            status=status.HTTP_200_OK,
        )
    except Exception as e:
        return layer_api_error_response("plantation_site_suitability", e, request=request)


@api_view(["POST"])
@schema(None)
def aquifer_vector(request):
    print("Inside Aquifer vector layer api")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        gee_account_id = request.data.get("gee_account_id")
        task = generate_aquifer_vector.apply_async(
            args=[state, district, block, gee_account_id], queue="nrm"
        )
        description = (
            "aquifer_vector_" + valid_gee_text(district) + "_" + valid_gee_text(block)
        )
        asset_id = _build_mws_asset_id(state, district, block, description)
        return _task_started_response(
            "aquifer vector task initiated", task=task, asset_id=asset_id
        )
    except Exception as e:
        return layer_api_error_response("aquifer_vector", e, request=request)


@api_view(["POST"])
@schema(None)
def soge_vector(request):
    print("Inside soge vector layer api")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        gee_account_id = request.data.get("gee_account_id")
        task = generate_soge_vector.apply_async(
            args=[state, district, block, gee_account_id], queue="nrm"
        )
        description = "soge_vector_" + valid_gee_text(district) + "_" + valid_gee_text(block)
        asset_id = _build_mws_asset_id(state, district, block, description)
        return _task_started_response(
            "SOGE vector task initiated", task=task, asset_id=asset_id
        )
    except Exception as e:
        return layer_api_error_response("soge_vector", e, request=request)


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
            DATA_DIR,
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
        task = generate_fes_clart_layer.apply_async(
            args=[state, district, block, file_path, gee_account_id],
            queue="nrm",
        )
        asset_id = layer_assets.fes_clart_asset_id(state, district, block)
        return _task_started_response(
            "Fes clart task Initiated", task=task, asset_id=asset_id
        )

    except Exception as e:
        return layer_api_error_response("fes_clart_upload_layer", e, request=request)


@api_view(["POST"])
@schema(None)
def swb_pond_merging(request):
    print("Inside merge_swb_ponds API.")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        gee_account_id = request.data.get("gee_account_id")
        task = merge_swb_ponds.apply_async(
            args=[state, district, block, gee_account_id], queue="nrm"
        )
        asset_id = layer_assets.merge_swb_ponds_asset_id(state, district, block)
        return _task_started_response("Successfully initiated", task=task, asset_id=asset_id)
    except Exception as e:
        return layer_api_error_response("swb_pond_merging", e, request=request)


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
        task = layer_generate_map.apply_async(
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
        asset_ids = [
            layer_assets.mws_filtered_asset_id(state, district, block),
            layer_assets.admin_boundary_asset_id(state, district, block),
        ]
        if end_year is not None:
            asset_ids.append(
                layer_assets.lulc_v3_asset_id(state, district, block, int(end_year))
            )
        return _task_started_response(
            "Successfully initiated",
            task=task,
            asset_ids=asset_ids,
        )
    except Exception as e:
        return layer_api_error_response("generate_layer_in_order", e, request=request)


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
        return layer_api_error_response("layer_status_dashboard", e, request=request)


@api_view(["POST"])
@schema(None)
def generate_lcw(request):
    print("Inside generate_lcw_conflict_data API.")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        gee_account_id = request.data.get("gee_account_id")
        task = generate_lcw_conflict_data.apply_async(
            args=[state, district, block, gee_account_id], queue="nrm"
        )
        asset_id = layer_assets.mws_asset_id(
            state,
            district,
            block,
            _tehsil_suffix(district, block) + "_lcw_conflict",
        )
        return _task_started_response("Successfully initiated", task=task, asset_id=asset_id)
    except Exception as e:
        return layer_api_error_response("generate_lcw", e, request=request)


@api_view(["POST"])
@schema(None)
def generate_agroecological(request):
    print("Inside generate_agroecological_data API.")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        gee_account_id = request.data.get("gee_account_id")
        task = generate_agroecological_data.apply_async(
            args=[state, district, block, gee_account_id], queue="nrm"
        )
        asset_id = layer_assets.mws_asset_id(
            state,
            district,
            block,
            _tehsil_suffix(district, block) + "_agroecological",
        )
        return _task_started_response("Successfully initiated", task=task, asset_id=asset_id)
    except Exception as e:
        return layer_api_error_response("generate_agroecological", e, request=request)


@api_view(["POST"])
@schema(None)
def generate_factory_csr(request):
    print("Inside generate_factory_csr_to_gee API.")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        gee_account_id = request.data.get("gee_account_id")
        task = generate_factory_csr_data.apply_async(
            args=[state, district, block, gee_account_id], queue="nrm"
        )
        asset_id = layer_assets.mws_asset_id(
            state,
            district,
            block,
            _tehsil_suffix(district, block) + "_factory_csr",
        )
        return _task_started_response("Successfully initiated", task=task, asset_id=asset_id)
    except Exception as e:
        return layer_api_error_response("generate_factory_csr", e, request=request)


@api_view(["POST"])
@schema(None)
def generate_green_credit(request):
    print("Inside generate_green_credit_to_gee API.")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        gee_account_id = request.data.get("gee_account_id")
        task = generate_green_credit_data.apply_async(
            args=[state, district, block, gee_account_id], queue="nrm"
        )
        asset_id = layer_assets.mws_asset_id(
            state,
            district,
            block,
            _tehsil_suffix(district, block) + "_green_credit",
        )
        return _task_started_response("Successfully initiated", task=task, asset_id=asset_id)
    except Exception as e:
        return layer_api_error_response("generate_green_credit", e, request=request)


@api_view(["POST"])
@schema(None)
def generate_mining(request):
    print("Inside generate_mining_to_gee API.")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        gee_account_id = request.data.get("gee_account_id")
        task = generate_mining_data.apply_async(
            args=[state, district, block, gee_account_id], queue="nrm"
        )
        asset_id = layer_assets.mws_asset_id(
            state,
            district,
            block,
            _tehsil_suffix(district, block) + "_mining",
        )
        return _task_started_response("Successfully initiated", task=task, asset_id=asset_id)
    except Exception as e:
        return layer_api_error_response("generate_mining", e, request=request)


@api_view(["GET"])
@schema(None)
def get_layers_for_workspace(request):
    print("inside get_layers_of_workspace API")
    try:
        workspace = request.query_params.get("workspace").lower()
        result = get_layers_of_workspace(workspace)
        return Response({"result": result}, status=status.HTTP_200_OK)
    except Exception as e:
        return layer_api_error_response("get_layers_for_workspace", e, request=request)


@api_view(["POST"])
@schema(None)
def generate_natural_depression(request):
    print("Inside generate_natural_depression_to_gee API.")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        gee_account_id = request.data.get("gee_account_id")
        task = generate_natural_depression_data.apply_async(
            args=[state, district, block, gee_account_id], queue="nrm"
        )
        asset_id = layer_assets.natural_depression_asset_id(state, district, block)
        return _task_started_response("Successfully initiated", task=task, asset_id=asset_id)
    except Exception as e:
        return layer_api_error_response("generate_natural_depression", e, request=request)


@api_view(["POST"])
@schema(None)
def generate_distance_nearest_upstream_DL(request):
    print("Inside generate_distance_nearest_upstream_DL_to_gee API.")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        gee_account_id = request.data.get("gee_account_id")
        task = generate_distance_to_nearest_drainage_line.apply_async(
            args=[state, district, block, gee_account_id], queue="nrm"
        )
        asset_id = layer_assets.distance_to_drainage_asset_id(state, district, block)
        return _task_started_response("Successfully initiated", task=task, asset_id=asset_id)
    except Exception as e:
        return layer_api_error_response("generate_distance_nearest_upstream_DL", e, request=request)


@api_view(["POST"])
@schema(None)
def generate_catchment_area_SF(request):
    print("Inside generate_catchment_area_SF_to_gee API.")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        gee_account_id = request.data.get("gee_account_id")
        task = generate_catchment_area_singleflow.apply_async(
            args=[state, district, block, gee_account_id], queue="nrm"
        )
        asset_id = layer_assets.catchment_area_asset_id(state, district, block)
        return _task_started_response("Successfully initiated", task=task, asset_id=asset_id)
    except Exception as e:
        return layer_api_error_response("generate_catchment_area_SF", e, request=request)


@api_view(["POST"])
@schema(None)
def generate_slope_percentage(request):
    print("Inside generate_slope_percentage_to_gee API.")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        gee_account_id = request.data.get("gee_account_id")
        task = generate_slope_percentage_data.apply_async(
            args=[state, district, block, gee_account_id], queue="nrm"
        )
        asset_id = layer_assets.slope_percentage_asset_id(state, district, block)
        return _task_started_response("Successfully initiated", task=task, asset_id=asset_id)
    except Exception as e:
        return layer_api_error_response("generate_slope_percentage", e, request=request)


@api_view(["POST"])
@schema(None)
def generate_ndvi_timeseries(request):
    print("Inside generate_ndvi_timeseries API.")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        start_year = request.data.get("start_year")
        end_year = request.data.get("end_year")
        gee_account_id = request.data.get("gee_account_id")
        mws_count = request.data.get("mws_count") or 150
        chunk_size = request.data.get("chunk_size") or 100

        task = ndvi_timeseries.apply_async(
            kwargs={
                "state": state,
                "district": district,
                "block": block,
                "start_year": start_year,
                "end_year": end_year,
                "gee_account_id": gee_account_id,
                "mws_count": mws_count,
                "chunk_size": chunk_size,
            },
            queue="nrm",
        )
        asset_suffix = valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
        asset_id = _build_mws_asset_id(state, district, block, "ndvi_timeseries_" + asset_suffix)
        return _task_started_response(
            "Successfully initiated generate_ndvi_timeseries", task=task, asset_id=asset_id
        )
    except Exception as e:
        return layer_api_error_response("generate_ndvi_timeseries", e, request=request)


@api_view(["POST"])
@schema(None)
def generate_zoi_to_gee(request):
    print("Inside generate zoi layers")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        gee_account_id = request.data.get("gee_account_id")
        start_date = request.data.get("start_date")
        end_date = request.data.get("end_date")

        if bool(start_date) ^ bool(end_date):
            return Response(
                {"error": "Pass both start_date and end_date together in YYYY-MM-DD format."},
                status=status.HTTP_400_BAD_REQUEST,
            )
        if start_date and end_date:
            try:
                datetime.strptime(start_date, "%Y-%m-%d")
                datetime.strptime(end_date, "%Y-%m-%d")
            except ValueError:
                return Response(
                    {"error": "start_date and end_date must be in YYYY-MM-DD format."},
                    status=status.HTTP_400_BAD_REQUEST,
                )

        task = generate_zoi.apply_async(
            kwargs={
                "state": state,
                "district": district,
                "block": block,
                "gee_account_id": gee_account_id,
                "start_date": start_date,
                "end_date": end_date,
            },
            queue="waterbody",
        )
        asset_id = get_gee_dir_path(
            [state, district, block], asset_path=GEE_PATHS["WATERBODY"]["GEE_ASSET_PATH"]
        ) + _tehsil_suffix(district, block)
        return _task_started_response("Successfully initiated", task=task, asset_id=asset_id)
    except Exception as e:
        return layer_api_error_response("generate_zoi_to_gee", e, request=request)


@api_view(["POST"])
@schema(None)
def generate_mws_connectivity(request):
    print("Inside generate_mws_connectivity_to_gee API.")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        gee_account_id = request.data.get("gee_account_id")
        task = generate_mws_connectivity_data.apply_async(
            args=[state, district, block, gee_account_id], queue="nrm"
        )
        asset_id = layer_assets.mws_connectivity_asset_id(state, district, block)
        return _task_started_response("Successfully initiated", task=task, asset_id=asset_id)
    except Exception as e:
        return layer_api_error_response("generate_mws_connectivity", e, request=request)


@api_view(["POST"])
@schema(None)
def generate_mws_centroid(request):
    print("Inside generate_mws_centroid API.")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        gee_account_id = request.data.get("gee_account_id")
        task = generate_mws_centroid_data.apply_async(
            args=[state, district, block, gee_account_id], queue="nrm"
        )
        asset_id = layer_assets.mws_centroid_asset_id(state, district, block)
        return _task_started_response("Successfully initiated", task=task, asset_id=asset_id)
    except Exception as e:
        return layer_api_error_response("generate_mws_centroid", e, request=request)


@api_view(["POST"])
@schema(None)
def generate_facilities_proximity(request):
    print("Inside generate_facilities_proximity API.")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        gee_account_id = request.data.get("gee_account_id")
        task = generate_facilities_proximity_task.apply_async(
            args=[state, district, block, gee_account_id], queue="nrm"
        )
        description = (
            "facilities_proximity_"
            + valid_gee_text(district.lower())
            + "_"
            + valid_gee_text(block.lower())
        )
        asset_id = _build_mws_asset_id(state, district, block, description)
        return _task_started_response(
            "Successfully initiated", task=task, asset_id=asset_id
        )
    except Exception as e:
        return layer_api_error_response("generate_facilities_proximity", e, request=request)


@api_view(["POST"])
@schema(None)
def generate_stac_collection(request):
    try:
        state = request.data.get("state")
        district = request.data.get("district")
        block = request.data.get("block")
        layer_name = request.data.get("layer_name")
        layer_type = request.data.get("layer_type")
        start_year = request.data.get("start_year", "")
        upload_to_s3 = request.data.get("upload_to_s3", False)
        overwrite = request.data.get("overwrite", False)
        overwrite_metadata = request.data.get("overwrite_metadata", False)

        if not all([state, district, block, layer_name, layer_type]):
            return Response(
                {
                    "error": "state, district, block, layer_name, and layer_type are required"
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        if layer_type not in ("raster", "vector"):
            return Response(
                {"error": "layer_type must be 'raster' or 'vector'"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        _make_stac_task().apply_async(
            kwargs={
                "layer_type": layer_type,
                "state": state,
                "district": district,
                "block": block,
                "layer_name": layer_name,
                "start_year": start_year,
                "upload_to_s3": upload_to_s3,
                "overwrite": overwrite,
                "overwrite_metadata": overwrite_metadata,
            },
            queue="nrm",
        )
        return Response(
            {"Success": "STAC collection generation initiated"},
            status=status.HTTP_200_OK,
        )
    except Exception as e:
        return layer_api_error_response("generate_stac_collection", e, request=request)


# ---------------------------------------------------------------------------
# STAC catalog read helpers
# ---------------------------------------------------------------------------

_STAC_ROOT = os.path.join(
    DATA_DIR,
    "STAC_specs",
    "CorestackCatalogs_merged_collection",
)
_TEHSIL = "tehsil_wise"


def _read_json(path):
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


@api_view(["GET"])
@schema(None)
def get_stac_catalog(request):
    state = request.query_params.get("state", "").strip()
    district = request.query_params.get("district", "").strip()
    block = request.query_params.get("block", "").strip()

    base = STACConfig().stac_files_dir

    if state:
        state = sanitize_text(state.lower())
    if district:
        district = sanitize_text(district.lower())
    if block:
        block = sanitize_text(block.lower())

    if not state:
        path = os.path.join(base, "catalog.json")
    elif not district:
        path = os.path.join(base, _TEHSIL, state, "collection.json")
    elif not block:
        path = os.path.join(base, _TEHSIL, state, district, "collection.json")
    else:
        path = os.path.join(base, _TEHSIL, state, district, block, "collection.json")

    data = _read_json(path)
    if data is None:
        return Response(
            {"error": f"Catalog not found at requested scope"},
            status=status.HTTP_404_NOT_FOUND,
        )

    from django.http import JsonResponse

    return JsonResponse(data, content_type="application/geo+json")


@api_view(["GET"])
@schema(None)
def stac_root_catalog(request):
    data = _read_json(os.path.join(_STAC_ROOT, "catalog.json"))
    if data is None:
        return Response(
            {"error": "Root catalog not found"}, status=status.HTTP_404_NOT_FOUND
        )
    return Response(data)


@api_view(["GET"])
@schema(None)
def stac_state_collection(request, state):
    path = os.path.join(_STAC_ROOT, _TEHSIL, state.lower(), "collection.json")
    data = _read_json(path)
    if data is None:
        return Response(
            {"error": f"State collection not found: {state}"},
            status=status.HTTP_404_NOT_FOUND,
        )
    return Response(data)


@api_view(["GET"])
@schema(None)
def stac_district_collection(request, state, district):
    path = os.path.join(
        _STAC_ROOT, _TEHSIL, state.lower(), district.lower(), "collection.json"
    )
    data = _read_json(path)
    if data is None:
        return Response(
            {"error": f"District collection not found: {district}"},
            status=status.HTTP_404_NOT_FOUND,
        )
    return Response(data)


@api_view(["GET"])
@schema(None)
def stac_block_collection(request, state, district, block):
    path = os.path.join(
        _STAC_ROOT,
        _TEHSIL,
        state.lower(),
        district.lower(),
        block.lower(),
        "collection.json",
    )
    data = _read_json(path)
    if data is None:
        return Response(
            {"error": f"Block collection not found: {block}"},
            status=status.HTTP_404_NOT_FOUND,
        )
    return Response(data)


@api_view(["GET"])
@schema(None)
def stac_item(request, state, district, block, item_id):
    path = os.path.join(
        _STAC_ROOT,
        _TEHSIL,
        state.lower(),
        district.lower(),
        block.lower(),
        item_id,
        f"{item_id}.json",
    )
    data = _read_json(path)
    if data is None:
        return Response(
            {"error": f"Item not found: {item_id}"}, status=status.HTTP_404_NOT_FOUND
        )
    return Response(data)


@api_view(["POST"])
@authentication_classes([])
@permission_classes([AllowAny])
@schema(None)
def update_layer_sync_remote(request):
    """
    Called by a local compute instance to update sync/STAC flags on a layer
    record in this (prod) backend.
    """

    api_key = getattr(settings, "PROD_BACKEND_API_KEY", "")
    if api_key and request.headers.get("X-Api-Key") != api_key:
        return Response({"error": "Unauthorized"}, status=status.HTTP_401_UNAUTHORIZED)

    try:
        d = request.data
        layer_id = d.get("layer_id")
        if layer_id is None:
            return Response(
                {"error": "layer_id is required"}, status=status.HTTP_400_BAD_REQUEST
            )

        result = update_layer_sync_status(
            layer_id=layer_id,
            sync_to_geoserver=d.get("sync_to_geoserver"),
            is_stac_specs_generated=d.get("is_stac_specs_generated"),
        )
        return Response({"layer_id": result}, status=status.HTTP_200_OK)
    except Exception as e:
        return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
@authentication_classes([])
@permission_classes([AllowAny])
@schema(None)
def sync_layer_remote(request):
    """
    Called by a local compute instance to persist a layer record on this (prod) backend.
    Validates the request API key against PROD_BACKEND_API_KEY before writing.
    """
    api_key = getattr(settings, "PROD_BACKEND_API_KEY", "")
    if api_key and request.headers.get("X-Api-Key") != api_key:
        return Response({"error": "Unauthorized"}, status=status.HTTP_401_UNAUTHORIZED)

    try:
        d = request.data
        layer_id = save_layer_info_to_db(
            state=d["state"],
            district=d["district"],
            block=d["block"],
            layer_name=d["layer_name"],
            asset_id=d["asset_id"],
            dataset_name=d["dataset_name"],
            sync_to_geoserver=d.get("sync_to_geoserver", False),
            layer_version=d.get("layer_version", "1.0"),
            algorithm=d.get("algorithm"),
            algorithm_version=d.get("algorithm_version", "1.0"),
            misc=d.get("misc"),
            is_override=d.get("is_override", False),
        )
        if layer_id is None:
            return Response(
                {
                    "error": "Failed to save layer — check state/district/block exist on this server."
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        return Response({"layer_id": layer_id}, status=status.HTTP_201_CREATED)
    except KeyError as e:
        return Response(
            {"error": f"Missing field: {e}"}, status=status.HTTP_400_BAD_REQUEST
        )
    except Exception as e:
        return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["GET"])
@schema(None)
def missing_layers(request):
    try:
        workspace = request.query_params.get("workspace").lower()
        result = check_missing_layers(workspace)
        return Response({"result": result}, status=status.HTTP_200_OK)
    except Exception as e:
        return layer_api_error_response("missing_layers", e, request=request)


def _auto_discover_computing_api_views(namespace):
    """
    Auto-discover request handlers in this module and wrap them once.
    This is intentionally broad so new APIs are automatically covered.
    """
    discovered = []
    for name, fn in namespace.items():
        if name.startswith("_") or not callable(fn):
            continue
        if getattr(fn, "__module__", None) != __name__:
            continue
        if getattr(fn, "__layer_generation_sync_wrapped__", False):
            continue
        try:
            target = inspect.unwrap(fn)
            sig = inspect.signature(target)
        except (OSError, TypeError, ValueError):
            continue

        params = list(sig.parameters.values())
        if len(params) == 0:
            continue
        first_param = params[0]
        if first_param.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        ) and first_param.name == "request":
            discovered.append(name)
    return discovered


for _view_name in _auto_discover_computing_api_views(globals()):
    wrapped = sync_layer_generation_if_enabled(
        layer_generation_api_logging(globals()[_view_name])
    )
    wrapped.__layer_generation_sync_wrapped__ = True
    globals()[_view_name] = wrapped

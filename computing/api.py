from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status

from computing.change_detection.change_detection_vector import (
    vectorise_change_detection,
)
from .lulc.lulc_vector import vectorise_lulc
from .lulc.v4.classify_raster import classify_raster
from .lulc.v4.lulc_v4 import generate_lulc_v4
from .lulc.v4.time_series import time_series
from .misc.restoration_opportunity import generate_restoration_opportunity
from .misc.stream_order import generate_stream_order_vector
from .utils import (
    Geoserver,
    kml_to_shp,
)
from utilities.gee_utils import download_gee_layer, check_gee_task_status
from django.core.files.storage import FileSystemStorage
from utilities.constants import KML_PATH
from .mws.mws import mws_layer
from .cropping_intensity.cropping_intensity import generate_cropping_intensity
from .surface_water_bodies.swb1 import generate_swb_layer
from .drought.drought import calculate_drought
from .terrain_descriptor.terrain_clusters import generate_terrain_clusters
from .terrain_descriptor.terrain_raster import terrain_raster
from computing.misc.drainage_lines import clip_drainage_lines
from computing.clart.drainage_density import drainage_density
from .lulc_X_terrain.lulc_on_slope_cluster import lulc_on_slope_cluster
from .lulc_X_terrain.lulc_on_plain_cluster import lulc_on_plain_cluster
from .clart.lithology import generate_lithology_layer
from .clart.clart import generate_clart_layer
from .misc.admin_boundary import generate_tehsil_shape_file_data
from .misc.nrega import clip_nrega_district_block
from computing.change_detection.change_detection import get_change_detection
from .lulc.lulc_v3_clip_river_basin import lulc_river_basin
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


@api_view(["POST"])
def generate_admin_boundary(request):
    print("Inside generate_block_layer API.")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        generate_tehsil_shape_file_data.apply_async(
            args=[state, district, block], queue="nrm"
        )
        return Response(
            {"Success": "Successfully initiated"}, status=status.HTTP_200_OK
        )
    except Exception as e:
        print("Exception in generate_block_layer api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
def generate_nrega_layer(request):
    print("Inside generate_nrega_layer API.")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        clip_nrega_district_block.apply_async(
            args=[state, district, block], queue="nrm"
        )
        return Response(
            {"Success": "Successfully initiated"}, status=status.HTTP_200_OK
        )
    except Exception as e:
        print("Exception in generate_nrega_layer api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
def generate_drainage_layer(request):
    print("Inside generate_drainage_layer API.")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        clip_drainage_lines.apply_async(args=[state, district, block], queue="nrm")
        return Response(
            {"Success": "Successfully initiated"}, status=status.HTTP_200_OK
        )
    except Exception as e:
        print("Exception in generate_drainage_layer api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
def generate_drainage_density(request):
    print("Inside generate_drainage_density API.")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        drainage_density.apply_async(args=[state, district, block], queue="nrm")
        return Response(
            {"Success": "Successfully initiated"}, status=status.HTTP_200_OK
        )
    except Exception as e:
        print("Exception in generate_drainage_layer api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
def generate_lithology(request):
    print("Inside generate_lithology API.")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        generate_lithology_layer.apply_async(args=[state, district], queue="nrm")
        return Response(
            {"Success": "Successfully initiated"}, status=status.HTTP_200_OK
        )
    except Exception as e:
        print("Exception in generate_lithology api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
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


@api_view(["POST"])
def generate_mws_layer(request):
    print("Inside generate_mws_layer")
    try:
        state = request.data.get("state")
        district = request.data.get("district")
        block = request.data.get("block")
        start_year = int(request.data.get("start_year"))
        end_year = int(request.data.get("end_year"))
        mws_layer.apply_async(
            args=[state, district, block, start_year, end_year, False], queue="nrm"
        )
        return Response(
            {"Success": "Successfully initiated"}, status=status.HTTP_200_OK
        )
    except Exception as e:
        print("Exception in generate_mws_layer api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
def generate_well_depth(request):
    print("Inside generate_well_depth")
    try:
        state = request.data.get("state")
        district = request.data.get("district")
        block = request.data.get("block")
        start_year = int(request.data.get("start_year"))
        end_year = int(request.data.get("end_year"))
        mws_layer.apply_async(
            args=[state, district, block, start_year, end_year, True], queue="nrm"
        )
        return Response(
            {"Success": "Successfully initiated"}, status=status.HTTP_200_OK
        )
    except Exception as e:
        print("Exception in generate_well_depth api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
def lulc_v3_river_basin(request):
    print("Inside generate_lulc_v3")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        start_year = request.data.get("start_year")
        end_year = request.data.get("end_year")
        lulc_river_basin.apply_async(
            args=[state, district, block, start_year, end_year], queue="nrm"
        )
        return Response({"Success": "LULC task initiated"}, status=status.HTTP_200_OK)
    except Exception as e:
        print("Exception in generate_lulc_layer api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
def lulc_vector(request):
    print("Inside lulc_vector")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        start_year = request.data.get("start_year")
        end_year = request.data.get("end_year")
        vectorise_lulc.apply_async(
            args=[state, district, block, start_year, end_year], queue="nrm"
        )
        return Response(
            {"Success": "lulc_vector task initiated"},
            status=status.HTTP_200_OK,
        )
    except Exception as e:
        print("Exception in lulc_vector api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
def lulc_v4(request):
    print("Inside lulc_time_series")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        start_year = request.data.get("start_year")
        end_year = request.data.get("end_year")
        generate_lulc_v4.apply_async(
            args=[state, district, block, start_year, end_year], queue="nrm"
        )
        return Response(
            {"Success": "lulc_time_series task initiated"},
            status=status.HTTP_200_OK,
        )
    except Exception as e:
        print("Exception in lulc_time_series api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
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


@api_view(["POST"])
def generate_ci_layer(request):
    print("Inside generate_cropping_intensity_layer")
    try:
        state = request.data.get("state")
        district = request.data.get("district")
        block = request.data.get("block")
        start_year = request.data.get("start_year")
        end_year = request.data.get("end_year")
        generate_cropping_intensity.apply_async(
            args=[state, district, block, start_year, end_year], queue="nrm"
        )
        return Response(
            {"Success": "Cropping Intensity task initiated"},
            status=status.HTTP_200_OK,
        )
    except Exception as e:
        print("Exception in generate_cropping_intensity_layer api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
def generate_swb(request):
    print("Inside generate_swf")
    try:
        state = request.data.get("state")
        district = request.data.get("district")
        block = request.data.get("block")
        start_year = request.data.get("start_year")
        end_year = request.data.get("end_year")
        generate_swb_layer.apply_async(
            args=[state, district, block, start_year, end_year], queue="nrm"
        )
        return Response(
            {"Success": "Generate swb task initiated"}, status=status.HTTP_200_OK
        )
    except Exception as e:
        print("Exception in generate_swf api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
def generate_drought_layer(request):
    print("Inside generate_drought_layer")
    try:
        state = request.data.get("state")
        district = request.data.get("district")
        block = request.data.get("block")
        start_year = request.data.get("start_year")
        end_year = request.data.get("end_year")
        calculate_drought.apply_async(
            args=[state, district, block, start_year, end_year], queue="nrm"
        )
        return Response(
            {"Success": "generate_drought_layer task initiated"},
            status=status.HTTP_200_OK,
        )
    except Exception as e:
        print("Exception in generate_drought_layer api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
def generate_terrain_descriptor(request):
    print("Inside generate_terrain_descriptor")
    try:
        state = request.data.get("state")
        district = request.data.get("district")
        block = request.data.get("block")
        generate_terrain_clusters.apply_async(
            args=[state, district, block], queue="nrm"
        )
        return Response(
            {"Success": "generate_terrain_descriptor task initiated"},
            status=status.HTTP_200_OK,
        )
    except Exception as e:
        print("Exception in generate_terrain_descriptor api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
def generate_terrain_raster(request):
    print("Inside generate_terrain_raster")
    try:
        state = request.data.get("state")
        district = request.data.get("district")
        block = request.data.get("block")
        terrain_raster.apply_async(args=[state, district, block], queue="nrm")
        return Response(
            {"Success": "generate_terrain_raster task initiated"},
            status=status.HTTP_200_OK,
        )
    except Exception as e:
        print("Exception in generate_terrain_raster api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
def terrain_lulc_slope_cluster(request):
    print("Inside terrain_lulc_slope_cluster")
    try:
        state = request.data.get("state")
        district = request.data.get("district")
        block = request.data.get("block")
        start_year = request.data.get("start_year")
        end_year = request.data.get("end_year")
        lulc_on_slope_cluster.apply_async(
            args=[state, district, block, start_year, end_year], queue="nrm"
        )
        return Response(
            {"Success": "terrain_lulc_slope_cluster task initiated"},
            status=status.HTTP_200_OK,
        )
    except Exception as e:
        print("Exception in terrain_lulc_slope_cluster api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
def terrain_lulc_plain_cluster(request):
    print("Inside terrain_lulc_plain_cluster")
    try:
        state = request.data.get("state")
        district = request.data.get("district")
        block = request.data.get("block")
        start_year = request.data.get("start_year")
        end_year = request.data.get("end_year")
        lulc_on_plain_cluster.apply_async(
            args=[state, district, block, start_year, end_year], queue="nrm"
        )
        return Response(
            {"Success": "terrain_lulc_plain_cluster task initiated"},
            status=status.HTTP_200_OK,
        )
    except Exception as e:
        print("Exception in terrain_lulc_plain_cluster api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
def generate_clart(request):
    print("Inside generate_clart")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        generate_clart_layer.apply_async(args=[state, district, block], queue="nrm")
        return Response(
            {"Success": "generate_clart task initiated"},
            status=status.HTTP_200_OK,
        )
    except Exception as e:
        print("Exception in generate_clart api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
def change_detection(request):
    print("Inside change_detection")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        start_year = request.data.get("start_year")
        end_year = request.data.get("end_year")
        get_change_detection.apply_async(
            args=[state, district, block, start_year, end_year], queue="nrm"
        )
        return Response(
            {"Success": "change_detection task initiated"},
            status=status.HTTP_200_OK,
        )
    except Exception as e:
        print("Exception in change_detection api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
def change_detection_vector(request):
    print("Inside change_detection_vector")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        vectorise_change_detection.apply_async(
            args=[state, district, block], queue="nrm"
        )
        return Response(
            {"Success": "change_detection_vector task initiated"},
            status=status.HTTP_200_OK,
        )
    except Exception as e:
        print("Exception in change_detection_vector api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
def crop_grid(request):
    print("Inside crop_grid api")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        create_crop_grids.apply_async(args=[state, district, block], queue="nrm")
        return Response(
            {"Success": "crop_grid task initiated"},
            status=status.HTTP_200_OK,
        )
    except Exception as e:
        print("Exception in crop_grid api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
def mws_drought_causality(request):
    print("Inside Drought Causality API")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        start_year = request.data.get("start_year")
        end_year = request.data.get("end_year")
        drought_causality.apply_async(
            args=[state, district, block, start_year, end_year], queue="nrm"
        )
        return Response(
            {"Success": "Drought Causality task initiated"},
            status=status.HTTP_200_OK,
        )
    except Exception as e:
        print("Exception in Drought Causality api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
def tree_health_raster(request):
    print("Inside tree_health_change API")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        start_year = request.data.get("start_year")
        end_year = request.data.get("end_year")
        tree_health_ccd_raster.apply_async(
            args=[state, district, block, start_year, end_year], queue="nrm"
        )
        tree_health_ch_raster.apply_async(
            args=[state, district, block, start_year, end_year], queue="nrm"
        )
        tree_health_overall_change_raster.apply_async(
            args=[state, district, block], queue="nrm"
        )
        return Response(
            {"Success": "tree_health task initiated"},
            status=status.HTTP_200_OK,
        )
    except Exception as e:
        print("Exception in change_detection api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
def tree_health_vector(request):
    print("Inside Overall_change_vector")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        start_year = request.data.get("start_year")
        end_year = request.data.get("end_year")
        tree_health_overall_change_vector.apply_async(
            args=[state, district, block], queue="nrm"
        )
        tree_health_ch_vector.apply_async(
            args=[state, district, block, start_year, end_year], queue="nrm"
        )
        tree_health_ccd_vector.apply_async(
            args=[state, district, block, start_year, end_year], queue="nrm"
        )
        return Response(
            {"Success": "Overall_change_vector task initiated"},
            status=status.HTTP_200_OK,
        )
    except Exception as e:
        print("Exception in Overall_change_vector api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
def gee_task_status(request):
    print("Inside gee_task_status API.")
    try:
        task_id = request.data.get("task_id")
        response = check_gee_task_status(task_id)
        return Response({"Response": response}, status=status.HTTP_200_OK)
    except Exception as e:
        print("Exception in gee_task_status api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
def stream_order_vector(request):
    print("Inside stream_order_vector api")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        generate_stream_order_vector.apply_async(
            args=[state, district, block], queue="nrm"
        )
        return Response(
            {"Success": "stream_order_vector task initiated"},
            status=status.HTTP_200_OK,
        )
    except Exception as e:
        print("Exception in stream_order_vector api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
def restoration_opportunity(request):
    print("Inside restoration_opportunity api")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        generate_restoration_opportunity.apply_async(
            args=[state, district, block], queue="nrm"
        )
        return Response(
            {"Success": "restoration_opportunity task initiated"},
            status=status.HTTP_200_OK,
        )
    except Exception as e:
        print("Exception in restoration_opportunity api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
def plantation_site_suitability(request):
    print("Inside plantation_site_suitability API")
    try:
        project_id = request.data.get("project_id")
        state = request.data.get("state").lower()
        start_year = request.data.get("start_year")
        end_year = request.data.get("end_year")
        site_suitability.apply_async(
            args=[project_id, state, start_year, end_year], queue="nrm"
        )
        return Response(
            {"Success": "Plantation_site_suitability task initiated"},
            status=status.HTTP_200_OK,
        )
    except Exception as e:
        print("Exception in Plantation_site_suitability api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
def aquifer_vector(request):
    print("Inside Aquifer vector layer api")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        generate_aquifer_vector.apply_async(
            args=[state, district, block], queue="nrm"
        )
        return Response(
            {"Success": "aquifer vector task initiated"},
            status=status.HTTP_200_OK,
        )
    except Exception as e:
        print("Exception in aquifer vector api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
def soge_vector(request):
    print("Inside soge vector layer api")
    try:
        state = request.data.get("state").lower()
        district = request.data.get("district").lower()
        block = request.data.get("block").lower()
        generate_soge_vector.apply_async(
            args=[state, district, block], queue="nrm"
        )
        return Response(
            {"Success": "SOGE vector task initiated"},
            status=status.HTTP_200_OK,
        )
    except Exception as e:
        print("Exception in SOGE vector api :: ", e)
        return Response({"Exception": e}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


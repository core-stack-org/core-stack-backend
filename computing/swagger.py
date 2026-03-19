from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from rest_framework import serializers


class LocationRequestSerializer(serializers.Serializer):
    state = serializers.CharField(help_text="State name.")
    district = serializers.CharField(help_text="District name.")
    block = serializers.CharField(help_text="Block or tehsil name.")


class LocationWithGeeRequestSerializer(LocationRequestSerializer):
    gee_account_id = serializers.CharField(help_text="GEE service account identifier.")


class LocationYearRangeRequestSerializer(LocationWithGeeRequestSerializer):
    start_year = serializers.IntegerField(help_text="Start year for the compute range.")
    end_year = serializers.IntegerField(help_text="End year for the compute range.")


class LocationYearRangeVersionRequestSerializer(LocationYearRangeRequestSerializer):
    version = serializers.ChoiceField(
        choices=["v2", "v3"],
        help_text="LULC algorithm version to run.",
    )


class WorkspaceCreateRequestSerializer(serializers.Serializer):
    workspace_name = serializers.CharField(help_text="Geoserver workspace name.")


class DeleteLayerRequestSerializer(serializers.Serializer):
    workspace = serializers.CharField(help_text="Geoserver workspace name.")
    layer_name = serializers.CharField(help_text="Layer name to delete.")


class RiverBasinLulcRequestSerializer(serializers.Serializer):
    basin_object_id = serializers.CharField(
        help_text="River basin object ID from the CGWB basin dataset."
    )
    start_year = serializers.IntegerField(help_text="Start year for the compute range.")
    end_year = serializers.IntegerField(help_text="End year for the compute range.")


class GeeTaskStatusRequestSerializer(serializers.Serializer):
    task_id = serializers.CharField(help_text="GEE task ID to inspect.")


class PlantationSiteSuitabilityRequestSerializer(serializers.Serializer):
    project_id = serializers.IntegerField(help_text="Project identifier.")
    start_year = serializers.IntegerField(help_text="Start year for the compute range.")
    end_year = serializers.IntegerField(help_text="End year for the compute range.")
    state = serializers.CharField(required=False, allow_null=True, allow_blank=True)
    district = serializers.CharField(required=False, allow_null=True, allow_blank=True)
    block = serializers.CharField(required=False, allow_null=True, allow_blank=True)
    gee_account_id = serializers.CharField(
        required=False, allow_null=True, allow_blank=True
    )


class LayerInOrderRequestSerializer(LocationWithGeeRequestSerializer):
    map = serializers.JSONField(
        help_text="Ordered map or layer dependency payload to execute."
    )
    start_year = serializers.IntegerField(required=False)
    end_year = serializers.IntegerField(required=False)


TASK_ACK_RESPONSE = openapi.Response(
    description="The request was accepted and the Celery job was queued.",
    schema=openapi.Schema(
        type=openapi.TYPE_OBJECT,
        properties={
            "Success": openapi.Schema(type=openapi.TYPE_STRING),
            "success": openapi.Schema(type=openapi.TYPE_STRING),
            "Response": openapi.Schema(type=openapi.TYPE_STRING),
            "result": openapi.Schema(type=openapi.TYPE_OBJECT),
        },
        additional_properties=openapi.Schema(type=openapi.TYPE_STRING),
    ),
)

ERROR_RESPONSE = openapi.Response(
    description="The request failed validation or the compute job could not be queued.",
    schema=openapi.Schema(
        type=openapi.TYPE_OBJECT,
        additional_properties=openapi.Schema(type=openapi.TYPE_STRING),
    ),
)

WORKSPACE_QUERY_PARAM = openapi.Parameter(
    "workspace",
    openapi.IN_QUERY,
    description="Workspace name to inspect.",
    type=openapi.TYPE_STRING,
    required=True,
)

KML_UPLOAD_PARAMETERS = [
    openapi.Parameter(
        "state",
        openapi.IN_FORM,
        description="State name.",
        type=openapi.TYPE_STRING,
        required=True,
    ),
    openapi.Parameter(
        "district",
        openapi.IN_FORM,
        description="District name.",
        type=openapi.TYPE_STRING,
        required=True,
    ),
    openapi.Parameter(
        "block",
        openapi.IN_FORM,
        description="Block or tehsil name.",
        type=openapi.TYPE_STRING,
        required=True,
    ),
    openapi.Parameter(
        "file",
        openapi.IN_FORM,
        description="KML file to upload.",
        type=openapi.TYPE_FILE,
        required=True,
    ),
]

FES_CLART_UPLOAD_PARAMETERS = [
    openapi.Parameter(
        "state",
        openapi.IN_FORM,
        description="State name.",
        type=openapi.TYPE_STRING,
        required=True,
    ),
    openapi.Parameter(
        "district",
        openapi.IN_FORM,
        description="District name.",
        type=openapi.TYPE_STRING,
        required=True,
    ),
    openapi.Parameter(
        "block",
        openapi.IN_FORM,
        description="Block or tehsil name.",
        type=openapi.TYPE_STRING,
        required=True,
    ),
    openapi.Parameter(
        "gee_account_id",
        openapi.IN_FORM,
        description="GEE service account identifier.",
        type=openapi.TYPE_STRING,
        required=True,
    ),
    openapi.Parameter(
        "clart_file",
        openapi.IN_FORM,
        description="Clart file to upload.",
        type=openapi.TYPE_FILE,
        required=True,
    ),
]

DEFAULT_SECURITY = [{"Bearer": []}]
DEFAULT_TAGS = ["computing"]
DEFAULT_RESPONSES = {
    200: TASK_ACK_RESPONSE,
    201: TASK_ACK_RESPONSE,
    400: ERROR_RESPONSE,
    401: ERROR_RESPONSE,
    500: ERROR_RESPONSE,
}


def _titleize(name):
    return name.replace("_", " ").strip().title()


def _build_post_decorator(request_body=None, *, summary=None, description=None):
    kwargs = {
        "method": "post",
        "tags": DEFAULT_TAGS,
        "security": DEFAULT_SECURITY,
        "responses": DEFAULT_RESPONSES,
        "operation_summary": summary,
    }
    if description:
        kwargs["operation_description"] = description
    if request_body is not None:
        kwargs["request_body"] = request_body
    return swagger_auto_schema(**kwargs)


def _build_form_post_decorator(manual_parameters, *, summary=None, description=None):
    return swagger_auto_schema(
        method="post",
        tags=DEFAULT_TAGS,
        security=DEFAULT_SECURITY,
        responses=DEFAULT_RESPONSES,
        operation_summary=summary,
        operation_description=description,
        manual_parameters=manual_parameters,
        consumes=["multipart/form-data"],
    )


def _build_get_decorator(*, summary=None, description=None, manual_parameters=None):
    kwargs = {
        "method": "get",
        "tags": DEFAULT_TAGS,
        "security": DEFAULT_SECURITY,
        "responses": DEFAULT_RESPONSES,
        "operation_summary": summary,
    }
    if description:
        kwargs["operation_description"] = description
    if manual_parameters:
        kwargs["manual_parameters"] = manual_parameters
    return swagger_auto_schema(**kwargs)


def _apply(namespace, name, decorator):
    view = namespace.get(name)
    if view is not None:
        namespace[name] = decorator(view)


def _apply_many(namespace, names, decorator_factory, serializer):
    for name in names:
        _apply(
            namespace,
            name,
            decorator_factory(serializer, summary=_titleize(name)),
        )


def apply_computing_swagger_schemas(namespace):
    _apply_many(
        namespace,
        [
            "generate_admin_boundary",
            "generate_nrega_layer",
            "generate_drainage_layer",
            "generate_mws_layer",
            "generate_terrain_descriptor",
            "generate_terrain_raster",
            "generate_clart",
            "crop_grid",
            "stream_order",
            "restoration_opportunity",
            "aquifer_vector",
            "soge_vector",
            "swb_pond_merging",
            "generate_lcw",
            "generate_agroecological",
            "generate_factory_csr",
            "generate_green_credit",
            "generate_mining",
            "generate_natural_depression",
            "generate_distance_nearest_upstream_DL",
            "generate_catchment_area_SF",
            "generate_slope_percentage",
            "generate_zoi_to_gee",
            "generate_mws_connectivity",
            "generate_mws_centroid",
            "generate_facilities_proximity",
        ],
        _build_post_decorator,
        LocationWithGeeRequestSerializer,
    )

    _apply_many(
        namespace,
        [
            "generate_fortnightly_hydrology",
            "generate_annual_hydrology",
            "lulc_v3",
            "lulc_vector",
            "lulc_v4",
            "generate_ci_layer",
            "generate_swb",
            "generate_drought_layer",
            "terrain_lulc_slope_cluster",
            "terrain_lulc_plain_cluster",
            "change_detection",
            "change_detection_vector",
            "mws_drought_causality",
            "tree_health_raster",
            "tree_health_vector",
            "generate_ndvi_timeseries",
        ],
        _build_post_decorator,
        LocationYearRangeRequestSerializer,
    )

    _apply_many(
        namespace,
        [
            "get_gee_layer",
            "lulc_farm_boundary",
            "ponds_compute",
            "wells_compute",
            "layer_status_dashboard",
        ],
        _build_post_decorator,
        LocationRequestSerializer,
    )

    _apply(
        namespace,
        "create_workspace",
        _build_post_decorator(
            WorkspaceCreateRequestSerializer,
            summary="Create Workspace",
            description="Create a Geoserver workspace from Swagger UI.",
        ),
    )
    _apply(
        namespace,
        "delete_layer",
        _build_post_decorator(
            DeleteLayerRequestSerializer,
            summary="Delete Layer",
            description="Delete a layer from a Geoserver workspace.",
        ),
    )
    _apply(
        namespace,
        "lulc_for_tehsil",
        _build_post_decorator(
            LocationYearRangeVersionRequestSerializer,
            summary="Lulc For Tehsil",
            description="Queue tehsil LULC generation for v2 or v3.",
        ),
    )
    _apply(
        namespace,
        "lulc_v2_river_basin",
        _build_post_decorator(
            RiverBasinLulcRequestSerializer,
            summary="Lulc V2 River Basin",
        ),
    )
    _apply(
        namespace,
        "lulc_v3_river_basin",
        _build_post_decorator(
            RiverBasinLulcRequestSerializer,
            summary="Lulc V3 River Basin",
        ),
    )
    _apply(
        namespace,
        "gee_task_status",
        _build_post_decorator(
            GeeTaskStatusRequestSerializer,
            summary="Gee Task Status",
            description="Inspect the current status of a GEE task.",
        ),
    )
    _apply(
        namespace,
        "plantation_site_suitability",
        _build_post_decorator(
            PlantationSiteSuitabilityRequestSerializer,
            summary="Plantation Site Suitability",
        ),
    )
    _apply(
        namespace,
        "generate_layer_in_order",
        _build_post_decorator(
            LayerInOrderRequestSerializer,
            summary="Generate Layer In Order",
            description="Queue a dependency-aware set of layer jobs.",
        ),
    )
    _apply(
        namespace,
        "upload_kml",
        _build_form_post_decorator(
            KML_UPLOAD_PARAMETERS,
            summary="Upload Kml",
            description="Upload a KML file and convert it for downstream compute use.",
        ),
    )
    _apply(
        namespace,
        "fes_clart_upload_layer",
        _build_form_post_decorator(
            FES_CLART_UPLOAD_PARAMETERS,
            summary="Fes Clart Upload Layer",
            description="Upload a FES clart file and queue the related processing task.",
        ),
    )
    _apply(
        namespace,
        "get_layers_for_workspace",
        _build_get_decorator(
            summary="Get Layers For Workspace",
            description="List layers available inside a workspace.",
            manual_parameters=[WORKSPACE_QUERY_PARAM],
        ),
    )

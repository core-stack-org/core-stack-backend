from drf_yasg.utils import swagger_auto_schema
from rest_framework import status
from rest_framework.response import Response

from utilities.auth_check_decorator import api_security_check
from utilities.openmeteo_format import error_envelope, success_envelope

from .catalog import (
    RFC9727_CONTENT_TYPE,
    catalog_detail,
    catalog_index_payload,
    get_catalog_entry,
    rfc9727_linkset,
)
from .swagger_schemas import (
    catalog_item_schema_v2,
    catalog_list_schema_v2,
    rfc9727_api_catalog_schema,
)


@swagger_auto_schema(**rfc9727_api_catalog_schema)
@api_security_check(auth_type="Auth_free")
def get_rfc9727_api_catalog(request):
    response = Response(rfc9727_linkset(request), status=status.HTTP_200_OK)
    response["Content-Type"] = RFC9727_CONTENT_TYPE
    response["Link"] = '</.well-known/api-catalog>; rel="api-catalog"'
    return response


@swagger_auto_schema(**catalog_list_schema_v2)
@api_security_check(auth_type="API_key")
def get_public_api_catalog(request):
    group = request.query_params.get("group")
    if group and group.strip().lower() not in {"dataset", "waterbody"}:
        return Response(
            error_envelope("group must be dataset or waterbody."),
            status=status.HTTP_400_BAD_REQUEST,
        )
    payload = catalog_index_payload(request, group=group)
    return Response(success_envelope(payload), status=status.HTTP_200_OK)


@swagger_auto_schema(**catalog_item_schema_v2)
@api_security_check(auth_type="API_key")
def get_public_api_catalog_item(request, api_id):
    entry = get_catalog_entry(api_id)
    if entry is None:
        return Response(
            error_envelope(f"Unknown catalog id '{api_id}'."),
            status=status.HTTP_404_NOT_FOUND,
        )
    return Response(
        success_envelope(catalog_detail(entry, request)),
        status=status.HTTP_200_OK,
    )

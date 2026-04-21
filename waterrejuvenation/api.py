import json
import os
from datetime import datetime, timezone

from rest_framework import status
from rest_framework.response import Response
from drf_yasg.utils import swagger_auto_schema

from nrm_app.settings import (
    MEDIA_ROOT,
    MONGODB_DB_NAME,
    MONGODB_URI,
    MONGODB_WATERBODIES_COLLECTION,
)
from utilities.openmeteo_format import (
    annual_structure_from_dict,
    error_envelope,
    success_envelope,
)
from utilities.renderers import round_floats
from utilities.auth_check_decorator import api_security_check
from .swagger_schemas import waterbodies_by_admin_schema, waterbodies_by_uuid
from .utils import get_merged_waterbodies_with_zoi

try:
    from pymongo import MongoClient
except Exception:
    MongoClient = None

_MONGO_CACHE_STATUS_LOGGED = False


def _error_response(message, http_status, details=None):
    return Response(error_envelope(message, details), status=http_status)


def _success_response_for_admin(merged_data, state_norm, district_l, block_l):
    data = list(merged_data.values()) if isinstance(merged_data, dict) else []
    timeseries_data = [annual_structure_from_dict(item) for item in data]
    body = success_envelope(timeseries_data)
    body["location"] = {
        "state": state_norm,
        "district": district_l,
        "block": block_l,
    }
    return Response(round_floats(body, precision=2), status=status.HTTP_200_OK)


def _success_response_for_uid(item, uid, state_norm, district_l, block_l):
    body = success_envelope(annual_structure_from_dict(item))
    body["uid"] = str(uid)
    body["location"] = {
        "state": state_norm,
        "district": district_l,
        "block": block_l,
    }
    return Response(round_floats(body, precision=2), status=status.HTTP_200_OK)


def _success_response_for_admin_v2(merged_data, state_norm, district_l, block_l):
    return _success_response_for_admin(merged_data, state_norm, district_l, block_l)


def _success_response_for_uid_v2(item, uid, state_norm, district_l, block_l):
    return _success_response_for_uid(item, uid, state_norm, district_l, block_l)


def _extract_admin_params(request):
    state = request.query_params.get("state")
    district = request.query_params.get("district")
    block = request.query_params.get("tehsil") or request.query_params.get("block")
    uid = request.query_params.get("uid")
    return state, district, block, uid


def _get_mongo_collection():
    global _MONGO_CACHE_STATUS_LOGGED
    if not MONGODB_URI or MongoClient is None:
        if not _MONGO_CACHE_STATUS_LOGGED:
            print(
                "Mongo cache disabled: set MONGODB_URI and install pymongo "
                "to enable waterbodies cache."
            )
            _MONGO_CACHE_STATUS_LOGGED = True
        return None, None
    try:
        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=3000)
        collection = client[MONGODB_DB_NAME][MONGODB_WATERBODIES_COLLECTION]
        if not _MONGO_CACHE_STATUS_LOGGED:
            print(
                f"Mongo cache enabled: db={MONGODB_DB_NAME}, "
                f"collection={MONGODB_WATERBODIES_COLLECTION}"
            )
            _MONGO_CACHE_STATUS_LOGGED = True
        return client, collection
    except Exception as exc:
        if not _MONGO_CACHE_STATUS_LOGGED:
            print(f"Mongo cache unavailable at startup: {exc}")
            _MONGO_CACHE_STATUS_LOGGED = True
        print(f"Mongo init failed, falling back to file cache: {exc}")
        return None, None


def _mongo_cache_key(state_norm, district_l, block_l):
    return {
        "state": state_norm,
        "district": district_l,
        "block": block_l,
    }


def _load_from_mongo(state_norm, district_l, block_l):
    client, collection = _get_mongo_collection()
    if collection is None:
        return None
    try:
        doc = collection.find_one(
            _mongo_cache_key(state_norm, district_l, block_l),
            {"_id": 0, "data": 1},
        )
        return doc.get("data") if doc else None
    except Exception as exc:
        print(f"Mongo read failed, falling back to file cache: {exc}")
        return None
    finally:
        if client is not None:
            client.close()


def _save_to_mongo(state_norm, district_l, block_l, merged_data):
    client, collection = _get_mongo_collection()
    if collection is None:
        return
    try:
        payload = {
            **_mongo_cache_key(state_norm, district_l, block_l),
            "data": merged_data,
            "updated_at": datetime.now(timezone.utc),
        }
        collection.update_one(
            _mongo_cache_key(state_norm, district_l, block_l),
            {"$set": payload},
            upsert=True,
        )
    except Exception as exc:
        print(f"Mongo write failed: {exc}")
    finally:
        if client is not None:
            client.close()


def _load_or_generate_merged_data(state_norm, district_l, block_l, regenerate=False):
    if not regenerate:
        mongo_data = _load_from_mongo(state_norm, district_l, block_l)
        if mongo_data is not None:
            return mongo_data

    base_dir = os.path.join(MEDIA_ROOT, "stats_excel_files")
    out_dir = os.path.join(base_dir, state_norm, district_l.upper())
    merged_path = os.path.join(out_dir, f"{district_l}_{block_l}_merged_data.json")

    merged_data = None
    if not regenerate and os.path.exists(merged_path):
        try:
            with open(merged_path, "r", encoding="utf-8") as fh:
                merged_data = json.load(fh)
            # Backfill Mongo cache when file cache exists but Mongo is empty.
            _save_to_mongo(state_norm, district_l, block_l, merged_data)
        except Exception as exc:
            print(f"Error reading cached merged file {merged_path}: {exc}")
            merged_data = None

    if merged_data is None:
        merged_data = get_merged_waterbodies_with_zoi(
            state=state_norm,
            district=district_l,
            block=block_l,
        )
        # Persist generated output to Mongo cache when configured.
        if merged_data is not None:
            _save_to_mongo(state_norm, district_l, block_l, merged_data)
    return merged_data


def _get_uid_item(merged_data, uid):
    uid_str = str(uid)
    item = merged_data.get(uid_str)
    if item is None and uid_str.isdigit():
        item = merged_data.get(int(uid_str))
    return uid_str, item


def _handle_waterbodies_request(request, require_uid=False):
    state, district, block, uid = _extract_admin_params(request)
    regenerate = str(request.query_params.get("regenerate", "")).lower() in {
        "1",
        "true",
        "yes",
    }

    if not state or not district or not block:
        return _error_response(
            "'state', 'district' and 'tehsil' (or 'block') parameters are required.",
            status.HTTP_400_BAD_REQUEST,
        )
    if require_uid and not uid:
        return _error_response(
            "'uid' parameter is required.",
            status.HTTP_400_BAD_REQUEST,
        )

    state_norm = state.upper()
    district_l = district.lower()
    block_l = block.lower()

    try:
        merged_data = _load_or_generate_merged_data(
            state_norm, district_l, block_l, regenerate=regenerate
        )
    except Exception as exc:
        print(f"Error generating merged data for {state_norm}/{district_l}/{block_l}: {exc}")
        return _error_response(
            "Failed to generate merged dataset.",
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            details=str(exc),
        )

    if merged_data is None:
        return _error_response(
            "Merged dataset not available for given area.",
            status.HTTP_502_BAD_GATEWAY,
        )

    if uid:
        uid_str, item = _get_uid_item(merged_data, uid)
        if item is None:
            return _error_response(
                f"UID '{uid}' not found for state={state_norm}, district={district_l}, block={block_l}.",
                status.HTTP_404_NOT_FOUND,
            )
        return _success_response_for_uid(
            item=item,
            uid=uid_str,
            state_norm=state_norm,
            district_l=district_l,
            block_l=block_l,
        )

    return _success_response_for_admin(
        merged_data=merged_data,
        state_norm=state_norm,
        district_l=district_l,
        block_l=block_l,
    )


def _handle_waterbodies_request_v2(request, require_uid=False):
    return _handle_waterbodies_request(request, require_uid=require_uid)


@swagger_auto_schema(**waterbodies_by_admin_schema)
@api_security_check(auth_type="API_key")
def get_waterbodies_by_admin_and_uid(request):
    try:
        return _handle_waterbodies_request(request)
    except Exception as exc:
        print(f"Unexpected error in get_waterbodies_by_admin_and_uid: {exc}")
        return _error_response(
            "Internal server error while retrieving waterbody data.",
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            details=str(exc),
        )


@swagger_auto_schema(**waterbodies_by_uuid)
@api_security_check(auth_type="API_key")
def get_waterbodies_by_uid(request):
    try:
        return _handle_waterbodies_request(request, require_uid=True)
    except Exception as exc:
        print(f"Unexpected error in get_waterbodies_by_uid: {exc}")
        return _error_response(
            "Internal server error while retrieving waterbody data.",
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            details=str(exc),
        )


@swagger_auto_schema(**waterbodies_by_admin_schema)
@api_security_check(auth_type="API_key")
def get_waterbodies_by_admin_and_uid_v2(request):
    try:
        return _handle_waterbodies_request_v2(request)
    except Exception as exc:
        print(f"Unexpected error in get_waterbodies_by_admin_and_uid_v2: {exc}")
        return _error_response(
            "Internal server error while retrieving waterbody data.",
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            details=str(exc),
        )


@swagger_auto_schema(**waterbodies_by_uuid)
@api_security_check(auth_type="API_key")
def get_waterbodies_by_uid_v2(request):
    try:
        return _handle_waterbodies_request_v2(request, require_uid=True)
    except Exception as exc:
        print(f"Unexpected error in get_waterbodies_by_uid_v2: {exc}")
        return _error_response(
            "Internal server error while retrieving waterbody data.",
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            details=str(exc),
        )

import logging

from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.http import require_GET

from .services.location_service import (
    get_states,
    get_districts,
    get_blocks,
    get_block_geometry,
)
from .services.layer_metadata_service import get_layers_for_location
from .services.gee_service import get_gee_map_config
from .services.geoserver_service import get_geoserver_wms_config

logger = logging.getLogger(__name__)


@login_required
def dashboard(request):
    return render(request, "layer_comparison/dashboard.html")


@login_required
@require_GET
def states_api(request):
    try:
        return JsonResponse({"states": get_states()})
    except Exception:
        logger.exception("Could not retrieve states")
        return JsonResponse({"error": "Could not retrieve states."}, status=500)


@login_required
@require_GET
def districts_api(request):
    state_id = request.GET.get("state_id")
    if not state_id:
        return JsonResponse({"error": "state_id is required."}, status=400)

    try:
        return JsonResponse({"districts": get_districts(state_id)})
    except Exception:
        logger.exception("Could not retrieve districts for state %s", state_id)
        return JsonResponse({"error": "Could not retrieve districts."}, status=500)


@login_required
@require_GET
def blocks_api(request):
    district_id = request.GET.get("district_id")
    if not district_id:
        return JsonResponse({"error": "district_id is required."}, status=400)

    try:
        return JsonResponse({"blocks": get_blocks(district_id)})
    except Exception:
        logger.exception("Could not retrieve blocks for district %s", district_id)
        return JsonResponse({"error": "Could not retrieve blocks."}, status=500)


def _location_params(request):
    state_id = request.GET.get("state_id")
    district_id = request.GET.get("district_id")
    block_id = request.GET.get("block_id")
    if not all((state_id, district_id, block_id)):
        raise ValueError("state_id, district_id and block_id are required.")
    return state_id, district_id, block_id


@login_required
@require_GET
def layers_api(request):
    try:
        state_id, district_id, block_id = _location_params(request)
        groups = get_layers_for_location(state_id, district_id, block_id)
        return JsonResponse({"groups": groups})
    except ValueError as exc:
        return JsonResponse({"error": str(exc)}, status=400)
    except Exception:
        logger.exception("Could not retrieve layers for selected location")
        return JsonResponse({"error": "Could not retrieve layers."}, status=500)


@login_required
@require_GET
def compare_api(request):
    try:
        state_id, district_id, block_id = _location_params(request)
        raw_ids = request.GET.get("layer_ids", "")
        try:
            layer_ids = list(
                dict.fromkeys(
                    int(value) for value in raw_ids.split(",") if value.strip()
                )
            )
        except ValueError:
            return JsonResponse(
                {"error": "layer_ids must be comma-separated integers."}, status=400
            )

        if not layer_ids:
            return JsonResponse({"error": "Select at least one layer."}, status=400)

        if len(layer_ids) > 30:
            return JsonResponse(
                {"error": "Select no more than 30 layers at once."}, status=400
            )

        # Re-query by location and selected IDs. This prevents a caller from
        # requesting a Layer belonging to a different administrative area.
        selected_groups = get_layers_for_location(
            state_id, district_id, block_id, layer_ids=layer_ids
        )

        results = []
        for group in selected_groups:
            for layer in group["layers"]:
                item = dict(layer)
                item["gee"] = None
                item["local"] = None

                if layer["gee_available"]:
                    try:
                        item["gee"] = get_gee_map_config(layer)
                    except Exception:
                        logger.exception(
                            "GEE tile generation failed for Layer %s", layer["id"]
                        )
                        item["gee_error"] = "GEE tile could not be generated."

                if layer["local_available"]:
                    try:
                        item["local"] = get_geoserver_wms_config(layer)
                        print("item['local']>>", item["local"])
                    except Exception:
                        logger.exception(
                            "GeoServer config failed for Layer %s", layer["id"]
                        )
                        item["local_error"] = "GeoServer layer could not be configured."

                results.append(item)

        geometry = get_block_geometry(state_id, district_id, block_id)

        return JsonResponse(
            {
                "layers": results,
                "geometry": geometry,
            }
        )

    except ValueError as exc:
        return JsonResponse({"error": str(exc)}, status=400)
    except Exception:
        logger.exception("Comparison request failed")
        return JsonResponse(
            {"error": "Could not prepare layer comparison."}, status=500
        )

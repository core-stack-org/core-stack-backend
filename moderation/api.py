from .views import *
from django.http import JsonResponse
from django.views.decorators.http import require_GET
from rest_framework.decorators import api_view, schema
from rest_framework.response import Response
import requests
from rest_framework import status
from .utils.form_mapping import model_map
from .api import FETCH_FIELD_MAP
import json
import os


@api_view(["GET"])
@schema(None)
def get_paginated_submissions(request, form, plan_id):
    page = request.GET.get("page")

    mapping = {
        "Settlement": SubmissionsOfPlan.get_settlement,
        "Well": SubmissionsOfPlan.get_well,
        "Waterbody": SubmissionsOfPlan.get_waterbody,
        "Groundwater": SubmissionsOfPlan.get_groundwater,
        "Agri": SubmissionsOfPlan.get_agri,
        "Livelihood": SubmissionsOfPlan.get_livelihood,
        "Crop": SubmissionsOfPlan.get_crop,
        "Agri Maintenance": SubmissionsOfPlan.get_agri_maintenance,
        "GroundWater Maintenance": SubmissionsOfPlan.get_gw_maintenance,
        "Surface Water Body Maintenance": SubmissionsOfPlan.get_swb_maintenance,
        "Surface Water Body Remotely Sensed Maintenance": SubmissionsOfPlan.get_swb_rs_maintenance,
        "Agrohorticulture": SubmissionsOfPlan.get_agrohorticulture,
    }

    if form not in mapping:
        return JsonResponse({"error": "Invalid form name"}, status=400)

    result = mapping[form](plan_id, page)
    return JsonResponse(result, safe=False)


@api_view(["GET"])
@schema(None)
def get_form_names(request):
    forms_path = os.path.join(os.path.dirname(__file__), "utils", "forms.json")
    with open(forms_path, "r") as file:
        data = json.load(file)
    return JsonResponse({"forms": data["Forms"]}, safe=False)


@api_view(["PUT"])
@schema(None)
def update_submission(request, form_name, uuid):
    Model = model_map.get(form_name)
    if not Model:
        return Response({"success": False, "message": "Invalid form"}, status=400)
    field_name = FETCH_FIELD_MAP.get(Model)
    if not field_name:
        return Response(
            {"success": False, "message": "No JSON field configured"},
            status=400,
        )
    try:
        obj = Model.objects.get(uuid=uuid)
    except Model.DoesNotExist:
        return Response({"success": False, "message": "Not found"}, status=404)
    existing_data = getattr(obj, field_name) or {}
    existing_data.update(request.data)
    setattr(obj, field_name, existing_data)
    obj.is_moderated = True
    obj.save(update_fields=[field_name, "is_moderated"])
    return Response({"success": True})


@api_view(["DELETE"])
@schema(None)
def delete_submission(request, form_name, uuid):
    Model = model_map.get(form_name)
    if not Model:
        return Response({"success": False, "message": "Invalid form"}, status=400)

    try:
        Model.objects.get(uuid=uuid).delete()
        return Response({"success": True})
    except Model.DoesNotExist:
        return Response({"success": False, "message": "Not found"}, status=404)


@api_view(["GET"])
@schema(None)
def sync_updated_submissions(request):
    print("syncing ODK to CSDB")
    res = sync_odk_to_csdb()
    return res

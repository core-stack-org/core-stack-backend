from .views import *
from django.http import JsonResponse
from django.views.decorators.http import require_GET
from rest_framework.decorators import api_view, schema
from rest_framework.response import Response
import requests
from rest_framework import status
from .utils.form_mapping import model_map
from .api import FETCH_FIELD_MAP
from .utils.get_submissions import ODKSubmissionsChecker
import json


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
    }

    if form not in mapping:
        return JsonResponse({"error": "Invalid form name"}, status=400)

    result = mapping[form](plan_id, page)
    return JsonResponse(result, safe=False)


@api_view(["GET"])
@schema(None)
def get_form_names(request):
    with open("moderation/utils/forms.json", "r") as file:
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
    (
        settlement_submissions,
        well_submissions,
        waterbody_submissions,
        groundwater_submissions,
        agri_submissions,
        livelihood_submissions,
        cropping_submissions,
        agri_maintenance_submissions,
        gw_maintenance_submissions,
        swb_maintenance_submissions,
        swb_rs_maintenance_submissions,
    ) = sync_odk_data(get_edited_updated_all_submissions)
    checker = ODKSubmissionsChecker()
    res = checker.process("updated")
    for form_name, status in res.items():
        if status.get("is_updated"):
            if form_name == "Settlement Form":
                resync_settlement(settlement_submissions)
            elif form_name == "Well Form":
                resync_well(well_submissions)
            elif form_name == "water body form":
                resync_waterbody(waterbody_submissions)
            elif form_name == "new recharge structure form":
                resync_gw(groundwater_submissions)
            elif form_name == "new irrigation form":
                resync_agri(agri_submissions)
            elif form_name == "livelihood form":
                resync_livelihood(livelihood_submissions)
            elif form_name == "cropping pattern form":
                resync_cropping(cropping_submissions)
            elif form_name == "propose maintenance on existing irrigation form":
                resync_agri_maintenance(agri_maintenance_submissions)
            elif form_name == "propose maintenance on water structure form":
                resync_gw_maintenance(gw_maintenance_submissions)
            elif form_name == "propose maintenance on existing water recharge form":
                resync_swb_maintenance(swb_maintenance_submissions)
            elif (
                form_name
                == "propose maintenance of remotely sensed water structure form"
            ):
                resync_swb_rs_maintenance(swb_rs_maintenance_submissions)
            else:
                print("passed wrong form name")
    return JsonResponse({"status": "Sync complete", "result": res})

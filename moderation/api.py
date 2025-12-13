from .views import *
from django.http import JsonResponse
from django.views.decorators.http import require_GET
from rest_framework.decorators import api_view, schema
from rest_framework.response import Response
import requests
from rest_framework import status
from .utils.form_mapping import model_map


@api_view(["GET"])
@schema(None)
def get_paginated_submissions(request, form, plan_id):
    page = request.GET.get("page", 1)

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
        "Surface Water Body Recharge Structure Maintenance": SubmissionsOfPlan.get_swb_rs_maintenance,
    }

    if form not in mapping:
        return JsonResponse({"error": "Invalid form name"}, status=400)

    result = mapping[form](plan_id, page)
    return JsonResponse(result, safe=False)


@api_view(["GET"])
@schema(None)
def get_form_names(request):
    form_names = [
        {"name": "Settlement", "form_id": "Add_Settlements_form%20_V1.0.1"},
        {"name": "Well", "form_id": "Add_Wells_form%20_V1.0.1"},
        {"name": "Waterbody", "form_id": "Add_Waterbody_form%20_V1.0.1"},
        {"name": "Groundwater", "form_id": "Add_Groundwater_form%20_V1.0.1"},
        {"name": "Agri", "form_id": "Add_Agri_form%20_V1.0.1"},
        {"name": "Livelihood", "form_id": "Add_Livelihood_form%20_V1.0.1"},
        {"name": "Crop", "form_id": "Add_Cropping_form%20_V1.0.1"},
        {"name": "Agri Maintenance", "form_id": "Agri_Maintenance_form%20_V1.0.1"},
        {
            "name": "GroundWater Maintenance",
            "form_id": "Groundwater_Maintenance_form%20_V1.0.1",
        },
        {
            "name": "Surface Water Body Maintenance",
            "form_id": "Surface_Waterbody_Maintenance_form%20_V1.0.1",
        },
        {
            "name": "Surface Water Body Recharge Structure Maintenance",
            "form_id": "Surface_Waterbody_RS_Maintenance_form%20_V1.0.1",
        },
    ]
    return JsonResponse({"forms": form_names})


@api_view(["PUT"])
@schema(None)
def update_submission(request, form_name, uuid):
    Model = model_map.get(form_name)
    if not Model:
        return Response({"success": False, "message": "Invalid form"}, status=400)

    try:
        obj = Model.objects.get(uuid=uuid)
    except Model.DoesNotExist:
        return Response({"success": False, "message": "Not found"}, status=404)

    for field, value in request.data.items():
        setattr(obj, field, value)

    obj.save()
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
    ) = sync_settlement_odk_data(get_edited_updated_all_submissions)
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

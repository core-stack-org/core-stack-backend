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
        "settlement": SubmissionsOfPlan.get_settlement,
        "well": SubmissionsOfPlan.get_well,
        "waterbody": SubmissionsOfPlan.get_waterbody,
        "groundwater": SubmissionsOfPlan.get_groundwater,
        "agri": SubmissionsOfPlan.get_agri,
        "livelihood": SubmissionsOfPlan.get_livelihood,
        "crop": SubmissionsOfPlan.get_crop,
        "agri-maint": SubmissionsOfPlan.get_agri_maintenance,
        "gw-maint": SubmissionsOfPlan.get_gw_maintenance,
        "swb-maint": SubmissionsOfPlan.get_swb_maintenance,
        "swb-rs-maint": SubmissionsOfPlan.get_swb_rs_maintenance,
    }

    if form not in mapping:
        return JsonResponse({"error": "Invalid form name"}, status=400)

    result = mapping[form](plan_id, page)
    return JsonResponse(result, safe=False)


@api_view(["GET"])
@schema(None)
def get_form_names(request):
    form_names = [
        {"name": "settlement", "form_id": "Add_Settlements_form%20_V1.0.1"},
        {"name": "well", "form_id": "Add_Wells_form%20_V1.0.1"},
        {"name": "waterbody", "form_id": "Add_Waterbody_form%20_V1.0.1"},
        {"name": "groundwater", "form_id": "Add_Groundwater_form%20_V1.0.1"},
        {"name": "agri", "form_id": "Add_Agri_form%20_V1.0.1"},
        {"name": "livelihood", "form_id": "Add_Livelihood_form%20_V1.0.1"},
        {"name": "crop", "form_id": "Add_Cropping_form%20_V1.0.1"},
        {"name": "agri-maint", "form_id": "Agri_Maintenance_form%20_V1.0.1"},
        {"name": "gw-maint", "form_id": "Groundwater_Maintenance_form%20_V1.0.1"},
        {
            "name": "swb-maint",
            "form_id": "Surface_Waterbody_Maintenance_form%20_V1.0.1",
        },
        {
            "name": "swb-rs-maint",
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

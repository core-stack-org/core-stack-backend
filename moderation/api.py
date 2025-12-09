from .views import *
from django.http import JsonResponse
from django.views.decorators.http import require_GET
from rest_framework.decorators import api_view, permission_classes
from rest_framework.response import Response
from .models import *
from nrm_app.settings import ODK_USERNAME, ODK_PASSWORD
import requests
from rest_framework import status
from rest_framework.permissions import AllowAny


@require_GET
def get_paginated_submissions(request, form, plan_id):
    """
    API: /api/submissions/<form>/<plan_id>/?page=1
    Returns paginated submissions + edit URLs
    """

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


# def get_form_names(request):
#     form_names = [
#         "settlement",
#         "well",
#         "waterbody",
#         "groundwater",
#         "agri",
#         "livelihood",
#         "crop",
#         "agri-maint",
#         "gw-maint",
#         "swb-maint",
#         "swb-rs-maint",
#     ]
#     return JsonResponse({"forms": form_names})


def get_form_names(request):
    # Map friendly names to actual ODK form IDs
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


MODEL_FORM_MAP = {
    "Add_Settlements_form%20_V1.0.1": ODK_settlement,
    "Add_well_form_V1.0.1": ODK_well,
}


@api_view(["DELETE"])
@permission_classes([AllowAny])
def delete_odk_submission(request, project_id, form_id, submission_uuid):
    """
    1. Get token
    2. Delete from ODK
    3. If success -> delete from DB
    """
    token = fetch_bearer_token(ODK_USERNAME, ODK_PASSWORD)

    if not token:
        return Response({"success": False, "message": "ODK Auth Failed"}, status=401)

    odk_url = (
        f"{ODK_BASE_URL}{project_id}/forms/{form_id}/submissions/{submission_uuid}"
    )
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    r = requests.delete(odk_url, headers=headers)

    try:
        data = r.json()

        if data.get("success") is True:

            # qs = MODEL_FORM_MAP[form_id].objects.filter(uuid=submission_uuid)

            # if qs.exists():
            #     deleted_count = qs.delete()
            #     print(f"Deleted from DB: {deleted_count}")
            #     return Response({"success": True, "message": "Deleted from ODK + DB"})
            # else:
            #     print("UUID not found in DB")
            return Response(
                {
                    "success": True,
                    "message": "Deleted from ODK but record not found in DB",
                }
            )

        return Response({"success": False, "message": data}, status=400)

    except:
        return Response(
            {"success": False, "message": "Unexpected ODK response"}, status=500
        )

from django.shortcuts import render
from dpr.models import *
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
from django.forms.models import model_to_dict
import requests
from django.http import JsonResponse
from plans.utils import fetch_bearer_token
from nrm_app.settings import ODK_USERNAME, ODK_PASSWORD
import requests
from utilities.constants import ODK_BASE_URL
from moderation.utils.get_submissions import ODKSubmissionsChecker
from moderation.utils.update_csdb import *


def delete_odk_submission(form_id, submission_uuid, project_id):
    token = fetch_bearer_token(ODK_USERNAME, ODK_PASSWORD)
    url = f"{ODK_BASE_URL}{project_id}/forms/{form_id}/submissions/{submission_uuid}"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    response = requests.delete(url, headers=headers)
    model_form_id = {"Add_Settlements_form%20_V1.0.1": ODK_settlement}
    try:
        data = response.json()
        if data.get("success") is True:
            print("Submission deleted successfully")
            sub_delete = (
                model_form_id[form_id].objects.filter(uuid=submission_uuid).delete()
            )
            if sub_delete:
                print(f"dleted from models as well {sub_delete}")
            else:
                print("object not found")
            # return True
        else:
            print("Failed to delete submission:", data)
            return False
    except:
        print(
            f"Unexpected response | status={response.status_code} | text={response.text}"
        )
        return False


# from django.core.paginator import Paginator, PageNotAnInteger, EmptyPage


def paginate_queryset(queryset, page=1, per_page=10):
    paginator = Paginator(queryset, per_page)

    try:
        obj_page = paginator.page(page)
    except PageNotAnInteger:
        obj_page = paginator.page(1)
    except EmptyPage:
        obj_page = paginator.page(paginator.num_pages)

    data = list(obj_page.object_list.values())

    return {
        "page": obj_page.number,
        "total_pages": paginator.num_pages,
        "total_objects": paginator.count,
        "data": data,
    }


FORM_ID_MAP = {
    "ODK_settlement": "Add_Settlements_form%20_V1.0.1",
    "ODK_well": "Add_well_form_V1.0.1",
    "ODK_waterbody": "Add_Waterbodies_Form_V1.0.3",
    "ODK_groundwater": "Groundwater_Form%20_V1.0.0",
    "ODK_agri": "NRM_form_Agri_Screen_V1.0.0",
    "ODK_livelihood": "NRM%20Livelihood%20Form",
    "ODK_crop": "crop_form_V1.0.0",
    "Agri_maintenance": "Propose_Maintenance_on_Existing_Irrigation_Structures_V1.1.1",
    "GW_maintenance": "GW_Maintenance_Form",
    "SWB_maintenance": "SWB_Maintenance_Form",
    "SWB_RS_maintenance": "SWB_RS_Maintenance_Form",
}

class SubmissionsOfPlan:

    @staticmethod
    def _fetch(model, plan_id, page):
        qs = model.objects.filter(plan_id=plan_id)
        result = paginate_queryset(qs, page)
        return result

    @staticmethod
    def get_settlement(plan_id, page=1):
        return SubmissionsOfPlan._fetch(ODK_settlement, plan_id, page)

    @staticmethod
    def get_well(plan_id, page=1):
        return SubmissionsOfPlan._fetch(ODK_well, plan_id, page)

    @staticmethod
    def get_waterbody(plan_id, page=1):
        return SubmissionsOfPlan._fetch(ODK_waterbody, plan_id, page)

    @staticmethod
    def get_groundwater(plan_id, page=1):
        return SubmissionsOfPlan._fetch(ODK_groundwater, plan_id, page)

    @staticmethod
    def get_agri(plan_id, page=1):
        return SubmissionsOfPlan._fetch(ODK_agri, plan_id, page)

    @staticmethod
    def get_livelihood(plan_id, page=1):
        return SubmissionsOfPlan._fetch(ODK_livelihood, plan_id, page)

    @staticmethod
    def get_crop(plan_id, page=1):
        return SubmissionsOfPlan._fetch(ODK_crop, plan_id, page)

    @staticmethod
    def get_agri_maintenance(plan_id, page=1):
        return SubmissionsOfPlan._fetch(Agri_maintenance, plan_id, page)

    @staticmethod
    def get_gw_maintenance(plan_id, page=1):
        return SubmissionsOfPlan._fetch(GW_maintenance, plan_id, page)

    @staticmethod
    def get_swb_maintenance(plan_id, page=1):
        return SubmissionsOfPlan._fetch(SWB_maintenance, plan_id, page)

    @staticmethod
    def get_swb_rs_maintenance(plan_id, page=1):
        return SubmissionsOfPlan._fetch(SWB_RS_maintenance, plan_id, page)


def sync_updated_submissions():
    checker = ODKSubmissionsChecker()
    res = checker.process("updated")
    for form_name, status in res.items():
        if status.get("is_edited"):
            if form_name == "Settlement Form":
                resync_settlement()
            elif form_name == "Well Form":
                resync_well()
            elif form_name == "water body form":
                resync_waterbody()
            elif form_name == "new recharge structure form":
                resync_gw()
            elif form_name == "new irrigation form":
                resync_agri()
            elif form_name == "livelihood form":
                resync_livelihood()
            elif form_name == "cropping pattern form":
                resync_cropping()
            elif form_name == "propose maintenance on existing irrigation form":
                resync_agri_maintenance()
            elif form_name == "propose maintenance on water structure form":
                resync_gw_maintenance()
            elif form_name == "propose maintenance on existing water recharge form":
                resync_swb_maintenance()
            elif (
                form_name
                == "propose maintenance of remotely sensed water structure form"
            ):
                resync_swb_rs_maintenance()
            else:
                print("passed wrong form name")
    return JsonResponse({"status": "Sync complete", "result": res})


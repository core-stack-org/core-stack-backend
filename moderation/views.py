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


def paginate_queryset(queryset, page=1, per_page=1):
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


class SubmissionsOfPlan:
    @staticmethod
    def get_settlement(plan_id, page=1):
        qs = ODK_settlement.objects.filter(plan_id=plan_id)
        return paginate_queryset(qs, page)

    @staticmethod
    def get_well(plan_id, page=1):
        qs = ODK_well.objects.filter(plan_id=plan_id)
        return paginate_queryset(qs, page)

    @staticmethod
    def get_waterbody(plan_id, page=1):
        qs = ODK_waterbody.objects.filter(plan_id=plan_id)
        return paginate_queryset(qs, page)

    @staticmethod
    def get_groundwater(plan_id, page=1):
        qs = ODK_groundwater.objects.filter(plan_id=plan_id)
        return paginate_queryset(qs, page)

    @staticmethod
    def get_agri(plan_id, page=1):
        qs = ODK_agri.objects.filter(plan_id=plan_id)
        return paginate_queryset(qs, page)

    @staticmethod
    def get_livelihood(plan_id, page=1):
        qs = ODK_livelihood.objects.filter(plan_id=plan_id)
        return paginate_queryset(qs, page)

    @staticmethod
    def get_crop(plan_id, page=1):
        qs = ODK_crop.objects.filter(plan_id=plan_id)
        return paginate_queryset(qs, page)

    @staticmethod
    def get_agri_maintenance(plan_id, page=1):
        qs = Agri_maintenance.objects.filter(plan_id=plan_id)
        return paginate_queryset(qs, page)

    @staticmethod
    def get_gw_maintenance(plan_id, page=1):
        qs = GW_maintenance.objects.filter(plan_id=plan_id)
        return paginate_queryset(qs, page)

    @staticmethod
    def get_swb_maintenance(plan_id, page=1):
        qs = SWB_maintenance.objects.filter(plan_id=plan_id)
        return paginate_queryset(qs, page)

    @staticmethod
    def get_swb_rs_maintenance(plan_id, page=1):
        qs = SWB_RS_maintenance.objects.filter(plan_id=plan_id)
        return paginate_queryset(qs, page)


def get_odk_edit_url(form_id: str, instance_id: str, project_id: int = 2):
    return f"{ODK_BASE_URL}{project_id}/forms/{form_id}/submissions/{instance_id}/edit"


def delete_odk_submission(form_id, submission_uuid, project_id=2):
    token = fetch_bearer_token(ODK_USERNAME, ODK_PASSWORD)
    url = f"{ODK_BASE_URL}{project_id}/forms/{form_id}/submissions/{submission_uuid}"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    response = requests.delete(url, headers=headers)
    try:
        data = response.json()
        if data.get("success") is True:
            print("Submission deleted successfully")
            return True
        else:
            print("Failed to delete submission:", data)
            return False
    except:
        print(
            f"Unexpected response | status={response.status_code} | text={response.text}"
        )
        return False

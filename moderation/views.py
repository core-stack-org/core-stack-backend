from django.shortcuts import render
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
from django.forms.models import model_to_dict
import requests
from django.http import JsonResponse
import requests
from moderation.utils.get_submissions import ODKSubmissionsChecker
from moderation.utils.update_csdb import *


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

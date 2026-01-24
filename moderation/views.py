from django.shortcuts import render
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
from django.forms.models import model_to_dict
import requests
from django.http import JsonResponse
import requests
from moderation.utils.update_csdb import *


FETCH_FIELD_MAP = {
    ODK_settlement: "data_settlement",
    ODK_well: "data_well",
    ODK_waterbody: "data_waterbody",
    ODK_groundwater: "data_groundwater",
    ODK_agri: "data_agri",
    ODK_livelihood: "data_livelihood",
    ODK_crop: "data_crop",
    Agri_maintenance: "data_agri_maintenance",
    GW_maintenance: "data_gw_maintenance",
    SWB_maintenance: "data_swb_maintenance",
    SWB_RS_maintenance: "data_swb_rs_maintenance",
}


def paginate_queryset(queryset, page=1, per_page=10):
    paginator = Paginator(queryset, per_page)

    try:
        obj_page = paginator.page(page)
    except PageNotAnInteger:
        obj_page = paginator.page(1)
    except EmptyPage:
        obj_page = paginator.page(paginator.num_pages)

    data = list(obj_page.object_list)

    return {
        "page": obj_page.number,
        "total_pages": paginator.num_pages,
        "total_objects": paginator.count,
        "data": data,
    }


class SubmissionsOfPlan:

    @staticmethod
    def _fetch(model, plan_id, page):
        field_name = FETCH_FIELD_MAP.get(model)
        if not field_name:
            raise ValueError(f"No fetch field configured for {model.__name__}")
        qs = model.objects.filter(plan_id=plan_id).values_list(
            field_name, "is_moderated"
        )
        if page is None:
            data = list(qs)
            return {
                "page": 1,
                "total_pages": 1,
                "total_objects": len(data),
                "data": data,
            }
        return paginate_queryset(qs, page)

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

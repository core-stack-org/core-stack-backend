from django.shortcuts import render
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
from django.forms.models import model_to_dict
import requests
from django.http import JsonResponse
import requests
from moderation.utils.update_csdb import *
from moderation.utils.get_submissions import ODKSubmissionsChecker
from moderation.utils.form_mapping import feedback_form
from moderation.models import SyncMetadata

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
    ODK_agrohorticulture: "data_agohorticulture",
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
        qs = (
            model.objects.filter(plan_id=plan_id)
            .exclude(is_deleted=True)
            .values_list(field_name, "is_moderated")
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

    @staticmethod
    def get_agrohorticulture(plan_id, page=1):
        return SubmissionsOfPlan._fetch(ODK_agrohorticulture, plan_id, page)


def sync_odk_to_csdb():
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
        agrohorticulture_submissions,
    ) = sync_odk_data(get_edited_updated_all_submissions)
    checker = ODKSubmissionsChecker()
    res = checker.process("updated")
    for form_name, status in res.items():
        if form_name in feedback_form:
            print("passed feedback form")
            continue
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
            elif form_name == "Agrohorticulture":
                resync_agrohorticulture(agrohorticulture_submissions)
            else:
                print("passed wrong form name")

    metadata = SyncMetadata.get_odk_sync_metadata()
    metadata.update_last_synced()
    return JsonResponse({"status": "Sync complete", "result": res})

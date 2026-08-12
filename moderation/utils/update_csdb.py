import logging
from datetime import timezone as dt_tz

import requests

from moderation.models import SyncMetadata
from nrm_app.settings import ODK_PASSWORD, ODK_USERNAME
from utilities.constants import ODK_BASE_URL, project_id

from .form_mapping import corestack
from .get_submissions import get_edited_updated_all_submissions
from .utils import *

_sync_logger = logging.getLogger(__name__)


def get_dynamic_filter_query():

    metadata = SyncMetadata.get_odk_sync_metadata()
    filter_date = metadata.get_filter_date()
    filter_date_utc = filter_date.astimezone(dt_tz.utc)
    date_str = filter_date_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    return (
        f"$filter=__system/submissionDate ge {date_str} "
        f"or __system/updatedAt ge {date_str}"
    )


def sync_odk_data(get_edited_updated_all_submissions):
    filter_query = get_dynamic_filter_query()

    get_edited_updated_all_submissions = get_edited_updated_all_submissions(
        username=ODK_USERNAME,
        password=ODK_PASSWORD,
        base_url=ODK_BASE_URL,
    )

    settlement_submissions = (
        get_edited_updated_all_submissions.get_edited_updated_submissions(
            project_id=project_id,
            form_id=corestack["Settlement Form"],
            filter_query=filter_query,
        )
    )

    well_submissions = (
        get_edited_updated_all_submissions.get_edited_updated_submissions(
            project_id=project_id,
            form_id=corestack["Well Form"],
            filter_query=filter_query,
        )
    )

    waterbody_submissions = (
        get_edited_updated_all_submissions.get_edited_updated_submissions(
            project_id=project_id,
            form_id=corestack["water body form"],
            filter_query=filter_query,
        )
    )

    groundwater_submissions = (
        get_edited_updated_all_submissions.get_edited_updated_submissions(
            project_id=project_id,
            form_id=corestack["new recharge structure form"],
            filter_query=filter_query,
        )
    )

    agri_submissions = (
        get_edited_updated_all_submissions.get_edited_updated_submissions(
            project_id=project_id,
            form_id=corestack["new irrigation form"],
            filter_query=filter_query,
        )
    )

    livelihood_submissions = (
        get_edited_updated_all_submissions.get_edited_updated_submissions(
            project_id=project_id,
            form_id=corestack["livelihood form"],
            filter_query=filter_query,
        )
    )

    cropping_submissions = (
        get_edited_updated_all_submissions.get_edited_updated_submissions(
            project_id=project_id,
            form_id=corestack["cropping pattern form"],
            filter_query=filter_query,
        )
    )

    agri_maintenance_submissions = (
        get_edited_updated_all_submissions.get_edited_updated_submissions(
            project_id=project_id,
            form_id=corestack["propose maintenance on existing irrigation form"],
            filter_query=filter_query,
        )
    )

    gw_maintenance_submissions = (
        get_edited_updated_all_submissions.get_edited_updated_submissions(
            project_id=project_id,
            form_id=corestack["propose maintenance on existing water recharge form"],
            filter_query=filter_query,
        )
    )

    swb_maintenance_submissions = (
        get_edited_updated_all_submissions.get_edited_updated_submissions(
            project_id=project_id,
            form_id=corestack["propose maintenance on water structure form"],
            filter_query=filter_query,
        )
    )

    swb_rs_maintenance_submissions = (
        get_edited_updated_all_submissions.get_edited_updated_submissions(
            project_id=project_id,
            form_id=corestack[
                "propose maintenance of remotely sensed water structure form"
            ],
            filter_query=filter_query,
        )
    )

    agrohorticulture_submissions = (
        get_edited_updated_all_submissions.get_edited_updated_submissions(
            project_id=project_id,
            form_id=corestack["Agrohorticulture"],
            filter_query=filter_query,
        )
    )

    return (
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
    )


def resync_settlement(settlement_submissions):
    count = 0
    for sub in settlement_submissions:
        sync_edited_updated_settlement(sub)
        count += 1
    print(f"{count} settlement submissions synced.")


def resync_well(well_submissions):
    count = 0
    for well_submission in well_submissions:
        sync_edited_updated_well(well_submission)
        count += 1
    print(f"{count} well submissions synced")


def resync_waterbody(waterbody_submissions):
    count = 0
    for waterbody_submission in waterbody_submissions:
        sync_edited_updated_waterbody(waterbody_submission)
        count += 1
    print(f"{count} waterbody submissions synced")


def resync_gw(groundwater_submissions):
    count = 0
    for groundwater_submission in groundwater_submissions:
        sync_edited_updated_gw(groundwater_submission)
        count += 1
    print(f"{count} gw submissions synced")


def resync_agri(agri_submissions):
    count = 0
    for agri_submission in agri_submissions:
        sync_edited_updated_agri(agri_submission)
        count += 1
    print(f"{count} agri submissions synced")


def resync_livelihood(livelihood_submissions):
    count = 0
    for livelihood_submission in livelihood_submissions:
        sync_edited_updated_livelihood(livelihood_submission)
        count += 1
    print(f"{count} livelihood submissions synced")


def resync_cropping(cropping_submissions):
    count = 0
    for cropping_submission in cropping_submissions:
        sync_edited_updated_cropping_pattern(cropping_submission)
        count += 1
    print(f"{count} cropping submissions synced")


def resync_agri_maintenance(agri_maintenance_submissions):
    count = 0
    for agri_maintenance_submission in agri_maintenance_submissions:
        sync_edited_updated_agri_maintenance(agri_maintenance_submission)
        count += 1
    print(f"{count} agri maintenance submissions synced")


def resync_gw_maintenance(gw_maintenance_submissions):
    count = 0
    for gw_maintenance_submission in gw_maintenance_submissions:
        sync_edited_updated_gw_maintenance(gw_maintenance_submission)
        count += 1
    print(f"{count} gw maintenance submissions synced")


def resync_swb_maintenance(swb_maintenance_submissions):
    count = 0
    for swb_maintenance_submission in swb_maintenance_submissions:
        sync_edited_updated_swb_maintenance(swb_maintenance_submission)
        count += 1
    print(f"{count} swb maintenance submissions synced")


def resync_swb_rs_maintenance(swb_rs_maintenance_submissions):
    count = 0
    for swb_rs_maintenance_submission in swb_rs_maintenance_submissions:
        sync_edited_updated_swb_rs_maintenance(swb_rs_maintenance_submission)
        count += 1
    print(f"{count} swb rs maintenance submissions synced")


def resync_agrohorticulture(agrohorticulture_submissions):
    count = 0
    for agrohorticulture_submission in agrohorticulture_submissions:
        sync_edited_updated_agrohorticulture(agrohorticulture_submission)
        count += 1
    print(f"{count} agrohorticulture submissions synced")


_FORM_SYNC_CONFIG = {
    # Resources
    "settlement":       ("Settlement Form",                                           resync_settlement),
    "well":             ("Well Form",                                                 resync_well),
    "waterbody":        ("water body form",                                           resync_waterbody),
    "cropping":         ("cropping pattern form",                                     resync_cropping),
    # New works
    "plan_gw":          ("new recharge structure form",                               resync_gw),
    "plan_agri":        ("new irrigation form",                                       resync_agri),
    "livelihood":       ("livelihood form",                                           resync_livelihood),
    # Maintenance works
    "main_swb":         ("propose maintenance on water structure form",               resync_swb_maintenance),
    "main_gw":          ("propose maintenance on existing water recharge form",      resync_gw_maintenance),
    "main_swb_rs":      ("propose maintenance of remotely sensed water structure form", resync_swb_rs_maintenance),
    "main_agri":        ("propose maintenance on existing irrigation form",          resync_agri_maintenance),
    # Other works
    "agrohorticulture": ("Agrohorticulture",                                          resync_agrohorticulture),
}


def sync_form_type(resource_type: str) -> bool:
    """
    Incremental sync for a single ODK form type to DB, using the same
    last_synced_at cursor as the full sync. Does not advance the cursor —
    that is reserved for the full resync_db_odk() run.
    """
    config = _FORM_SYNC_CONFIG.get(resource_type)
    if not config:
        _sync_logger.warning(f"sync_form_type: unknown resource_type '{resource_type}'")
        return False
    form_key, resync_fn = config
    _sync_logger.info(
        f"sync_form_type: starting incremental sync for resource_type='{resource_type}' "
        f"form='{form_key}'"
    )
    try:
        filter_query = get_dynamic_filter_query()
        _sync_logger.info(f"sync_form_type: using filter_query='{filter_query}'")
        client = get_edited_updated_all_submissions(
            username=ODK_USERNAME,
            password=ODK_PASSWORD,
            base_url=ODK_BASE_URL,
        )
        submissions = client.get_edited_updated_submissions(
            project_id=project_id,
            form_id=corestack[form_key],
            filter_query=filter_query,
        )
        _sync_logger.info(
            f"sync_form_type: fetched {len(submissions)} submission(s) from ODK "
            f"for resource_type='{resource_type}'"
        )
        resync_fn(submissions)
        _sync_logger.info(
            f"sync_form_type: completed sync for resource_type='{resource_type}'"
        )
        return True
    except Exception as e:
        _sync_logger.error(
            f"sync_form_type: failed for resource_type='{resource_type}': {e}",
            exc_info=True,
        )
        return False


def resync_db_odk():
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

    resync_settlement(settlement_submissions)
    resync_well(well_submissions)
    resync_waterbody(waterbody_submissions)
    resync_gw(groundwater_submissions)
    resync_agri(agri_submissions)
    resync_livelihood(livelihood_submissions)
    resync_cropping(cropping_submissions)
    resync_agri_maintenance(agri_maintenance_submissions)
    resync_gw_maintenance(gw_maintenance_submissions)
    resync_swb_maintenance(swb_maintenance_submissions)
    resync_swb_rs_maintenance(swb_rs_maintenance_submissions)
    resync_agrohorticulture(agrohorticulture_submissions)
    print("ODK data resynced successfully")

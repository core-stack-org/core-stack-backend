from nrm_app.celery import app
from utilities.logger import setup_logger

from .gen_dpr import (
    create_dpr_document,
    get_mws_ids_for_report,
    get_plan_details,
    send_dpr_email,
)
from .gen_report_download import render_pdf_with_firefox
from .utils import transform_name

logger = setup_logger(__name__)


@app.task(bind=True, name="dpr.generate_dpr_task")
def generate_dpr_task(self, plan_id: int, email_id: str):
    plan = get_plan_details(plan_id)
    if plan is None:
        logger.error(f"Plan not found for ID: {plan_id}")
        return {"error": "Plan not found"}

    doc = create_dpr_document(plan)
    mws_Ids = get_mws_ids_for_report(plan)

    mws_reports = []
    successful_mws_ids = []

    state = transform_name(str(plan.state_soi.state_name))
    district = transform_name(str(plan.district_soi.district_name))
    block = transform_name(str(plan.tehsil_soi.tehsil_name))

    for ids in mws_Ids:
        try:
            report_html_url = (
                f"https://geoserver.core-stack.org/api/v1/download_mws_report/"
                f"?state={state}&district={district}&block={block}&uid={ids}"
            )
            mws_reports.append(report_html_url)
            successful_mws_ids.append(ids)
        except Exception as e:
            logger.error(f"Failed to generate MWS report for ID {ids}: {e}")

    resource_report = None
    resource_html_url = (
        f"https://geoserver.core-stack.org/api/v1/generate_resource_report/"
        f"?district={district}&block={block}&plan_id={plan_id}&plan_name={plan.plan}"
    )

    try:
        resource_report = render_pdf_with_firefox(resource_html_url)
    except Exception as e:
        logger.error(f"Failed to generate resource report: {e}")

    send_dpr_email(
        doc=doc,
        email_id=email_id,
        plan_name=plan.plan,
        mws_reports=mws_reports,
        mws_Ids=successful_mws_ids,
        resource_report=resource_report,
        resource_report_url=resource_html_url,
        state_name=plan.state_soi.state_name,
        district_name=plan.district_soi.district_name,
        tehsil_name=plan.tehsil_soi.tehsil_name,
    )

    return {"status": "success", "email_id": email_id, "plan_id": plan_id}

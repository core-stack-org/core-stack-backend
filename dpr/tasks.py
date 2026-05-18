from django.utils import timezone
from nrm_app.celery import app
from utilities.logger import setup_logger
import requests

from .gen_dpr import (
    create_dpr_document,
    get_mws_ids_for_report,
    get_plan_details,
)
from .models import DPR_Report
from .utils import (
    transform_name,
    send_dpr_email,
    upload_dpr_to_s3,
    check_dpr_exists_on_s3,
)

logger = setup_logger(__name__)


def get_or_generate_dpr(plan, regenerate=False):
    dpr_report, created = DPR_Report.objects.get_or_create(
        plan_id=plan,
        defaults={"plan_name": plan.plan, "status": "PENDING"}
    )
    
    if not regenerate:
        s3_exists = check_dpr_exists_on_s3(dpr_report.dpr_report_s3_url)
        if not created and not dpr_report.needs_regeneration() and s3_exists:
            logger.info(f"Using cached DPR for plan {plan.id} from S3: {dpr_report.dpr_report_s3_url}")
            return dpr_report, False
    
    logger.info(f"Generating new DPR for plan {plan.id}")
    dpr_report.status = "GENERATING"
    dpr_report.save(update_fields=["status"])
    
    doc = create_dpr_document(plan)
    s3_url = upload_dpr_to_s3(doc, plan.id, plan.plan)
    
    dpr_report.dpr_report_s3_url = s3_url
    dpr_report.dpr_generated_at = timezone.now()
    dpr_report.status = "COMPLETED"
    dpr_report.last_updated_at = timezone.now()
    dpr_report.save(update_fields=[
        "dpr_report_s3_url", "dpr_generated_at", "status", "last_updated_at"
    ])
    
    logger.info(f"DPR generated and saved to S3: {s3_url}")
    return dpr_report, True


@app.task(bind=True, name="dpr.generate_dpr_task")
def generate_dpr_task(self, plan_id: int, email_id: str, regenerate: bool = False):
    plan = get_plan_details(plan_id)
    if plan is None:
        logger.error(f"Plan not found for ID: {plan_id}")
        return {"error": "Plan not found"}

    dpr_report, was_regenerated = get_or_generate_dpr(plan, regenerate=regenerate)
    mws_Ids = get_mws_ids_for_report(plan)

    mws_reports = []
    successful_mws_ids = []

    state = transform_name(str(plan.state_soi.state_name))
    district = transform_name(str(plan.district_soi.district_name))
    block = transform_name(str(plan.tehsil_soi.tehsil_name))

    for ids in mws_Ids:
        try:
            report_url = (
                f"http://127.0.0.1:8000/api/v1/download_report/"
                f"?report_type=mws&state={state}&district={district}&block={block}&uid={ids}"
            )
            mws_reports.append(report_url)
            successful_mws_ids.append(ids)
        except Exception as e:
            logger.error(f"Failed to generate MWS report for ID {ids}: {e}")

    # Fetch Resource Report PDF
    resource_report_url = (
        f"http://127.0.0.1:8000/api/v1/download_report/"
        f"?report_type=resource&district={district}&block={block}&plan_id={plan_id}&plan_name={plan.plan}"
    )
    
    resource_report = None
    try:
        response = requests.get(resource_report_url, timeout=30)
        response.raise_for_status()
        resource_report = response.content
    except Exception as e:
        logger.error(f"Failed to fetch resource report: {e}")

    send_dpr_email(
        email_id=email_id,
        plan_name=plan.plan,
        mws_reports=mws_reports,
        mws_Ids=successful_mws_ids,
        resource_report=resource_report,
        resource_report_url=resource_report_url,
        dpr_s3_url=dpr_report.dpr_report_s3_url,
        state_name=plan.state_soi.state_name,
        district_name=plan.district_soi.district_name,
        tehsil_name=plan.tehsil_soi.tehsil_name,
    )

    return {
        "status": "success",
        "email_id": email_id,
        "plan_id": plan_id,
        "s3_url": dpr_report.dpr_report_s3_url,
        "was_regenerated": was_regenerated,
    }

"""
Celery tasks for Drought Live Alerts.

Provides async tasks for:
1. SPEI computation on GEE
2. IDM alert ingestion
"""

import logging
from datetime import date, datetime

from nrm_app.celery import app
from computing.drought.models import DroughtAlert
from computing.drought.spei import compute_spei_for_aoi
from computing.drought.idm_ingest import ingest_idm_alerts
from utilities.gee_utils import (
    ee_initialize,
    valid_gee_text,
    get_gee_dir_path,
)
from utilities.constants import GEE_PATHS

logger = logging.getLogger(__name__)


@app.task(bind=True)
def compute_spei_alerts(
    self,
    state=None,
    district=None,
    block=None,
    roi_path=None,
    target_date=None,
    accumulation_months=1,
    app_type="MWS",
    gee_account_id=None,
):
    """
    Celery task: compute SPEI-based drought alerts for an AoI.

    Args:
        state, district, block: Location identifiers
        roi_path: GEE asset path for AoI (overrides state/district/block)
        target_date: ISO date string (defaults to today)
        accumulation_months: SPEI accumulation period
        app_type: Application type for GEE paths
        gee_account_id: GEE service account ID

    Returns:
        dict with created alert count and details
    """
    logger.info(f"Starting SPEI alert computation for {state}/{district}/{block}")

    try:
        ee_initialize(gee_account_id)

        # Build ROI path from state/district/block if not provided
        if roi_path is None and state and district and block:
            asset_suffix = (
                valid_gee_text(district.lower()) + "_" + valid_gee_text(block.lower())
            )
            asset_folder_list = [state, district, block]
            roi_path = (
                get_gee_dir_path(
                    asset_folder_list,
                    asset_path=GEE_PATHS[app_type]["GEE_ASSET_PATH"],
                )
                + f"filtered_mws_{asset_suffix}_uid"
            )

        if roi_path is None:
            return {"error": "No ROI path or state/district/block provided"}

        # Parse target date
        if isinstance(target_date, str):
            target = datetime.strptime(target_date, "%Y-%m-%d").date()
        elif target_date is None:
            target = date.today()
        else:
            target = target_date

        # Compute SPEI
        spei_results = compute_spei_for_aoi(
            roi_path=roi_path,
            target_date=target,
            accumulation_months=accumulation_months,
            gee_account_id=gee_account_id,
        )

        # Build AoI name
        aoi_parts = [p for p in [state, district, block] if p]
        aoi_name = " / ".join(aoi_parts) if aoi_parts else roi_path

        # Store alerts in DB
        created_count = 0
        alert_details = []
        for result in spei_results:
            severity = result.get("severity")
            if severity and severity != "NONE":
                alert, created = DroughtAlert.objects.update_or_create(
                    alert_type="spei",
                    alert_date=target,
                    aoi_name=aoi_name,
                    severity=severity,
                    defaults={
                        "spei_value": result.get("spei_value"),
                        "state": state,
                        "district": district,
                        "block": block,
                        "area_sq_km": result.get("area_sq_km"),
                        "geometry": result.get("geometry"),
                        "is_active": True,
                        "metadata": {
                            "source": "SPEI Computation",
                            "accumulation_months": accumulation_months,
                            "roi_path": roi_path,
                            "computed_at": datetime.now().isoformat(),
                            "uid": result.get("uid", ""),
                        },
                    },
                )
                if created:
                    created_count += 1
                alert_details.append(
                    {
                        "id": alert.id,
                        "severity": severity,
                        "spei_value": result.get("spei_value"),
                        "area_sq_km": result.get("area_sq_km"),
                    }
                )

        logger.info(f"SPEI computation complete: {created_count} new alerts created")
        return {
            "status": "success",
            "created_count": created_count,
            "total_drought_regions": len(alert_details),
            "alerts": alert_details,
        }

    except Exception as e:
        logger.error(f"SPEI computation failed: {e}", exc_info=True)
        return {"status": "error", "error": str(e)}


@app.task(bind=True)
def ingest_idm_alerts_task(
    self,
    state=None,
    district=None,
    block=None,
):
    """
    Celery task: ingest drought alerts from India Drought Monitor.

    Args:
        state, district, block: Optional location filters

    Returns:
        dict with ingested alert count
    """
    logger.info(f"Starting IDM alert ingestion for {state}/{district}/{block}")

    try:
        alerts_data = ingest_idm_alerts(state=state, district=district, block=block)

        created_count = 0
        for alert_data in alerts_data:
            alert_date = alert_data.pop("alert_date", date.today())
            severity = alert_data.pop("severity", None)
            aoi_name = alert_data.pop("aoi_name", "")

            if severity is None:
                continue

            _, created = DroughtAlert.objects.update_or_create(
                alert_type="idm",
                alert_date=alert_date,
                aoi_name=aoi_name,
                severity=severity,
                defaults=alert_data,
            )
            if created:
                created_count += 1

        logger.info(f"IDM ingestion complete: {created_count} new alerts")
        return {
            "status": "success",
            "created_count": created_count,
            "total_parsed": len(alerts_data),
        }

    except Exception as e:
        logger.error(f"IDM ingestion failed: {e}", exc_info=True)
        return {"status": "error", "error": str(e)}

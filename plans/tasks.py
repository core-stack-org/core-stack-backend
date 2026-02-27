import calendar

from django.conf import settings
from django.utils import timezone

from nrm_app.celery import app
from utilities.logger import setup_logger
from utilities.mailutils import send_email

from .reports import generate_plan_details_csv, generate_summary_csv

logger = setup_logger(__name__)


@app.task(bind=True, name="plans.send_monthly_plan_report")
def send_monthly_plan_report(self, force=False):
    now = timezone.now()
    _, last_day = calendar.monthrange(now.year, now.month)
    if not force and now.day != last_day:
        logger.info(f"Not last day of month ({now.day}/{last_day}), skipping.")
        return {"status": "skipped", "reason": "not_last_day"}

    recipients = getattr(settings, "PLAN_REPORT_RECIPIENTS", [])
    if not recipients:
        logger.warning("PLAN_REPORT_RECIPIENTS not configured, skipping.")
        return {"status": "skipped", "reason": "no_recipients"}

    month_label = now.strftime("%B %Y")
    logger.info(f"Generating monthly plan report for {month_label}")

    details_csv = generate_plan_details_csv(now)
    summary_csv = generate_summary_csv(now)
    suffix = now.strftime("%b_%Y").lower()

    send_email(
        subject=f"Monthly Plan Report - {month_label}",
        text_body=(
            f"Please find attached the monthly plan report for {month_label}.\n\n"
            f"1. Plan Details - Complete list of all plans\n"
            f"2. Summary - Overview with organization-level breakdown\n"
        ),
        to_emails=recipients,
        attachments=[
            {
                "filename": f"plan_details_{suffix}.csv",
                "content": details_csv,
                "mimetype": "text/csv",
            },
            {
                "filename": f"plan_summary_{suffix}.csv",
                "content": summary_csv,
                "mimetype": "text/csv",
            },
        ],
    )

    logger.info(f"Monthly plan report sent to {', '.join(recipients)}")
    return {"status": "sent", "month": month_label, "recipients": recipients}

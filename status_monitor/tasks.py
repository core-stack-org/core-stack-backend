import logging
import time
from datetime import timedelta

import requests
from celery import shared_task
from django.utils import timezone

logger = logging.getLogger(__name__)


@shared_task(name="status_monitor.check_all_endpoints")
def check_all_endpoints():
    from .models import Endpoint, StatusCheck

    endpoints = list(Endpoint.objects.filter(is_active=True))
    if not endpoints:
        logger.warning("No active endpoints found — nothing to check")
        return "No active endpoints"

    results = []
    for ep in endpoints:
        start = time.monotonic()
        try:
            resp = requests.get(ep.url, headers=ep.headers or {}, timeout=15, allow_redirects=True)
            elapsed_ms = int((time.monotonic() - start) * 1000)
            is_up = resp.status_code < 400
            results.append(
                StatusCheck(
                    endpoint=ep,
                    status_code=resp.status_code,
                    response_time_ms=elapsed_ms,
                    is_up=is_up,
                )
            )
            logger.info("%s → %s (%dms)", ep.name, resp.status_code, elapsed_ms)
        except Exception as exc:
            elapsed_ms = int((time.monotonic() - start) * 1000)
            results.append(
                StatusCheck(
                    endpoint=ep,
                    response_time_ms=elapsed_ms,
                    is_up=False,
                    error=str(exc)[:500],
                )
            )
            logger.warning("%s → FAILED (%s)", ep.name, str(exc)[:200])

    StatusCheck.objects.bulk_create(results)
    up = sum(1 for r in results if r.is_up)
    logger.info("Checked %d endpoints: %d up, %d down", len(results), up, len(results) - up)
    return f"Checked {len(results)} endpoints: {up} up, {len(results) - up} down"


@shared_task(name="status_monitor.purge_old_checks")
def purge_old_checks():
    from .models import StatusCheck

    cutoff = timezone.now() - timedelta(days=90)
    deleted, _ = StatusCheck.objects.filter(checked_at__lt=cutoff).delete()
    return f"Deleted {deleted} old status checks"

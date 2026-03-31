import time

import requests
from celery import shared_task
from django.utils import timezone


@shared_task(name="status_monitor.check_all_endpoints")
def check_all_endpoints():
    from .models import Endpoint, StatusCheck

    endpoints = Endpoint.objects.filter(is_active=True)
    results = []
    for ep in endpoints:
        start = time.monotonic()
        try:
            resp = requests.get(ep.url, timeout=15, allow_redirects=True)
            elapsed_ms = int((time.monotonic() - start) * 1000)
            results.append(
                StatusCheck(
                    endpoint=ep,
                    status_code=resp.status_code,
                    response_time_ms=elapsed_ms,
                    is_up=resp.status_code < 400,
                )
            )
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
    StatusCheck.objects.bulk_create(results)


@shared_task(name="status_monitor.purge_old_checks")
def purge_old_checks():
    from .models import StatusCheck

    cutoff = timezone.now() - timezone.timedelta(days=90)
    deleted, _ = StatusCheck.objects.filter(checked_at__lt=cutoff).delete()
    return f"Deleted {deleted} old status checks"

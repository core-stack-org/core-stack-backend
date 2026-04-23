from collections import defaultdict
from datetime import timedelta

from django.utils import timezone
from django.views.generic import TemplateView

from django_celery_beat.models import PeriodicTask

from .models import Endpoint, StatusCheck


class StatusPageView(TemplateView):
    template_name = "status.html"

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        now = timezone.localtime(timezone.now())
        thirty_days_ago = now - timedelta(days=30)

        endpoints = Endpoint.objects.filter(is_active=True).order_by("name")
        checks = StatusCheck.objects.filter(
            endpoint__in=endpoints, checked_at__gte=thirty_days_ago
        ).values("endpoint_id", "checked_at", "is_up")

        daily = defaultdict(lambda: defaultdict(lambda: {"up": 0, "total": 0}))
        for c in checks:
            day = timezone.localtime(c["checked_at"]).date().isoformat()
            daily[c["endpoint_id"]][day]["total"] += 1
            if c["is_up"]:
                daily[c["endpoint_id"]][day]["up"] += 1

        date_range = [
            (thirty_days_ago + timedelta(days=i)).date() for i in range(31)
        ]

        all_operational = True
        endpoint_data = []
        for ep in endpoints:
            latest = StatusCheck.objects.filter(endpoint=ep).first()
            current_up = latest.is_up if latest else None

            if current_up is not True:
                all_operational = False

            ep_daily = daily.get(ep.id, {})
            overall_up = sum(d["up"] for d in ep_daily.values())
            overall_total = sum(d["total"] for d in ep_daily.values())
            uptime_pct = (overall_up / overall_total * 100) if overall_total else None

            days = []
            for d in date_range:
                ds = d.isoformat()
                info = ep_daily.get(ds)
                if info and info["total"] > 0:
                    pct = info["up"] / info["total"] * 100
                    if pct == 100:
                        color = "operational"
                    elif pct > 0:
                        color = "degraded"
                    else:
                        color = "outage"
                else:
                    pct = None
                    color = "nodata"
                days.append({"date": d.strftime("%b %d"), "pct": pct, "color": color})

            endpoint_data.append({
                "name": ep.name,
                "url": ep.url,
                "current_up": current_up,
                "uptime_pct": uptime_pct,
                "response_time_ms": latest.response_time_ms if latest else None,
                "days": days,
            })

        check_interval = ""
        try:
            pt = PeriodicTask.objects.get(task="status_monitor.check_all_endpoints")
            if pt.interval:
                every, period = pt.interval.every, pt.interval.period
                check_interval = f"every {every} {period}" if every > 1 else f"every {period.rstrip('s')}"
        except PeriodicTask.DoesNotExist:
            pass

        ctx["endpoints"] = endpoint_data
        ctx["all_operational"] = all_operational
        ctx["last_checked"] = now
        ctx["check_interval"] = check_interval
        ctx["date_range_start"] = date_range[0].strftime("%b %d")
        ctx["date_range_end"] = date_range[-1].strftime("%b %d")
        return ctx

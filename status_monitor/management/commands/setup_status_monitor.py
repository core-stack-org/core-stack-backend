from django.core.management.base import BaseCommand
from django_celery_beat.models import IntervalSchedule, PeriodicTask

from status_monitor.models import Endpoint

ENDPOINTS = [
    ("Geoserver", "https://geoserver.core-stack.org:8443/geoserver/web/"),
    ("Active Locations API", "https://geoserver.core-stack.org/api/v1/get_active_locations/"),
    ("Django Admin", "https://geoserver.core-stack.org/admin/"),
    ("Dashboard", "https://dashboard.core-stack.org/"),
    ("Landscape Explorer", "https://www.explorer.core-stack.org/"),
    ("ODK", "https://odk.core-stack.org/#/login"),
    (
        "ODK Form - Settlement",
        "https://odk.core-stack.org/-/single/AOV0NchVMqkZVpCgZyWwJylCdnOIXwi"
        "?st=TBomGhMfOetjH6thCwy$zzXyj!5bZs6Q20MejPCDCdNmX7IO9MqzRB6DkJ$PEOpl",
    ),
]


class Command(BaseCommand):
    help = "Seed status monitor endpoints and register periodic Celery Beat tasks"

    def handle(self, *args, **options):
        for name, url in ENDPOINTS:
            _, created = Endpoint.objects.get_or_create(url=url, defaults={"name": name})
            action = "Created" if created else "Already exists"
            self.stdout.write(f"  {action}: {name}")

        schedule_5min, _ = IntervalSchedule.objects.get_or_create(
            every=5, period=IntervalSchedule.MINUTES
        )
        task, created = PeriodicTask.objects.update_or_create(
            name="Status Monitor - Check All Endpoints",
            defaults={
                "task": "status_monitor.check_all_endpoints",
                "interval": schedule_5min,
                "enabled": True,
            },
        )
        self.stdout.write(
            self.style.SUCCESS(
                f"{'Created' if created else 'Updated'} periodic task: '{task.name}'"
            )
        )

        schedule_daily, _ = IntervalSchedule.objects.get_or_create(
            every=1, period=IntervalSchedule.DAYS
        )
        purge_task, created = PeriodicTask.objects.update_or_create(
            name="Status Monitor - Purge Old Checks",
            defaults={
                "task": "status_monitor.purge_old_checks",
                "interval": schedule_daily,
                "enabled": True,
            },
        )
        self.stdout.write(
            self.style.SUCCESS(
                f"{'Created' if created else 'Updated'} periodic task: '{purge_task.name}'"
            )
        )

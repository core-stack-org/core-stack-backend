from django.core.management.base import BaseCommand
from django_celery_beat.models import IntervalSchedule, PeriodicTask

from computing.pending_geoserver_sync import queue_unsynced_layers

PERIODIC_TASK_NAME = "Sync Unsynced Local Layers To GeoServer"


class Command(BaseCommand):
    help = (
        "Push locally generated layers (Layer.is_sync_to_geoserver=False) to "
        "GeoServer for tehsils that are active now. Use --schedule to run it "
        "periodically via Celery Beat."
    )

    def add_arguments(self, parser):
        parser.add_argument("--state")
        parser.add_argument("--district")
        parser.add_argument("--block")
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument(
            "--schedule",
            type=int,
            metavar="MINUTES",
            help="Register a Celery Beat task running every MINUTES, then exit.",
        )

    def handle(self, *args, **options):
        if options["schedule"]:
            self._schedule(options["schedule"])
            return
        try:
            summary = queue_unsynced_layers(
                options["state"],
                options["district"],
                options["block"],
                dry_run=options["dry_run"],
            )
        except Exception as exc:
            self.stderr.write(f"Could not fetch unsynced layers: {exc}")
            return
        verb = "would queue" if options["dry_run"] else "queued"
        self.stdout.write(
            f"{summary['found']} unsynced layer(s) in active tehsils; "
            f"{verb} {summary['queued']}, {summary['skipped_unusable']} have no local file."
        )

    def _schedule(self, minutes):
        interval, _ = IntervalSchedule.objects.get_or_create(
            every=minutes, period=IntervalSchedule.MINUTES
        )
        task, created = PeriodicTask.objects.update_or_create(
            name=PERIODIC_TASK_NAME,
            defaults={
                "task": "computing.tasks.sync_unsynced_local_layers",
                "interval": interval,
                "enabled": True,
            },
        )
        self.stdout.write(
            self.style.SUCCESS(
                f"{'Created' if created else 'Updated'} periodic task "
                f"'{task.name}' (every {minutes} min)."
            )
        )

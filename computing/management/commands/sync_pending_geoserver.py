from django.core.management.base import BaseCommand, CommandError
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
        parser.add_argument(
            "--layer-type",
            help=(
                "Bulk pipeline name (as in bulk_generate_layers). Only that "
                "layer type is pushed; without it all local layers are."
            ),
        )
        parser.add_argument(
            "--source",
            choices=("all", "local", "gee"),
            default="all",
            help="Which layers to push: locally generated, GEE-generated or both.",
        )
        parser.add_argument("--gee-account-id")
        parser.add_argument(
            "--force",
            action="store_true",
            help=(
                "Also re-push layers already marked as synced (active tehsils "
                "only). Requires --yes unless --dry-run."
            ),
        )
        parser.add_argument("--yes", action="store_true")
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
        if options["force"] and not (options["dry_run"] or options["yes"]):
            raise CommandError(
                "--force re-pushes layers already on GeoServer; "
                "check with --dry-run, then pass --yes."
            )
        try:
            summary = queue_unsynced_layers(
                options["state"],
                options["district"],
                options["block"],
                layer_type=options["layer_type"],
                dry_run=options["dry_run"],
                source=options["source"],
                gee_account_id=options["gee_account_id"],
                force=options["force"],
            )
        except ValueError as exc:
            raise CommandError(str(exc))
        except Exception as exc:
            self.stderr.write(f"Could not fetch unsynced layers: {exc}")
            return
        verb = "would queue" if options["dry_run"] else "queued"
        scope = "layer(s) in active tehsils" if options["force"] else "unsynced layer(s) in active tehsils"
        self._report("local", summary, verb, scope)
        self._report("gee", summary["gee"], verb, scope)

    def _report(self, label, summary, verb, scope):
        self.stdout.write(
            f"[{label}] {summary['found']} {scope}; {verb} {summary['queued']}, "
            f"{summary['skipped_unusable']} skipped."
        )
        for item in summary["queued_layers"]:
            self.stdout.write(
                f"  {verb}: {item['workspace']}:{item['layer_name']} "
                f"({item['state']}/{item['district']}/{item['block']})"
            )
        for item in summary["skipped_layers"]:
            self.stdout.write(
                f"  skipped: {item['workspace']}:{item['layer_name']} - {item['reason']}"
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

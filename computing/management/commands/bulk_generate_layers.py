from django.core.management.base import BaseCommand, CommandError

from computing.bulk_layer_generation import (
    get_active_locations,
    get_active_locations_from_api,
    pipeline_names,
    validate_pipeline,
)
from computing.tasks import bulk_generate_layer


DEFAULT_QUEUE = "layer_bulk"


class Command(BaseCommand):
    help = "Queue a registered layer pipeline for active locations."

    def add_arguments(self, parser):
        parser.add_argument("pipeline", nargs="?")
        parser.add_argument("--all-active", action="store_true")
        parser.add_argument(
            "--from-prod-api",
            action="store_true",
            help=(
                "Load active locations from PROD_BACKEND_URL instead of "
                "the local database."
            ),
        )
        parser.add_argument("--state")
        parser.add_argument("--district")
        parser.add_argument(
            "--block",
            dest="blocks",
            action="append",
            help="Tehsil/block name. Repeat to select multiple blocks.",
        )
        parser.add_argument("--limit", type=int)
        parser.add_argument("--queue", default=DEFAULT_QUEUE)
        parser.add_argument("--compute", choices=("local", "gee"), default="local")
        parser.add_argument("--start-year", type=int)
        parser.add_argument("--end-year", type=int)
        parser.add_argument("--gee-account-id")
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument("--list-pipelines", action="store_true")

        overwrite_group = parser.add_mutually_exclusive_group()
        overwrite_group.add_argument(
            "--overwrite", dest="overwrite", action="store_true"
        )
        overwrite_group.add_argument(
            "--no-overwrite", dest="overwrite", action="store_false"
        )
        parser.set_defaults(overwrite=True)

    def handle(self, *args, **options):
        if options["list_pipelines"]:
            self._list_pipelines(options["compute"])
            return

        pipeline = options["pipeline"]
        if not pipeline:
            raise CommandError("A pipeline name is required.")
        try:
            validate_pipeline(
                pipeline,
                compute=options["compute"],
                start_year=options["start_year"],
                end_year=options["end_year"],
                gee_account_id=options["gee_account_id"],
                overwrite=options["overwrite"],
            )
        except ValueError as exc:
            raise CommandError(str(exc)) from exc

        filters = {
            name: options[name]
            for name in ("state", "district")
            if options[name]
        }
        if options["blocks"]:
            filters["blocks"] = options["blocks"]
        if not options["all_active"] and not filters:
            raise CommandError(
                "Specify --all-active or at least one of "
                "--state, --district, or --block."
            )
        if options["all_active"] and filters:
            raise CommandError(
                "--all-active cannot be combined with location filters."
            )
        if options["limit"] is not None and options["limit"] < 1:
            raise CommandError("--limit must be greater than zero.")
        queue = options["queue"].strip()
        if not queue:
            raise CommandError("--queue cannot be empty.")

        location_loader = (
            get_active_locations_from_api
            if options["from_prod_api"]
            else get_active_locations
        )
        try:
            locations = location_loader(
                **filters,
                limit=options["limit"],
            )
        except ValueError as exc:
            raise CommandError(str(exc)) from exc
        if not locations:
            raise CommandError("No active locations matched the requested scope.")

        action = "Would queue" if options["dry_run"] else "Queueing"
        self.stdout.write(
            f"{action} {options['compute']} pipeline '{pipeline}' for "
            f"{len(locations)} active "
            f"location(s) on queue '{queue}'."
        )

        for location in locations:
            location_data = location.asdict()
            label = (
                f"{location.state}/{location.district}/{location.block}"
            )
            if options["dry_run"]:
                self.stdout.write(f"  {label}")
                continue

            result = bulk_generate_layer.apply_async(
                kwargs={
                    "pipeline": pipeline,
                    "location": location_data,
                    "overwrite": options["overwrite"],
                    "compute": options["compute"],
                    "start_year": options["start_year"],
                    "end_year": options["end_year"],
                    "gee_account_id": options["gee_account_id"],
                },
                queue=queue,
            )
            self.stdout.write(f"  {label}: {result.id}")

        if options["dry_run"]:
            self.stdout.write(self.style.SUCCESS("Dry run complete; no tasks queued."))
        else:
            self.stdout.write(
                self.style.SUCCESS(f"Queued {len(locations)} task(s).")
            )

    def _list_pipelines(self, compute):
        self.stdout.write(f"Registered {compute} pipelines:")
        for name in pipeline_names(compute):
            self.stdout.write(f"  - {name}")

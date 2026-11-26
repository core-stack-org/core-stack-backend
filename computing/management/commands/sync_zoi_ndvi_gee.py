from django.core.management.base import BaseCommand, CommandError

from computing.bulk_layer_generation import (
    get_active_locations,
    get_active_locations_from_api,
)
from computing.zoi_layers.zoi import _resolve_zoi_time_window
from computing.zoi_layers.zoi_local import (
    sync_local_zoi_to_gee_and_run_ndvi,
    sync_zoi_ndvi_gee,
    zoi_output_path,
)

DEFAULT_QUEUE = "waterbody"


class Command(BaseCommand):
    help = (
        "Upload locally generated ZOI (zoi1/zoi2) GeoPackages for active "
        "locations to GEE, compute NDVI (zoi3) on GEE, save the enriched "
        "output locally, and update the GeoServer layer."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--all-active",
            action="store_true",
            help="Process all active locations.",
        )
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

        parser.add_argument("--start-year", type=int)
        parser.add_argument("--end-year", type=int)
        parser.add_argument("--start-date")
        parser.add_argument("--end-date")

        parser.add_argument("--gee-account-id")
        parser.add_argument("--app-type", default="MWS")
        parser.add_argument(
            "--upload-only",
            action="store_true",
            help="Upload local ZOI GeoPackage to GEE without running NDVI.",
        )
        parser.add_argument(
            "--overwrite-gee-upload",
            action="store_true",
            help="Re-upload local ZOI GeoPackage even if the GEE input asset exists.",
        )
        parser.add_argument(
            "--skip-existing-ndvi",
            action="store_true",
            help=(
                "Reuse an existing GEE NDVI asset instead of recomputing it, "
                "while still refreshing the local GeoPackage and GeoServer."
            ),
        )
        parser.add_argument(
            "--fast-ndvi",
            action="store_true",
            help=(
                "Use the timeseries band-stack NDVI runner "
                "(zoi_ndvi_from_timeseries) instead of standard zoi3."
            ),
        )
        parser.add_argument(
            "--queue",
            default=DEFAULT_QUEUE,
            help=f"Celery queue when running asynchronously (default: {DEFAULT_QUEUE}).",
        )
        parser.add_argument(
            "--sync",
            action="store_true",
            help="Run synchronously in-process instead of queueing Celery tasks.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="List matched active locations and local ZOI availability without running.",
        )

    def handle(self, *args, **options):
        filters = {
            name: options[name]
            for name in ("state", "district")
            if options[name]
        }
        if options["blocks"]:
            filters["blocks"] = options["blocks"]

        if not options["all_active"] and not filters:
            raise CommandError(
                "Specify --all-active or at least one of --state, --district, or --block."
            )
        if options["all_active"] and filters:
            raise CommandError(
                "--all-active cannot be combined with location filters."
            )
        if options["limit"] is not None and options["limit"] < 1:
            raise CommandError("--limit must be greater than zero.")

        queue = options["queue"].strip()
        if not options["sync"] and not queue:
            raise CommandError("--queue cannot be empty.")

        start_date = options["start_date"]
        end_date = options["end_date"]
        start_year = options["start_year"]
        end_year = options["end_year"]

        if not options["upload_only"]:
            try:
                start_date, end_date, start_year, end_year = _resolve_zoi_time_window(
                    start_date=start_date,
                    end_date=end_date,
                    start_year=start_year,
                    end_year=end_year,
                )
            except ValueError as exc:
                raise CommandError(str(exc)) from exc

        location_loader = (
            get_active_locations_from_api
            if options["from_prod_api"]
            else get_active_locations
        )
        try:
            locations = location_loader(**filters, limit=options["limit"])
        except ValueError as exc:
            raise CommandError(str(exc)) from exc

        if not locations:
            raise CommandError("No active locations matched the requested scope.")

        mode = (
            "upload-only"
            if options["upload_only"]
            else ("fast NDVI (timeseries)" if options["fast_ndvi"] else "standard NDVI")
        )
        if options["dry_run"]:
            action = "Dry run"
        elif options["sync"]:
            action = "Running synchronously"
        else:
            action = f"Queueing on '{queue}'"

        self.stdout.write(
            f"{action} ZOI GEE {mode} for {len(locations)} active location(s)."
        )

        processed = 0
        skipped_missing = 0
        failed = 0

        for location in locations:
            label = f"{location.state}/{location.district}/{location.block}"
            local_path = zoi_output_path(
                location.state, location.district, location.block, create_dir=False
            )
            if not local_path.exists():
                skipped_missing += 1
                self.stdout.write(
                    f"  {label}: missing local ZOI file ({local_path}), skipped"
                )
                continue

            if options["dry_run"]:
                processed += 1
                self.stdout.write(f"  {label}: ready ({local_path})")
                continue

            common_kwargs = {
                "state": location.state,
                "district": location.district,
                "block": location.block,
                "start_date": start_date,
                "end_date": end_date,
                "start_year": start_year,
                "end_year": end_year,
                "app_type": options["app_type"],
                "gee_account_id": options["gee_account_id"],
                "upload_only": options["upload_only"],
                "overwrite_gee_upload": options["overwrite_gee_upload"],
                "skip_existing_ndvi": options["skip_existing_ndvi"],
                "fast_ndvi": options["fast_ndvi"],
            }

            if options["sync"]:
                try:
                    result = sync_local_zoi_to_gee_and_run_ndvi(
                        **common_kwargs,
                        sync_geoserver_inline=True,
                    )
                except Exception as exc:
                    failed += 1
                    self.stderr.write(self.style.ERROR(f"  {label}: FAILED ({exc})"))
                    continue
                processed += 1
                self.stdout.write(
                    f"  {label}: {result['stage']} (gee={result['ndvi_asset_id'] or result['zoi_asset_id']})"
                )
            else:
                result = sync_zoi_ndvi_gee.apply_async(
                    kwargs=common_kwargs,
                    queue=queue,
                )
                processed += 1
                self.stdout.write(f"  {label}: queued ({result.id})")

        if options["dry_run"]:
            self.stdout.write(
                self.style.SUCCESS(
                    f"Dry run complete; {processed} ready, {skipped_missing} missing local ZOI."
                )
            )
        elif options["sync"]:
            summary = (
                f"Completed {processed} location(s); "
                f"{skipped_missing} missing local ZOI; {failed} failed."
            )
            if failed:
                self.stderr.write(self.style.ERROR(summary))
                raise CommandError(summary)
            self.stdout.write(self.style.SUCCESS(summary))
        else:
            self.stdout.write(
                self.style.SUCCESS(
                    f"Queued {processed} task(s); {skipped_missing} missing local ZOI."
                )
            )

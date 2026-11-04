from django.core.management.base import BaseCommand, CommandError

from computing.bulk_layer_generation import (
    get_active_locations,
    get_active_locations_from_api,
)
from computing.local_compute_helper import sync_tehsil_boundaries_to_geoserver


class Command(BaseCommand):
    help = (
        "Publish the local admin boundary (panchayat_boundaries:{district}_{block}) "
        "and tehsil MWS boundary (mws:mws_{district}_{block}) to GeoServer for "
        "active locations."
    )

    def add_arguments(self, parser):
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
        parser.add_argument(
            "--overwrite",
            action="store_true",
            help="Re-push layers that already exist on GeoServer.",
        )
        parser.add_argument("--dry-run", action="store_true")

    def handle(self, *args, **options):
        if options["limit"] is not None and options["limit"] < 1:
            raise CommandError("--limit must be greater than zero.")

        loader = (
            get_active_locations_from_api
            if options["from_prod_api"]
            else get_active_locations
        )
        filters = {
            name: options[name] for name in ("state", "district") if options[name]
        }
        if options["blocks"]:
            filters["blocks"] = options["blocks"]
        try:
            locations = loader(**filters, limit=options["limit"])
        except ValueError as exc:
            raise CommandError(str(exc)) from exc
        if not locations:
            raise CommandError("No locations matched the requested scope.")

        verb = "Would sync" if options["dry_run"] else "Syncing"
        self.stdout.write(f"{verb} boundaries for {len(locations)} location(s).")

        pushed = skipped = failed = 0
        for loc in locations:
            label = f"{loc.state}/{loc.district}/{loc.block}"
            if options["dry_run"]:
                self.stdout.write(f"  {label}")
                continue
            try:
                result = sync_tehsil_boundaries_to_geoserver(
                    loc.state,
                    loc.district,
                    loc.block,
                    overwrite=options["overwrite"],
                )
            except Exception as exc:  # keep going; report per-tehsil failures
                failed += 1
                self.stderr.write(f"  {label}: FAILED - {exc}")
                continue
            # A None entry means the layer was already on GeoServer.
            admin = "pushed" if result["admin_boundary"] else "exists"
            mws = "pushed" if result["mws"] else "exists"
            if result["admin_boundary"] or result["mws"]:
                pushed += 1
            else:
                skipped += 1
            self.stdout.write(f"  {label}: admin={admin}, mws={mws}")

        if options["dry_run"]:
            return
        summary = (
            f"Done: {pushed} pushed, {skipped} already on GeoServer, "
            f"{failed} failed."
        )
        self.stdout.write(
            self.style.ERROR(summary) if failed else self.style.SUCCESS(summary)
        )

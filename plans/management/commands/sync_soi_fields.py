from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Q

from geoadmin.models import DistrictSOI, StateSOI, TehsilSOI
from plans.models import PlanApp


class Command(BaseCommand):
    help = "Sync SOI fields in PlanApp by matching names from State/District/Block with SOI tables"

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Run command without making any changes to the database",
        )
        parser.add_argument(
            "--verbose",
            action="store_true",
            help="Display verbose output during execution",
        )
        parser.add_argument(
            "--limit",
            type=int,
            help="Limit the number of records to process",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        verbose = options["verbose"]
        limit = options.get("limit")

        if dry_run:
            self.stdout.write(
                self.style.WARNING(
                    "DRY RUN MODE - No changes will be made to the database"
                )
            )

        planapps = PlanApp.objects.select_related(
            "state", "district", "block", "state_soi", "district_soi", "tehsil_soi"
        ).all()

        if limit:
            planapps = planapps[:limit]
            self.stdout.write(f"Processing limited to {limit} records")

        total_records = planapps.count()
        if verbose:
            self.stdout.write(f"Found {total_records} PlanApp records to process")

        if total_records == 0:
            self.stdout.write(self.style.WARNING("No PlanApp records found"))
            return

        stats = {
            "state_matched": 0,
            "state_multiple": 0,
            "state_not_found": 0,
            "state_already_set": 0,
            "district_matched": 0,
            "district_multiple": 0,
            "district_not_found": 0,
            "district_already_set": 0,
            "block_matched": 0,
            "block_multiple": 0,
            "block_not_found": 0,
            "block_already_set": 0,
            "total_updated": 0,
            "errors": 0,
        }

        state_not_found_list = set()
        district_not_found_list = set()
        block_not_found_list = set()

        with transaction.atomic():
            if dry_run:
                savepoint = transaction.savepoint()

            for idx, planapp in enumerate(planapps, 1):
                try:
                    updated = False

                    # Process State SOI
                    if planapp.state and not planapp.state_soi:
                        state_name = planapp.state.state_name.strip()
                        state_soi_matches = StateSOI.objects.filter(
                            state_name__iexact=state_name
                        )

                        if state_soi_matches.count() == 1:
                            planapp.state_soi = state_soi_matches.first()
                            stats["state_matched"] += 1
                            updated = True
                            if verbose:
                                self.stdout.write(
                                    f"  [State] Matched '{state_name}' -> StateSOI ID {planapp.state_soi.id}"
                                )
                        elif state_soi_matches.count() > 1:
                            stats["state_multiple"] += 1
                            self.stdout.write(
                                self.style.WARNING(
                                    f"  [State] Multiple matches for '{state_name}' (IDs: {list(state_soi_matches.values_list('id', flat=True))})"
                                )
                            )
                        else:
                            stats["state_not_found"] += 1
                            state_not_found_list.add(state_name)
                            if verbose:
                                self.stdout.write(
                                    self.style.WARNING(
                                        f"  [State] No match found for '{state_name}'"
                                    )
                                )
                    elif planapp.state_soi:
                        stats["state_already_set"] += 1

                    # Process District SOI
                    if planapp.district and not planapp.district_soi:
                        district_name = planapp.district.district_name.strip()
                        district_soi_matches = DistrictSOI.objects.filter(
                            district_name__iexact=district_name
                        )

                        # If state_soi is set, filter by state_soi for better accuracy
                        if planapp.state_soi:
                            district_soi_matches = district_soi_matches.filter(
                                state=planapp.state_soi
                            )

                        if district_soi_matches.count() == 1:
                            planapp.district_soi = district_soi_matches.first()
                            stats["district_matched"] += 1
                            updated = True
                            if verbose:
                                self.stdout.write(
                                    f"  [District] Matched '{district_name}' -> DistrictSOI ID {planapp.district_soi.id}"
                                )
                        elif district_soi_matches.count() > 1:
                            stats["district_multiple"] += 1
                            self.stdout.write(
                                self.style.WARNING(
                                    f"  [District] Multiple matches for '{district_name}' (IDs: {list(district_soi_matches.values_list('id', flat=True))})"
                                )
                            )
                        else:
                            stats["district_not_found"] += 1
                            district_not_found_list.add(district_name)
                            if verbose:
                                self.stdout.write(
                                    self.style.WARNING(
                                        f"  [District] No match found for '{district_name}'"
                                    )
                                )
                    elif planapp.district_soi:
                        stats["district_already_set"] += 1

                    if planapp.block and not planapp.tehsil_soi:
                        block_name = planapp.block.block_name.strip()
                        tehsil_soi_matches = TehsilSOI.objects.filter(
                            tehsil_name__iexact=block_name
                        )

                        if planapp.district_soi:
                            tehsil_soi_matches = tehsil_soi_matches.filter(
                                district=planapp.district_soi
                            )

                        if tehsil_soi_matches.count() == 1:
                            planapp.tehsil_soi = tehsil_soi_matches.first()
                            stats["block_matched"] += 1
                            updated = True
                            if verbose:
                                self.stdout.write(
                                    f"  [Block] Matched '{block_name}' -> TehsilSOI ID {planapp.tehsil_soi.id}"
                                )
                        elif tehsil_soi_matches.count() > 1:
                            stats["block_multiple"] += 1
                            self.stdout.write(
                                self.style.WARNING(
                                    f"  [Block] Multiple matches for '{block_name}' (IDs: {list(tehsil_soi_matches.values_list('id', flat=True))})"
                                )
                            )
                        else:
                            stats["block_not_found"] += 1
                            block_not_found_list.add(block_name)
                            if verbose:
                                self.stdout.write(
                                    self.style.WARNING(
                                        f"  [Block] No match found for '{block_name}'"
                                    )
                                )
                    elif planapp.tehsil_soi:
                        stats["block_already_set"] += 1

                    if updated:
                        if not dry_run:
                            planapp.save()
                        stats["total_updated"] += 1

                        if verbose:
                            self.stdout.write(
                                self.style.SUCCESS(
                                    f"{'[DRY RUN] ' if dry_run else ''}Updated PlanApp ID {planapp.id}"
                                )
                            )
                    elif verbose:
                        self.stdout.write(
                            f"No updates needed for PlanApp ID {planapp.id}"
                        )

                    if not verbose and idx % 100 == 0:
                        self.stdout.write(f"Processed {idx}/{total_records} records...")

                except Exception as e:
                    stats["errors"] += 1
                    self.stdout.write(
                        self.style.ERROR(
                            f"Error processing PlanApp ID {planapp.id}: {str(e)}"
                        )
                    )

            if dry_run:
                transaction.savepoint_rollback(savepoint)

        self.stdout.write("\n" + "=" * 70)
        self.stdout.write(
            self.style.SUCCESS(f"{'DRY RUN ' if dry_run else ''}SUMMARY:")
        )
        self.stdout.write(f"Total records processed: {total_records}")
        self.stdout.write(
            f"Records {'would be ' if dry_run else ''}updated: {stats['total_updated']}"
        )
        self.stdout.write("")

        self.stdout.write("STATE SOI:")
        self.stdout.write(f"Matched: {stats['state_matched']}")
        self.stdout.write(f"Multiple matches: {stats['state_multiple']}")
        self.stdout.write(f"Not found: {stats['state_not_found']}")
        self.stdout.write(f"Already set: {stats['state_already_set']}")

        self.stdout.write("\nDISTRICT SOI:")
        self.stdout.write(f"Matched: {stats['district_matched']}")
        self.stdout.write(f"Multiple matches: {stats['district_multiple']}")
        self.stdout.write(f"Not found: {stats['district_not_found']}")
        self.stdout.write(f"Already set: {stats['district_already_set']}")

        self.stdout.write("\nBLOCK/TEHSIL SOI:")
        self.stdout.write(f"Matched: {stats['block_matched']}")
        self.stdout.write(f"Multiple matches: {stats['block_multiple']}")
        self.stdout.write(f"Not found: {stats['block_not_found']}")
        self.stdout.write(f"Already set: {stats['block_already_set']}")

        self.stdout.write(f"\nErrors encountered: {stats['errors']}")

        if state_not_found_list:
            self.stdout.write("\n" + "-" * 70)
            self.stdout.write("States not found in SOI table:")
            for name in sorted(state_not_found_list):
                self.stdout.write(f"  - {name}")

        if district_not_found_list:
            self.stdout.write("\n" + "-" * 70)
            self.stdout.write("Districts not found in SOI table:")
            for name in sorted(district_not_found_list):
                self.stdout.write(f"  - {name}")

        if block_not_found_list:
            self.stdout.write("\n" + "-" * 70)
            self.stdout.write("Blocks not found in Tehsil SOI table:")
            for name in sorted(block_not_found_list):
                self.stdout.write(f"  - {name}")

        if dry_run:
            self.stdout.write("\n" + "=" * 70)
            self.stdout.write(
                self.style.WARNING(
                    "This was a DRY RUN. No changes were made to the database."
                )
            )
            self.stdout.write(
                "Run the command without --dry-run to actually update the records."
            )
        else:
            self.stdout.write("\n" + "=" * 70)
            self.stdout.write(
                self.style.SUCCESS(
                    f"Successfully synced SOI fields for {stats['total_updated']} PlanApp records!"
                )
            )

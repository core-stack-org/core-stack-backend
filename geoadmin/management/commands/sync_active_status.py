from django.core.management.base import BaseCommand

from geoadmin.models import Block, District, DistrictSOI, State, StateSOI, TehsilSOI


class Command(BaseCommand):
    help = """Sync active_status from Census models (State, District, Block)
    to SOI models (StateSOI, DistrictSOI, TehsilSOI) based on name matching.
    Only processes records where active_status=True in source models."""

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Run the command without making any changes to the database",
        )
        parser.add_argument(
            "--verbose",
            action="store_true",
            help="Show detailed output for each match/mismatch",
        )
        parser.add_argument(
            "--include-inactive",
            action="store_true",
            help="Include inactive items from source models (by default only active items are processed)",
        )
        parser.add_argument(
            "--case-sensitive",
            action="store_true",
            help="Use case-sensitive matching (by default matching is case-insensitive)",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        verbose = options["verbose"]
        include_inactive = options["include_inactive"]
        case_sensitive = options["case_sensitive"]

        if dry_run:
            self.stdout.write(self.style.WARNING("=" * 60))
            self.stdout.write(
                self.style.WARNING("DRY RUN MODE - No changes will be made")
            )
            self.stdout.write(self.style.WARNING("=" * 60))
            self.stdout.write("")

        # Display matching mode
        self.stdout.write(
            self.style.NOTICE(
                f"Matching mode: {'Case-sensitive' if case_sensitive else 'Case-insensitive'}"
            )
        )
        self.stdout.write(
            self.style.NOTICE(
                f"Processing: {'All items' if include_inactive else 'Only active items (active_status=True)'}"
            )
        )
        self.stdout.write("")

        # Sync States -> StateSOI
        self.stdout.write(self.style.MIGRATE_HEADING("Syncing State -> StateSOI"))
        self.stdout.write("-" * 40)
        self.sync_states(dry_run, verbose, include_inactive, case_sensitive)

        # Sync Districts -> DistrictSOI
        self.stdout.write("")
        self.stdout.write(self.style.MIGRATE_HEADING("Syncing District -> DistrictSOI"))
        self.stdout.write("-" * 40)
        self.sync_districts(dry_run, verbose, include_inactive, case_sensitive)

        # Sync Blocks -> TehsilSOI
        self.stdout.write("")
        self.stdout.write(self.style.MIGRATE_HEADING("Syncing Block -> TehsilSOI"))
        self.stdout.write("-" * 40)
        self.sync_blocks_to_tehsils(dry_run, verbose, include_inactive, case_sensitive)

        if dry_run:
            self.stdout.write("")
            self.stdout.write(self.style.WARNING("=" * 60))
            self.stdout.write(
                self.style.WARNING("DRY RUN COMPLETE - No changes were made")
            )
            self.stdout.write(self.style.WARNING("=" * 60))
        else:
            self.stdout.write("")
            self.stdout.write(self.style.SUCCESS("=" * 60))
            self.stdout.write(self.style.SUCCESS("SYNC COMPLETE - Database updated"))
            self.stdout.write(self.style.SUCCESS("=" * 60))

    def normalize_name(self, name, case_sensitive):
        """Normalize name for matching"""
        if name:
            name = name.strip()
            if not case_sensitive:
                name = name.lower()
            return name
        return ""

    def sync_states(self, dry_run, verbose, include_inactive, case_sensitive):
        """Sync State active_status to StateSOI"""
        # ONLY get states with active_status=True by default
        if include_inactive:
            states = State.objects.all()
        else:
            states = State.objects.filter(active_status=True)

        matched = []
        unmatched_states = []
        unmatched_soi = []
        updated = []
        deactivated = []

        # Build dictionary of StateSOI for efficient lookup
        state_soi_dict = {}
        all_state_soi = {}
        for state_soi in StateSOI.objects.all():
            all_state_soi[state_soi.id] = state_soi
            key = self.normalize_name(state_soi.state_name, case_sensitive)
            state_soi_dict[key] = state_soi

        # Process each active state
        for state in states:
            state_key = self.normalize_name(state.state_name, case_sensitive)

            if state_key in state_soi_dict:
                state_soi = state_soi_dict[state_key]
                matched.append((state, state_soi))

                if state_soi.active_status != state.active_status:
                    if not dry_run:
                        state_soi.active_status = state.active_status
                        state_soi.save()
                    updated.append((state, state_soi))

                    if verbose:
                        status_change = "Active" if state.active_status else "Inactive"
                        self.stdout.write(
                            self.style.SUCCESS(
                                f"  ✓ {state.state_name} -> {state_soi.state_name}: Set to {status_change}"
                            )
                        )
                elif verbose:
                    self.stdout.write(
                        f"  - {state.state_name}: Already synced (active={state.active_status})"
                    )

                # Remove from dict to track unmatched SOI states
                del state_soi_dict[state_key]
                del all_state_soi[state_soi.id]
            else:
                unmatched_states.append(state)
                if verbose:
                    self.stdout.write(
                        self.style.ERROR(
                            f"  ✗ State '{state.state_name}' not found in StateSOI"
                        )
                    )

        # Remaining StateSOI that didn't match any active State should be set to inactive
        if not include_inactive:
            for state_soi in all_state_soi.values():
                if state_soi.active_status:  # Only if it's currently active
                    if not dry_run:
                        state_soi.active_status = False
                        state_soi.save()
                    deactivated.append(state_soi)
                    if verbose:
                        self.stdout.write(
                            self.style.WARNING(
                                f"  ⚠ Deactivating StateSOI '{state_soi.state_name}' (no matching active State)"
                            )
                        )

        # Remaining items in dict are unmatched SOI states
        unmatched_soi = list(state_soi_dict.values())

        # Print summary
        self.stdout.write("\nState Summary:")
        self.stdout.write(f"  Total Active States to sync: {states.count()}")
        self.stdout.write(f"  Matched: {len(matched)}")
        self.stdout.write(
            self.style.SUCCESS(f"  Activated/Updated in SOI: {len(updated)}")
        )
        if not include_inactive:
            self.stdout.write(
                self.style.WARNING(
                    f"  Deactivated in SOI (no active match): {len(deactivated)}"
                )
            )
        self.stdout.write(
            self.style.ERROR(
                f"  Active States not found in StateSOI: {len(unmatched_states)}"
            )
        )

        if unmatched_states and not verbose:
            self.stdout.write(
                self.style.ERROR("\n  Active States not found in StateSOI:")
            )
            for state in unmatched_states[:10]:
                self.stdout.write(
                    f"    - {state.state_name} (Census Code: {state.state_census_code})"
                )
            if len(unmatched_states) > 10:
                self.stdout.write(f"    ... and {len(unmatched_states) - 10} more")

        if deactivated and not verbose and not include_inactive:
            self.stdout.write(
                self.style.WARNING(
                    "\n  StateSOI deactivated (no matching active State):"
                )
            )
            for state_soi in deactivated[:10]:
                self.stdout.write(f"    - {state_soi.state_name}")
            if len(deactivated) > 10:
                self.stdout.write(f"    ... and {len(deactivated) - 10} more")

    def sync_districts(self, dry_run, verbose, include_inactive, case_sensitive):
        """Sync District active_status to DistrictSOI"""
        # ONLY get districts with active_status=True by default
        if include_inactive:
            districts = District.objects.select_related("state").all()
        else:
            districts = District.objects.select_related("state").filter(
                active_status=True
            )

        matched = []
        unmatched_districts = []
        updated = []
        deactivated = []

        # Build dictionary of DistrictSOI for efficient lookup
        district_soi_dict = {}
        all_district_soi = {}
        for district_soi in DistrictSOI.objects.select_related("state").all():
            all_district_soi[district_soi.id] = district_soi
            key = self.normalize_name(district_soi.district_name, case_sensitive)
            if key not in district_soi_dict:
                district_soi_dict[key] = []
            district_soi_dict[key].append(district_soi)

        # Track which DistrictSOI entries have been matched
        matched_soi_ids = set()

        for district in districts:
            district_key = self.normalize_name(district.district_name, case_sensitive)

            if district_key in district_soi_dict:
                # May have multiple districts with same name in different states
                district_soi_list = district_soi_dict[district_key]
                found_match = False

                for district_soi in district_soi_list:
                    # Try to match by state as well for better accuracy
                    state_match = self.normalize_name(
                        district.state.state_name, case_sensitive
                    ) == self.normalize_name(
                        district_soi.state.state_name, case_sensitive
                    )

                    if state_match or len(district_soi_list) == 1:
                        found_match = True
                        matched.append((district, district_soi))
                        matched_soi_ids.add(district_soi.id)

                        if district_soi.active_status != district.active_status:
                            if not dry_run:
                                district_soi.active_status = district.active_status
                                district_soi.save()
                            updated.append((district, district_soi))

                            if verbose:
                                status_change = (
                                    "Active" if district.active_status else "Inactive"
                                )
                                self.stdout.write(
                                    self.style.SUCCESS(
                                        f"  ✓ {district.district_name} ({district.state.state_name}) -> {district_soi.district_name}: Set to {status_change}"
                                    )
                                )
                        elif verbose:
                            self.stdout.write(
                                f"  - {district.district_name} ({district.state.state_name}): Already synced (active={district.active_status})"
                            )
                        break

                if not found_match:
                    unmatched_districts.append(district)
                    if verbose:
                        self.stdout.write(
                            self.style.ERROR(
                                f"  ✗ District '{district.district_name}' ({district.state.state_name}) not matched in DistrictSOI"
                            )
                        )
            else:
                unmatched_districts.append(district)
                if verbose:
                    self.stdout.write(
                        self.style.ERROR(
                            f"  ✗ District '{district.district_name}' ({district.state.state_name}) not found in DistrictSOI"
                        )
                    )

        # Deactivate DistrictSOI that didn't match any active District
        if not include_inactive:
            for district_soi in all_district_soi.values():
                if (
                    district_soi.id not in matched_soi_ids
                    and district_soi.active_status
                ):
                    if not dry_run:
                        district_soi.active_status = False
                        district_soi.save()
                    deactivated.append(district_soi)
                    if verbose:
                        self.stdout.write(
                            self.style.WARNING(
                                f"  ⚠ Deactivating DistrictSOI '{district_soi.district_name}' (no matching active District)"
                            )
                        )

        # Print summary
        self.stdout.write("\nDistrict Summary:")
        self.stdout.write(f"  Total Active Districts to sync: {districts.count()}")
        self.stdout.write(f"  Matched: {len(matched)}")
        self.stdout.write(
            self.style.SUCCESS(f"  Activated/Updated in SOI: {len(updated)}")
        )
        if not include_inactive:
            self.stdout.write(
                self.style.WARNING(
                    f"  Deactivated in SOI (no active match): {len(deactivated)}"
                )
            )
        self.stdout.write(
            self.style.ERROR(
                f"  Active Districts not found in DistrictSOI: {len(unmatched_districts)}"
            )
        )

        if unmatched_districts and not verbose:
            self.stdout.write(
                self.style.ERROR("\n  Active Districts not found in DistrictSOI:")
            )
            for district in unmatched_districts[:10]:
                self.stdout.write(
                    f"    - {district.district_name} (State: {district.state.state_name}, Census Code: {district.district_census_code})"
                )
            if len(unmatched_districts) > 10:
                self.stdout.write(f"    ... and {len(unmatched_districts) - 10} more")

        if deactivated and not verbose and not include_inactive:
            self.stdout.write(
                self.style.WARNING(
                    "\n  DistrictSOI deactivated (no matching active District):"
                )
            )
            for district_soi in deactivated[:10]:
                self.stdout.write(
                    f"    - {district_soi.district_name} (State: {district_soi.state.state_name})"
                )
            if len(deactivated) > 10:
                self.stdout.write(f"    ... and {len(deactivated) - 10} more")

    def sync_blocks_to_tehsils(
        self, dry_run, verbose, include_inactive, case_sensitive
    ):
        """Sync Block active_status to TehsilSOI"""
        # ONLY get blocks with active_status=True by default
        if include_inactive:
            blocks = Block.objects.select_related("district", "district__state").all()
        else:
            blocks = Block.objects.select_related("district", "district__state").filter(
                active_status=True
            )

        matched = []
        unmatched_blocks = []
        updated = []
        deactivated = []

        # Build dictionary of TehsilSOI for efficient lookup
        tehsil_soi_dict = {}
        all_tehsil_soi = {}
        for tehsil_soi in TehsilSOI.objects.select_related(
            "district", "district__state"
        ).all():
            all_tehsil_soi[tehsil_soi.id] = tehsil_soi
            key = self.normalize_name(tehsil_soi.tehsil_name, case_sensitive)
            if key not in tehsil_soi_dict:
                tehsil_soi_dict[key] = []
            tehsil_soi_dict[key].append(tehsil_soi)

        # Track which TehsilSOI entries have been matched
        matched_soi_ids = set()

        for block in blocks:
            block_key = self.normalize_name(block.block_name, case_sensitive)

            if block_key in tehsil_soi_dict:
                # May have multiple tehsils with same name in different districts
                tehsil_soi_list = tehsil_soi_dict[block_key]
                found_match = False

                for tehsil_soi in tehsil_soi_list:
                    # Try to match by district as well for better accuracy
                    district_match = self.normalize_name(
                        block.district.district_name, case_sensitive
                    ) == self.normalize_name(
                        tehsil_soi.district.district_name, case_sensitive
                    )

                    if district_match or len(tehsil_soi_list) == 1:
                        found_match = True
                        matched.append((block, tehsil_soi))
                        matched_soi_ids.add(tehsil_soi.id)

                        if tehsil_soi.active_status != block.active_status:
                            if not dry_run:
                                tehsil_soi.active_status = block.active_status
                                tehsil_soi.save()
                            updated.append((block, tehsil_soi))

                            if verbose:
                                status_change = (
                                    "Active" if block.active_status else "Inactive"
                                )
                                self.stdout.write(
                                    self.style.SUCCESS(
                                        f"  ✓ {block.block_name} ({block.district.district_name}) -> {tehsil_soi.tehsil_name}: Set to {status_change}"
                                    )
                                )
                        elif verbose:
                            self.stdout.write(
                                f"  - {block.block_name} ({block.district.district_name}): Already synced (active={block.active_status})"
                            )
                        break

                if not found_match:
                    unmatched_blocks.append(block)
                    if verbose:
                        self.stdout.write(
                            self.style.ERROR(
                                f"  ✗ Block '{block.block_name}' ({block.district.district_name}) not matched in TehsilSOI"
                            )
                        )
            else:
                unmatched_blocks.append(block)
                if verbose:
                    self.stdout.write(
                        self.style.ERROR(
                            f"  ✗ Block '{block.block_name}' ({block.district.district_name}) not found in TehsilSOI"
                        )
                    )

        # Deactivate TehsilSOI that didn't match any active Block
        if not include_inactive:
            for tehsil_soi in all_tehsil_soi.values():
                if tehsil_soi.id not in matched_soi_ids and tehsil_soi.active_status:
                    if not dry_run:
                        tehsil_soi.active_status = False
                        tehsil_soi.save()
                    deactivated.append(tehsil_soi)
                    if verbose:
                        self.stdout.write(
                            self.style.WARNING(
                                f"  ⚠ Deactivating TehsilSOI '{tehsil_soi.tehsil_name}' (no matching active Block)"
                            )
                        )

        # Print summary
        self.stdout.write("\nBlock -> Tehsil Summary:")
        self.stdout.write(f"  Total Active Blocks to sync: {blocks.count()}")
        self.stdout.write(f"  Matched: {len(matched)}")
        self.stdout.write(
            self.style.SUCCESS(f"  Activated/Updated in SOI: {len(updated)}")
        )
        if not include_inactive:
            self.stdout.write(
                self.style.WARNING(
                    f"  Deactivated in SOI (no active match): {len(deactivated)}"
                )
            )
        self.stdout.write(
            self.style.ERROR(
                f"  Active Blocks not found in TehsilSOI: {len(unmatched_blocks)}"
            )
        )

        if unmatched_blocks and not verbose:
            self.stdout.write(
                self.style.ERROR("\n  Active Blocks not found in TehsilSOI:")
            )
            for block in unmatched_blocks[:10]:
                self.stdout.write(
                    f"    - {block.block_name} (District: {block.district.district_name}, State: {block.district.state.state_name}, Census Code: {block.block_census_code})"
                )
            if len(unmatched_blocks) > 10:
                self.stdout.write(f"    ... and {len(unmatched_blocks) - 10} more")

        if deactivated and not verbose and not include_inactive:
            self.stdout.write(
                self.style.WARNING(
                    "\n  TehsilSOI deactivated (no matching active Block):"
                )
            )
            for tehsil_soi in deactivated[:10]:
                self.stdout.write(
                    f"    - {tehsil_soi.tehsil_name} (District: {tehsil_soi.district.district_name})"
                )
            if len(deactivated) > 10:
                self.stdout.write(f"    ... and {len(deactivated) - 10} more")

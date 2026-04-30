import json
import os
import re

from django.core.management.base import BaseCommand

from computing.STAC_specs.stac_collection import STACConfig, sanitize_text
from geoadmin.models import DistrictSOI, StateSOI, TehsilSOI


def _stac_key(state, district, block):
    return (
        sanitize_text(state.lower()),
        sanitize_text(district.lower()),
        sanitize_text(block.lower()),
    )


def _scan_stac_filesystem(tehsil_dir):
    """
    Walk tehsil_wise/{state}/{district}/{block}/ and return:
        dict[(state, district, block)] -> list[layer_id]
    """
    coverage = {}
    if not os.path.isdir(tehsil_dir):
        return coverage

    for state_entry in os.scandir(tehsil_dir):
        if not state_entry.is_dir():
            continue
        for district_entry in os.scandir(state_entry.path):
            if not district_entry.is_dir():
                continue
            for block_entry in os.scandir(district_entry.path):
                if not block_entry.is_dir():
                    continue
                coll_path = os.path.join(block_entry.path, "collection.json")
                if not os.path.exists(coll_path):
                    continue
                layers = _list_layers(block_entry.path, coll_path)
                key = (state_entry.name, district_entry.name, block_entry.name)
                coverage[key] = layers

    return coverage


def _list_layers(block_dir, collection_path):
    """Return layer ids from the block collection's item links."""
    try:
        with open(collection_path) as f:
            coll = json.load(f)
    except (OSError, json.JSONDecodeError):
        return []

    layers = []
    for link in coll.get("links", []):
        if link.get("rel") == "item":
            href = link.get("href", "")
            item_path = os.path.normpath(os.path.join(block_dir, href))
            if os.path.exists(item_path):
                try:
                    with open(item_path) as f:
                        item = json.load(f)
                    layers.append(item.get("id", os.path.basename(item_path)))
                except (OSError, json.JSONDecodeError):
                    layers.append(os.path.basename(item_path))
    return layers


def _load_active_locations(state_filter=None):
    """
    Returns list of (state_name, district_name, tehsil_name) for all active tehsils.
    Applies optional state name filter (case-insensitive substring).
    """
    qs = StateSOI.objects.filter(active_status=True).prefetch_related(
        "districtsoi_set__tehsilsoi_set"
    )
    if state_filter:
        qs = qs.filter(state_name__icontains=state_filter)

    locations = []
    for state in qs:
        for district in state.districtsoi_set.filter(active_status=True):
            for tehsil in district.tehsilsoi_set.filter(active_status=True):
                locations.append((state.state_name, district.district_name, tehsil.tehsil_name))
    return locations


class Command(BaseCommand):
    help = (
        "Show STAC coverage: which active tehsils have STAC generated "
        "and which are missing."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--state",
            default=None,
            help="Filter by state name (case-insensitive substring match)",
        )
        parser.add_argument(
            "--missing-only",
            action="store_true",
            default=False,
            help="Only print tehsils that are missing STAC",
        )
        parser.add_argument(
            "--show-layers",
            action="store_true",
            default=False,
            help="Print the individual layer IDs for each block that has STAC",
        )

    def handle(self, *args, **options):
        config = STACConfig()
        tehsil_dir = os.path.join(config.stac_files_dir, config.tehsil_dirname)

        self.stdout.write(f"Scanning STAC directory: {tehsil_dir}")
        stac_coverage = _scan_stac_filesystem(tehsil_dir)
        self.stdout.write(
            f"Found STAC collections for {len(stac_coverage)} block(s).\n"
        )

        active_locations = _load_active_locations(options["state"])
        if not active_locations:
            self.stderr.write(
                self.style.WARNING("No active locations found in the database.")
            )
            return

        has_stac = []
        missing_stac = []

        for state_name, district_name, tehsil_name in active_locations:
            key = _stac_key(state_name, district_name, tehsil_name)
            if key in stac_coverage:
                has_stac.append((state_name, district_name, tehsil_name, stac_coverage[key]))
            else:
                missing_stac.append((state_name, district_name, tehsil_name))

        if not options["missing_only"]:
            self.stdout.write(self.style.SUCCESS(
                f"Active blocks WITH STAC ({len(has_stac)}):"
            ))
            for state_name, district_name, tehsil_name, layers in sorted(has_stac):
                self.stdout.write(
                    f"  {state_name} / {district_name} / {tehsil_name}"
                    f"  [{len(layers)} layer(s)]"
                )
                if options["show_layers"]:
                    for layer_id in sorted(layers):
                        self.stdout.write(f"      - {layer_id}")
            self.stdout.write("")

        self.stdout.write(self.style.ERROR(
            f"Active blocks WITHOUT STAC ({len(missing_stac)}):"
        ))
        for state_name, district_name, tehsil_name in sorted(missing_stac):
            self.stdout.write(f"  {state_name} / {district_name} / {tehsil_name}")

        total = len(active_locations)
        covered = len(has_stac)
        pct = (covered / total * 100) if total else 0.0

        self.stdout.write("")
        self.stdout.write("Summary")
        self.stdout.write("-------")
        self.stdout.write(f"  Total active blocks : {total}")
        self.stdout.write(self.style.SUCCESS(
            f"  With STAC           : {covered} ({pct:.1f}%)"
        ))
        self.stdout.write(self.style.ERROR(
            f"  Without STAC        : {len(missing_stac)} ({100 - pct:.1f}%)"
        ))

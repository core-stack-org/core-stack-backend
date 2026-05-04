import json
import os

from django.core.management.base import BaseCommand

from computing.STAC_specs.stac_collection import STACConfig, sanitize_text
from geoadmin.models import StateSOI


# ---------------------------------------------------------------------------
# Canonical 33 layer suffixes derived from a fully-covered reference block.
# Every block should have exactly these layers (prefixed with
# {state}_{district}_{block}_).
# ---------------------------------------------------------------------------
CANONICAL_LAYER_SUFFIXES = [
    "land_use_land_cover_raster_2017",
    "land_use_land_cover_raster_2018",
    "land_use_land_cover_raster_2019",
    "land_use_land_cover_raster_2020",
    "land_use_land_cover_raster_2021",
    "land_use_land_cover_raster_2022",
    "land_use_land_cover_raster_2023",
    "land_use_land_cover_raster_2024",
    "terrain_raster",
    "clart_raster",
    "wri_restoration_raster",
    "natural_depressions_raster",
    "stream_order_raster",
    "distance_to_upstream_drainage_line_raster",
    "catchment_area_singleflow_raster",
    "slope_percentage_raster",
    "admin_boundaries_vector",
    "aquifer_vector",
    "drainage_lines_vector",
    "surface_water_bodies_vector",
    "nrega_vector",
    "terrain_vector",
    "cropping_intensity_vector",
    "stage_of_groundwater_extraction_vector",
    "drought_frequency_vector",
    "change_in_well_depth_vector",
    "water_balance_fortnightly_vector",
    "change_tree_cover_gain_raster",
    "change_tree_cover_loss_raster",
    "change_cropping_reduction_raster",
    "change_urbanization_raster",
    "change_cropping_intensity_raster",
    "mws_connectivity_vector",
]


def _sanitize(text: str) -> str:
    """Lowercase + collapse whitespace/special chars to underscores (mirrors sanitize_text)."""
    return sanitize_text(text.lower())


def _layer_suffix_from_item_id(item_id: str, state_s: str, district_s: str, block_s: str) -> str:
    """
    Strip the {state}_{district}_{block}_ prefix from a layer item ID to get
    the canonical suffix.  Falls back to returning the full item_id if the
    prefix is not found.
    """
    prefix = f"{state_s}_{district_s}_{block_s}_"
    if item_id.startswith(prefix):
        return item_id[len(prefix):]
    # Some IDs may use slightly different sanitisation; try a simple strip of
    # the first three underscore-segments.
    parts = item_id.split("_")
    # state, district, block may each be multi-token (e.g. "north_twenty_four_parganas")
    # We can't reliably split without knowing lengths, so fall back to full id.
    return item_id


def _get_present_suffixes(block_dir: str, collection_path: str,
                          state_s: str, district_s: str, block_s: str) -> set:
    """Return the set of layer suffixes present in the block's STAC collection."""
    try:
        with open(collection_path) as f:
            coll = json.load(f)
    except (OSError, json.JSONDecodeError):
        return set()

    suffixes = set()
    for link in coll.get("links", []):
        if link.get("rel") != "item":
            continue
        href = link.get("href", "")
        item_path = os.path.normpath(os.path.join(block_dir, href))
        if not os.path.exists(item_path):
            # Try to infer suffix from href filename even if file is missing
            fname = os.path.basename(item_path).replace(".json", "")
            suffix = _layer_suffix_from_item_id(fname, state_s, district_s, block_s)
            suffixes.add(suffix)
            continue
        try:
            with open(item_path) as f:
                item = json.load(f)
            item_id = item.get("id", os.path.basename(item_path).replace(".json", ""))
        except (OSError, json.JSONDecodeError):
            item_id = os.path.basename(item_path).replace(".json", "")

        suffix = _layer_suffix_from_item_id(item_id, state_s, district_s, block_s)
        suffixes.add(suffix)

    return suffixes


def _load_active_locations(state_filter=None):
    qs = StateSOI.objects.filter(active_status=True).prefetch_related(
        "districtsoi_set__tehsilsoi_set"
    )
    if state_filter:
        qs = qs.filter(state_name__icontains=state_filter)

    locations = []
    for state in qs:
        for district in state.districtsoi_set.filter(active_status=True):
            for tehsil in district.tehsilsoi_set.filter(active_status=True):
                locations.append(
                    (state.state_name, district.district_name, tehsil.tehsil_name)
                )
    return locations


class Command(BaseCommand):
    help = (
        "For every active block that has a STAC collection, report which of "
        "the 33 canonical layers are missing.  Blocks with no STAC at all are "
        "also listed."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--state",
            default=None,
            help="Filter by state name (case-insensitive substring match)",
        )
        parser.add_argument(
            "--incomplete-only",
            action="store_true",
            default=False,
            help="Skip blocks that already have all 33 layers",
        )
        parser.add_argument(
            "--csv",
            default=None,
            metavar="FILE",
            help="Also write results to a CSV file at this path",
        )

    def handle(self, *args, **options):
        config = STACConfig()
        tehsil_dir = os.path.join(config.stac_files_dir, config.tehsil_dirname)

        self.stdout.write(f"STAC directory : {tehsil_dir}")
        self.stdout.write(f"Canonical layers: {len(CANONICAL_LAYER_SUFFIXES)}\n")

        canonical_set = set(CANONICAL_LAYER_SUFFIXES)

        active_locations = _load_active_locations(options["state"])
        if not active_locations:
            self.stderr.write(self.style.WARNING("No active locations found in the database."))
            return

        csv_rows = []  # (state, district, block, missing_layer)

        total_complete = 0
        total_incomplete = 0
        total_no_stac = 0

        for state_name, district_name, tehsil_name in sorted(active_locations):
            state_s   = _sanitize(state_name)
            district_s = _sanitize(district_name)
            block_s   = _sanitize(tehsil_name)

            block_dir = os.path.join(tehsil_dir, state_s, district_s, block_s)
            coll_path = os.path.join(block_dir, "collection.json")

            if not os.path.exists(coll_path):
                total_no_stac += 1
                if not options["incomplete_only"]:
                    self.stdout.write(
                        self.style.ERROR(
                            f"NO STAC  | {state_name} / {district_name} / {tehsil_name}"
                        )
                    )
                csv_rows.append((state_name, district_name, tehsil_name, "NO_STAC_COLLECTION"))
                continue

            present = _get_present_suffixes(block_dir, coll_path, state_s, district_s, block_s)
            missing = sorted(canonical_set - present)

            if not missing:
                total_complete += 1
                if not options["incomplete_only"]:
                    self.stdout.write(
                        self.style.SUCCESS(
                            f"COMPLETE | {state_name} / {district_name} / {tehsil_name}"
                            f"  [33/33]"
                        )
                    )
                continue

            total_incomplete += 1
            self.stdout.write(
                self.style.WARNING(
                    f"MISSING  | {state_name} / {district_name} / {tehsil_name}"
                    f"  [{len(present)}/33 present, {len(missing)} missing]"
                )
            )
            for layer in missing:
                self.stdout.write(f"           - {layer}")
                csv_rows.append((state_name, district_name, tehsil_name, layer))

        # Summary
        total = len(active_locations)
        self.stdout.write("")
        self.stdout.write("=" * 60)
        self.stdout.write("SUMMARY")
        self.stdout.write("=" * 60)
        self.stdout.write(f"  Total active blocks  : {total}")
        self.stdout.write(self.style.SUCCESS(
            f"  Complete (33/33)     : {total_complete}"
        ))
        self.stdout.write(self.style.WARNING(
            f"  Incomplete (<33)     : {total_incomplete}"
        ))
        self.stdout.write(self.style.ERROR(
            f"  No STAC at all       : {total_no_stac}"
        ))

        # Optional CSV export
        if options["csv"]:
            import csv
            with open(options["csv"], "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["state", "district", "block", "missing_layer"])
                writer.writerows(csv_rows)
            self.stdout.write(f"\nCSV written to: {options['csv']}")

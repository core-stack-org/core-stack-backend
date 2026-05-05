import json
import os

from django.core.management.base import BaseCommand

from computing.STAC_specs.stac_collection import STACConfig, sanitize_text
from geoadmin.models import StateSOI
from pathlib import Path
from django.conf import settings

# ---------------------------------------------------------------------------
# Canonical layer suffixes are loaded from:
#   data/STAC_specs/canonical_layers.json
# Maintain that file to add/remove layers — no code changes needed.
# ---------------------------------------------------------------------------
CANONICAL_LAYERS_JSON = (
    Path(settings.BASE_DIR) / "data" / "STAC_specs" / "STATS" / "canonical_layers.json"
)


def _load_canonical_layers() -> list:
    path = os.path.normpath(CANONICAL_LAYERS_JSON)
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"canonical_layers.json not found at: {path}\n"
            "Please create data/STAC_specs/canonical_layers.json as a JSON list of layer suffixes."
        )
    with open(path) as f:
        layers = json.load(f)
    if not isinstance(layers, list) or not layers:
        raise ValueError(
            f"canonical_layers.json must be a non-empty JSON list. Got: {type(layers)}"
        )
    return layers


def _sanitize(text: str) -> str:
    """Lowercase + collapse whitespace/special chars to underscores (mirrors sanitize_text)."""
    return sanitize_text(text.lower())


def _layer_suffix_from_item_id(
    item_id: str, state_s: str, district_s: str, block_s: str
) -> str:
    """
    Strip the {state}_{district}_{block}_ prefix from a layer item ID to get
    the canonical suffix.  Falls back to returning the full item_id if the
    prefix is not found.
    """
    prefix = f"{state_s}_{district_s}_{block_s}_"
    if item_id.startswith(prefix):
        return item_id[len(prefix) :]
    # Some IDs may use slightly different sanitisation; try a simple strip of
    # the first three underscore-segments.
    parts = item_id.split("_")
    # state, district, block may each be multi-token (e.g. "north_twenty_four_parganas")
    # We can't reliably split without knowing lengths, so fall back to full id.
    return item_id


def _get_present_suffixes(
    block_dir: str, collection_path: str, state_s: str, district_s: str, block_s: str
) -> set:
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
        "For every active block that has a STAC collection, report which "
        "canonical layers are missing (loaded from canonical_layers.json). "
        "Blocks with no STAC at all are also listed."
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
            help="Skip blocks that already have all canonical layers",
        )
        parser.add_argument(
            "--csv",
            default=None,
            metavar="FILE",
            help="Also write results to a CSV file at this path",
        )

    def handle(self, *args, **options):
        # Load canonical layers from JSON at runtime
        try:
            canonical_layer_suffixes = _load_canonical_layers()
        except (FileNotFoundError, ValueError) as e:
            self.stderr.write(self.style.ERROR(str(e)))
            return

        canonical_count = len(canonical_layer_suffixes)
        canonical_set = set(canonical_layer_suffixes)

        config = STACConfig()
        tehsil_dir = os.path.join(config.stac_files_dir, config.tehsil_dirname)

        self.stdout.write(f"STAC directory   : {tehsil_dir}")
        self.stdout.write(
            f"Canonical layers : {canonical_count} (loaded from canonical_layers.json)\n"
        )

        active_locations = _load_active_locations(options["state"])
        if not active_locations:
            self.stderr.write(
                self.style.WARNING("No active locations found in the database.")
            )
            return

        csv_rows = []  # (state, district, block, missing_layer)

        total_complete = 0
        total_incomplete = 0
        total_no_stac = 0

        for state_name, district_name, tehsil_name in sorted(active_locations):
            state_s = _sanitize(state_name)
            district_s = _sanitize(district_name)
            block_s = _sanitize(tehsil_name)

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
                csv_rows.append(
                    (state_name, district_name, tehsil_name, "NO_STAC_COLLECTION")
                )
                continue

            present = _get_present_suffixes(
                block_dir, coll_path, state_s, district_s, block_s
            )
            missing = sorted(canonical_set - present)

            if not missing:
                total_complete += 1
                if not options["incomplete_only"]:
                    self.stdout.write(
                        self.style.SUCCESS(
                            f"COMPLETE | {state_name} / {district_name} / {tehsil_name}"
                            f"  [{canonical_count}/{canonical_count}]"
                        )
                    )
                continue

            total_incomplete += 1
            self.stdout.write(
                self.style.WARNING(
                    f"MISSING  | {state_name} / {district_name} / {tehsil_name}"
                    f"  [{len(present)}/{canonical_count} present, {len(missing)} missing]"
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
        self.stdout.write(
            self.style.SUCCESS(
                f"  Complete ({canonical_count}/{canonical_count})     : {total_complete}"
            )
        )
        self.stdout.write(
            self.style.WARNING(
                f"  Incomplete (<{canonical_count})     : {total_incomplete}"
            )
        )
        self.stdout.write(self.style.ERROR(f"  No STAC at all       : {total_no_stac}"))

        # Optional CSV export
        if options["csv"]:
            import csv

            with open(options["csv"], "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["state", "district", "block", "missing_layer"])
                writer.writerows(csv_rows)
            self.stdout.write(f"\nCSV written to: {options['csv']}")

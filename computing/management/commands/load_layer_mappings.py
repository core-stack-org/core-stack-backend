import csv
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from computing.models import LayerMapping


DEFAULT_CSV = (
    Path(settings.BASE_DIR)
    / "data"
    / "STAC_specs"
    / "input"
    / "metadata"
    / "layer_mapping.csv"
)

_REQUIRED_FIELDS = ("layer_name", "layer_type", "db_dataset_name")
_KNOWN_LAYER_TYPES = {"raster", "vector", "point", "custom"}


class Command(BaseCommand):
    help = (
        "Load / upsert STAC layer mappings from "
        "data/STAC_specs/input/metadata/layer_mapping.csv into the LayerMapping table."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--csv",
            default=str(DEFAULT_CSV),
            help="Path to layer_mapping.csv (defaults to the in-repo file).",
        )
        parser.add_argument(
            "--prune",
            action="store_true",
            help="Delete LayerMapping rows that are not present in the CSV.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Parse the CSV and report what would change, without writing.",
        )

    def handle(self, *args, **options):
        csv_path = Path(options["csv"]).expanduser().resolve()
        if not csv_path.is_file():
            raise CommandError(f"CSV not found: {csv_path}")

        rows = list(self._iter_valid_rows(csv_path))
        if not rows:
            raise CommandError(f"No valid rows found in {csv_path}")

        self.stdout.write(f"Parsed {len(rows)} valid rows from {csv_path}")

        if options["dry_run"]:
            for r in rows:
                self.stdout.write(
                    f"  would upsert: {r['layer_name']} "
                    f"({r['layer_type']}, ee={r['ee_layer_name'] or '-'})"
                )
            return

        created, updated = 0, 0
        seen_keys = set()
        with transaction.atomic():
            for row in rows:
                key = (row["layer_name"], row["layer_type"], row["ee_layer_name"])
                seen_keys.add(key)
                _, was_created = LayerMapping.objects.update_or_create(
                    layer_name=row["layer_name"],
                    layer_type=row["layer_type"],
                    ee_layer_name=row["ee_layer_name"],
                    defaults={
                        "display_name": row["display_name"],
                        "spatial_resolution_in_meters": row[
                            "spatial_resolution_in_meters"
                        ],
                        "db_dataset_name": row["db_dataset_name"],
                        "geoserver_workspace_name": row["geoserver_workspace_name"],
                        "geoserver_layer_name": row["geoserver_layer_name"],
                        "start_year": row["start_year"],
                        "end_year": row["end_year"],
                        "style_file_url": row["style_file_url"],
                        "theme": row["theme"],
                    },
                )
                if was_created:
                    created += 1
                else:
                    updated += 1

            pruned = 0
            if options["prune"]:
                qs = LayerMapping.objects.all()
                stale = [
                    m
                    for m in qs
                    if (m.layer_name, m.layer_type, m.ee_layer_name) not in seen_keys
                ]
                pruned = len(stale)
                for m in stale:
                    m.delete()

        msg = f"LayerMapping load: created={created} updated={updated}"
        if options["prune"]:
            msg += f" pruned={pruned}"
        self.stdout.write(self.style.SUCCESS(msg))

    @staticmethod
    def _iter_valid_rows(csv_path):
        with csv_path.open(newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for raw in reader:
                row = {k: (v or "").strip() for k, v in raw.items()}
                if any(not row.get(f) for f in _REQUIRED_FIELDS):
                    continue
                if row["layer_type"] not in _KNOWN_LAYER_TYPES:
                    continue
                spatial = row.get("spatial_resolution_in_meters") or ""
                try:
                    spatial_val = float(spatial) if spatial else None
                except ValueError:
                    spatial_val = None
                yield {
                    "display_name": row.get("display_name", ""),
                    "layer_type": row["layer_type"],
                    "layer_name": row["layer_name"],
                    "spatial_resolution_in_meters": spatial_val,
                    "ee_layer_name": row.get("ee_layer_name", ""),
                    "db_dataset_name": row["db_dataset_name"],
                    "geoserver_workspace_name": row.get("geoserver_workspace_name", ""),
                    "geoserver_layer_name": row.get("geoserver_layer_name", ""),
                    "start_year": row.get("start_year", ""),
                    "end_year": row.get("end_year", ""),
                    "style_file_url": row.get("style_file_url", ""),
                    "theme": row.get("theme", ""),
                }

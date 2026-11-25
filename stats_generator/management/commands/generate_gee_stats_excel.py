"""Regenerate stats Excel workbooks from the ORIGINAL GEE layers.

GeoServer now serves new locally-computed layers, so this command reads each
layer's original FeatureCollection directly from Earth Engine (see
stats_generator/gee_layer_source.py) and runs the regular sheet builders on it.
Output goes to a separate tree so the regular workbooks are untouched:

    <EXCEL_PATH>/data/stats_excel_files_gee/<STATE>/<DISTRICT>/<district>_<block>.xlsx

Example:
    python manage.py generate_gee_stats_excel \
        --location jharkhand/dumka/masalia --project ee-corestackdev
"""

import os

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from stats_generator.gee_layer_source import DEFAULT_GEE_PROJECT, GEELayerSource
from stats_generator.utils import (
    fetch_layers_for_excel_generation,
    get_vector_layer_geoserver,
    use_layer_source,
)
from utilities.gee_utils import ee_initialize, valid_gee_text

DEFAULT_OUTPUT_DIR = "data/stats_excel_files_gee"


class Command(BaseCommand):
    help = "Build stats Excel workbooks from the original GEE layers instead of GeoServer."

    def add_arguments(self, parser):
        parser.add_argument(
            "--location",
            action="append",
            required=True,
            metavar="STATE/DISTRICT/BLOCK",
            help="Block to generate; repeat for several, e.g. jharkhand/dumka/masalia.",
        )
        parser.add_argument(
            "--project",
            default=DEFAULT_GEE_PROJECT,
            help=f"GEE project holding the original assets (default: {DEFAULT_GEE_PROJECT}).",
        )
        parser.add_argument(
            "--gee-account-id",
            default=settings.GEE_DEFAULT_ACCOUNT_ID,
            help="GEEAccount id used to initialise Earth Engine (default: GEE_DEFAULT_ACCOUNT_ID).",
        )
        parser.add_argument(
            "--output-dir",
            default=DEFAULT_OUTPUT_DIR,
            help=f"Output tree relative to EXCEL_PATH (default: {DEFAULT_OUTPUT_DIR}).",
        )
        parser.add_argument(
            "--workspace",
            action="append",
            dest="workspaces",
            help="Only process these LayerInfo workspaces (repeatable).",
        )
        parser.add_argument(
            "--keep-existing",
            action="store_true",
            help="Update sheets in an existing workbook instead of rebuilding it.",
        )

    def handle(self, *args, **options):
        locations = [self._parse_location(value) for value in options["location"]]
        layers = fetch_layers_for_excel_generation()
        workspaces = options["workspaces"]
        if workspaces:
            layers = [layer for layer in layers if layer["workspace"] in workspaces]
        if not layers:
            layer_filter = (
                f" matching --workspace {', '.join(workspaces)}"
                if workspaces
                else ""
            )
            raise CommandError(
                "No vector layers are configured for stats Excel generation"
                f"{layer_filter}. Check LayerInfo records have "
                "layer_type='vector' and excel_to_be_generated=True."
            )

        ee_initialize(account_id=options["gee_account_id"], strict=True)

        for state, district, block in locations:
            self.stdout.write(self.style.MIGRATE_HEADING(f"\n{state} / {district} / {block}"))
            if not options["keep_existing"]:
                self._remove_existing(options["output_dir"], state, district, block)

            source = GEELayerSource(state, district, block, project=options["project"])
            self.stdout.write(f"GEE folder: {source.folder}")
            try:
                source.asset_ids()
            except Exception as exc:
                raise CommandError(f"Cannot list {source.folder}: {exc}") from exc

            with use_layer_source(source):
                results = get_vector_layer_geoserver(
                    state,
                    district,
                    block,
                    specific_sheets=options["workspaces"],
                    base_dir=options["output_dir"],
                )
            self._report(results, source)

    @staticmethod
    def _parse_location(value):
        parts = [part.strip() for part in value.split("/")]
        if len(parts) != 3 or not all(parts):
            raise CommandError(f"--location must be STATE/DISTRICT/BLOCK, got {value!r}")
        # Same normalisation as the stats API (stats_generator/api.py).
        return tuple(valid_gee_text(part.lower()) for part in parts)

    def _remove_existing(self, output_dir, state, district, block):
        path = os.path.join(
            settings.EXCEL_PATH,
            output_dir,
            state.replace(" ", "_").upper(),
            district.replace(" ", "_").upper(),
            f"{district}_{block}.xlsx",
        )
        if os.path.exists(path):
            os.remove(path)
            self.stdout.write(f"Removed previous workbook {path}")

    def _report(self, results, source):
        by_status = {}
        for result in results:
            by_status.setdefault(result["status"], []).append(result)

        for result in by_status.get("success", []):
            asset = source.used_assets.get(result["layer"], "?")
            self.stdout.write(self.style.SUCCESS(f"  ok      {result['workspace']}:{result['layer']} <- {asset}"))
        for result in by_status.get("skipped", []):
            self.stdout.write(self.style.WARNING(f"  skipped {result['workspace']}:{result['layer']} ({result.get('reason', '')})"))
        for result in by_status.get("failed", []):
            self.stdout.write(self.style.ERROR(f"  failed  {result['workspace']}:{result['layer']} ({result.get('reason', '')})"))

        self.stdout.write(
            f"{len(by_status.get('success', []))} ok, "
            f"{len(by_status.get('skipped', []))} skipped, "
            f"{len(by_status.get('failed', []))} failed"
        )

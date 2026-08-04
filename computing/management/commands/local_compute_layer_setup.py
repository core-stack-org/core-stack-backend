import logging

from django.core.management.base import BaseCommand, CommandError

from computing.base_layer_setup import (
    DEFAULT_BASE_LAYERS,
    _BASE_LAYER_ENSURERS,
    _manifest_layer_groups,
    setup_base_layers,
)


class Command(BaseCommand):
    help = (
        "Set up required local compute layers into the local data directory. "
        "Defaults to static_layers and periodic_layers."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "layers",
            nargs="*",
            help=(
                "Layer names or groups to download, for example: terrain, mws, "
                "lulc_v3, static_layers, periodic_layers. Defaults to the bootstrap set."
            ),
        )
        parser.add_argument(
            "--ensure-soi-tehsil",
            action="store_true",
            help="Download the SOI tehsil layer if it is missing.",
        )
        parser.add_argument(
            "--ensure-admin-boundary",
            action="store_true",
            help="Download and extract admin boundary data if it is missing.",
        )
        parser.add_argument(
            "--ensure-lulc-rasters",
            action="store_true",
            help="Download missing LULC raster files.",
        )
        parser.add_argument(
            "--ensure-microwatershed",
            action="store_true",
            help="Download the microwatershed layer if it is missing.",
        )
        parser.add_argument(
            "--ensure-tehsil-watersheds",
            action="store_true",
            help="Generate per-tehsil watershed files if they are missing.",
        )
        parser.add_argument(
            "--geoserver",
            action="store_true",
            help=(
                "Download watershed GPKGs from the mws GeoServer workspace for "
                "active tehsils only."
            ),
        )
        parser.add_argument(
            "--force",
            action="store_true",
            help="Replace existing tehsil watershed GPKGs.",
        )
        parser.add_argument(
            "--ensure-village-boundaries",
            action="store_true",
            help="Create the village boundaries directory if it is missing.",
        )
        parser.add_argument(
            "--list",
            action="store_true",
            help="List available layer selectors without downloading anything.",
        )

    def handle(self, *args, **options):
        logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

        if options["list"]:
            self._print_available_layers()
            return

        if options["geoserver"] and not options["ensure_tehsil_watersheds"]:
            raise CommandError(
                "--geoserver requires --ensure-tehsil-watersheds."
            )
        if options["force"] and not options["ensure_tehsil_watersheds"]:
            raise CommandError("--force requires --ensure-tehsil-watersheds.")

        layers = self._selected_layers(options)
        self.stdout.write(f"Setting up local compute layers: {', '.join(layers)}")

        try:
            setup_base_layers(
                *layers,
                geoserver=options["geoserver"],
                force=options["force"],
            )
        except (RuntimeError, ValueError) as exc:
            raise CommandError(str(exc)) from exc

        self.stdout.write(self.style.SUCCESS("Local compute layer setup complete."))

    def _selected_layers(self, options):
        layers = list(options["layers"])
        flag_layers = {
            "ensure_soi_tehsil": "soi_tehsil",
            "ensure_admin_boundary": "admin_boundary",
            "ensure_lulc_rasters": "lulc_rasters",
            "ensure_microwatershed": "microwatershed",
            "ensure_tehsil_watersheds": "tehsil_watersheds",
            "ensure_village_boundaries": "village_boundaries",
        }

        for option_name, layer_name in flag_layers.items():
            if options[option_name]:
                layers.append(layer_name)

        return tuple(dict.fromkeys(layers)) or DEFAULT_BASE_LAYERS

    def _print_available_layers(self):
        self.stdout.write("Downloadable manifest groups:")
        for group_name, layers in _manifest_layer_groups().items():
            if not layers:
                continue
            self.stdout.write(f"  {group_name}")
            for layer in layers:
                source = layer.get("source")
                status = (
                    "downloadable"
                    if source and layer.get("type") == "file"
                    else "manual"
                )
                label = layer["name"]
                if "year" in layer:
                    label = f"{label} {layer['year']}-{layer['year'] + 1}"
                self.stdout.write(f"    - {label} ({status})")

        self.stdout.write("")
        self.stdout.write("Local/generated selectors:")
        for selector in sorted(_BASE_LAYER_ENSURERS):
            self.stdout.write(f"  - {selector}")

        self.stdout.write("")
        self.stdout.write("Local/generated flags:")
        for selector in sorted(_BASE_LAYER_ENSURERS):
            self.stdout.write(f"  --ensure-{selector.replace('_', '-')}")

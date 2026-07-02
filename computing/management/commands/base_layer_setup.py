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
        "Download required base layers into the local data directory. "
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
            "--list",
            action="store_true",
            help="List available layer selectors without downloading anything.",
        )

    def handle(self, *args, **options):
        logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

        if options["list"]:
            self._print_available_layers()
            return

        layers = tuple(options["layers"]) or DEFAULT_BASE_LAYERS
        self.stdout.write(f"Setting up base layers: {', '.join(layers)}")

        try:
            setup_base_layers(*layers)
        except ValueError as exc:
            raise CommandError(str(exc)) from exc

        self.stdout.write(self.style.SUCCESS("Base layer setup complete."))

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
        self.stdout.write("Legacy/generated selectors:")
        for selector in sorted(_BASE_LAYER_ENSURERS):
            self.stdout.write(f"  - {selector}")

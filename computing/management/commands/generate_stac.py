from django.core.management.base import BaseCommand

from computing.STAC_specs.stac_collection import STACCollectionGenerator


class Command(BaseCommand):
    help = "Generate a STAC collection for a layer (runs synchronously, no Celery)"

    def add_arguments(self, parser):
        parser.add_argument("--state", required=True)
        parser.add_argument("--district", required=True)
        parser.add_argument("--block", required=True)
        parser.add_argument("--layer-name", required=True)
        parser.add_argument("--layer-type", required=True, choices=["raster", "vector"])
        parser.add_argument("--start-year", default="")
        parser.add_argument("--overwrite", action="store_true", default=False)

    def handle(self, *args, **options):
        generator = STACCollectionGenerator()
        layer_type = options["layer_type"]
        common = dict(
            state=options["state"],
            district=options["district"],
            block=options["block"],
            layer_name=options["layer_name"],
            overwrite=options["overwrite"],
        )

        self.stdout.write(
            f"Generating {layer_type} STAC for "
            f"{common['state']}/{common['district']}/{common['block']} "
            f"layer={common['layer_name']}"
        )

        if layer_type == "raster":
            ok = generator.generate_raster(**common, start_year=options["start_year"])
        else:
            ok = generator.generate_vector(**common)

        if ok:
            self.stdout.write(self.style.SUCCESS(
                f"STAC collection written to {generator.config.stac_files_dir}"
            ))
        else:
            self.stderr.write(self.style.ERROR("STAC generation failed"))

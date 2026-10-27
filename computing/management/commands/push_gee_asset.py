from django.core.management.base import BaseCommand, CommandError

from computing.tasks import GEOSERVER_QUEUE, publish_gee_layer


class Command(BaseCommand):
    help = (
        "Push a GEE vector asset to GeoServer by name, without needing a Layer "
        "row. An existing GeoServer layer with the same name is replaced. Pass "
        "--layer-id to also flip that Layer row's sync flag."
    )

    def add_arguments(self, parser):
        parser.add_argument("--asset", required=True, help="Full GEE asset id.")
        parser.add_argument("--layer-name", required=True)
        parser.add_argument("--workspace", required=True)
        parser.add_argument(
            "--folder",
            help="Scratch folder under data/fc_to_shape (default: layer name).",
        )
        parser.add_argument("--style-name")
        parser.add_argument("--layer-id", type=int)
        parser.add_argument("--gee-account-id", type=int)
        parser.add_argument(
            "--sync",
            action="store_true",
            help="Run in this process and print the GeoServer response, "
            "instead of queueing it for the geoserver worker.",
        )

    def handle(self, *args, **options):
        if not options["asset"].startswith("projects/"):
            raise CommandError("--asset must be a GEE asset id (projects/...).")
        kwargs = {
            "layer_type": "vector",
            "asset_id": options["asset"],
            "layer_name": options["layer_name"],
            "workspace": options["workspace"],
            "state": options["folder"] or options["layer_name"],
            "style_name": options["style_name"],
            "layer_id": options["layer_id"],
            "gee_account_id": options["gee_account_id"],
        }
        if options["sync"]:
            try:
                response = publish_gee_layer.apply(kwargs=kwargs, throw=True).get()
            except Exception as exc:
                raise CommandError(f"Push failed: {exc}")
            self.stdout.write(f"{options['workspace']}:{options['layer_name']} {response}")
            return
        result = publish_gee_layer.apply_async(kwargs=kwargs, queue=GEOSERVER_QUEUE)
        self.stdout.write(
            f"queued {options['workspace']}:{options['layer_name']} (task {result.id})"
        )

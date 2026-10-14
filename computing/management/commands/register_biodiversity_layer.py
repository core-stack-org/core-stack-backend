"""
Idempotently register the Biodiversity (GBIF) layer in the stats_generator LayerInfo registry.

LayerInfo is an operational/admin-managed registry (it is not shipped in seed fixtures), so — like
`seed_default_plantation` and `load_layer_mappings` — registration is done via a one-time idempotent
management command rather than a fixture. Safe to run any number of times, on fresh or existing DBs.

Usage:
    python manage.py register_biodiversity_layer
"""

from django.core.management.base import BaseCommand

from stats_generator.models import LayerInfo
from computing.gbif import config


class Command(BaseCommand):
    help = "Register the Biodiversity LayerInfo row for Excel/KYL generation (idempotent)."

    def handle(self, *args, **options):
        # layer_name keeps the {district}_{block} placeholders — stats_generator formats them at runtime.
        obj, created = LayerInfo.objects.get_or_create(
            layer_name="{district}_{block}_biodiversity",
            workspace=config.WORKSPACE,
            defaults={
                "layer_type": "vector",
                "excel_to_be_generated": True,
                "style_name": config.VECTOR_STYLE_NAME,
                "layer_desc": "GBIF per-MWS biodiversity indicators",
            },
        )
        if created:
            self.stdout.write(
                self.style.SUCCESS(f"Registered Biodiversity LayerInfo (id={obj.id}).")
            )
        else:
            self.stdout.write(
                f"Biodiversity LayerInfo already registered (id={obj.id})."
            )

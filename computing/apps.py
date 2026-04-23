from django.apps import AppConfig
from computing.base_layer_setup import setup_base_layers


class ComputingConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "computing"

    def ready(self):
        setup_base_layers()

from django.apps import AppConfig


class ComputingConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "computing"

    def ready(self):
        from computing.base_layer_setup import setup_base_layers

        setup_base_layers()

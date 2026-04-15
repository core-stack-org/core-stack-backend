from django.apps import AppConfig


class ComputingConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "computing"

    def ready(self):
        import computing.tasks  # noqa: F401  (task autodiscovery)
        import computing.signals  # noqa: F401  (post_save -> auto STAC trigger)
        from computing.base_layer_setup import setup_base_layers

        setup_base_layers()

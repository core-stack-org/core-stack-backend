from django.apps import AppConfig


class ComputingConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "computing"

    def ready(self):
        import computing.tasks  # noqa: F401  (task autodiscovery)
        import computing.signals  # noqa: F401  (post_save -> auto STAC trigger)

        from utilities.skip_existing import install

        install()  # API requests skip layers that already exist (regenerate_all)

from django.apps import AppConfig


class GeeComputingConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "gee_computing"

    def ready(self):
        import gee_computing.signals

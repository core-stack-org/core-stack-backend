from django.core.management.base import BaseCommand
from django.db.models import Count
from django.apps import apps

# List all 12 models here (import them directly, or resolve via apps.get_model)
MODELS_TO_CHECK = [
    "dpr.ODK_crop",
    "dpr.ODK_settlement",
    "dpr.ODK_well",
    "dpr.ODK_waterbody",
    "dpr.ODK_groundwater",
    "dpr.ODK_agri",
    "dpr.ODK_livelihood",
    "dpr.ODK_agrohorticulture",
    "dpr.GW_maintenance",
    "dpr.SWB_RS_maintenance",
    "dpr.SWB_maintenance",
    "dpr.Agri_maintenance",
]


class Command(BaseCommand):
    help = "Finds duplicate uuid values across a set of models and reports their PKs"

    def add_arguments(self, parser):
        parser.add_argument(
            "--include-deleted",
            action="store_true",
            help="Include rows where is_deleted=True (if the model has that field)",
        )

    def handle(self, *args, **options):
        include_deleted = options["include_deleted"]
        overall_summary = {}

        for model_path in MODELS_TO_CHECK:
            app_label, model_name = model_path.split(".")
            model = apps.get_model(app_label, model_name)

            pk_field = model._meta.pk.name  # handles custom PK names like crop_grid_id

            qs = model.objects.all()

            # exclude soft-deleted rows unless asked to include them
            if not include_deleted and hasattr(model, "is_deleted"):
                qs = qs.filter(is_deleted=False)

            # exclude null/blank uuids from the duplicate check
            # qs = qs.exclude(uuid__isnull=True).exclude(uuid="")

            duplicate_uuids = (
                qs.values("uuid")
                .annotate(cnt=Count(pk_field, distinct=True))
                .filter(cnt__gt=1)
                .order_by("-cnt")
            )

            if not duplicate_uuids.exists():
                self.stdout.write(
                    self.style.SUCCESS(f"[{model.__name__}] No duplicate uuids found.")
                )
                continue

            self.stdout.write(
                self.style.WARNING(
                    f"\n[{model.__name__}] Found {duplicate_uuids.count()} duplicate uuid group(s):"
                )
            )

            model_result = []
            for row in duplicate_uuids:
                dup_uuid = row["uuid"]
                count = row["cnt"]
                pks = list(qs.filter(uuid=dup_uuid).values_list(pk_field, flat=True))
                model_result.append({"uuid": dup_uuid, "count": count, "pks": pks})
                self.stdout.write(f"  uuid={dup_uuid}  count={count}  pks={pks}")

            overall_summary[model.__name__] = model_result

        self.stdout.write(self.style.SUCCESS("\nDone."))
        return overall_summary

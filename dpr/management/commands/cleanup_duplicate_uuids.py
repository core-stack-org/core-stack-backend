from django.core.management.base import BaseCommand
from django.db import connection
from django.utils import timezone
from django.apps import apps

# Tables to clean up (in the format (model_path, pk_column))
MODELS_TO_CLEAN = [
    ("dpr.ODK_settlement", "settlement_id"),
    ("dpr.ODK_well", "well_id"),
    ("dpr.ODK_waterbody", "waterbody_id"),
    ("dpr.ODK_groundwater", "recharge_structure_id"),
    ("dpr.ODK_agri", "irrigation_work_id"),
    ("dpr.ODK_crop", "crop_grid_id"),
    ("dpr.ODK_livelihood", "livelihood_id"),
    ("dpr.ODK_agrohorticulture", "agrohorticulture_id"),
    ("dpr.GW_maintenance", "gw_maintenance_id"),
    ("dpr.SWB_RS_maintenance", "swb_rs_maintenance_id"),
    ("dpr.SWB_maintenance", "swb_maintenance_id"),
    ("dpr.Agri_maintenance", "agri_maintenance_id"),
]


class Command(BaseCommand):
    help = "Finds and soft-deletes duplicate uuid records by suffixing their uuid and setting is_deleted=True"

    def handle(self, *args, **options):
        self.stdout.write(self.style.NOTICE("Starting ODK duplicate uuid cleanup..."))

        for model_path, pk_col in MODELS_TO_CLEAN:
            app_label, model_name = model_path.split(".")
            model = apps.get_model(app_label, model_name)
            table_name = model._meta.db_table

            with connection.cursor() as cursor:
                # Find uuid values that appear more than once (excluding null/empty)
                cursor.execute(
                    f"""
                    SELECT uuid, COUNT(*) as cnt
                    FROM {table_name}
                    WHERE uuid IS NOT NULL AND uuid != ''
                    GROUP BY uuid
                    HAVING COUNT(*) > 1
                    """
                )
                duplicate_groups = cursor.fetchall()

                if not duplicate_groups:
                    self.stdout.write(
                        self.style.SUCCESS(f"[{model_name}] No duplicate uuid groups found.")
                    )
                    continue

                self.stdout.write(
                    self.style.WARNING(
                        f"[{model_name}] Found {len(duplicate_groups)} duplicate uuid group(s). Soft-deleting duplicates..."
                    )
                )

                total_deleted = 0
                for uuid_val, count in duplicate_groups:
                    # Query all rows for this uuid, ranking them:
                    # 1. Moderated rows first (is_moderated=True before False)
                    # 2. Non-deleted rows second (is_deleted=False before True)
                    # 3. Lowest PK third
                    cursor.execute(
                        f"""
                        SELECT {pk_col}
                        FROM {table_name}
                        WHERE uuid = %s
                        ORDER BY 
                          CASE WHEN is_moderated = TRUE THEN 0 ELSE 1 END,
                          CASE WHEN is_deleted = FALSE THEN 0 ELSE 1 END,
                          {pk_col}
                        """,
                        [uuid_val],
                    )
                    rows = cursor.fetchall()

                    # Keep the highest-ranked one (index 0), soft-delete and suffix all others
                    if len(rows) > 1:
                        to_soft_delete = [row[0] for row in rows[1:]]
                        now_ts = timezone.now()
                        for pk in to_soft_delete:
                            new_uuid = f"{uuid_val}-deleted-{pk}"
                            cursor.execute(
                                f"""
                                UPDATE {table_name}
                                SET is_deleted = TRUE,
                                    deleted_at = %s,
                                    uuid = %s
                                WHERE {pk_col} = %s
                                """,
                                [now_ts, new_uuid, pk],
                            )
                            total_deleted += 1

                self.stdout.write(
                    self.style.SUCCESS(
                        f"[{model_name}] Successfully soft-deleted {total_deleted} duplicate row(s)."
                    )
                )

        self.stdout.write(self.style.SUCCESS("ODK duplicate uuid cleanup completed."))

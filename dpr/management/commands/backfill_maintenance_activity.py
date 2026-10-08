from django.core.management.base import BaseCommand
from django.db import transaction

from dpr.mapping import populate_maintenance_from_waterbody
from plans.models import PlanApp


class Command(BaseCommand):
    help = (
        "Recompute demand_type/select_one_activities on GW_maintenance, "
        "Agri_maintenance and SWB_maintenance records from the underlying "
        "ODK_waterbody/ODK_well data, and backfill modified_data with the raw "
        "pre-transform values wherever a correction is applied. Moderated and "
        "soft-deleted records are never touched. By default this ONLY refreshes "
        "records that already exist -- pass --create-missing to also generate "
        "records for plans that never had Section E populated."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--plan-id",
            type=int,
            help="Only process this plan id (default: all plans)",
        )
        parser.add_argument(
            "--create-missing",
            action="store_true",
            help="Also create maintenance records where none exist yet",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Roll back all writes after computing them, reporting what would change",
        )

    def handle(self, *args, **options):
        plan_id = options["plan_id"]
        create_missing = options["create_missing"]
        dry_run = options["dry_run"]

        plans = PlanApp.objects.order_by("id")
        if plan_id:
            plans = plans.filter(id=plan_id)

        total = plans.count()
        mode = "create-missing" if create_missing else "update-only"
        self.stdout.write(
            self.style.NOTICE(
                f"Processing {total} plan(s) [{mode}{', dry-run' if dry_run else ''}]..."
            )
        )

        created = 0
        refreshed = 0
        failed = []

        for i, plan in enumerate(plans, 1):
            try:
                with transaction.atomic():
                    stats = populate_maintenance_from_waterbody(
                        plan, create_missing=create_missing
                    )
                    if dry_run:
                        transaction.set_rollback(True)
                created += stats["created"]
                refreshed += stats["refreshed"]
            except Exception as exc:
                failed.append((plan.id, str(exc)))
                self.stderr.write(self.style.ERROR(f"[plan {plan.id}] {exc}"))

            if i % 50 == 0 or i == total:
                self.stdout.write(
                    f"Progress: {i}/{total} (created={created}, refreshed={refreshed}, failed={len(failed)})"
                )

        self.stdout.write(
            self.style.SUCCESS(
                f"\nDone. plans={total} created={created} refreshed={refreshed} failed={len(failed)}"
            )
        )
        for pid, err in failed:
            self.stdout.write(self.style.ERROR(f"  plan_id={pid}: {err}"))

from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

from organization.models import Organization
from plans.models import Plan, PlanApp


class Command(BaseCommand):
    help = "Copy plan details from Plan model to PlanApp model"

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Run command without making any changes to the database",
        )
        parser.add_argument(
            "--verbose",
            action="store_true",
            help="Display verbose output during execution",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        verbose = options["verbose"]

        if dry_run:
            self.stdout.write(
                self.style.WARNING(
                    "DRY RUN MODE - No changes will be made to the database"
                )
            )

        try:
            cfpt_org = Organization.objects.get(name="CFPT")
            if verbose:
                self.stdout.write(f"Found CFPT organization: {cfpt_org.id}")
        except Organization.DoesNotExist:
            self.stdout.write(
                self.style.ERROR("CFPT organization not found. Please create it first.")
            )
            return
        except Organization.MultipleObjectsReturned:
            self.stdout.write(
                self.style.ERROR(
                    "Multiple CFPT organizations found. Please ensure only one exists."
                )
            )
            return

        plans = Plan.objects.all().order_by("plan_id")
        total_plans = plans.count()

        if verbose:
            self.stdout.write(f"Found {total_plans} plans to copy")

        if total_plans == 0:
            self.stdout.write(self.style.WARNING("No plans found to copy"))
            return

        created_count = 0
        skipped_count = 0
        error_count = 0

        with transaction.atomic():
            if dry_run:
                savepoint = transaction.savepoint()

            for plan in plans:
                try:
                    if PlanApp.objects.filter(id=plan.plan_id).exists():
                        if verbose:
                            self.stdout.write(
                                self.style.WARNING(
                                    f"Skipping Plan ID {plan.plan_id} - PlanApp with this ID already exists"
                                )
                            )
                        skipped_count += 1
                        continue

                    planapp = PlanApp(
                        id=plan.plan_id,
                        plan=plan.plan,
                        project=None,
                        organization=cfpt_org,
                        facilitator_name=plan.facilitator_name,
                        state=plan.state,
                        district=plan.district,
                        block=plan.block,
                        state_soi=None,
                        district_soi=None,
                        tehsil_soi=None,
                        village_name=plan.village_name,
                        gram_panchayat=plan.gram_panchayat,
                        created_by=None,
                        created_at=timezone.now(),
                        updated_by=None,
                        updated_at=timezone.now(),
                        enabled=True,
                        is_completed=False,
                        is_dpr_generated=False,
                        is_dpr_reviewed=False,
                        is_dpr_approved=False,
                        latitude=None,
                        longitude=None,
                    )

                    if not dry_run:
                        planapp.save()

                    created_count += 1

                    if verbose:
                        self.stdout.write(
                            f"{'[DRY RUN] ' if dry_run else ''}Created PlanApp ID {plan.plan_id}: {plan.plan}"
                        )
                    elif created_count % 100 == 0:
                        self.stdout.write(
                            f"{'[DRY RUN] ' if dry_run else ''}Processed {created_count} plans..."
                        )

                except Exception as e:
                    error_count += 1
                    self.stdout.write(
                        self.style.ERROR(
                            f"Error processing Plan ID {plan.plan_id}: {str(e)}"
                        )
                    )

            if dry_run:
                transaction.savepoint_rollback(savepoint)

        # Summary
        self.stdout.write("\n" + "=" * 50)
        self.stdout.write(
            self.style.SUCCESS(f"{'DRY RUN ' if dry_run else ''}SUMMARY:")
        )
        self.stdout.write(f"Total plans found: {total_plans}")
        self.stdout.write(
            f"Plans {'would be ' if dry_run else ''}created: {created_count}"
        )
        self.stdout.write(f"Plans skipped (already exist): {skipped_count}")
        self.stdout.write(f"Errors encountered: {error_count}")

        if dry_run:
            self.stdout.write(
                self.style.WARNING(
                    "\nThis was a DRY RUN. No changes were made to the database."
                )
            )
            self.stdout.write(
                "Run the command without --dry-run to actually copy the data."
            )
        else:
            self.stdout.write(
                self.style.SUCCESS(
                    f"\nSuccessfully copied {created_count} plans to PlanApp model!"
                )
            )

            if created_count > 0:
                self.stdout.write(
                    self.style.WARNING(
                        "\nIMPORTANT: Don't forget to reset the sequence manually:"
                    )
                )
                self.stdout.write(
                    "SELECT setval('plans_planapp_id_seq', (SELECT MAX(id) FROM plans_planapp) + 1);"
                )

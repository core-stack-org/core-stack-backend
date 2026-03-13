from django.core.management.base import BaseCommand
from django_celery_beat.models import CrontabSchedule, PeriodicTask


class Command(BaseCommand):
    help = "Create or update the monthly plan report periodic task"

    def handle(self, *args, **options):
        schedule, _ = CrontabSchedule.objects.get_or_create(
            minute="0",
            hour="23",
            day_of_month="28-31",
            month_of_year="*",
            day_of_week="*",
            timezone="Asia/Kolkata",
        )
        task, created = PeriodicTask.objects.update_or_create(
            name="Monthly Plan Report Email",
            defaults={
                "task": "plans.send_monthly_plan_report",
                "crontab": schedule,
                "enabled": True,
            },
        )

        action = "Created" if created else "Updated"
        self.stdout.write(
            self.style.SUCCESS(
                f"{action} periodic task: '{task.name}' "
                f"(runs at 23:00 on days 28-31, sends only on last day)"
            )
        )

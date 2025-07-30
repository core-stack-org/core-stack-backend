import json
import os

from django.conf import settings
from django.core.management.base import BaseCommand

from geoadmin.models import StateSOI


class Command(BaseCommand):
    help = "Populate StateSOI table from JSON data"

    def add_arguments(self, parser):
        parser.add_argument(
            "--file",
            type=str,
            help="Path to JSON file containing state data",
            default="data/state_soi_data.json",
        )
        parser.add_argument(
            "--clear",
            action="store_true",
            help="Clear existing data before importing",
        )

    def handle(self, *args, **options):
        json_file = options["file"]
        clear_data = options["clear"]

        if not os.path.isabs(json_file):
            json_file = os.path.join(settings.BASE_DIR, json_file)

        if not os.path.exists(json_file):
            self.stdout.write(self.style.ERROR(f"JSON file not found: {json_file}"))
            return

        if clear_data:
            StateSOI.objects.all().delete()
            self.stdout.write(self.style.SUCCESS("Cleared existing StateSOI data"))

        try:
            with open(json_file, "r", encoding="utf-8") as file:
                data = json.load(file)

            created_count = 0
            updated_count = 0

            for item in data:
                if item.get("model") != "geoadmin.state_soi":
                    continue

                fields = item.get("fields", {})
                state_name = fields.get("state_name")
                active_status = fields.get("active_status", False)

                if not state_name:
                    self.stdout.write(
                        self.style.WARNING(
                            f"Skipping item with missing state_name: {item}"
                        )
                    )
                    continue

                state_obj, created = StateSOI.objects.get_or_create(
                    state_name=state_name, defaults={"active_status": active_status}
                )

                if created:
                    created_count += 1
                    self.stdout.write(f"Created: {state_name}")
                else:
                    # Update active_status if different
                    if state_obj.active_status != active_status:
                        state_obj.active_status = active_status
                        state_obj.save()
                        updated_count += 1
                        self.stdout.write(f"Updated: {state_name}")

            self.stdout.write(
                self.style.SUCCESS(
                    f"Successfully processed StateSOI data: "
                    f"{created_count} created, {updated_count} updated"
                )
            )

        except json.JSONDecodeError as e:
            self.stdout.write(self.style.ERROR(f"Invalid JSON format: {e}"))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error processing data: {e}"))

import json
import os

from django.conf import settings
from django.core.management.base import BaseCommand

from geoadmin.models import DistrictSOI, StateSOI


class Command(BaseCommand):
    help = "Populate DistrictSOI table from JSON data"

    def add_arguments(self, parser):
        parser.add_argument(
            "--file",
            type=str,
            help="Path to JSON file containing district data",
            default="data/district_soi_data.json",
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
            DistrictSOI.objects.all().delete()
            self.stdout.write(self.style.SUCCESS("Cleared existing DistrictSOI data"))

        try:
            with open(json_file, "r", encoding="utf-8") as file:
                data = json.load(file)

            created_count = 0
            updated_count = 0
            skipped_count = 0

            for item in data:
                if item.get("model") != "geoadmin.district_soi":
                    continue

                pk = item.get("pk")
                fields = item.get("fields", {})
                district_name = fields.get("district_name")
                state = fields.get("state")
                active_status = fields.get("active_status", False)

                if not district_name or not pk:
                    self.stdout.write(
                        self.style.WARNING(
                            f"Skipping item with missing district_name or pk: {item}"
                        )
                    )
                    skipped_count += 1
                    continue

                try:
                    state_obj = StateSOI.objects.get(id=state)
                except StateSOI.DoesNotExist:
                    self.stdout.write(
                        self.style.WARNING(
                            f"Skipping district '{district_name}' - State with id {state} not found"
                        )
                    )
                    skipped_count += 1
                    continue

                try:
                    district_obj = DistrictSOI.objects.get(id=pk)
                    updated = False
                    if district_obj.district_name != district_name:
                        district_obj.district_name = district_name
                        updated = True
                    if district_obj.state != state_obj:
                        district_obj.state = state_obj
                        updated = True
                    if district_obj.active_status != active_status:
                        district_obj.active_status = active_status
                        updated = True

                    if updated:
                        district_obj.save()
                        updated_count += 1
                        self.stdout.write(f"Updated: {district_name} (ID: {pk})")

                except DistrictSOI.DoesNotExist:
                    district_obj = DistrictSOI.objects.create(
                        id=pk,
                        district_name=district_name,
                        state=state_obj,
                        active_status=active_status,
                    )
                    created_count += 1
                    self.stdout.write(f"Created: {district_name} (ID: {pk})")

            self.stdout.write(
                self.style.SUCCESS(
                    f"Successfully processed DistrictSOI data: "
                    f"{created_count} created, {updated_count} updated, {skipped_count} skipped"
                )
            )

        except json.JSONDecodeError as e:
            self.stdout.write(self.style.ERROR(f"Invalid JSON format: {e}"))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error processing data: {e}"))

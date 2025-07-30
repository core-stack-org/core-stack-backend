import json
import os

from django.conf import settings
from django.core.management.base import BaseCommand

from geoadmin.models import DistrictSOI, TehsilSOI


class Command(BaseCommand):
    help = "Populate TehsilSOI table from JSON data"

    def add_arguments(self, parser):
        parser.add_argument(
            "--file",
            type=str,
            help="Path to JSON file containing tehsil data",
            default="data/tehsil_soi_data.json",
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
            TehsilSOI.objects.all().delete()
            self.stdout.write(self.style.SUCCESS("Cleared existing TehsilSOI data"))

        try:
            with open(json_file, "r", encoding="utf-8") as file:
                data = json.load(file)

            created_count = 0
            updated_count = 0
            skipped_count = 0

            for item in data:
                if item.get("model") != "geoadmin.block_soi":
                    continue

                pk = item.get("pk")
                fields = item.get("fields", {})
                tehsil_name = fields.get("block_name")
                district = fields.get("district")
                active_status = fields.get("active_status", False)

                if not tehsil_name or not pk:
                    self.stdout.write(
                        self.style.WARNING(
                            f"Skipping item with missing tehsil_name or pk: {item}"
                        )
                    )
                    skipped_count += 1
                    continue

                try:
                    district_obj = DistrictSOI.objects.get(id=district)
                except DistrictSOI.DoesNotExist:
                    self.stdout.write(
                        self.style.WARNING(
                            f"Skipping tehsil '{tehsil_name}' - District with id {district} not found"
                        )
                    )
                    skipped_count += 1
                    continue

                try:
                    tehsil_obj = TehsilSOI.objects.get(id=pk)
                    # Update existing tehsil
                    updated = False
                    if tehsil_obj.tehsil_name != tehsil_name:
                        tehsil_obj.tehsil_name = tehsil_name
                        updated = True
                    if tehsil_obj.district != district_obj:
                        tehsil_obj.district = district_obj
                        updated = True
                    if tehsil_obj.active_status != active_status:
                        tehsil_obj.active_status = active_status
                        updated = True

                    if updated:
                        tehsil_obj.save()
                        updated_count += 1
                        self.stdout.write(f"Updated: {tehsil_name} (ID: {pk})")

                except TehsilSOI.DoesNotExist:
                    tehsil_obj = TehsilSOI.objects.create(
                        id=pk,
                        tehsil_name=tehsil_name,
                        district=district_obj,
                        active_status=active_status,
                    )
                    created_count += 1
                    self.stdout.write(f"Created: {tehsil_name} (ID: {pk})")

            self.stdout.write(
                self.style.SUCCESS(
                    f"Successfully processed TehsilSOI data: "
                    f"{created_count} created, {updated_count} updated, {skipped_count} skipped"
                )
            )

        except json.JSONDecodeError as e:
            self.stdout.write(self.style.ERROR(f"Invalid JSON format: {e}"))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error processing data: {e}"))

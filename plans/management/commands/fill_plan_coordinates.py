import re
from decimal import Decimal

import requests
from django.core.management.base import BaseCommand
from django.db import transaction

from nrm_app.settings import GEOSERVER_PASSWORD, GEOSERVER_URL, GEOSERVER_USERNAME
from plans.models import PlanApp


def normalize_name(name: str) -> str:
    name = re.sub(r"[()]", "", name)
    name = re.sub(r"\s+", "_", name)
    name = re.sub(r"_+", "_", name)
    name = re.sub(r"^_|_$", "", name)
    return name.lower()


class Command(BaseCommand):
    help = (
        "Fill latitude/longitude for PlanApp entries using GeoServer layers "
        "(settlement → plan_gw → plan_agri → livelihood)"
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Run without making changes to the database",
        )
        parser.add_argument(
            "--plan-id",
            type=int,
            help="Process only a specific plan ID",
        )
        parser.add_argument(
            "--force",
            action="store_true",
            help="Overwrite existing coordinates",
        )

    def _layer_suffix(self, plan):
        district_name = normalize_name(plan.district_soi.district_name)
        tehsil_name = normalize_name(plan.tehsil_soi.tehsil_name)
        return f"{plan.id}_{district_name}_{tehsil_name}"

    def get_candidate_layers(self, plan):
        suffix = self._layer_suffix(plan)
        return [
            ("resources", f"settlement_{suffix}"),
            ("works", f"plan_gw_{suffix}"),
            ("works", f"plan_agri_{suffix}"),
            ("works", f"livelihood_{suffix}"),
        ]

    def fetch_coordinates(self, layer_name, workspace="resources"):
        url = (
            f"{GEOSERVER_URL}/{workspace}/ows?"
            f"service=WFS&version=1.0.0&request=GetFeature"
            f"&typeName={workspace}:{layer_name}&outputFormat=application/json"
        )
        try:
            response = requests.get(
                url,
                auth=(GEOSERVER_USERNAME, GEOSERVER_PASSWORD),
                timeout=30,
            )
            if response.status_code != 200:
                return None, f"HTTP {response.status_code}"

            # Check if response has content and is JSON
            if not response.content:
                return None, "Empty response"

            content_type = response.headers.get("Content-Type", "")
            if (
                "application/json" not in content_type
                and "application/geo+json" not in content_type
            ):
                return None, f"Non-JSON response: {content_type}"

            try:
                geojson = response.json()
            except ValueError as e:
                return None, f"Invalid JSON: {e}"

            features = geojson.get("features", [])
            if not features:
                return None, "No features found"

            coords = self._compute_centroid(features)
            if coords is None:
                return None, "No valid coordinates in features"
            return coords, None

        except requests.exceptions.RequestException as e:
            return None, str(e)
        except (KeyError, ValueError) as e:
            return None, f"Parse error: {e}"

    def _compute_centroid(self, features):
        all_coords = []
        for feature in features:
            geom = feature.get("geometry")
            if geom is None:
                continue

            geom_type = geom.get("type", "")
            coords = geom.get("coordinates", [])

            if not coords:
                continue

            if geom_type == "Point":
                all_coords.append(coords)
            elif geom_type == "MultiPoint":
                all_coords.extend(coords)
            elif geom_type in ("Polygon", "LineString"):
                if geom_type == "Polygon" and coords:
                    all_coords.extend(coords[0])
                else:
                    all_coords.extend(coords)

        if not all_coords:
            return None

        lon = sum(c[0] for c in all_coords) / len(all_coords)
        lat = sum(c[1] for c in all_coords) / len(all_coords)
        return (Decimal(str(round(lat, 8))), Decimal(str(round(lon, 8))))

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        plan_id = options.get("plan_id")
        force = options["force"]

        if dry_run:
            self.stdout.write(self.style.WARNING("DRY RUN MODE"))

        queryset = PlanApp.objects.filter(enabled=True)
        if plan_id:
            queryset = queryset.filter(id=plan_id)
        if not force:
            queryset = queryset.filter(latitude__isnull=True, longitude__isnull=True)

        plans = list(queryset)
        self.stdout.write(f"Found {len(plans)} plans to process")

        updated = 0
        errors = 0

        with transaction.atomic():
            for plan in plans:
                coords = None
                matched_layer = None
                last_errors = []

                for workspace, layer_name in self.get_candidate_layers(plan):
                    coords, error = self.fetch_coordinates(layer_name, workspace)
                    if coords:
                        matched_layer = f"{workspace}:{layer_name}"
                        break
                    last_errors.append(f"{workspace}:{layer_name} → {error}")

                if coords:
                    lat, lon = coords
                    if not dry_run:
                        plan.latitude = lat
                        plan.longitude = lon
                        plan.save(update_fields=["latitude", "longitude"])
                    updated += 1
                    self.stdout.write(
                        self.style.SUCCESS(
                            f"Plan {plan.id}: Set ({lat}, {lon}) from {matched_layer}"
                        )
                    )
                else:
                    errors += 1
                    self.stdout.write(
                        self.style.ERROR(
                            f"Plan {plan.id}: No coordinates found. Tried:\n"
                            + "\n".join(f"  {e}" for e in last_errors)
                        )
                    )

        self.stdout.write("\n" + "=" * 50)
        self.stdout.write(f"Updated: {updated}, Errors: {errors}")

from django.core.management.base import BaseCommand

from geoadmin.models import DistrictSOI, StateSOI, TehsilSOI
from projects.models import Project


class Command(BaseCommand):
    help = "Copy state, district, block values to state_soi, district_soi, tehsil_soi fields in Project model"

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Run without making changes",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]

        if dry_run:
            self.stdout.write(self.style.WARNING("DRY RUN MODE"))

        state_soi_map = {
            self.normalize(s.state_name): s for s in StateSOI.objects.all()
        }

        district_soi_map = {}
        for d in DistrictSOI.objects.select_related("state").all():
            key = (self.normalize(d.state.state_name), self.normalize(d.district_name))
            district_soi_map[key] = d

        tehsil_soi_map = {}
        for t in TehsilSOI.objects.select_related("district", "district__state").all():
            key = (
                self.normalize(t.district.state.state_name),
                self.normalize(t.district.district_name),
                self.normalize(t.tehsil_name),
            )
            tehsil_soi_map[key] = t

        projects = Project.objects.select_related(
            "state", "district", "district__state", "block", "block__district", "block__district__state"
        ).all()

        updated = 0
        failed = []

        for project in projects:
            state_soi = None
            district_soi = None
            tehsil_soi = None
            errors = []

            if project.state:
                state_key = self.normalize(project.state.state_name)
                state_soi = state_soi_map.get(state_key)
                if not state_soi:
                    errors.append(f"StateSOI not found for '{project.state.state_name}'")

            if project.district:
                district_key = (
                    self.normalize(project.district.state.state_name),
                    self.normalize(project.district.district_name),
                )
                district_soi = district_soi_map.get(district_key)
                if not district_soi:
                    errors.append(f"DistrictSOI not found for '{project.district.district_name}'")

            if project.block:
                tehsil_key = (
                    self.normalize(project.block.district.state.state_name),
                    self.normalize(project.block.district.district_name),
                    self.normalize(project.block.block_name),
                )
                tehsil_soi = tehsil_soi_map.get(tehsil_key)
                if not tehsil_soi:
                    errors.append(f"TehsilSOI not found for '{project.block.block_name}'")

            if errors:
                failed.append((project, errors))
                continue

            changed = False
            if state_soi and project.state_soi != state_soi:
                project.state_soi = state_soi
                changed = True
            if district_soi and project.district_soi != district_soi:
                project.district_soi = district_soi
                changed = True
            if tehsil_soi and project.tehsil_soi != tehsil_soi:
                project.tehsil_soi = tehsil_soi
                changed = True

            if changed:
                if not dry_run:
                    project.save(update_fields=["state_soi", "district_soi", "tehsil_soi"])
                updated += 1
                self.stdout.write(f"Updated: {project.name}")

        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS(f"Updated: {updated} projects"))

        if failed:
            self.stdout.write(self.style.ERROR(f"Failed: {len(failed)} projects"))
            for project, errors in failed:
                self.stdout.write(f"  {project.name}: {', '.join(errors)}")

    def normalize(self, name):
        return name.strip().lower() if name else ""


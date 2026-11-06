import subprocess
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from computing.config_loader import (
    PAN_INDIA_CANAL_PATH,
    PAN_INDIA_RIVER_PATH,
    PAN_INDIA_VILLAGE_PATH,
    PAN_INDIA_WATERBODIES_CENSUS_PATH,
)

LAYERS = {
    "river": PAN_INDIA_RIVER_PATH,
    "canal": PAN_INDIA_CANAL_PATH,
    "village": PAN_INDIA_VILLAGE_PATH,
    "census": PAN_INDIA_WATERBODIES_CENSUS_PATH,
}


class Command(BaseCommand):
    help = (
        "Convert the large pan-India GeoJSON layers used by the local SWB3/SWB4 "
        "(river, canal, village, waterbodies census) to spatially indexed .fgb "
        "files next to the originals, so bbox reads do not scan the whole file. "
        "The originals are left untouched."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--layers",
            nargs="+",
            choices=sorted(LAYERS),
            default=sorted(LAYERS),
        )
        parser.add_argument("--overwrite", action="store_true")

    def handle(self, *args, **options):
        for name in options["layers"]:
            source = Path(LAYERS[name])
            if source.suffix.lower() != ".geojson":
                self.stdout.write(f"{name}: {source.name} is not a GeoJSON, skipping")
                continue
            if not source.exists():
                raise CommandError(f"{name}: source not found: {source}")
            target = source.with_suffix(".fgb")
            if target.exists() and not options["overwrite"]:
                self.stdout.write(f"{name}: {target} exists, skipping")
                continue

            self.stdout.write(f"{name}: {source} -> {target}")
            tmp = target.with_suffix(".fgb.tmp")
            tmp.unlink(missing_ok=True)
            result = subprocess.run(
                [
                    "ogr2ogr",
                    "-f",
                    "FlatGeobuf",
                    "-nlt",
                    "POINT" if name == "census" else "PROMOTE_TO_MULTI",
                    "-lco",
                    "SPATIAL_INDEX=YES",
                    str(tmp),
                    str(source),
                ],
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                tmp.unlink(missing_ok=True)
                raise CommandError(f"{name}: ogr2ogr failed: {result.stderr.strip()}")
            tmp.replace(target)
            self.stdout.write(self.style.SUCCESS(f"{name}: wrote {target}"))

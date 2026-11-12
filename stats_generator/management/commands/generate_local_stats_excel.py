"""
Generate stats excels from locally generated layers, pan-India by default.

    # what would be filled from where (no files written)
    python manage.py generate_local_stats_excel --dry-run

    # all tehsils, 4 processes
    python manage.py generate_local_stats_excel --workers 4

    # one tehsil
    python manage.py generate_local_stats_excel --state bihar --district nalanda --block hilsa

Per layer: the local file (path from computing/config.yaml) if present, else
GeoServer for active tehsils, else the layer's sheets with blank columns.
Output: data/stats_excel_files/<STATE>/<DISTRICT>/<district>_<block>.xlsx,
the same files the GeoServer flow writes. A per-layer CSV report is written to
data/stats_excel_files/_meta/.
"""

import csv
import logging
import os
import time
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from multiprocessing import get_context
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db import connections

from computing.bulk_layer_generation import get_active_locations, get_all_locations
from stats_generator import local_layers
from stats_generator.local_layers import (
    ORIGIN_GEOSERVER,
    ORIGIN_LOCAL,
    ORIGIN_MISSING,
    slug,
)
from stats_generator.utils import (
    fetch_layers_for_excel_generation,
    generate_local_stats_excel,
)

logger = logging.getLogger("stats_generator.local_excel")

REPORT_FIELDS = [
    "state", "district", "block", "active", "workspace", "layer",
    "origin", "status", "path", "error",
]


def _format_layer_name(template, district, block):
    if "{district}" in template and "{block}" in template:
        return template.format(district=district, block=block)
    return template


def _generate_one(loc, is_active, geoserver_fallback, skip_existing):
    """Runs in a worker process. Returns (loc, is_active, rows, seconds, error)."""
    state, district, block = loc
    started = time.monotonic()
    final = local_layers.stats_excel_path(state, district, block)
    if skip_existing and final.exists():
        return loc, is_active, [], 0.0, "skipped (exists)"

    final.parent.mkdir(parents=True, exist_ok=True)
    tmp = final.with_name(f".{final.stem}.tmp.xlsx")
    tmp.unlink(missing_ok=True)
    try:
        results = generate_local_stats_excel(
            state,
            district,
            block,
            is_active=is_active,
            geoserver_fallback=geoserver_fallback,
            xlsx_file=str(tmp),
        )
        if not tmp.exists():
            raise RuntimeError("no workbook was written")
        os.replace(tmp, final)
        return loc, is_active, results, time.monotonic() - started, ""
    except Exception as exc:  # keep the batch going
        logger.exception("[%s/%s/%s] Stats excel generation failed", *loc)
        tmp.unlink(missing_ok=True)
        return loc, is_active, [], time.monotonic() - started, repr(exc)


class Command(BaseCommand):
    help = (
        "Build stats excels from locally generated layers (config.yaml paths) "
        "for all tehsils. Missing layers come from GeoServer for active tehsils, "
        "otherwise their sheets are written with blank columns."
    )

    def add_arguments(self, parser):
        parser.add_argument("--state")
        parser.add_argument("--district")
        parser.add_argument(
            "--block", dest="blocks", action="append",
            help="Tehsil/block name. Repeat to select several.",
        )
        parser.add_argument(
            "--active-only", action="store_true",
            help="Only active tehsils (default: every tehsil in TehsilSOI).",
        )
        parser.add_argument("--limit", type=int)
        parser.add_argument(
            "--no-geoserver-fallback", action="store_true",
            help="Never call GeoServer; layers missing locally are left blank.",
        )
        parser.add_argument(
            "--skip-existing", action="store_true",
            help="Skip tehsils whose excel already exists (default: regenerate).",
        )
        parser.add_argument("--workers", type=int, default=1)
        parser.add_argument(
            "--dry-run", action="store_true",
            help="Report where each layer would come from; write nothing.",
        )
        parser.add_argument(
            "--no-probe", action="store_true",
            help="Skip learning blank-sheet headers from sample tehsils first.",
        )
        parser.add_argument("--report", help="CSV report path.")

    # ------------------------------------------------------------------

    def handle(self, *args, **opts):
        filters = dict(
            state=opts["state"], district=opts["district"],
            blocks=opts["blocks"], limit=opts["limit"],
        )
        fallback = not opts["no_geoserver_fallback"]
        locator = get_active_locations if opts["active_only"] else get_all_locations
        locations = list(dict.fromkeys(
            (slug(l.state), slug(l.district), slug(l.block)) for l in locator(**filters)
        ))
        if not locations:
            raise CommandError("No tehsils matched the filters.")
        active = local_layers.active_location_slugs()
        layers = fetch_layers_for_excel_generation()
        n_active = sum(loc in active for loc in locations)
        self.stdout.write(
            f"{len(locations)} tehsils ({n_active} active), {len(layers)} layers, "
            f"GeoServer fallback {'on' if fallback else 'off'}"
        )

        if opts["dry_run"]:
            self._coverage(locations, active, layers, fallback)
            return

        if not opts["no_probe"]:
            self._probe_templates(layers, active, fallback)

        report_path = Path(opts["report"]) if opts["report"] else (
            local_layers.STATS_EXCEL_BASE_DIR / "_meta"
            / f"local_generation_report_{datetime.now():%Y%m%d_%H%M%S}.csv"
        )
        report_path.parent.mkdir(parents=True, exist_ok=True)
        self._run(locations, active, fallback, opts, report_path)

    # ------------------------------------------------------------------

    def _coverage(self, locations, active, layers, fallback):
        """Per layer: tehsils served locally / from GeoServer / blank."""
        counts = defaultdict(Counter)
        for state, district, block in locations:
            is_active = (state, district, block) in active
            for layer in layers:
                name = _format_layer_name(layer["layer_name"], district, block)
                spec, params = local_layers.find_spec(layer["workspace"], name, district, block)
                if spec is None:
                    origin = "no config.yaml entry"
                elif local_layers.local_paths_for(spec, params, state, district, block):
                    origin = ORIGIN_LOCAL
                elif is_active and fallback:
                    origin = ORIGIN_GEOSERVER
                else:
                    origin = "blank"
                counts[local_layers.layer_key(layer["workspace"], layer["layer_name"])][origin] += 1

        width = max(len(k) for k in counts)
        self.stdout.write(f"\n{'layer'.ljust(width)}  local  geoserver  blank  no-config")
        for key in sorted(counts):
            c = counts[key]
            self.stdout.write(
                f"{key.ljust(width)}  {c[ORIGIN_LOCAL]:5d}  {c[ORIGIN_GEOSERVER]:9d}  "
                f"{c['blank']:5d}  {c['no config.yaml entry']:9d}"
            )

    def _probe_templates(self, layers, active, fallback):
        """
        Blank sheets need the columns a layer produces. For each layer without a
        stored header template, generate just that workspace for one tehsil that
        has it (locally, else an active tehsil via GeoServer) into a scratch file.
        """
        store = local_layers.sheet_templates
        missing = defaultdict(list)
        for layer in layers:
            key = local_layers.layer_key(layer["workspace"], layer["layer_name"])
            if not store.get(key):
                missing[layer["workspace"]].append(layer)
        if not missing:
            return
        self.stdout.write(f"Learning sheet headers for {len(missing)} workspaces...")
        active_sorted = sorted(active)
        for workspace, ws_layers in sorted(missing.items()):
            sample, use_fallback = None, False
            for layer in ws_layers:
                spec, params = local_layers.find_spec(
                    workspace, _format_layer_name(layer["layer_name"], "{district}", "{block}"),
                    "{district}", "{block}",
                )
                if spec is not None:
                    sample = local_layers.sample_location_for(spec, params)
                if sample:
                    break
            if sample is None and fallback and active_sorted:
                sample, use_fallback = active_sorted[0], True
            if sample is None:
                self.stdout.write(self.style.WARNING(
                    f"  {workspace}: no tehsil has it locally; blank sheets omitted"
                ))
                continue
            scratch = local_layers.STATS_EXCEL_BASE_DIR / "_meta" / f".probe_{workspace}.xlsx"
            scratch.parent.mkdir(parents=True, exist_ok=True)
            scratch.unlink(missing_ok=True)
            try:
                generate_local_stats_excel(
                    *sample,
                    is_active=use_fallback,
                    geoserver_fallback=use_fallback,
                    specific_sheets=[workspace],
                    xlsx_file=str(scratch),
                )
            except Exception:
                logger.exception("Header probe for %s failed", workspace)
            finally:
                scratch.unlink(missing_ok=True)
            learned = [
                k for k in (local_layers.layer_key(workspace, l["layer_name"]) for l in ws_layers)
                if store.get(k)
            ]
            self.stdout.write(
                f"  {workspace}: {len(learned)}/{len(ws_layers)} learned from "
                f"{'/'.join(sample)}{' (GeoServer)' if use_fallback else ''}"
            )

    def _run(self, locations, active, fallback, opts, report_path):
        totals = Counter()
        failures = []
        done = 0
        started = time.monotonic()
        jobs = [(loc, loc in active, fallback, opts["skip_existing"]) for loc in locations]

        with open(report_path, "w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=REPORT_FIELDS)
            writer.writeheader()

            def record(loc, is_active, rows, seconds, error):
                nonlocal done
                done += 1
                base = dict(zip(("state", "district", "block"), loc), active=is_active)
                if error:
                    totals["skipped" if error.startswith("skipped") else "failed"] += 1
                    if not error.startswith("skipped"):
                        failures.append((loc, error))
                    writer.writerow({**base, "status": "error", "error": error})
                else:
                    totals["written"] += 1
                    for row in rows:
                        totals[row.get("origin", ORIGIN_MISSING)] += 1
                        writer.writerow({
                            **base, **{k: row.get(k, "") for k in
                                       ("workspace", "layer", "origin", "status", "path")},
                        })
                fh.flush()
                elapsed = time.monotonic() - started
                eta = elapsed / done * (len(jobs) - done)
                origins = Counter(r.get("origin") for r in rows)
                self.stdout.write(
                    f"[{done}/{len(jobs)}] {'/'.join(loc)} "
                    + (error or f"local={origins[ORIGIN_LOCAL]} geoserver={origins[ORIGIN_GEOSERVER]} "
                       f"blank={origins[ORIGIN_MISSING]} ({seconds:.0f}s)")
                    + f"  eta {eta / 60:.0f}m"
                )

            if opts["workers"] <= 1:
                for job in jobs:
                    record(*_generate_one(*job))
            else:
                connections.close_all()  # children open their own DB connections
                with ProcessPoolExecutor(
                    max_workers=opts["workers"], mp_context=get_context("fork")
                ) as pool:
                    futures = [pool.submit(_generate_one, *job) for job in jobs]
                    for future in as_completed(futures):
                        record(*future.result())

        self.stdout.write(self.style.SUCCESS(
            f"\nDone in {(time.monotonic() - started) / 60:.1f} min: "
            f"{totals['written']} written, {totals['skipped']} skipped, {totals['failed']} failed. "
            f"Layers: {totals[ORIGIN_LOCAL]} local, {totals[ORIGIN_GEOSERVER]} GeoServer, "
            f"{totals[ORIGIN_MISSING]} blank.\nReport: {report_path}"
        ))
        for loc, error in failures[:20]:
            self.stdout.write(self.style.ERROR(f"  {'/'.join(loc)}: {error}"))

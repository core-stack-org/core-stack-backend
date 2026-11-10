import csv
import sys
from collections import Counter, defaultdict
from pathlib import Path

from django.core.management.base import BaseCommand

from computing.layer_paths import LAYERS, block_coverage, get_data_dir, iter_blocks


class Command(BaseCommand):
    help = (
        "Read-only report of which local-compute layers exist for every block. "
        "Writes a block x layer matrix CSV and prints a per-layer summary."
    )

    def add_arguments(self, parser):
        parser.add_argument("--data-dir", help="Override DATA_DIR")
        parser.add_argument("--state", help="Only this state slug")
        parser.add_argument("--district", help="Only this district slug")
        parser.add_argument("--active-only", action="store_true",
                            help="Only blocks in activated_locations/active_locations.json")
        parser.add_argument("--no-legacy", action="store_true",
                            help="Only look in the new layout (default: new, then legacy)")
        parser.add_argument("--layers", help="Comma separated layer_type list")
        parser.add_argument("--output", default="layer_coverage.csv", help="CSV path")

    def handle(self, *args, **opts):
        data_dir = get_data_dir(opts["data_dir"])
        layer_types = set(opts["layers"].split(",")) if opts["layers"] else None
        specs = [s for s in LAYERS if not layer_types or s.layer_type in layer_types]

        present = Counter()
        partial = Counter()
        in_new = Counter()
        missing_by_state = defaultdict(Counter)
        n_blocks = 0

        out_path = Path(opts["output"])
        with open(out_path, "w", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(["state", "district", "block"] + [s.layer_type for s in specs])
            for state, district, block in iter_blocks(data_dir, opts["active_only"]):
                if opts["state"] and state != opts["state"]:
                    continue
                if opts["district"] and district != opts["district"]:
                    continue
                n_blocks += 1
                cov = block_coverage(state, district, block, data_dir,
                                     include_legacy=not opts["no_legacy"], layer_types=layer_types)
                row = []
                for s in specs:
                    c = cov[s.layer_type]
                    if c["present"]:
                        present[s.layer_type] += 1
                        in_new[s.layer_type] += c["where"] == "new"
                        row.append(f"{c['where']}:{c['n_files']}")
                    elif c["partial"]:
                        partial[s.layer_type] += 1
                        row.append(f"partial:{c['n_files']}")
                    else:
                        missing_by_state[state][s.layer_type] += 1
                        row.append("")
                writer.writerow([state, district, block] + row)

        self.stdout.write(f"data dir: {data_dir}")
        self.stdout.write(f"blocks scanned: {n_blocks}\n")
        self.stdout.write(f"{'layer_type':40s} {'present':>8s} {'partial':>8s} {'missing':>8s} {'in new':>8s}")
        for s in specs:
            miss = n_blocks - present[s.layer_type] - partial[s.layer_type]
            flag = "" if s.local_computable else "  (no local source)"
            self.stdout.write(
                f"{s.layer_type:40s} {present[s.layer_type]:8d} {partial[s.layer_type]:8d} {miss:8d} "
                f"{in_new[s.layer_type]:8d}{flag}"
            )
        self.stdout.write(f"\nmatrix written to {out_path.resolve()}")

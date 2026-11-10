from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from computing import layer_migration as lm
from computing.layer_paths import get_data_dir


class Command(BaseCommand):
    help = (
        "Move legacy per-layer output folders into "
        "DATA_DIR/layers_output/{state}/{district}/{block}/{layer_type}/. "
        "Dry-run by default. --apply moves files (atomic renames, same filesystem only) and "
        "records a manifest; --rollback restores a previous run from its manifest."
    )

    def add_arguments(self, parser):
        parser.add_argument("--data-dir", help="Override DATA_DIR")
        parser.add_argument("--layers", help="Comma separated layer_type list (default: all)")
        parser.add_argument("--states", help="Comma separated state slugs (default: all)")
        parser.add_argument("--apply", action="store_true", help="Actually move the files")
        parser.add_argument("--manifest", help="Manifest path (for --apply: where to write; for --rollback / resume: which to use)")
        parser.add_argument("--resume", action="store_true", help="With --apply: continue an existing --manifest instead of planning again")
        parser.add_argument("--rollback", action="store_true", help="Undo the moves recorded in --manifest")
        parser.add_argument("--prune-empty", action="store_true", help="After --apply, remove empty legacy directories")
        parser.add_argument("--report", help="Write unmatched / collision details to this text file")

    def handle(self, *args, **o):
        data_dir = get_data_dir(o["data_dir"])
        log = self.stdout.write

        if o["rollback"]:
            if not o["manifest"]:
                raise CommandError("--rollback needs --manifest")
            stats = lm.rollback_migration(Path(o["manifest"]), log=log)
            log(f"rollback: {stats}")
            return

        if o["resume"]:
            if not o["manifest"]:
                raise CommandError("--resume needs --manifest")
            manifest = Path(o["manifest"])
            log(f"applying {manifest}: {lm.apply_migration(manifest, log=log)}")
            return

        plan = lm.plan_migration(
            data_dir,
            layer_types=o["layers"].split(",") if o["layers"] else None,
            states=o["states"].split(",") if o["states"] else None,
        )
        s = plan.summary()
        log(f"data dir: {data_dir}")
        log(f"files to move: {s['moves']}  ({s['bytes'] / 1e9:.1f} GB)")
        for k, v in sorted(s["per_layer"].items()):
            log(f"  {k:40s} {v:8d}")
        log(f"collisions (destination exists / duplicate): {s['collisions']}")
        log(f"ambiguous (matches several layers, left in place): {s['ambiguous']}")
        log(f"unknown state folders (left in place): {s['unknown_state_dirs']}")
        log(f"unmatched files (left in place): {s['unmatched']}")

        if o["report"]:
            with open(o["report"], "w") as fh:
                for legacy_dir, files in sorted(plan.unmatched.items()):
                    fh.write(f"## unmatched {legacy_dir} ({len(files)})\n" + "\n".join(files) + "\n")
                for legacy_dir, files in sorted(plan.unknown_states.items()):
                    fh.write(f"## unknown state {legacy_dir} ({len(files)})\n" + "\n".join(files) + "\n")
                fh.write(f"## collisions ({len(plan.collisions)})\n")
                fh.writelines(f"{a} -> {b}\n" for a, b in plan.collisions)
                fh.write(f"## ambiguous ({len(plan.ambiguous)})\n")
                fh.writelines(f"{a} -> {b}\n" for a, b in plan.ambiguous)
            log(f"details written to {o['report']}")

        if not o["apply"]:
            log("\nDRY RUN - nothing moved. Re-run with --apply to move files.")
            return

        new_root = data_dir / "layers_output"
        new_root.mkdir(parents=True, exist_ok=True)
        if not lm.same_filesystem(data_dir / "layers", new_root):
            raise CommandError("layers_output is on a different filesystem; refusing to copy-move.")

        manifest = Path(o["manifest"]) if o["manifest"] else lm.default_manifest_path(data_dir)
        lm.write_manifest(plan, manifest)
        log(f"manifest: {manifest}")
        log(f"applied: {lm.apply_migration(manifest, log=log)}")
        if o["prune_empty"]:
            log(f"removed {lm.prune_empty_legacy_dirs(data_dir)} empty legacy directories")
        log(f"to undo: --rollback --manifest {manifest}")

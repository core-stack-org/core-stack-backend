"""
Move legacy per-layer output folders into the organised layout

    {DATA_DIR}/layers_output/{state}/{district}/{block}/{layer_type}/{file}

Design:
* ``plan_migration`` is read-only and returns every move it would make plus
  everything it refuses to touch (unmatched files, collisions, unknown states).
* ``apply_migration`` renames files (same filesystem only) and appends one line
  per completed move to a ``.done`` log, so an interrupted run can be resumed and
  any run can be undone with ``rollback_migration``.
* Nothing is ever deleted or overwritten. A destination that already exists is
  reported as a collision and skipped.

No Django / geopandas imports; used by the ``migrate_layers_layout`` command.
"""

from __future__ import annotations

import csv
import os
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from computing.layer_paths import (
    LAYERS,
    NEW_ROOT,
    STATE_ALIASES,
    LayerSpec,
    get_data_dir,
    matching_files,
)

MIGRATION_DIRNAME = "_migration"
MANIFEST_FIELDS = ["layer_type", "state", "district", "block", "src", "dst", "bytes"]


@dataclass
class Move:
    layer_type: str
    state: str
    district: str
    block: str
    src: Path
    dst: Path
    size: int

    def row(self) -> List[str]:
        return [self.layer_type, self.state, self.district, self.block, str(self.src), str(self.dst), str(self.size)]


@dataclass
class Plan:
    data_dir: Path
    moves: List[Move] = field(default_factory=list)
    unmatched: Dict[str, List[str]] = field(default_factory=lambda: defaultdict(list))  # legacy_dir -> files
    collisions: List[Tuple[Path, Path]] = field(default_factory=list)
    unknown_states: Dict[str, List[str]] = field(default_factory=lambda: defaultdict(list))
    ambiguous: List[Tuple[Path, List[str]]] = field(default_factory=list)

    def summary(self) -> Dict[str, object]:
        per_layer = Counter(m.layer_type for m in self.moves)
        per_state = Counter(m.state for m in self.moves)
        return {
            "moves": len(self.moves),
            "bytes": sum(m.size for m in self.moves),
            "per_layer": dict(per_layer),
            "per_state": dict(per_state),
            "unmatched": {k: len(v) for k, v in self.unmatched.items()},
            "collisions": len(self.collisions),
            "ambiguous": len(self.ambiguous),
            "unknown_state_dirs": {k: len(v) for k, v in self.unknown_states.items()},
        }


def _known_states(data_dir: Path) -> set:
    boundary = data_dir / "base_layers" / "admin-boundary" / "output"
    states = set(os.listdir(boundary)) if boundary.is_dir() else set()
    return states | set(STATE_ALIASES.values())


def _candidates(filename: str) -> List[str]:
    """Names a file could be matched as. ``x.gpkg.zip`` and ``x.zip`` are the zipped
    copies that were created for GeoServer pushes and travel with ``x.gpkg``."""
    out = [filename]
    if filename.endswith(".zip"):
        stem = filename[:-4]
        out += [stem, stem + ".gpkg", stem + ".tif"]
    return out


def _claims(spec: LayerSpec, filename: str, district: str, block: str) -> bool:
    for cand in _candidates(filename):
        for part in spec.parts:
            if matching_files(part, [cand], district, block):
                return True
    return False


def plan_migration(data_dir=None, layer_types: Optional[Iterable[str]] = None,
                   states: Optional[Iterable[str]] = None) -> Plan:
    data_dir = get_data_dir(data_dir)
    plan = Plan(data_dir=data_dir)
    known_states = _known_states(data_dir)
    wanted_layers = set(layer_types) if layer_types else None
    wanted_states = set(states) if states else None
    new_root = data_dir / NEW_ROOT

    # Group specs by legacy dir so a shared folder (tree_health, fabdem_local, ...) is walked once.
    by_dir: Dict[str, List[LayerSpec]] = defaultdict(list)
    for spec in LAYERS:
        if spec.legacy_dir and (not wanted_layers or spec.layer_type in wanted_layers):
            by_dir[spec.legacy_dir].append(spec)

    claimed_dst: Dict[Path, Path] = {}

    for legacy_dir, specs in sorted(by_dir.items()):
        root = data_dir / legacy_dir
        if not root.is_dir():
            continue
        for dirpath, _dirs, files in os.walk(root):
            if not files:
                continue
            rel = Path(dirpath).relative_to(root).parts
            if len(rel) != 3:
                plan.unmatched[legacy_dir] += [str(Path(dirpath) / f) for f in files]
                continue
            state_dir, district, block = rel
            state = STATE_ALIASES.get(state_dir, state_dir)
            if state not in known_states:
                plan.unknown_states[legacy_dir] += [str(Path(dirpath) / f) for f in files]
                continue
            if wanted_states and state not in wanted_states:
                continue

            for f in files:
                owners = [s for s in specs if _claims(s, f, district, block)]
                src = Path(dirpath) / f
                if not owners:
                    plan.unmatched[legacy_dir].append(str(src))
                    continue
                if len(owners) > 1:
                    plan.ambiguous.append((src, [o.layer_type for o in owners]))
                    continue
                spec = owners[0]
                dst = new_root / state / district / block / spec.layer_type / f
                if dst.exists() or dst in claimed_dst:
                    plan.collisions.append((src, dst))
                    continue
                claimed_dst[dst] = src
                plan.moves.append(Move(spec.layer_type, state, district, block, src, dst, src.stat().st_size))
    return plan


def default_manifest_path(data_dir: Path) -> Path:
    stamp = time.strftime("%Y%m%d_%H%M%S")
    return data_dir / NEW_ROOT / MIGRATION_DIRNAME / f"manifest_{stamp}.tsv"


def write_manifest(plan: Plan, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as fh:
        w = csv.writer(fh, delimiter="\t")
        w.writerow(MANIFEST_FIELDS)
        for m in plan.moves:
            w.writerow(m.row())


def read_manifest(path: Path) -> List[Move]:
    with open(path, newline="") as fh:
        return [
            Move(r["layer_type"], r["state"], r["district"], r["block"], Path(r["src"]), Path(r["dst"]), int(r["bytes"]))
            for r in csv.DictReader(fh, delimiter="\t")
        ]


def same_filesystem(a: Path, b: Path) -> bool:
    """True when a rename between a and b is guaranteed to be a cheap atomic move."""
    probe = b
    while not probe.exists():
        probe = probe.parent
    return a.stat().st_dev == probe.stat().st_dev


def done_log_path(manifest: Path) -> Path:
    return manifest.with_suffix(".done")


def _done_set(manifest: Path) -> set:
    p = done_log_path(manifest)
    if not p.exists():
        return set()
    with open(p) as fh:
        return {line.rstrip("\n") for line in fh if line.strip()}


def apply_migration(manifest: Path, progress_every: int = 5000, log=print) -> Dict[str, int]:
    """Execute a manifest. Safe to re-run: finished moves are skipped."""
    moves = read_manifest(manifest)
    done = _done_set(manifest)
    stats = Counter()
    with open(done_log_path(manifest), "a") as dlog:
        for i, m in enumerate(moves, 1):
            key = str(m.src)
            if key in done:
                stats["already_done"] += 1
                continue
            if not m.src.exists():
                stats["src_missing"] += 1
                continue
            if m.dst.exists():
                stats["dst_exists"] += 1
                continue
            m.dst.parent.mkdir(parents=True, exist_ok=True)
            os.rename(m.src, m.dst)  # same filesystem: atomic, never copies
            dlog.write(key + "\n")
            dlog.flush()
            stats["moved"] += 1
            if progress_every and i % progress_every == 0:
                log(f"  {i}/{len(moves)} processed")
    return dict(stats)


def rollback_migration(manifest: Path, log=print) -> Dict[str, int]:
    """Move every file listed in the done log back to its original place."""
    moves = {str(m.src): m for m in read_manifest(manifest)}
    done = _done_set(manifest)
    stats = Counter()
    for key in sorted(done):
        m = moves.get(key)
        if m is None:
            stats["not_in_manifest"] += 1
            continue
        if not m.dst.exists():
            stats["dst_missing"] += 1
            continue
        if m.src.exists():
            stats["src_exists"] += 1
            continue
        m.src.parent.mkdir(parents=True, exist_ok=True)
        os.rename(m.dst, m.src)
        stats["restored"] += 1
    # Tidy empty directories left in the new tree (never touches non-empty ones).
    new_root = manifest.parents[1]
    for dirpath, dirs, files in os.walk(new_root, topdown=False):
        p = Path(dirpath)
        if p != new_root and MIGRATION_DIRNAME not in p.parts and not os.listdir(p):
            p.rmdir()
    if stats["restored"] and not stats["src_exists"] and not stats["dst_missing"]:
        done_log_path(manifest).rename(done_log_path(manifest).with_suffix(".rolledback"))
    return dict(stats)


def prune_empty_legacy_dirs(data_dir, log=print) -> int:
    """Remove empty directories (never files) below each legacy layer root."""
    data_dir = get_data_dir(data_dir)
    removed = 0
    for legacy_dir in sorted({s.legacy_dir for s in LAYERS if s.legacy_dir}):
        root = data_dir / legacy_dir
        if not root.is_dir():
            continue
        for dirpath, _d, _f in os.walk(root, topdown=False):
            p = Path(dirpath)
            if p != root and not os.listdir(p):
                p.rmdir()
                removed += 1
    return removed

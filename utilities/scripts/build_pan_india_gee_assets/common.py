"""Shared config, path, logging, and run-state helpers for pan-India asset builds."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

import yaml


log = logging.getLogger("pan_india_gee_assets")

DEFAULT_CONFIG = Path(__file__).with_name("pan_india_assets.yaml")


def setup_logging(debug: bool = False, log_file: Path | None = None) -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="%(asctime)s  [%(levelname)-7s]  %(message)s",
        datefmt="%H:%M:%S",
        handlers=handlers,
    )


def find_repo_root(start: Path) -> Path:
    for candidate in [start, *start.parents]:
        if (candidate / ".git").exists() or (candidate / "manage.py").exists():
            return candidate
    return start


REPO_ROOT = find_repo_root(Path(__file__).resolve())


def repo_path(value: str | Path) -> Path:
    path = Path(value)
    return path if path.is_absolute() else REPO_ROOT / path


def read_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def write_yaml(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(payload, handle, sort_keys=False, allow_unicode=True)


def load_config(path: str | Path | None = None) -> dict[str, Any]:
    return read_yaml(Path(path) if path else DEFAULT_CONFIG)


def utc_now_text() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def scope_key(pipeline: str, scope: dict[str, Any]) -> str:
    """Stable identity for one pipeline request over one admin scope."""

    def norm(value: Any) -> str:
        return " ".join(str(value or "").strip().lower().split())

    return "|".join(
        [
            pipeline,
            norm(scope.get("level")),
            norm(scope.get("state_name")),
            norm(scope.get("district_name")),
            norm(scope.get("tehsil_name")),
        ]
    )


class RunState:
    """Append-only JSONL run state; the last record per scope key wins."""

    def __init__(self, path: Path):
        self.path = path
        self.records: dict[str, dict[str, Any]] = {}
        if path.exists():
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    record = json.loads(line)
                    self.records[record["key"]] = record

    def get(self, key: str) -> dict[str, Any] | None:
        return self.records.get(key)

    def append(self, record: dict[str, Any]) -> None:
        self.records[record["key"]] = record
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")

    def iter_records(self, *, pipeline: str | None = None, status: str | None = None) -> Iterator[dict[str, Any]]:
        for record in self.records.values():
            if pipeline and record.get("pipeline") != pipeline:
                continue
            if status and record.get("status") != status:
                continue
            yield record

    def counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for record in self.records.values():
            counts[record.get("status", "unknown")] = counts.get(record.get("status", "unknown"), 0) + 1
        return counts

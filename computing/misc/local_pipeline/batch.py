"""Batch request parsing for local pipelines."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

import yaml

from .schema import StandardRequest


def load_request_file(path: str | Path) -> list[StandardRequest]:
    """Load one or more standard requests from JSON, YAML, CSV, or text."""

    path = Path(path)
    suffix = path.suffix.lower()
    if suffix == ".json":
        data = json.loads(path.read_text())
        return _requests_from_payload(data)
    if suffix in {".yaml", ".yml"}:
        data = yaml.safe_load(path.read_text()) or {}
        return _requests_from_payload(data)
    if suffix == ".csv":
        with path.open(newline="") as handle:
            return [StandardRequest.from_mapping(row) for row in csv.DictReader(handle)]
    return [_request_from_text_line(line) for line in path.read_text().splitlines() if line.strip()]


def _requests_from_payload(data: Any) -> list[StandardRequest]:
    if isinstance(data, list):
        return [StandardRequest.from_mapping(item) for item in data]
    if isinstance(data, dict) and isinstance(data.get("requests"), list):
        return [StandardRequest.from_mapping(item) for item in data["requests"]]
    if isinstance(data, dict):
        return [StandardRequest.from_mapping(data)]
    raise ValueError("Request payload must be an object, list, or {requests: [...]}")


def _request_from_text_line(line: str) -> StandardRequest:
    """Parse `state,district,tehsil` or `level,state,district,tehsil` text."""

    parts = [part.strip() for part in line.split(",")]
    if len(parts) == 3:
        level = "tehsil"
        state, district, tehsil = parts
    elif len(parts) == 4:
        level, state, district, tehsil = parts
    else:
        raise ValueError(f"Cannot parse request line: {line}")
    return StandardRequest.from_mapping(
        {
            "scope": {
                "level": level,
                "state_name": state,
                "district_name": district,
                "tehsil_name": tehsil,
            }
        }
    )

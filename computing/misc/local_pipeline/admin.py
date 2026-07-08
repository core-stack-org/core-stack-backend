"""Admin-boundary selection helpers for local pipelines."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Sequence

import pandas as pd

try:
    from utilities.constants import ADMIN_BOUNDARY_GPKG
except ImportError:
    ADMIN_BOUNDARY_GPKG = "data/admin-boundary/cs_admin_standard.gpkg"

from .gpkg import (
    IndexSpec,
    column_expression,
    ensure_indexes,
    lower_key_expression,
    quote_identifier,
    read_features,
    read_table,
)


ADMIN_TABLE = "cs_admin_standard"
ADMIN_GEOMETRY_COLUMN = "geom"
ADMIN_DEFAULT_COLUMNS = (
    "fid",
    "cs_feature_id",
    "cs_admin_uid",
    "core_admin_uid",
    "state_name",
    "district_name",
    "TEHSIL",
    "pc11_state_id",
    "pc11_district_id",
    "pc11_subdistrict_id",
    "pc11_village_id",
    "village_id",
    "NAME",
)
ADMIN_PRESENTATION_COLUMNS = (
    "index",
    "state_id",
    "district_id",
    "tehsil_id",
    "village_id",
    "state_name",
    "district_name",
    "tehsil_name",
    "village_name",
)
INTERNAL_ADMIN_COLUMNS = (
    "cs_feature_id",
    "cs_admin_uid",
    "core_admin_uid",
    "pc11_village_id",
)


def normalize_key(value: Any) -> str:
    """Normalize an admin name for consistent lowercase lookup."""

    return " ".join(str(value or "").strip().lower().split())


def format_admin_name(value: Any) -> str | None:
    """Return a readable title-case admin name without changing lookup keys."""

    if value is None or value != value:
        return None
    text = re.sub(r"\s+", " ", str(value).strip())
    if not text:
        return None
    text = re.sub(r"\s*-\s*", " - ", text)
    text = re.sub(r"(?<=\b[A-Za-z])\.(?=[A-Za-z]\b)", ". ", text)
    text = re.sub(r"(?<=\b[A-Za-z])\.(?=[A-Za-z]\.)", ". ", text)
    text = re.sub(r"\s+", " ", text)

    words: list[str] = []
    for word in text.split(" "):
        if word == "-":
            words.append(word)
            continue
        pieces = word.split(".")
        if len(pieces) > 1:
            formatted = ".".join(piece.upper() if len(piece) == 1 else piece.title() for piece in pieces)
            words.append(formatted)
            continue
        words.append(f"{word.upper()}." if len(word) == 1 and word.isalpha() else word.title())
    return " ".join(words)


def admin_presentation_frame(rows: Any, *, include_geometry: bool = False):
    """Return standard, compact admin columns for user-facing outputs."""

    frame = rows.copy()
    rename_map = {
        "fid": "index",
        "pc11_state_id": "state_id",
        "pc11_district_id": "district_id",
        "pc11_subdistrict_id": "tehsil_id",
        "TEHSIL": "tehsil_name",
        "NAME": "village_name",
    }
    frame = frame.rename(columns={src: dst for src, dst in rename_map.items() if src in frame.columns})
    for column in ("state_name", "district_name", "tehsil_name", "village_name"):
        if column in frame.columns:
            frame[column] = frame[column].map(format_admin_name)
    columns = [column for column in ADMIN_PRESENTATION_COLUMNS if column in frame.columns]
    if include_geometry and "geometry" in frame.columns:
        columns.append("geometry")
    return frame[columns].copy()


def admin_output_frame(
    rows: Any,
    *,
    value_columns: Sequence[str] = (),
    include_geometry: bool = False,
):
    """Return a user-facing frame with standard admin columns and configured values."""

    frame = rows.copy()
    presentation = admin_presentation_frame(frame, include_geometry=False)
    ordered = list(presentation.columns)
    for column in value_columns:
        if column in frame.columns and column not in ordered and column not in INTERNAL_ADMIN_COLUMNS:
            ordered.append(column)
    output = frame.rename(
        columns={
            "fid": "index",
            "pc11_state_id": "state_id",
            "pc11_district_id": "district_id",
            "pc11_subdistrict_id": "tehsil_id",
            "TEHSIL": "tehsil_name",
            "NAME": "village_name",
        }
    )
    for column in ("state_name", "district_name", "tehsil_name", "village_name"):
        if column in output.columns:
            output[column] = output[column].map(format_admin_name)
    if include_geometry and "geometry" in frame.columns:
        ordered.append("geometry")
    return output.reindex(columns=ordered)


def _repo_path(path: str | Path, base_dir: str | Path | None = None) -> Path:
    path = Path(path)
    if path.is_absolute():
        return path
    return Path(base_dir or Path.cwd()) / path


def _as_tuple(value: Any) -> tuple[Any, ...]:
    if value is None:
        return ()
    if isinstance(value, (list, tuple, set)):
        return tuple(value)
    return (value,)


@dataclass(frozen=True)
class AdminScope:
    """Administrative selection requested by an API, CLI, or batch row."""

    level: str
    state_name: str | None = None
    district_name: str | None = None
    tehsil_name: str | None = None
    village_ids: tuple[Any, ...] = field(default_factory=tuple)

    @classmethod
    def from_mapping(cls, data: dict[str, Any]) -> "AdminScope":
        """Build a scope from common API/CLI key names."""

        return cls(
            level=str(data.get("level") or data.get("scope_level") or "tehsil").lower(),
            state_name=data.get("state_name") or data.get("state"),
            district_name=data.get("district_name") or data.get("district"),
            tehsil_name=data.get("tehsil_name") or data.get("block_name") or data.get("block"),
            village_ids=_as_tuple(data.get("village_ids") or data.get("village_id")),
        )


@dataclass
class AdminSelection:
    """Rows and identifiers selected from the standard admin boundary."""

    scope: AdminScope
    rows: Any
    created_indexes: list[str]
    where_sql: str
    params: tuple[Any, ...]

    @property
    def village_ids(self) -> list[Any]:
        if "village_id" not in self.rows.columns:
            return []
        return self.rows["village_id"].dropna().drop_duplicates().tolist()

    @property
    def pc11_village_ids(self) -> list[Any]:
        if "pc11_village_id" not in self.rows.columns:
            return []
        return self.rows["pc11_village_id"].dropna().drop_duplicates().tolist()

    @property
    def cs_feature_ids(self) -> list[str]:
        if "cs_feature_id" not in self.rows.columns:
            return []
        return self.rows["cs_feature_id"].dropna().astype(str).drop_duplicates().tolist()

    def id_frame(self) -> pd.DataFrame:
        """Return the standard identifier columns for joins and QA."""

        columns = [col for col in ADMIN_DEFAULT_COLUMNS if col in self.rows.columns]
        return pd.DataFrame(self.rows[columns]).copy()


class CSAdminSource:
    """Fast selector for `data/admin-boundary/cs_admin_standard.gpkg`."""

    def __init__(
        self,
        path: str | Path = ADMIN_BOUNDARY_GPKG,
        *,
        table_name: str = ADMIN_TABLE,
        base_dir: str | Path | None = None,
    ) -> None:
        self.path = _repo_path(path, base_dir)
        self.table_name = table_name

    @property
    def required_indexes(self) -> tuple[IndexSpec, ...]:
        """Indexes used by standard local admin lookups."""

        table = self.table_name
        return (
            IndexSpec(
                name=f"idx_{table}_lower_state_district_tehsil",
                table=table,
                expressions=(
                    lower_key_expression("state_name"),
                    lower_key_expression("district_name"),
                    lower_key_expression("TEHSIL"),
                ),
            ),
            IndexSpec(
                name=f"idx_{table}_village_id",
                table=table,
                expressions=(column_expression("village_id"),),
            ),
            IndexSpec(
                name=f"idx_{table}_pc11_village_id",
                table=table,
                expressions=(column_expression("pc11_village_id"),),
            ),
            IndexSpec(
                name=f"idx_{table}_cs_feature_id",
                table=table,
                expressions=(column_expression("cs_feature_id"),),
            ),
        )

    def ensure_indexes(self) -> list[str]:
        """Create missing admin lookup indexes once."""

        return ensure_indexes(self.path, self.required_indexes)

    def _where_for_scope(self, scope: AdminScope) -> tuple[str, tuple[Any, ...]]:
        level = scope.level.lower()
        clauses: list[str] = []
        params: list[Any] = []

        if level in {"state", "district", "tehsil", "block"}:
            if not scope.state_name:
                raise ValueError("state_name is required for state/district/tehsil scopes")
            clauses.append(f"{lower_key_expression('state_name')} = ?")
            params.append(normalize_key(scope.state_name))

        if level in {"district", "tehsil", "block"}:
            if not scope.district_name:
                raise ValueError("district_name is required for district/tehsil scopes")
            clauses.append(f"{lower_key_expression('district_name')} = ?")
            params.append(normalize_key(scope.district_name))

        if level in {"tehsil", "block"}:
            if not scope.tehsil_name:
                raise ValueError("tehsil_name or block_name is required for tehsil scopes")
            clauses.append(f"{lower_key_expression('TEHSIL')} = ?")
            params.append(normalize_key(scope.tehsil_name))

        if level == "village":
            if not scope.village_ids:
                raise ValueError("village_ids are required for village scope")
            placeholders = ", ".join(["?"] * len(scope.village_ids))
            clauses.append(
                "("
                f"{quote_identifier('village_id')} IN ({placeholders}) OR "
                f"{quote_identifier('pc11_village_id')} IN ({placeholders}) OR "
                f"{quote_identifier('cs_feature_id')} IN ({placeholders})"
                ")"
            )
            values = list(scope.village_ids)
            params.extend(values)
            params.extend(values)
            params.extend(values)

        if not clauses:
            raise ValueError(f"Unsupported admin scope level: {scope.level}")
        return " AND ".join(clauses), tuple(params)

    def read_scope(
        self,
        scope: AdminScope,
        *,
        columns: Sequence[str] | None = None,
        include_geometry: bool = True,
    ) -> AdminSelection:
        """Read only the admin rows required by a scope."""

        created_indexes = self.ensure_indexes()
        where, params = self._where_for_scope(scope)
        read_columns = list(columns or ADMIN_DEFAULT_COLUMNS)
        if include_geometry:
            if ADMIN_GEOMETRY_COLUMN not in read_columns:
                read_columns.append(ADMIN_GEOMETRY_COLUMN)
            rows = read_features(
                self.path,
                self.table_name,
                columns=read_columns,
                where=where,
                params=params,
                geometry_column_name=ADMIN_GEOMETRY_COLUMN,
            )
        else:
            rows = read_table(
                self.path,
                self.table_name,
                columns=read_columns,
                where=where,
                params=params,
            )
        if rows.empty:
            raise ValueError(f"No admin rows found for scope: {scope}")
        return AdminSelection(scope, rows, created_indexes, where, params)

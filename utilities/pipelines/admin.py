"""Admin-boundary selection helpers for local pipelines."""

from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Sequence

import pandas as pd

try:
    from utilities.constants import ADMIN_BOUNDARY_GPKG
except ImportError:
    ADMIN_BOUNDARY_GPKG = "data/base_resources/cs_admin_standard.gpkg"

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
#: Shared descriptions for the standard admin columns emitted by every local
#: pipeline output, used to build column dictionaries and README references.
ADMIN_COLUMN_DESCRIPTIONS = {
    "index": "Stable row index of the village in the Core Stack standard admin boundary.",
    "state_id": "Census 2011 (PC11) state code.",
    "district_id": "Census 2011 (PC11) district code.",
    "tehsil_id": "Census 2011 (PC11) sub-district (tehsil/block) code.",
    "village_id": "Core Stack village identifier used for keyed dataset joins.",
    "state_name": "State name.",
    "district_name": "District name.",
    "tehsil_name": "Tehsil/block name.",
    "village_name": "Village name.",
}

INTERNAL_ADMIN_COLUMNS = (
    "fid",
    "cs_feature_id",
    "cs_admin_uid",
    "core_admin_uid",
    "pc11_state_id",
    "pc11_district_id",
    "pc11_subdistrict_id",
    "pc11_village_id",
    "TEHSIL",
    "NAME",
)


def is_internal_admin_column(column: Any) -> bool:
    """Return true for fixed source admin fields that should not leak to outputs."""

    return str(column) in INTERNAL_ADMIN_COLUMNS


def normalize_scope_name(value: Any) -> str | None:
    """Normalize API-safe separators without changing an admin name."""

    if value is None:
        return None
    text = re.sub(r"_+", " ", str(value).strip())
    return " ".join(text.split()) or None


def normalize_key(value: Any) -> str:
    """Normalize an admin name for consistent lowercase lookup."""

    return (normalize_scope_name(value) or "").lower()


def admin_name_match_keys(value: Any) -> frozenset[str]:
    """Return conservative comparison keys for common admin-name spellings."""

    text = normalize_scope_name(value) or ""
    variants = [text]
    variants.extend(re.findall(r"\(([^()]*)\)", text))
    variants.append(re.sub(r"\([^()]*\)", " ", text))
    keys: set[str] = set()
    for variant in variants:
        words = re.sub(r"[^0-9a-z]+", " ", variant.lower()).split()
        if words:
            keys.add(" ".join(words))
            keys.add("".join(words))
    return frozenset(keys)


def _unique_admin_name_match(
    candidates: Sequence[str], requested: str, label: str
) -> str:
    """Resolve one name without fuzzy spelling or word-order matching."""

    exact = [
        candidate
        for candidate in candidates
        if normalize_key(candidate) == normalize_key(requested)
    ]
    if len(exact) == 1:
        return exact[0]
    requested_keys = admin_name_match_keys(requested)
    matches = [
        candidate
        for candidate in candidates
        if requested_keys & admin_name_match_keys(candidate)
    ]
    if len(matches) != 1:
        raise ValueError(
            f"Could not uniquely resolve {label} {requested!r}."
        )
    return matches[0]


def resolve_registration_scope(scope: "AdminScope") -> "AdminScope":
    """Resolve a tehsil scope to the exact names stored in Django."""

    if not (scope.state_name and scope.district_name and scope.tehsil_name):
        raise ValueError(
            "Layer registration requires state, district, and tehsil names."
        )

    from geoadmin.models import DistrictSOI, StateSOI, TehsilSOI

    def unique_match(queryset, field: str, requested: str, label: str):
        exact = list(queryset.filter(**{f"{field}__iexact": requested})[:2])
        if len(exact) == 1:
            return exact[0]
        matches = [
            item
            for item in queryset
            if admin_name_match_keys(getattr(item, field))
            & admin_name_match_keys(requested)
        ]
        if len(matches) != 1:
            raise ValueError(
                f"Could not uniquely resolve {label} {requested!r} for layer registration."
            )
        return matches[0]

    state = unique_match(
        StateSOI.objects.all(),
        "state_name",
        scope.state_name,
        "state",
    )
    district = unique_match(
        DistrictSOI.objects.filter(state=state),
        "district_name",
        scope.district_name,
        "district",
    )
    tehsil = unique_match(
        TehsilSOI.objects.filter(district=district),
        "tehsil_name",
        scope.tehsil_name,
        "tehsil",
    )
    return AdminScope(
        level=scope.level,
        state_name=state.state_name,
        district_name=district.district_name,
        tehsil_name=tehsil.tehsil_name,
        village_ids=scope.village_ids,
    )


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
        if (
            column in frame.columns
            and column not in ordered
            and not is_internal_admin_column(column)
        ):
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
    """Administrative selection requested by an API or CLI request."""

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
            state_name=normalize_scope_name(data.get("state_name") or data.get("state")),
            district_name=normalize_scope_name(
                data.get("district_name") or data.get("district")
            ),
            tehsil_name=normalize_scope_name(
                data.get("tehsil_name") or data.get("block_name") or data.get("block")
            ),
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
        )

    def ensure_indexes(self) -> list[str]:
        """Create missing admin lookup indexes once."""

        return ensure_indexes(self.path, self.required_indexes)

    def _resolve_scope_aliases(self, scope: AdminScope) -> AdminScope:
        """Resolve conservative punctuation/spacing aliases in the GPKG."""

        level = scope.level.lower()
        if level == "village":
            return scope
        with sqlite3.connect(self.path) as connection:
            table = quote_identifier(self.table_name)
            state_names = [
                row[0]
                for row in connection.execute(
                    f"SELECT DISTINCT {quote_identifier('state_name')} "
                    f"FROM {table}"
                )
                if row[0]
            ]
            state = _unique_admin_name_match(
                state_names, scope.state_name or "", "state"
            )
            district = scope.district_name
            tehsil = scope.tehsil_name
            if level in {"district", "tehsil", "block"}:
                district_names = [
                    row[0]
                    for row in connection.execute(
                        f"SELECT DISTINCT {quote_identifier('district_name')} "
                        f"FROM {table} WHERE {lower_key_expression('state_name')} = ?",
                        (normalize_key(state),),
                    )
                    if row[0]
                ]
                district = _unique_admin_name_match(
                    district_names, district or "", "district"
                )
            if level in {"tehsil", "block"}:
                tehsil_names = [
                    row[0]
                    for row in connection.execute(
                        f"SELECT DISTINCT {quote_identifier('TEHSIL')} "
                        f"FROM {table} "
                        f"WHERE {lower_key_expression('state_name')} = ? "
                        f"AND {lower_key_expression('district_name')} = ?",
                        (normalize_key(state), normalize_key(district)),
                    )
                    if row[0]
                ]
                tehsil = _unique_admin_name_match(
                    tehsil_names, tehsil or "", "tehsil"
                )
        return AdminScope(
            level=scope.level,
            state_name=state,
            district_name=district,
            tehsil_name=tehsil,
            village_ids=scope.village_ids,
        )

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
                f"{quote_identifier('pc11_village_id')} IN ({placeholders})"
                ")"
            )
            values = list(scope.village_ids)
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
            resolved_scope = self._resolve_scope_aliases(scope)
            resolved_where, resolved_params = self._where_for_scope(
                resolved_scope
            )
            if include_geometry:
                rows = read_features(
                    self.path,
                    self.table_name,
                    columns=read_columns,
                    where=resolved_where,
                    params=resolved_params,
                    geometry_column_name=ADMIN_GEOMETRY_COLUMN,
                )
            else:
                rows = read_table(
                    self.path,
                    self.table_name,
                    columns=read_columns,
                    where=resolved_where,
                    params=resolved_params,
                )
            if rows.empty:
                raise ValueError(f"No admin rows found for scope: {scope}")
            return AdminSelection(
                resolved_scope,
                rows,
                created_indexes,
                resolved_where,
                resolved_params,
            )
        return AdminSelection(scope, rows, created_indexes, where, params)

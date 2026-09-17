"""SQLite-backed GeoPackage helpers for local pipeline reads.

GeoPackages are SQLite databases. For the local runtime pipelines, using SQLite
directly keeps scope resolution and attribute filtering explicit and fast while
still allowing GeoPandas/Fiona to handle output writing.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

import pandas as pd


def quote_identifier(name: str) -> str:
    """Return a SQLite identifier quoted with double quotes."""

    return '"' + str(name).replace('"', '""') + '"'


def connect_gpkg(path: str | Path, *, read_only: bool = False) -> sqlite3.Connection:
    """Open a GeoPackage SQLite connection with practical runtime pragmas."""

    path = Path(path)
    if read_only:
        connection = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
    else:
        connection = sqlite3.connect(path)
    connection.execute("PRAGMA temp_store = MEMORY")
    return connection


@dataclass(frozen=True)
class GPKGLayer:
    """Minimal metadata for a GeoPackage content table."""

    table_name: str
    data_type: str
    identifier: str | None = None


@dataclass(frozen=True)
class IndexSpec:
    """Description of a SQLite index required by a pipeline."""

    name: str
    table: str
    expressions: tuple[str, ...]
    unique: bool = False

    def create_sql(self) -> str:
        unique = "UNIQUE " if self.unique else ""
        expressions = ", ".join(self.expressions)
        return (
            f"CREATE {unique}INDEX IF NOT EXISTS {quote_identifier(self.name)} "
            f"ON {quote_identifier(self.table)} ({expressions})"
        )


def column_expression(column: str) -> str:
    """Return a quoted column expression for an index or SELECT list."""

    return quote_identifier(column)


def lower_key_expression(column: str) -> str:
    """Return a normalized lowercase string expression usable in an index."""

    return f"lower(trim({quote_identifier(column)}))"


def table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
    """Return whether a table or view exists in a SQLite database."""

    return (
        connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type IN ('table', 'view') AND name = ?",
            (table_name,),
        ).fetchone()
        is not None
    )


def index_exists(connection: sqlite3.Connection, index_name: str) -> bool:
    """Return whether an index exists in a SQLite database."""

    return (
        connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = ?",
            (index_name,),
        ).fetchone()
        is not None
    )


def gpkg_layers(connection: sqlite3.Connection) -> list[GPKGLayer]:
    """Read GeoPackage content entries."""

    if not table_exists(connection, "gpkg_contents"):
        return []
    rows = connection.execute(
        "SELECT table_name, data_type, identifier FROM gpkg_contents ORDER BY table_name"
    ).fetchall()
    return [GPKGLayer(str(row[0]), str(row[1]), row[2]) for row in rows]


def feature_layers(connection: sqlite3.Connection) -> list[str]:
    """Return feature layer table names in a GeoPackage."""

    return [layer.table_name for layer in gpkg_layers(connection) if layer.data_type == "features"]


def table_columns(connection: sqlite3.Connection, table_name: str) -> list[str]:
    """Return column names for a table."""

    return [str(row[1]) for row in connection.execute(f"PRAGMA table_info({quote_identifier(table_name)})")]


def geometry_column(connection: sqlite3.Connection, table_name: str) -> str | None:
    """Return the geometry column for a GeoPackage feature table."""

    if not table_exists(connection, "gpkg_geometry_columns"):
        return None
    row = connection.execute(
        "SELECT column_name FROM gpkg_geometry_columns WHERE table_name = ?",
        (table_name,),
    ).fetchone()
    return str(row[0]) if row else None


def layer_crs(connection: sqlite3.Connection, table_name: str) -> str | None:
    """Return an EPSG CRS string for a GeoPackage feature table when known."""

    if not table_exists(connection, "gpkg_geometry_columns"):
        return None
    row = connection.execute(
        "SELECT srs_id FROM gpkg_geometry_columns WHERE table_name = ?",
        (table_name,),
    ).fetchone()
    if not row or row[0] is None:
        return None
    try:
        srs_id = int(row[0])
    except (TypeError, ValueError):
        return None
    return f"EPSG:{srs_id}" if srs_id > 0 else None


def ensure_indexes(path: str | Path, specs: Sequence[IndexSpec]) -> list[str]:
    """Create missing indexes and return the names that were created."""

    created: list[str] = []
    with connect_gpkg(path) as connection:
        for spec in specs:
            if not table_exists(connection, spec.table):
                raise RuntimeError(f"Cannot create {spec.name}: missing table {spec.table}")
            if index_exists(connection, spec.name):
                continue
            connection.execute(spec.create_sql())
            created.append(spec.name)
        connection.commit()
    return created


def gpkg_geometry_to_shape(blob: bytes | memoryview | None):
    """Decode a GeoPackage geometry blob into a Shapely geometry."""

    if blob is None:
        return None
    from shapely import wkb

    data = bytes(blob)
    if data[:2] != b"GP":
        return wkb.loads(data)
    flags = data[3]
    if flags & 0b00010000:
        return None
    envelope_code = (flags >> 1) & 0b00000111
    envelope_bytes = {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}.get(envelope_code)
    if envelope_bytes is None:
        raise ValueError(f"Unsupported GeoPackage geometry envelope code: {envelope_code}")
    return wkb.loads(data[8 + envelope_bytes :])


def placeholders(values: Iterable[Any]) -> str:
    """Return SQLite placeholders for a non-empty iterable."""

    values = list(values)
    if not values:
        raise ValueError("At least one value is required for SQL placeholders")
    return ", ".join(["?"] * len(values))


def read_table(
    path: str | Path,
    table_name: str,
    *,
    columns: Sequence[str] | None = None,
    where: str | None = None,
    params: Sequence[Any] = (),
) -> pd.DataFrame:
    """Read an attribute table through SQLite into a pandas DataFrame."""

    select_columns = "*" if columns is None else ", ".join(quote_identifier(col) for col in columns)
    sql = f"SELECT {select_columns} FROM {quote_identifier(table_name)}"
    if where:
        sql += f" WHERE {where}"
    with connect_gpkg(path, read_only=True) as connection:
        return pd.read_sql_query(sql, connection, params=tuple(params))


def read_features(
    path: str | Path,
    table_name: str,
    *,
    columns: Sequence[str] | None = None,
    where: str | None = None,
    params: Sequence[Any] = (),
    geometry_column_name: str | None = None,
) :
    """Read filtered GeoPackage features without scanning through GDAL.

    The function decodes GeoPackage geometry blobs directly after SQLite has
    applied the indexed filter.
    """

    import geopandas as gpd

    with connect_gpkg(path, read_only=True) as connection:
        geom_col = geometry_column_name or geometry_column(connection, table_name)
        crs = layer_crs(connection, table_name)
        if not geom_col:
            raise RuntimeError(f"{table_name} is not registered as a GeoPackage feature table")
        read_columns = list(columns or table_columns(connection, table_name))
        if geom_col not in read_columns:
            read_columns.append(geom_col)
        select_columns = ", ".join(quote_identifier(col) for col in read_columns)
        sql = f"SELECT {select_columns} FROM {quote_identifier(table_name)}"
        if where:
            sql += f" WHERE {where}"
        frame = pd.read_sql_query(sql, connection, params=tuple(params))

    if frame.empty:
        return gpd.GeoDataFrame(frame.drop(columns=[geom_col], errors="ignore"), geometry=[], crs=crs)
    geometry = frame.pop(geom_col).map(gpkg_geometry_to_shape)
    return gpd.GeoDataFrame(frame, geometry=geometry, crs=crs)

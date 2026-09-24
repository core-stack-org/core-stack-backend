"""CSV and SQLite-sidecar helpers for keyed local datasets."""

from __future__ import annotations

import csv
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

import pandas as pd

from .gpkg import IndexSpec, column_expression, ensure_indexes, quote_identifier


def csv_header(path: str | Path) -> list[str]:
    """Read only the header row from a CSV file."""

    with Path(path).open(newline="") as handle:
        return next(csv.reader(handle))


def default_sidecar_path(csv_path: str | Path) -> Path:
    """Return the default SQLite sidecar path for a CSV source."""

    csv_path = Path(csv_path)
    return csv_path.with_suffix(csv_path.suffix + ".sqlite")


def _source_signature(path: Path) -> tuple[int, int]:
    stat = path.stat()
    return stat.st_size, stat.st_mtime_ns


@dataclass
class CSVSQLiteSidecar:
    """Materialize a large keyed CSV into a local SQLite sidecar table."""

    csv_path: str | Path
    table_name: str
    key_columns: tuple[str, ...]
    sidecar_path: str | Path | None = None
    source_columns: tuple[str, ...] | None = None
    chunksize: int = 50_000

    def __post_init__(self) -> None:
        self.csv_path = Path(self.csv_path)
        self.sidecar_path = Path(self.sidecar_path or default_sidecar_path(self.csv_path))

    def is_fresh(self) -> bool:
        """Return whether the sidecar exists and matches the CSV size/mtime."""

        if not self.sidecar_path.exists():
            return False
        source_size, source_mtime_ns = _source_signature(self.csv_path)
        with sqlite3.connect(self.sidecar_path) as connection:
            if not self._metadata_table_exists(connection):
                return False
            try:
                row = connection.execute(
                    """
                    SELECT source_size, source_mtime_ns, materialized_columns
                    FROM local_pipeline_sidecar_metadata
                    WHERE source_path = ?
                    """,
                    (self.csv_path.as_posix(),),
                ).fetchone()
            except sqlite3.OperationalError:
                return False
        expected_columns = tuple(dict.fromkeys([*(self.source_columns or ()), *self.key_columns]))
        columns_match = True
        if expected_columns:
            materialized_columns = tuple((row[2] or "").split(",")) if row and row[2] else ()
            columns_match = set(expected_columns).issubset(materialized_columns)
        return bool(row and int(row[0]) == source_size and int(row[1]) == source_mtime_ns and columns_match)

    def materialize(self, *, force: bool = False) -> dict[str, Any]:
        """Create or refresh the SQLite sidecar table from CSV chunks."""

        if not force and self.is_fresh():
            return {"sidecar_path": self.sidecar_path.as_posix(), "refreshed": False}

        self.sidecar_path.parent.mkdir(parents=True, exist_ok=True)
        if self.sidecar_path.exists():
            self.sidecar_path.unlink()

        rows = 0
        first_chunk = True
        with sqlite3.connect(self.sidecar_path) as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA temp_store = MEMORY")
            read_columns = list(dict.fromkeys([*(self.source_columns or ()), *self.key_columns]))
            for chunk in pd.read_csv(
                self.csv_path,
                chunksize=self.chunksize,
                low_memory=False,
                usecols=read_columns or None,
            ):
                if first_chunk:
                    missing = [column for column in self.key_columns if column not in chunk.columns]
                    if missing:
                        raise ValueError(f"CSV is missing key column(s): {missing}")
                chunk.to_sql(self.table_name, connection, if_exists="replace" if first_chunk else "append", index=False)
                rows += len(chunk)
                first_chunk = False

            self._write_metadata(connection, rows, tuple(chunk.columns) if rows else tuple(read_columns))
            connection.commit()

        self.ensure_key_indexes()
        return {"sidecar_path": self.sidecar_path.as_posix(), "refreshed": True, "rows": rows}

    def ensure_key_indexes(self) -> list[str]:
        """Create exact key indexes for configured key columns."""

        specs = tuple(
            IndexSpec(
                name=f"idx_{self.table_name}_{column}_lookup",
                table=self.table_name,
                expressions=(column_expression(column),),
            )
            for column in self.key_columns
        )
        return ensure_indexes(self.sidecar_path, specs)

    def fetch_by_values(
        self,
        key_column: str,
        values: Iterable[Any],
        *,
        columns: Sequence[str] | None = None,
    ) -> pd.DataFrame:
        """Fetch sidecar rows by a configured key column."""

        values = [value for value in values if value is not None and value == value]
        if not values:
            empty_columns = (
                columns
                if columns is not None
                else (*self.key_columns, *(self.source_columns or ()))
            )
            return pd.DataFrame(columns=list(dict.fromkeys(empty_columns)))
        if not self.is_fresh():
            self.materialize()
        select_columns = "*" if columns is None else ", ".join(quote_identifier(col) for col in columns)
        with sqlite3.connect(self.sidecar_path) as connection:
            connection.execute("PRAGMA temp_store = MEMORY")
            connection.execute("DROP TABLE IF EXISTS temp.local_pipeline_requested_values")
            connection.execute("CREATE TEMP TABLE local_pipeline_requested_values (value PRIMARY KEY)")
            connection.executemany(
                "INSERT OR IGNORE INTO local_pipeline_requested_values (value) VALUES (?)",
                [(value,) for value in values],
            )
            return pd.read_sql_query(
                f"""
                SELECT {select_columns}
                FROM {quote_identifier(self.table_name)}
                WHERE {quote_identifier(key_column)} IN (
                    SELECT value FROM local_pipeline_requested_values
                )
                """,
                connection,
            )

    def _metadata_table_exists(self, connection: sqlite3.Connection) -> bool:
        return (
            connection.execute(
                """
                SELECT 1 FROM sqlite_master
                WHERE type = 'table' AND name = 'local_pipeline_sidecar_metadata'
                """
            ).fetchone()
            is not None
        )

    def _write_metadata(self, connection: sqlite3.Connection, rows: int, materialized_columns: tuple[str, ...]) -> None:
        source_size, source_mtime_ns = _source_signature(self.csv_path)
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS local_pipeline_sidecar_metadata (
                source_path TEXT PRIMARY KEY,
                source_size INTEGER NOT NULL,
                source_mtime_ns INTEGER NOT NULL,
                table_name TEXT NOT NULL,
                row_count INTEGER NOT NULL,
                materialized_columns TEXT,
                created_at_utc TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            INSERT OR REPLACE INTO local_pipeline_sidecar_metadata
            (source_path, source_size, source_mtime_ns, table_name, row_count, materialized_columns, created_at_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                self.csv_path.as_posix(),
                source_size,
                source_mtime_ns,
                self.table_name,
                int(rows),
                ",".join(materialized_columns),
                datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            ),
        )

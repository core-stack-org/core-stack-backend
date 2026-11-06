import io
import re
import uuid
from pathlib import Path

from django.db import connection, transaction

ALL_SECTIONS = "BCDEFG"
# Leading identity columns (org ... Record ID) that are always exported.
_IDENTITY_COLUMNS = 19
_VIEWS_SQL = Path(__file__).resolve().parent / "utils" / "dpr_dump_views.sql"

_ORDER_BY = '"Plan ID", "Section", "DPR Table", "Record ID"'


def parse_sections(raw):
    """Normalise the `sections` param (e.g. "efg") -> "EFG"; ValueError if invalid."""
    sections = (raw or ALL_SECTIONS).strip().upper()
    if not re.fullmatch(r"[B-G]+", sections):
        raise ValueError("'sections' must only contain the letters B, C, D, E, F, G")
    # drop duplicates, keep the canonical order
    return "".join(s for s in ALL_SECTIONS if s in sections)


def export_plans_csv(org_id, sections=ALL_SECTIONS):
    """
    Build the wide DPR CSV for one organization and return it as bytes.

    Uses the same SQL as dpr/utils/plans.sql (shared views file). Temp views and
    functions are per connection, so everything runs in one transaction.
    """
    org_id = str(uuid.UUID(str(org_id)))
    sections = parse_sections(sections)
    views_sql = _VIEWS_SQL.read_text(encoding="utf-8")

    buf = io.BytesIO()
    with transaction.atomic(), connection.cursor() as cur:
        cur.execute("SELECT set_config('dpr.org_id', %s, true)", [org_id])
        cur.execute(views_sql)  # no params: '%' in the SQL stays untouched

        # Same pruning as the psql script: with a subset of sections, drop the
        # data columns that stay empty for those sections.
        cur.execute(
            """
            SELECT string_agg(quote_ident(a.attname), ', ' ORDER BY a.attnum)
            FROM pg_attribute a
            WHERE a.attrelid = 'pradan_dpr_dump'::regclass
              AND a.attnum > 0 AND NOT a.attisdropped
              AND (a.attnum <= %s
                   OR %s = %s
                   OR EXISTS (SELECT 1
                                FROM pradan_dpr_dump r, jsonb_each_text(to_jsonb(r)) e
                               WHERE position(left(r."Section", 1) in %s) > 0
                                 AND e.key = a.attname AND e.value IS NOT NULL))
            """,
            [_IDENTITY_COLUMNS, sections, ALL_SECTIONS, sections],
        )
        columns = cur.fetchone()[0]

        cur.execute("DROP VIEW IF EXISTS pg_temp.pradan_dpr_out")
        cur.execute(
            f"""
            CREATE TEMP VIEW pradan_dpr_out AS
            SELECT {columns} FROM pradan_dpr_dump
            WHERE position(left("Section", 1) in %s) > 0
            """,
            [sections],
        )

        # raw psycopg2 cursor: server-side COPY ... TO file would need superuser
        cur.cursor.copy_expert(
            f"COPY (SELECT * FROM pradan_dpr_out ORDER BY {_ORDER_BY}) "
            "TO STDOUT WITH (FORMAT csv, HEADER true, ENCODING 'UTF8')",
            buf,
        )
    return buf.getvalue()

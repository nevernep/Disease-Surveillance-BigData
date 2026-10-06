"""Load the Spark star-schema exports into the PostgreSQL Data Warehouse.

Reads data/processed/warehouse/*.csv (written by spark.main on every run),
then in ONE transaction: apply sql/ddl.sql (drop + recreate), COPY every table,
verify row counts, recreate sql/analytics_views.sql and write mart.load_audit.
Any failure rolls back, so BI tools never see a half-loaded warehouse.

Does not import pyspark, so it can run as a plain Airflow task.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
from pathlib import Path
from typing import Dict, List

# Dimensions first so fact foreign keys resolve.
LOAD_ORDER = [
    "dim_date",
    "dim_district",
    "dim_disease",
    "dim_age_group",
    "dim_sex",
    "fact_disease_cases",
    "fact_population",
]
SCHEMA = "mart"


def connection_settings() -> Dict[str, str]:
    """Defaults target the host-mapped port; Compose overrides for containers."""

    return {
        "host": os.getenv("WAREHOUSE_DB_HOST", "localhost"),
        "port": os.getenv("WAREHOUSE_DB_PORT", "5433"),
        "dbname": os.getenv("WAREHOUSE_DB_NAME", "surveillance_dw"),
        "user": os.getenv("WAREHOUSE_DB_USER", "dw_user"),
        "password": os.getenv("WAREHOUSE_DB_PASSWORD", "warehouse-local-only"),
    }


def read_csv_header(path: Path) -> List[str]:
    with path.open("r", encoding="utf-8", newline="") as source:
        header = next(csv.reader(source), None)
    if not header:
        raise ValueError(f"{path}: empty CSV, expected a header row")
    return [column.replace("﻿", "").strip() for column in header]


def count_csv_rows(path: Path) -> int:
    with path.open("r", encoding="utf-8", newline="") as source:
        return max(sum(1 for _ in csv.reader(source)) - 1, 0)


def build_copy_sql(table: str, columns: List[str]) -> str:
    """COPY statement with an explicit column list taken from the CSV header."""

    for column in columns:
        if not column.replace("_", "").isalnum():
            raise ValueError(f"Unsafe column name in {table}: {column!r}")
    column_list = ", ".join(f'"{column}"' for column in columns)
    return (
        f"COPY {SCHEMA}.{table} ({column_list}) "
        "FROM STDIN WITH (FORMAT csv, HEADER true, ENCODING 'UTF8')"
    )


def load_warehouse(project_root: Path) -> Dict[str, int]:
    import psycopg2

    warehouse_dir = project_root / "data" / "processed" / "warehouse"
    ddl_sql = (project_root / "sql" / "ddl.sql").read_text(encoding="utf-8")
    views_sql = (project_root / "sql" / "analytics_views.sql").read_text(
        encoding="utf-8"
    )

    sources = {table: warehouse_dir / f"{table}.csv" for table in LOAD_ORDER}
    missing = [str(path) for path in sources.values() if not path.is_file()]
    if missing:
        raise FileNotFoundError(
            "Warehouse exports not found (run spark.main first): "
            + ", ".join(missing)
        )

    row_counts: Dict[str, int] = {}
    connection = psycopg2.connect(**connection_settings())
    try:
        with connection:  # commit on success, rollback on any exception
            with connection.cursor() as cursor:
                cursor.execute(ddl_sql)

                for table in LOAD_ORDER:
                    path = sources[table]
                    copy_sql = build_copy_sql(table, read_csv_header(path))
                    with path.open("r", encoding="utf-8") as source:
                        cursor.copy_expert(copy_sql, source)

                    cursor.execute(f"SELECT count(*) FROM {SCHEMA}.{table}")
                    loaded = cursor.fetchone()[0]
                    expected = count_csv_rows(path)
                    if loaded != expected:
                        raise RuntimeError(
                            f"{table}: loaded {loaded} rows, CSV has {expected}"
                        )
                    row_counts[table] = loaded

                cursor.execute(views_sql)
                cursor.execute(
                    f"INSERT INTO {SCHEMA}.load_audit (source, row_counts) "
                    "VALUES (%s, %s)",
                    (str(warehouse_dir), json.dumps(row_counts)),
                )
    finally:
        connection.close()

    return row_counts


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Load the star schema CSV exports into PostgreSQL"
    )
    parser.add_argument("--project-root", type=Path, default=Path("."))
    args = parser.parse_args()

    row_counts = load_warehouse(args.project_root.resolve())
    for table, rows in row_counts.items():
        print(f"Loaded {rows:>6} rows into {SCHEMA}.{table}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Warehouse: incremental, idempotent loads from silver into DuckDB, then SQL models.

Schemas
* staging - cleaned source tables, one row per city per hour (upserted)
* marts   - star schema + business marts, rebuilt by the SQL models in ``sql/``
* ops     - pipeline run log and data-quality results (observability)
"""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb

from hazewatch.config import City
from hazewatch.extract import SOURCES, run_partition
from hazewatch.silver import _sql_path, silver_columns

log = logging.getLogger(__name__)

STAGING_TABLES = {"air_quality": "staging.air_quality_hourly", "weather": "staging.weather_hourly"}

DDL = """
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS staging.cities (
    city_id          VARCHAR PRIMARY KEY,
    city_name        VARCHAR NOT NULL,
    country          VARCHAR NOT NULL,
    latitude         DOUBLE  NOT NULL,
    longitude        DOUBLE  NOT NULL,
    utc_offset_hours INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
    run_id        VARCHAR   NOT NULL,
    step          VARCHAR   NOT NULL,
    status        VARCHAR   NOT NULL,
    rows_affected BIGINT,
    started_at    TIMESTAMP NOT NULL,
    finished_at   TIMESTAMP NOT NULL,
    message       VARCHAR
);

CREATE TABLE IF NOT EXISTS ops.dq_results (
    run_id       VARCHAR   NOT NULL,
    stage        VARCHAR   NOT NULL,
    check_name   VARCHAR   NOT NULL,
    severity     VARCHAR   NOT NULL,
    failing_rows BIGINT    NOT NULL,
    passed       BOOLEAN   NOT NULL,
    checked_at   TIMESTAMP NOT NULL
);
"""


def connect(warehouse_path: Path, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    warehouse_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(warehouse_path), read_only=read_only)


def init_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(DDL)
    for source, table in STAGING_TABLES.items():
        cols = silver_columns(source)
        not_null = {"city_id", "observed_at", "ingested_at", "is_forecast", "run_id"}
        col_sql = ",\n    ".join(f"{n} {t}{' NOT NULL' if n in not_null else ''}" for n, t in cols)
        con.execute(
            f"CREATE TABLE IF NOT EXISTS {table} (\n    {col_sql},\n    PRIMARY KEY (city_id, observed_at)\n)"
        )


def seed_cities(con: duckdb.DuckDBPyConnection, cities: list[City]) -> None:
    con.executemany(
        """
        INSERT INTO staging.cities VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT (city_id) DO UPDATE SET
            city_name = EXCLUDED.city_name, country = EXCLUDED.country,
            latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude,
            utc_offset_hours = EXCLUDED.utc_offset_hours
        """,
        [(c.id, c.name, c.country, c.latitude, c.longitude, c.utc_offset_hours) for c in cities],
    )


def upsert_from_silver(con: duckdb.DuckDBPyConnection, source: str, parquet_path: Path) -> int:
    """Merge one silver file into its staging table.

    * New (city, hour) keys are inserted.
    * Existing keys are updated only when the incoming row is at least as fresh, so
      replaying an old run (backfill) can never overwrite newer data, and a forecast
      row is replaced by the observation once a later run has it.
    """
    table = STAGING_TABLES[source]
    cols = [name for name, _ in silver_columns(source)]
    updatable = [c for c in cols if c not in ("city_id", "observed_at")]
    set_sql = ", ".join(f"{c} = EXCLUDED.{c}" for c in updatable)
    before = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    changed = con.execute(
        f"""
        INSERT INTO {table} ({', '.join(cols)})
        SELECT {', '.join(cols)} FROM read_parquet('{_sql_path(parquet_path)}')
        ON CONFLICT (city_id, observed_at) DO UPDATE SET {set_sql}
        WHERE EXCLUDED.ingested_at >= {table}.ingested_at
        """
    ).fetchone()[0]
    after = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    log.info("%s: %d rows inserted or updated (%d new keys)", table, changed, after - before)
    return changed


def load_run(con: duckdb.DuckDBPyConnection, run_id: str, lake_dir: Path, cities: list[City]) -> int:
    init_schema(con)
    seed_cities(con, cities)
    total = 0
    con.execute("BEGIN TRANSACTION")
    try:
        for source in SOURCES:
            path = run_partition(lake_dir / "silver", source, run_id) / "part-0.parquet"
            if not path.exists():
                raise FileNotFoundError(f"Silver file missing: {path}")
            total += upsert_from_silver(con, source, path)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    return total


def run_models(con: duckdb.DuckDBPyConnection, sql_dir: Path) -> list[str]:
    """Run the SQL models in filename order (01_..., 02_...), all-or-nothing."""
    models = sorted(sql_dir.glob("*.sql"))
    con.execute("BEGIN TRANSACTION")
    try:
        for model in models:
            log.info("Running model %s", model.name)
            con.execute(model.read_text(encoding="utf-8"))
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    return [m.stem for m in models]

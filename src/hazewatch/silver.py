"""Bronze -> silver: validate and flatten nested JSON into typed, columnar Parquet.

The API returns column-oriented arrays (``hourly.time[]``, ``hourly.pm2_5[]`` ...).
Silver turns them into one row per city per hour, enforces a schema contract, and
flags rows that are forecasts rather than observations at ingestion time.

Layout: lake/silver/<source>/ingest_date=YYYY-MM-DD/run_id=<run_id>/part-0.parquet
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from hazewatch.extract import SOURCES, run_partition

log = logging.getLogger(__name__)


class SchemaError(ValueError):
    """Raised when a raw payload breaks the expected schema contract."""


def silver_columns(source: str) -> list[tuple[str, str]]:
    return [
        ("city_id", "VARCHAR"),
        ("observed_at", "TIMESTAMP"),
        *[(field, "DOUBLE") for field in SOURCES[source]["hourly"]],
        ("ingested_at", "TIMESTAMP"),
        ("is_forecast", "BOOLEAN"),
        ("run_id", "VARCHAR"),
    ]


def _to_naive_utc(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is not None:
        dt = dt.astimezone(UTC).replace(tzinfo=None)
    return dt


def flatten(record: dict) -> list[tuple]:
    """Turn one bronze record into silver rows, enforcing the schema contract."""
    meta = record["_meta"]
    source = meta["source"]
    fields = SOURCES[source]["hourly"]
    hourly = record.get("payload", {}).get("hourly")
    if not isinstance(hourly, dict):
        raise SchemaError(f"{source}/{meta['city_id']}: payload has no 'hourly' object")

    missing = [f for f in ["time", *fields] if f not in hourly]
    if missing:
        raise SchemaError(f"{source}/{meta['city_id']}: missing fields {missing}")

    n = len(hourly["time"])
    ragged = {f: len(hourly[f]) for f in fields if len(hourly[f]) != n}
    if ragged:
        raise SchemaError(f"{source}/{meta['city_id']}: arrays differ in length from time ({n}): {ragged}")

    ingested_at = _to_naive_utc(meta["ingested_at"])
    rows = []
    for i, ts in enumerate(hourly["time"]):
        observed_at = _to_naive_utc(ts)  # API is called with timezone=GMT
        values = [None if hourly[f][i] is None else float(hourly[f][i]) for f in fields]
        rows.append(
            (meta["city_id"], observed_at, *values, ingested_at, observed_at > ingested_at, meta["run_id"])
        )
    return rows


def _sql_path(path: Path) -> str:
    return path.as_posix().replace("'", "''")


def bronze_to_silver(run_id: str, lake_dir: Path) -> dict[str, Path]:
    """Convert every bronze file of a run into one Parquet file per source."""
    outputs: dict[str, Path] = {}
    for source in SOURCES:
        in_dir = run_partition(lake_dir / "bronze", source, run_id)
        files = sorted(in_dir.glob("*.json"))
        if not files:
            raise FileNotFoundError(f"No bronze files for {source} in {in_dir}")

        rows: list[tuple] = []
        for path in files:
            rows.extend(flatten(json.loads(path.read_text(encoding="utf-8"))))

        out_dir = run_partition(lake_dir / "silver", source, run_id)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "part-0.parquet"

        cols = silver_columns(source)
        with duckdb.connect() as con:
            con.execute(f"CREATE TABLE t ({', '.join(f'{n} {t}' for n, t in cols)})")
            con.executemany(f"INSERT INTO t VALUES ({', '.join('?' for _ in cols)})", rows)
            con.execute(f"COPY t TO '{_sql_path(out_path)}' (FORMAT parquet, COMPRESSION zstd)")
        log.info("Wrote %d %s rows to %s", len(rows), source, out_path)
        outputs[source] = out_path
    return outputs

"""Data-quality checks that gate the pipeline.

Each check is a SQL query returning the number of failing rows (0 = pass).
* ``error`` checks stop the pipeline, so bad data never reaches the marts/dashboard.
* ``warn`` checks are recorded but let the run continue.
Every result is written to ``ops.dq_results`` for monitoring over time.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime

import duckdb

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Check:
    name: str
    stage: str  # "staging" (before transforms) or "marts" (after transforms)
    severity: str  # "error" or "warn"
    sql: str


@dataclass(frozen=True)
class CheckResult:
    check: Check
    failing_rows: int

    @property
    def passed(self) -> bool:
        return self.failing_rows == 0


class DataQualityError(RuntimeError):
    def __init__(self, failures: list[CheckResult]):
        self.failures = failures
        names = ", ".join(f"{r.check.name} ({r.failing_rows} rows)" for r in failures)
        super().__init__(f"Data-quality checks failed: {names}")


CHECKS: list[Check] = [
    # --- staging: validate source data before it is modelled -----------------------
    Check("air_quality_not_empty", "staging", "error",
          "SELECT CASE WHEN count(*) = 0 THEN 1 ELSE 0 END FROM staging.air_quality_hourly"),
    Check("weather_not_empty", "staging", "error",
          "SELECT CASE WHEN count(*) = 0 THEN 1 ELSE 0 END FROM staging.weather_hourly"),
    Check("every_city_has_air_quality", "staging", "error",
          """SELECT count(*) FROM staging.cities c
             WHERE NOT EXISTS (SELECT 1 FROM staging.air_quality_hourly a WHERE a.city_id = c.city_id)"""),
    Check("pm2_5_within_physical_range", "staging", "error",
          "SELECT count(*) FROM staging.air_quality_hourly WHERE pm2_5 < 0 OR pm2_5 > 1000"),
    Check("pm10_not_below_zero", "staging", "error",
          "SELECT count(*) FROM staging.air_quality_hourly WHERE pm10 < 0"),
    Check("humidity_between_0_and_100", "staging", "error",
          "SELECT count(*) FROM staging.weather_hourly WHERE relative_humidity_2m NOT BETWEEN 0 AND 100"),
    Check("us_aqi_within_scale", "staging", "warn",
          "SELECT count(*) FROM staging.air_quality_hourly WHERE us_aqi < 0 OR us_aqi > 500"),
    Check("pm2_5_null_rate_under_10pct", "staging", "warn",
          """SELECT CASE WHEN count(*) FILTER (WHERE pm2_5 IS NULL) > 0.10 * count(*)
                         THEN count(*) FILTER (WHERE pm2_5 IS NULL) ELSE 0 END
             FROM staging.air_quality_hourly WHERE NOT is_forecast"""),
    Check("air_quality_fresh_within_6h", "staging", "warn",
          """SELECT CASE WHEN max(observed_at) IS NULL OR max(observed_at) < $now - INTERVAL 6 HOUR
                         THEN 1 ELSE 0 END
             FROM staging.air_quality_hourly WHERE NOT is_forecast"""),
    # --- marts: validate the modelled output ----------------------------------------
    Check("fct_unique_city_hour", "marts", "error",
          """SELECT count(*) FROM (
                 SELECT city_id, observed_at_utc FROM marts.fct_air_quality_hourly
                 GROUP BY city_id, observed_at_utc HAVING count(*) > 1)"""),
    Check("fct_has_no_orphan_cities", "marts", "error",
          """SELECT count(*) FROM marts.fct_air_quality_hourly f
             LEFT JOIN marts.dim_city c ON c.city_id = f.city_id WHERE c.city_id IS NULL"""),
    Check("daily_hours_at_most_24", "marts", "error",
          "SELECT count(*) FROM marts.mart_city_daily WHERE hours_observed > 24"),
    Check("fct_weather_join_coverage_95pct", "marts", "warn",
          """SELECT CASE WHEN count(*) FILTER (WHERE temperature_c IS NULL) > 0.05 * count(*)
                         THEN count(*) FILTER (WHERE temperature_c IS NULL) ELSE 0 END
             FROM marts.fct_air_quality_hourly"""),
]


def run_checks(
    con: duckdb.DuckDBPyConnection,
    run_id: str,
    stage: str,
    now_utc: datetime,
    checks: list[Check] | None = None,
) -> list[CheckResult]:
    """Run all checks for a stage, persist the results, and raise on any failed error check."""
    selected = [c for c in (checks or CHECKS) if c.stage == stage]
    results: list[CheckResult] = []
    for check in selected:
        params = {"now": now_utc} if "$now" in check.sql else None
        failing = con.execute(check.sql, params).fetchone()[0] if params else con.execute(check.sql).fetchone()[0]
        result = CheckResult(check, int(failing or 0))
        results.append(result)
        level = logging.INFO if result.passed else (logging.ERROR if check.severity == "error" else logging.WARNING)
        log.log(level, "DQ %-32s %s (%d failing)", check.name, "PASS" if result.passed else "FAIL", result.failing_rows)

    con.executemany(
        "INSERT INTO ops.dq_results VALUES (?, ?, ?, ?, ?, ?, ?)",
        [(run_id, stage, r.check.name, r.check.severity, r.failing_rows, r.passed, now_utc) for r in results],
    )
    blocking = [r for r in results if not r.passed and r.check.severity == "error"]
    if blocking:
        raise DataQualityError(blocking)
    return results

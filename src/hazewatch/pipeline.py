"""Pipeline steps and CLI.

Each step is a plain function keyed by ``run_id`` so it can be called from the CLI,
from tests, or as an individual Airflow task (and retried on its own).

    extract -> bronze_to_silver -> load -> quality(staging) -> transform -> quality(marts)
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Callable
from datetime import UTC, datetime

from hazewatch import extract as ex
from hazewatch import quality, silver, warehouse
from hazewatch.config import Settings, get_settings, load_cities

log = logging.getLogger("hazewatch")


def _now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _record(settings: Settings, run_id: str, step: str, started: datetime, status: str,
            rows: int | None = None, message: str | None = None) -> None:
    """Write one row to ops.pipeline_runs (the pipeline's audit log)."""
    with warehouse.connect(settings.warehouse_path) as con:
        warehouse.init_schema(con)
        con.execute(
            "INSERT INTO ops.pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?)",
            [run_id, step, status, rows, started, _now(), (message or "")[:1000]],
        )


def _tracked(step: str, fn: Callable[[], int | None], settings: Settings, run_id: str) -> int | None:
    started = _now()
    log.info("[%s] %s started", run_id, step)
    try:
        rows = fn()
    except Exception as exc:
        _record(settings, run_id, step, started, "failed", message=f"{type(exc).__name__}: {exc}")
        log.exception("[%s] %s failed", run_id, step)
        raise
    _record(settings, run_id, step, started, "success", rows=rows)
    log.info("[%s] %s finished (%s rows)", run_id, step, rows)
    return rows


def step_extract(run_id: str, settings: Settings, past_days: int = 2,
                 fetcher: ex.Fetcher | None = None) -> int:
    cities = load_cities(settings.cities_file)
    return _tracked(
        "extract",
        lambda: len(ex.extract(cities, run_id, settings.lake_dir, past_days, fetcher or ex.http_fetcher)),
        settings, run_id,
    )


def step_bronze_to_silver(run_id: str, settings: Settings) -> int:
    return _tracked(
        "bronze_to_silver",
        lambda: len(silver.bronze_to_silver(run_id, settings.lake_dir)),
        settings, run_id,
    )


def step_load(run_id: str, settings: Settings) -> int:
    cities = load_cities(settings.cities_file)

    def _load() -> int:
        with warehouse.connect(settings.warehouse_path) as con:
            return warehouse.load_run(con, run_id, settings.lake_dir, cities)

    return _tracked("load", _load, settings, run_id)


def step_quality(run_id: str, settings: Settings, stage: str) -> int:
    def _check() -> int:
        with warehouse.connect(settings.warehouse_path) as con:
            return len(quality.run_checks(con, run_id, stage, _now()))

    return _tracked(f"quality_{stage}", _check, settings, run_id)


def step_transform(run_id: str, settings: Settings) -> int:
    def _transform() -> int:
        with warehouse.connect(settings.warehouse_path) as con:
            return len(warehouse.run_models(con, settings.sql_dir))

    return _tracked("transform", _transform, settings, run_id)


def run_all(settings: Settings, run_id: str | None = None, past_days: int = 2,
            fetcher: ex.Fetcher | None = None) -> str:
    run_id = run_id or ex.new_run_id()
    step_extract(run_id, settings, past_days, fetcher)
    step_bronze_to_silver(run_id, settings)
    step_load(run_id, settings)
    step_quality(run_id, settings, "staging")
    step_transform(run_id, settings)
    step_quality(run_id, settings, "marts")
    log.info("[%s] pipeline finished successfully", run_id)
    return run_id


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="hazewatch", description="Haze Watch data pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="run the full pipeline end to end")
    run.add_argument("--past-days", type=int, default=2, help="days of history to (re)ingest (max 92)")
    run.add_argument("--run-id", help="override the generated run id (YYYYMMDDTHHMMSS)")

    step = sub.add_parser("step", help="run a single step for an existing run id")
    step.add_argument("name", choices=["extract", "silver", "load", "quality-staging", "transform", "quality-marts"])
    step.add_argument("--run-id", required=True)
    step.add_argument("--past-days", type=int, default=2)

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    settings = get_settings()

    try:
        if args.command == "run":
            run_all(settings, args.run_id, args.past_days)
        else:
            {
                "extract": lambda: step_extract(args.run_id, settings, args.past_days),
                "silver": lambda: step_bronze_to_silver(args.run_id, settings),
                "load": lambda: step_load(args.run_id, settings),
                "quality-staging": lambda: step_quality(args.run_id, settings, "staging"),
                "transform": lambda: step_transform(args.run_id, settings),
                "quality-marts": lambda: step_quality(args.run_id, settings, "marts"),
            }[args.name]()
    except quality.DataQualityError as exc:
        log.error("%s", exc)
        return 2
    except Exception:  # noqa: BLE001 - already logged with traceback by _tracked
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Airflow DAG: run the Haze Watch pipeline every hour.

Each pipeline step is its own task, so a failed step (e.g. an API outage during
extract) is retried on its own without redoing the others. All tasks share the
Airflow logical timestamp as the run id, which keeps every lake partition and
warehouse row traceable back to the DAG run that produced it.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task

from hazewatch.config import get_settings
from hazewatch.pipeline import step_bronze_to_silver, step_extract, step_load, step_quality, step_transform

PAST_DAYS = 2  # re-ingest a 2-day window each hour to pick up late corrections

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}


@dag(
    dag_id="haze_watch_hourly",
    description="Open-Meteo air quality + weather -> data lake -> DuckDB warehouse -> marts",
    schedule="15 * * * *",  # quarter past each hour, after the API refreshes
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,  # DuckDB allows one writer at a time
    default_args=default_args,
    tags=["haze-watch", "air-quality"],
)
def haze_watch_hourly():
    @task(execution_timeout=timedelta(minutes=10))
    def extract(ts_nodash=None):
        return step_extract(ts_nodash, get_settings(), past_days=PAST_DAYS)

    @task
    def bronze_to_silver(ts_nodash=None):
        return step_bronze_to_silver(ts_nodash, get_settings())

    @task
    def load_warehouse(ts_nodash=None):
        return step_load(ts_nodash, get_settings())

    @task
    def quality_gate_staging(ts_nodash=None):
        return step_quality(ts_nodash, get_settings(), "staging")

    @task
    def transform(ts_nodash=None):
        return step_transform(ts_nodash, get_settings())

    @task
    def quality_gate_marts(ts_nodash=None):
        return step_quality(ts_nodash, get_settings(), "marts")

    (
        extract()
        >> bronze_to_silver()
        >> load_warehouse()
        >> quality_gate_staging()
        >> transform()
        >> quality_gate_marts()
    )


haze_watch_hourly()

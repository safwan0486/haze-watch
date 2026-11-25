"""Extract: pull hourly air-quality and weather data from Open-Meteo into the bronze layer.

Bronze keeps the API response exactly as received (plus ingestion metadata), so any
downstream bug can be fixed and replayed without calling the API again.

Layout: lake/bronze/<source>/ingest_date=YYYY-MM-DD/run_id=<run_id>/<city_id>.json
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import requests

from hazewatch.config import City

log = logging.getLogger(__name__)

SOURCES: dict[str, dict] = {
    "air_quality": {
        "url": "https://air-quality-api.open-meteo.com/v1/air-quality",
        "hourly": [
            "pm10",
            "pm2_5",
            "carbon_monoxide",
            "nitrogen_dioxide",
            "sulphur_dioxide",
            "ozone",
            "us_aqi",
        ],
    },
    "weather": {
        "url": "https://api.open-meteo.com/v1/forecast",
        "hourly": [
            "temperature_2m",
            "relative_humidity_2m",
            "precipitation",
            "wind_speed_10m",
        ],
    },
}

RUN_ID_FORMAT = "%Y%m%dT%H%M%S"

# A fetcher takes (url, params) and returns the decoded JSON body.
# Injecting it keeps the pipeline testable offline.
Fetcher = Callable[[str, dict], dict]


class ExtractError(RuntimeError):
    """Raised when one or more API calls failed after all retries."""


def new_run_id(now: datetime | None = None) -> str:
    return (now or datetime.now(UTC)).strftime(RUN_ID_FORMAT)


def run_id_date(run_id: str) -> str:
    """Return the YYYY-MM-DD ingest date encoded in a run id (accepts a trailing 'Z')."""
    return datetime.strptime(run_id.rstrip("Z")[:15], RUN_ID_FORMAT).strftime("%Y-%m-%d")


def run_partition(layer_dir: Path, source: str, run_id: str) -> Path:
    return layer_dir / source / f"ingest_date={run_id_date(run_id)}" / f"run_id={run_id}"


def _is_retryable(exc: Exception) -> bool:
    if isinstance(exc, (requests.ConnectionError, requests.Timeout)):
        return True
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        return exc.response.status_code == 429 or exc.response.status_code >= 500
    return False


def http_fetcher(
    url: str,
    params: dict,
    *,
    retries: int = 4,
    backoff_seconds: float = 2.0,
    timeout: float = 30.0,
) -> dict:
    """GET a JSON endpoint with exponential backoff on throttling and server errors."""
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            if not _is_retryable(exc) or attempt == retries:
                raise
            wait = backoff_seconds * 2 ** (attempt - 1)
            log.warning("Attempt %d/%d failed (%s); retrying in %.1fs", attempt, retries, exc, wait)
            time.sleep(wait)
    raise AssertionError("unreachable")


def build_params(source: str, city: City, past_days: int) -> dict:
    return {
        "latitude": city.latitude,
        "longitude": city.longitude,
        "hourly": ",".join(SOURCES[source]["hourly"]),
        "past_days": past_days,
        "forecast_days": 1,
        "timezone": "GMT",  # always ingest in UTC; convert to local time in the warehouse
    }


def extract(
    cities: list[City],
    run_id: str,
    lake_dir: Path,
    past_days: int = 2,
    fetcher: Fetcher = http_fetcher,
) -> list[Path]:
    """Fetch every source for every city and land the raw responses in bronze.

    One failing city does not stop the others; failures are collected and raised at the
    end so the orchestrator retries the task. Re-running overwrites the same files, so
    retries are idempotent.
    """
    written: list[Path] = []
    failures: list[str] = []
    for source, spec in SOURCES.items():
        out_dir = run_partition(lake_dir / "bronze", source, run_id)
        out_dir.mkdir(parents=True, exist_ok=True)
        for city in cities:
            params = build_params(source, city, past_days)
            try:
                payload = fetcher(spec["url"], params)
            except Exception as exc:  # noqa: BLE001 - collect and report all failures
                log.error("Failed to fetch %s for %s: %s", source, city.id, exc)
                failures.append(f"{source}/{city.id}: {exc}")
                continue
            record = {
                "_meta": {
                    "run_id": run_id,
                    "source": source,
                    "city_id": city.id,
                    "ingested_at": datetime.now(UTC).isoformat(),
                    "request": {"url": spec["url"], "params": params},
                },
                "payload": payload,
            }
            path = out_dir / f"{city.id}.json"
            path.write_text(json.dumps(record), encoding="utf-8")
            written.append(path)
    log.info("Extracted %d raw files for run %s", len(written), run_id)
    if failures:
        raise ExtractError(f"{len(failures)} extract call(s) failed: {failures}")
    return written

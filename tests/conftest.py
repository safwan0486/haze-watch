"""Shared fixtures: an isolated data dir and a fake Open-Meteo API.

The fake returns realistic, deterministic payloads so the full pipeline can run in CI
with no network. Jakarta gets a 5-hour pollution spike so haze-episode logic is tested.
"""

from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from hazewatch.config import Settings
from hazewatch.extract import SOURCES

PROJECT_ROOT = Path(__file__).resolve().parents[1]
JAKARTA_LAT = -6.2088
SPIKE_HOURS = range(10, 15)  # 5 consecutive hours, hour offsets from the start of the window


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(home=PROJECT_ROOT, data_dir=tmp_path / "data")


class FakeOpenMeteo:
    """Callable fetcher. ``overrides`` lets a test corrupt a field to exercise DQ checks."""

    def __init__(self, now: datetime | None = None):
        self.now = (now or datetime.now(UTC)).replace(minute=0, second=0, microsecond=0, tzinfo=None)
        self.calls: list[tuple[str, dict]] = []
        self.overrides: dict[str, object] = {}

    def __call__(self, url: str, params: dict) -> dict:
        self.calls.append((url, params))
        source = next(name for name, spec in SOURCES.items() if spec["url"] == url)
        start = (self.now - timedelta(days=params["past_days"])).replace(hour=0)
        n_hours = (params["past_days"] + params["forecast_days"]) * 24
        times = [start + timedelta(hours=h) for h in range(n_hours)]
        is_jakarta = math.isclose(params["latitude"], JAKARTA_LAT)

        hourly: dict[str, list] = {"time": [t.strftime("%Y-%m-%dT%H:%M") for t in times]}
        for field in SOURCES[source]["hourly"]:
            hourly[field] = [self._value(field, h, is_jakarta) for h in range(n_hours)]
        for field, value in self.overrides.items():
            if field in hourly:
                hourly[field] = [value] * n_hours
        return {"latitude": params["latitude"], "longitude": params["longitude"], "timezone": "GMT",
                "hourly_units": {}, "hourly": hourly}

    @staticmethod
    def _value(field: str, hour: int, is_jakarta: bool) -> float:
        spike = is_jakarta and hour in SPIKE_HOURS
        base = {
            "pm2_5": 20 + 5 * math.sin(hour / 4),
            "pm10": 35 + 6 * math.sin(hour / 5),
            "us_aqi": 65 + 10 * math.sin(hour / 4),
            "carbon_monoxide": 300.0,
            "nitrogen_dioxide": 18.0,
            "sulphur_dioxide": 6.0,
            "ozone": 40.0,
            "temperature_2m": 28 + 3 * math.sin(hour / 3.8),
            "relative_humidity_2m": 75.0,
            "precipitation": 0.2 if hour % 7 == 0 else 0.0,
            "wind_speed_10m": 8.0,
        }[field]
        if spike and field == "pm2_5":
            return 85.0
        if spike and field == "us_aqi":
            return 165.0
        return round(base, 2)


@pytest.fixture
def fake_api() -> FakeOpenMeteo:
    return FakeOpenMeteo()

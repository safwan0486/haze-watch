from __future__ import annotations

import json

import pytest
import requests

from hazewatch import extract as ex
from hazewatch import silver
from hazewatch.config import load_cities


def test_extract_lands_raw_json_with_metadata(settings, fake_api):
    cities = load_cities(settings.cities_file)
    paths = ex.extract(cities, "20261125T100000", settings.lake_dir, past_days=2, fetcher=fake_api)

    assert len(paths) == len(cities) * len(ex.SOURCES)
    record = json.loads(paths[0].read_text(encoding="utf-8"))
    assert record["_meta"]["run_id"] == "20261125T100000"
    assert "ingest_date=2026-11-25" in paths[0].as_posix()
    assert record["payload"]["hourly"]["time"]  # raw payload kept as-is


def test_extract_continues_after_one_city_fails_then_raises(settings, fake_api):
    cities = load_cities(settings.cities_file)

    def flaky(url, params):
        if params["latitude"] == cities[0].latitude:
            raise requests.ConnectionError("boom")
        return fake_api(url, params)

    with pytest.raises(ex.ExtractError, match="2 extract call"):
        ex.extract(cities, "20261125T100000", settings.lake_dir, fetcher=flaky)
    written = list((settings.lake_dir / "bronze").rglob("*.json"))
    assert len(written) == (len(cities) - 1) * len(ex.SOURCES)


def test_http_fetcher_retries_on_503(monkeypatch):
    responses = iter([503, 503, 200])

    class FakeResponse:
        def __init__(self, status):
            self.status_code = status

        def raise_for_status(self):
            if self.status_code >= 400:
                raise requests.HTTPError(f"{self.status_code}", response=self)

        def json(self):
            return {"ok": True}

    monkeypatch.setattr(ex.requests, "get", lambda *a, **k: FakeResponse(next(responses)))
    monkeypatch.setattr(ex.time, "sleep", lambda s: None)
    assert ex.http_fetcher("https://example.test", {}) == {"ok": True}


def test_http_fetcher_does_not_retry_client_errors(monkeypatch):
    calls = []

    class BadRequest:
        status_code = 400

        def raise_for_status(self):
            raise requests.HTTPError("400", response=self)

    monkeypatch.setattr(ex.requests, "get", lambda *a, **k: calls.append(1) or BadRequest())
    with pytest.raises(requests.HTTPError):
        ex.http_fetcher("https://example.test", {})
    assert len(calls) == 1


def _record(hourly: dict) -> dict:
    return {
        "_meta": {"run_id": "r1", "source": "weather", "city_id": "singapore",
                  "ingested_at": "2026-11-25T10:30:00+00:00"},
        "payload": {"hourly": hourly},
    }


def test_flatten_marks_future_hours_as_forecast():
    hourly = {"time": ["2026-11-25T10:00", "2026-11-25T11:00"], "temperature_2m": [30, 31],
              "relative_humidity_2m": [70, 71], "precipitation": [0, None], "wind_speed_10m": [5, 6]}
    rows = silver.flatten(_record(hourly))
    assert [r[-2] for r in rows] == [False, True]  # is_forecast
    assert rows[1][4] is None  # nulls preserved, not coerced to 0


def test_flatten_rejects_ragged_arrays():
    hourly = {"time": ["2026-11-25T10:00", "2026-11-25T11:00"], "temperature_2m": [30],
              "relative_humidity_2m": [70, 71], "precipitation": [0, 0], "wind_speed_10m": [5, 6]}
    with pytest.raises(silver.SchemaError, match="differ in length"):
        silver.flatten(_record(hourly))


def test_flatten_rejects_missing_fields():
    with pytest.raises(silver.SchemaError, match="missing fields"):
        silver.flatten(_record({"time": []}))

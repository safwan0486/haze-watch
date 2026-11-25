"""End-to-end tests: fake API -> bronze -> silver -> warehouse -> marts -> DQ."""

from __future__ import annotations

import pytest

from hazewatch import pipeline, warehouse
from hazewatch.config import load_cities
from hazewatch.quality import DataQualityError
from tests.conftest import FakeOpenMeteo


def _q(settings, sql):
    with warehouse.connect(settings.warehouse_path, read_only=True) as con:
        return con.execute(sql).fetchall()


def test_full_run_builds_marts(settings, fake_api):
    run_id = pipeline.run_all(settings, past_days=3, fetcher=fake_api)

    n_cities = _q(settings, "SELECT count(*) FROM marts.dim_city")[0][0]
    assert n_cities == 6

    # Facts contain only observed (past) hours, one per city per hour.
    dupes = _q(settings, """SELECT count(*) FROM (SELECT city_id, observed_at_utc FROM marts.fct_air_quality_hourly
                            GROUP BY ALL HAVING count(*) > 1)""")[0][0]
    assert dupes == 0
    future = _q(settings, """SELECT count(*) FROM marts.fct_air_quality_hourly
                             WHERE observed_at_utc > (SELECT max(ingested_at) FROM staging.air_quality_hourly)""")
    assert future[0][0] == 0
    forecasts_staged = _q(settings, "SELECT count(*) FROM staging.air_quality_hourly WHERE is_forecast")
    assert forecasts_staged[0][0] > 0  # forecasts are kept in staging, just not treated as facts

    # Weather joined onto every air-quality hour.
    missing_weather = _q(settings, "SELECT count(*) FROM marts.fct_air_quality_hourly WHERE temperature_c IS NULL")
    assert missing_weather[0][0] == 0

    # Jakarta's 5-hour spike is detected as exactly one haze episode.
    episodes = _q(settings, "SELECT city_id, duration_hours, peak_us_aqi FROM marts.mart_haze_episodes")
    assert episodes == [("jakarta", 5, 165.0)]

    # Every step is in the audit log and every DQ check was recorded.
    steps = _q(settings, f"SELECT step, status FROM ops.pipeline_runs WHERE run_id = '{run_id}' ORDER BY started_at")
    assert [s for s, _ in steps] == ["extract", "bronze_to_silver", "load", "quality_staging",
                                     "transform", "quality_marts"]
    assert all(status == "success" for _, status in steps)
    assert _q(settings, "SELECT count(*) FROM ops.dq_results WHERE NOT passed")[0][0] == 0


def test_rerun_is_idempotent(settings, fake_api):
    pipeline.run_all(settings, run_id="20261125T100000", past_days=2, fetcher=fake_api)
    first = _q(settings, "SELECT count(*) FROM staging.air_quality_hourly")[0][0]
    pipeline.run_all(settings, run_id="20261125T110000", past_days=2, fetcher=fake_api)
    second = _q(settings, "SELECT count(*) FROM staging.air_quality_hourly")[0][0]
    assert first == second > 0


def test_old_run_replay_does_not_overwrite_newer_data(settings, fake_api):
    pipeline.run_all(settings, run_id="20261125T100000", past_days=2, fetcher=fake_api)

    newer = FakeOpenMeteo(now=fake_api.now)
    newer.overrides = {"pm2_5": 42.0}
    pipeline.run_all(settings, run_id="20261125T110000", past_days=2, fetcher=newer)

    # Replay the first run's silver files: they are older, so nothing may change.
    with warehouse.connect(settings.warehouse_path) as con:
        con.execute("""UPDATE staging.air_quality_hourly SET ingested_at = ingested_at + INTERVAL 1 DAY
                       WHERE run_id = '20261125T110000'""")
        warehouse.load_run(con, "20261125T100000", settings.lake_dir, load_cities(settings.cities_file))
    values = _q(settings, "SELECT DISTINCT pm2_5 FROM staging.air_quality_hourly")
    assert values == [(42.0,)]


def test_bad_data_is_blocked_before_marts(settings):
    bad_api = FakeOpenMeteo()
    bad_api.overrides = {"pm2_5": -5.0}  # physically impossible

    with pytest.raises(DataQualityError, match="pm2_5_within_physical_range"):
        pipeline.run_all(settings, past_days=1, fetcher=bad_api)

    # The pipeline stopped at the staging gate: no marts were built from bad data.
    tables = _q(settings, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'marts'")
    assert tables == []
    status = _q(settings, "SELECT status FROM ops.pipeline_runs WHERE step = 'quality_staging'")
    assert status == [("failed",)]


def test_cli_returns_exit_code_2_on_dq_failure(settings, monkeypatch):
    bad_api = FakeOpenMeteo()
    bad_api.overrides = {"relative_humidity_2m": 140.0}
    monkeypatch.setenv("HAZEWATCH_HOME", str(settings.home))
    monkeypatch.setenv("HAZEWATCH_DATA_DIR", str(settings.data_dir))
    monkeypatch.setattr(pipeline.ex, "http_fetcher", bad_api)
    assert pipeline.main(["run", "--past-days", "1"]) == 2

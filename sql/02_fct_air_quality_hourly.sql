-- Fact: one row per city per observed hour (grain = city_id x observed_at_utc).
-- Joins air quality with weather, converts UTC to local time, and labels the
-- US EPA AQI category. Forecast hours are excluded; only observations are facts.
CREATE OR REPLACE TABLE marts.fct_air_quality_hourly AS
SELECT
    a.city_id,
    a.observed_at                                              AS observed_at_utc,
    a.observed_at + to_hours(c.utc_offset_hours)               AS observed_at_local,
    CAST(a.observed_at + to_hours(c.utc_offset_hours) AS DATE) AS local_date,
    a.pm2_5,
    a.pm10,
    a.nitrogen_dioxide,
    a.ozone,
    a.carbon_monoxide,
    a.sulphur_dioxide,
    a.us_aqi,
    CASE
        WHEN a.us_aqi IS NULL THEN NULL
        WHEN a.us_aqi <= 50  THEN 'Good'
        WHEN a.us_aqi <= 100 THEN 'Moderate'
        WHEN a.us_aqi <= 150 THEN 'Unhealthy for Sensitive Groups'
        WHEN a.us_aqi <= 200 THEN 'Unhealthy'
        WHEN a.us_aqi <= 300 THEN 'Very Unhealthy'
        ELSE 'Hazardous'
    END                                                        AS aqi_category,
    w.temperature_2m                                           AS temperature_c,
    w.relative_humidity_2m                                     AS relative_humidity_pct,
    w.precipitation                                            AS precipitation_mm,
    w.wind_speed_10m                                           AS wind_speed_kmh
FROM staging.air_quality_hourly AS a
JOIN marts.dim_city AS c
    ON c.city_id = a.city_id
LEFT JOIN staging.weather_hourly AS w
    ON  w.city_id = a.city_id
    AND w.observed_at = a.observed_at
    AND NOT w.is_forecast
WHERE NOT a.is_forecast;

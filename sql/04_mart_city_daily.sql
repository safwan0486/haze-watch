-- Mart: daily air-quality summary per city (local calendar day).
-- exceeds_who_pm2_5_24h uses the WHO 2021 guideline of 15 ug/m3 (24-hour mean).
-- pm2_5_completeness lets consumers ignore partial days (e.g. today).
CREATE OR REPLACE TABLE marts.mart_city_daily AS
SELECT
    f.city_id,
    c.city_name,
    c.country,
    f.local_date,
    count(*)                                         AS hours_observed,
    round(count(f.pm2_5) / 24.0, 3)                  AS pm2_5_completeness,
    round(avg(f.pm2_5), 2)                           AS avg_pm2_5,
    round(max(f.pm2_5), 2)                           AS max_pm2_5,
    max(f.us_aqi)                                    AS max_us_aqi,
    count(*) FILTER (WHERE f.us_aqi > 100)           AS hours_aqi_over_100,
    count(*) FILTER (WHERE f.us_aqi > 150)           AS hours_unhealthy,
    coalesce(avg(f.pm2_5) > 15, FALSE)               AS exceeds_who_pm2_5_24h,
    round(avg(f.temperature_c), 1)                   AS avg_temperature_c,
    round(sum(f.precipitation_mm), 1)                AS total_precipitation_mm,
    round(avg(f.wind_speed_kmh), 1)                  AS avg_wind_speed_kmh
FROM marts.fct_air_quality_hourly AS f
JOIN marts.dim_city AS c
    ON c.city_id = f.city_id
GROUP BY f.city_id, c.city_name, c.country, f.local_date
ORDER BY f.local_date, f.city_id;

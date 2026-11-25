-- Mart: haze episodes = runs of 3+ consecutive hours with US AQI above 100.
--
-- Classic "gaps and islands": for consecutive hours, timestamp minus row_number()
-- (in hours) is constant, so it identifies each unbroken run. A missing hour or a
-- clean hour breaks the run, which is exactly the behaviour we want.
CREATE OR REPLACE TABLE marts.mart_haze_episodes AS
WITH polluted_hours AS (
    SELECT
        city_id,
        observed_at_local,
        us_aqi,
        pm2_5,
        observed_at_local
            - to_hours(CAST(row_number() OVER (PARTITION BY city_id ORDER BY observed_at_local) AS BIGINT))
            AS island_key
    FROM marts.fct_air_quality_hourly
    WHERE us_aqi > 100
),
episodes AS (
    SELECT
        city_id,
        min(observed_at_local)  AS episode_start_local,
        max(observed_at_local)  AS episode_end_local,
        count(*)                AS duration_hours,
        max(us_aqi)             AS peak_us_aqi,
        round(avg(pm2_5), 2)    AS avg_pm2_5
    FROM polluted_hours
    GROUP BY city_id, island_key
)
SELECT
    e.city_id || '_' || strftime(e.episode_start_local, '%Y%m%dT%H') AS episode_id,
    e.city_id,
    c.city_name,
    e.episode_start_local,
    e.episode_end_local,
    e.duration_hours,
    e.peak_us_aqi,
    e.avg_pm2_5
FROM episodes AS e
JOIN marts.dim_city AS c
    ON c.city_id = e.city_id
WHERE e.duration_hours >= 3
ORDER BY e.episode_start_local DESC;

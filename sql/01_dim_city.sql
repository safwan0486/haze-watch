-- Dimension: one row per monitored city.
CREATE OR REPLACE TABLE marts.dim_city AS
SELECT
    city_id,
    city_name,
    country,
    latitude,
    longitude,
    utc_offset_hours
FROM staging.cities;

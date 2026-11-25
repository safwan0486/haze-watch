-- Dimension: calendar attributes for every local date present in the fact table.
CREATE OR REPLACE TABLE marts.dim_date AS
SELECT
    d                     AS date_day,
    year(d)               AS year,
    month(d)              AS month,
    day(d)                AS day_of_month,
    dayname(d)            AS day_name,
    isodow(d) IN (6, 7)   AS is_weekend
FROM (SELECT DISTINCT local_date AS d FROM marts.fct_air_quality_hourly)
ORDER BY d;

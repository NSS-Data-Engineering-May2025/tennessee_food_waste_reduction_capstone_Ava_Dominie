-- GOLD_WEATHER_YIELD:
-- This model combines yield efficiency metrics with aggregated weather features 
-- to analyze how climate factors may be influencing crop performance at the county level.
-- Purpose:
--   • Join GOLD_YIELD_EFFICIENCY (acres harvested, production, yield) 
--     with GOLD_WEATHER_YIELD_CORRELATION (temperature, precipitation, extreme events).
--   • Enable side-by-side analysis of crop outcomes against key weather drivers 
--     such as heat, freeze, and heavy rain days.
-- Why needed:
--   • Provides an integrated dataset for investigating weather–yield relationships.
--   • Supports predictive insights into how changing climate conditions may impact 
--     agricultural productivity.
--   • Acts as a foundation for downstream visualizations, modeling, and forecasting.
{{ config(materialized='table') }}

WITH weather AS (
    SELECT
        LATITUDE,
        LONGITUDE,
        TIMEZONE,
        DATEADD(DAY, DAY_NUMBER - 1, '2024-01-01') AS DATE,
        VALUE,
        DAILY_UNITS
    FROM {{ source('SILVER', 'SILVER_WEATHER_OPEN_METEO') }}
),

agg_weather AS (
    SELECT
        YEAR(DATE) AS YEAR,
        AVG(CASE WHEN DAILY_UNITS = '°C' THEN VALUE END) AS AVG_TEMP,
        SUM(CASE WHEN DAILY_UNITS = 'mm' THEN VALUE END) AS TOTAL_PRECIP,
        SUM(CASE WHEN VALUE > 35 AND DAILY_UNITS = '°C' THEN 1 ELSE 0 END) AS EXTREME_HEAT_DAYS,
        SUM(CASE WHEN VALUE < 0 AND DAILY_UNITS = '°C' THEN 1 ELSE 0 END) AS FREEZE_DAYS,
        SUM(CASE WHEN VALUE > 25 AND DAILY_UNITS = 'mm' THEN 1 ELSE 0 END) AS HEAVY_RAIN_DAYS
    FROM weather
    GROUP BY YEAR
)

SELECT
    y.YEAR,
    y.COUNTY_NAME,
    y.COMMODITY_DESC,
    y.YIELD,
    w.AVG_TEMP,
    w.TOTAL_PRECIP,
    w.EXTREME_HEAT_DAYS,
    w.FREEZE_DAYS,
    w.HEAVY_RAIN_DAYS
FROM {{ source('SILVER','SILVER_FIELD_YIELDS') }} y
LEFT JOIN agg_weather w
  ON y.YEAR = w.YEAR

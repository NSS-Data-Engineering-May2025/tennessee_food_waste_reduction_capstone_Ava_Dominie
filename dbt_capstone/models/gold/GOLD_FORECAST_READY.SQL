-- GOLD_FORECAST_READY:
-- Joins yield efficiency with weather correlation metrics at the county level.  
-- Enables analysis of how production, yield, and acres harvested interact with 
-- climate factors (temperature, precipitation, extreme events).  
-- Serves as a key dataset for identifying trends and building predictive insights.
{{ config(materialized='table') }}

SELECT
    e.YEAR,
    e.COUNTY_NAME,
    e.COUNTY_CODE,
    e.COMMODITY_DESC,
    e.ACRES_HARVESTED,
    e.PRODUCTION,
    e.YIELD,
    w.AVG_TEMP,
    w.TOTAL_PRECIP,
    w.EXTREME_HEAT_DAYS,
    w.FREEZE_DAYS,
    w.HEAVY_RAIN_DAYS
FROM {{ ref('GOLD_YIELD_EFFICIENCY') }} e
LEFT JOIN {{ ref('GOLD_WEATHER_YIELD_CORRELATION') }} w
  ON e.YEAR = w.YEAR
 AND e.COUNTY_NAME = w.COUNTY_NAME
 AND e.COMMODITY_DESC = w.COMMODITY_DESC

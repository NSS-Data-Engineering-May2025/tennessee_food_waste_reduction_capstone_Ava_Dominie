{{ config(materialized='table') }}

-- This model flattens the DAILY JSON array for each location and metric (temperature or precipitation)
-- Each row in the output represents a single daily value for a location
-- Columns:
--   LATITUDE, LONGITUDE, TIMEZONE: Location metadata
--   DAY_NUMBER: The day index in the array (1-based)
--   VALUE: The daily weather value (temperature or precipitation, depending on DAILY_UNITS)
--   DAILY_UNITS: The unit of measurement (e.g., '°C' for temperature, 'mm' for precipitation)
--   LOAD_TIMESTAMP_UTC: When the data was loaded

SELECT
  LATITUDE,
  LONGITUDE,
  TIMEZONE,
  f.index + 1 AS DAY_NUMBER, -- 1-based index for the day in the array
  TRY_CAST(f.value::STRING AS FLOAT) AS VALUE, -- Cast each array value to FLOAT (ignores non-numeric values)
  DAILY_UNITS, -- Unit of measurement for VALUE
  LOAD_TIMESTAMP_UTC -- Pipeline load timestamp
FROM {{ source('BRONZE', 'WEATHER_OPEN_METEO') }},
  LATERAL FLATTEN(INPUT => TRY_PARSE_JSON(DAILY)) f -- Expand the DAILY JSON array into rows
WHERE TRY_CAST(f.value::STRING AS FLOAT) IS NOT NULL -- Only keep numeric values (filter out dates)
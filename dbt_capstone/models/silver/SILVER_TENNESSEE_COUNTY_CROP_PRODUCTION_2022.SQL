{{ config(materialized='table') }}

-- Cleaned county-level crop production table for Tennessee, 2022 Census
-- Only key columns for analysis and joining are selected
-- Set Value to 0 if null
SELECT
	YEAR,
	STATE,
	"STATE ANSI" AS STATE_ANSI,
	COUNTY,
	"COUNTY ANSI" AS COUNTY_ANSI,
	COMMODITY,
	"DATA ITEM" AS DATA_ITEM,
	COALESCE(TRY_CAST(VALUE AS FLOAT), 0) AS VALUE,
	"CV (%)" AS CV_PERCENT,
	LOAD_TIMESTAMP_UTC
FROM {{ source('BRONZE', 'TENNESSEE_COUNTY_CROP_PRODUCTION_2022') }}

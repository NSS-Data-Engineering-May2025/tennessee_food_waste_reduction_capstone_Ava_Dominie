{{ config(materialized='table') }}

-- Cleaned county-level crop production table for Tennessee, 2022 Census
-- Only key columns for analysis and joining are selected
SELECT
	YEAR,
	STATE,
	"STATE ANSI" AS STATE_ANSI,
	COUNTY,
	"COUNTY ANSI" AS COUNTY_ANSI,
	COMMODITY,
	"DATA ITEM" AS DATA_ITEM,
	TRY_CAST(VALUE AS FLOAT) AS VALUE,
	"CV (%)" AS CV_PERCENT,
	LOAD_TIMESTAMP_UTC
FROM {{ source('BRONZE', 'TENNESSEE_COUNTY_CROP_PRODUCTION_2022') }}
WHERE VALUE IS NOT NULL

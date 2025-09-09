-- GOLD_CENSUS_VALIDATION:
-- This model compares NASS 2024 production data with the USDA Census of Agriculture 2022 (baseline).
-- Purpose:
--   • Use the 2022 Census as a "ground truth snapshot" for county-level crop production.
--   • Calculate absolute and percent differences between 2024 NASS estimates and 2022 Census values
--     to track trends and shifts in production since the last Census.
--   • Identify coverage issues with OVERLAP_STATUS (e.g., Missing NASS, Missing Census, Missing Both),
--     which helps flag data quality problems or mismatches in commodity/county reporting.
-- Why needed:
--   • Ensures transparency in how NASS estimates deviate from the Census baseline.
--   • Provides a validation layer for downstream forecasting models.
--   • Supports storytelling: highlights whether production is stable, increasing, or decreasing 
--     relative to the last comprehensive Census benchmark.

{{ config(materialized='table') }}

WITH nass AS (
    SELECT
        YEAR,
        COUNTY_NAME,
        COUNTY_CODE,
        COMMODITY_DESC,
        CASE 
            WHEN REGEXP_REPLACE(TRIM(PRODUCTION), '[^0-9.]', '') <> '' 
            THEN TRY_CAST(REGEXP_REPLACE(TRIM(PRODUCTION), '[^0-9.]', '') AS NUMBER)
            ELSE NULL
        END AS PRODUCTION
    FROM {{ source('SILVER','SILVER_PRODUCTION') }}
    WHERE YEAR = 2024
),

census AS (
    SELECT
        YEAR AS CENSUS_YEAR,
        COUNTY,
        COUNTY_ANSI AS COUNTY_CODE,
        COMMODITY,
        CASE 
            WHEN REGEXP_REPLACE(TRIM(VALUE), '[^0-9.]', '') <> '' 
            THEN TRY_CAST(REGEXP_REPLACE(TRIM(VALUE), '[^0-9.]', '') AS NUMBER)
            ELSE NULL
        END AS CENSUS_PRODUCTION
    FROM {{ source('SILVER','SILVER_TENNESSEE_COUNTY_CROP_PRODUCTION_2022') }}
)

SELECT
    n.COUNTY_NAME,
    n.COUNTY_CODE,
    n.COMMODITY_DESC,
    c.CENSUS_YEAR,
    n.YEAR AS NASS_YEAR,
    c.CENSUS_PRODUCTION,
    n.PRODUCTION AS NASS_PRODUCTION,
    (n.PRODUCTION - c.CENSUS_PRODUCTION) AS DIFFERENCE,
    CASE 
        WHEN c.CENSUS_PRODUCTION > 0 
        THEN ROUND((n.PRODUCTION - c.CENSUS_PRODUCTION) / c.CENSUS_PRODUCTION * 100, 2) 
    END AS PERCENT_DIFFERENCE

        ,CASE 
            WHEN n.PRODUCTION IS NULL AND c.CENSUS_PRODUCTION IS NOT NULL THEN 'Missing NASS'
            WHEN c.CENSUS_PRODUCTION IS NULL AND n.PRODUCTION IS NOT NULL THEN 'Missing Census'
            WHEN n.PRODUCTION IS NULL AND c.CENSUS_PRODUCTION IS NULL THEN 'Missing Both'
            ELSE 'Overlap'
        END AS OVERLAP_STATUS
    FROM census c
    LEFT JOIN nass n
      ON n.COUNTY_CODE = c.COUNTY_CODE
     AND n.COMMODITY_DESC = c.COMMODITY

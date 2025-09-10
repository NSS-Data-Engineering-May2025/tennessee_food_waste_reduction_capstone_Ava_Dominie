# Tennessee Food Waste Reduction Capstone – Ava Dominie

## Project Overview  
This project supports **predictive insights for Tennessee’s food supply chain** by integrating historical crop data, weather conditions, and yield modeling.  

The goal is to **identify potential overstock or understock situations before they occur**, allowing suppliers, retailers, and policymakers to make informed decisions that **reduce food waste and optimize distribution**.  

The pipeline is built to:  
- Collect and automate ingestion of multiple datasets (census, survey, and API sources).  
- Clean and transform these datasets into structured layers (**Bronze → Silver → Gold**).  
- Integrate yield, acreage, production, and weather data into a unified modeling layer.  
- Provide baseline comparisons (**Census 2022 vs. NASS 2024**) and real-time weather-adjusted insights.  

---

## Data Sources  
- **USDA NASS Quick Stats (county-level data for Tennessee)**  
  - Crop yields  
  - Acres harvested  
  - Total production  

- **USDA Census of Agriculture (2022)**  
  - County-level “ground truth” baseline snapshot of crop production  

- **Open-Meteo API**  
  - Daily weather data (temperature, precipitation)  
  - Extreme weather indicators (heat days, freeze days, heavy rain)  

---

## Pipeline Architecture  

The project follows a **medallion architecture**:  

### 1. Bronze Layer – Raw Ingestion  
- Direct ingestion of source files (NASS, Census, API).  
- Preserves original structure for traceability.  

**Examples:**  
- `TENNESSEE_COUNTY_CROP_PRODUCTION_2022` (raw Census)  
- `FIELD_YIELDS`, `ACRES_HARVESTED`, `PRODUCTION` (raw NASS)  
- `WEATHER_OPEN_METEO` (raw JSON API results)  

---

### 2. Silver Layer – Standardized & Cleaned  
- Cleans raw datasets (removes nulls, casts datatypes, normalizes county/commodity naming).  
- Splits into domain-specific tables:  
  - `SILVER_ACRES_HARVESTED` – acres harvested by county/commodity  
  - `SILVER_FIELD_YIELDS` – yield values (bushels/acre, etc.)  
  - `SILVER_PRODUCTION` – total production  
  - `SILVER_TENNESSEE_COUNTY_CROP_PRODUCTION_2022` – cleaned Census 2022 snapshot  
  - `SILVER_WEATHER_OPEN_METEO` – flattened daily weather metrics  

---

### 3. Gold Layer – Analytical Models  
- **Yield Efficiency (`GOLD_YIELD_EFFICIENCY`)**  
  Combines acres harvested, yields, and production into yield-per-acre metrics.  

- **Weather-Yield Correlation (`GOLD_WEATHER_YIELD_CORRELATION`)**  
  Aggregates weather data into seasonal metrics: average temp, total precipitation, extreme heat/freezes.  

- **Census Validation (`GOLD_CENSUS_VALIDATION`)**  
  Compares NASS 2024 production against Census 2022 baseline.  
  Flags overlaps, missing data, and calculates percent differences.  

- **Climate-Yield Integration (`GOLD_CLIMATE_YIELD_INTEGRATION`)**  
  Links efficiency and weather metrics, enabling analysis of how climate affects production.  

---

## Why This Pipeline Matters  
- **Baseline Accuracy** – Census 2022 serves as a “ground truth snapshot” for validating and contextualizing newer NASS data.  
- **Real-Time Context** – Weather impacts are incorporated to explain deviations from historical yield expectations.  
- **Actionable Insights** – Identifying counties at risk of overproduction or underproduction helps reduce waste and stabilize supply chains.  
- **Scalable Design** – The medallion architecture supports automated updates, making the pipeline extensible for future datasets or states.  

---

## Example Use Cases  
- **Suppliers** – Plan storage and logistics for regions at risk of overstock.  
- **Retailers** – Anticipate understock and secure contracts from other counties.  
- **Policy Makers** – Monitor food security risks and optimize resource allocation.  

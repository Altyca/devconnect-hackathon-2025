-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## 🌱 ESG emissions Declarative Pipeline - Public companies data
-- MAGIC
-- MAGIC **Purpose:** Extract, clean and load the CSV data containing the emissions and revenue of public companies
-- MAGIC
-- MAGIC **Data Source:** CSV files
-- MAGIC
-- MAGIC **Architecture:** Bronze → Silver → Gold (Medallion Pattern)  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 🥉 BRONZE LAYER - RAW DATA INGESTION
-- MAGIC
-- MAGIC **Purpose:** Ingest raw CSV files into Bronze layer
-- MAGIC

-- COMMAND ----------

CREATE VIEW companies_data_bronze
AS
SELECT * , 
  regexp_extract(_metadata.file_path, '([^/]+\.csv)$') AS filename 
FROM read_files('/Volumes/devconnect_2025/esg/sample-data/emmissions*.csv', format => "csv");

-- COMMAND ----------

CREATE VIEW companies_annual_revenue_bronze
AS
SELECT *, 
regexp_extract(_metadata.file_path, '([^/]+\.csv)$') AS filename 
FROM read_files('/Volumes/devconnect_2025/esg/sample-data/companies-annual-revenue.csv', format => "csv");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 🥈 SILVER LAYER - DATA CLEANSING & STANDARDIZATION
-- MAGIC
-- MAGIC **Purpose:** Clean, standardize, and type-cast raw emissions data
-- MAGIC
-- MAGIC **Transformations:**
-- MAGIC - ✅ Rename columns for consistency and ease of use
-- MAGIC - ✅ Convert string emissions values to FLOAT for calculations
-- MAGIC - ✅ Enrich data with companies revenue
-- MAGIC - ✅ Add data quality constraints

-- COMMAND ----------

CREATE MATERIALIZED VIEW companies_annual_revenue_silver
AS
SELECT
  `Organization Legal Name` AS org_name,
  `Annual Revenue (Billion USD)` AS revenue_usd_billions
FROM companies_annual_revenue_bronze;

-- COMMAND ----------

CREATE MATERIALIZED VIEW companies_data_silver_1
AS
SELECT
  `Organization Legal Name` AS org_name,
  `Reporting year` AS reporting_year,
  `Data model for disclosure` AS disclosure_model,
  `Jurisdiction` AS jurisdiction,
  CAST(`Total Scope 1 GHG emissions` AS FLOAT) AS scope_1_ghg,
  `Total Scope 1 GHG emissions units` AS scope_1_units,
  CAST(`Total Scope 2 location-based GHG emissions` AS FLOAT) AS scope_2_ghg,
  `Total Scope 2 location-based GHG emissions units` AS scope_2_units,
  CAST(`Total Scope 3 GHG emissions` AS FLOAT) AS scope_3_ghg,
  `Total Scope 3 GHG emissions units` AS scope_3_units,
  `filename` AS filename,
  scope_1_ghg + scope_2_ghg + scope_3_ghg AS scope_all_ghg
FROM companies_data_bronze;

-- COMMAND ----------

CREATE MATERIALIZED VIEW companies_data_silver_2
(
  CONSTRAINT valid_company_name EXPECT (org_name IS NOT NULL AND length(trim(org_name)) > 1) ON VIOLATION DROP ROW,
  CONSTRAINT has_revenue EXPECT (revenue_usd IS NOT NULL OR revenue_usd = 0) ON VIOLATION DROP ROW
)
AS
SELECT 
  silver.*,
  LAG(scope_all_ghg) OVER (PARTITION BY silver.org_name ORDER BY reporting_year) AS scope_all_ghg_prev_year,
  scope_all_ghg - scope_all_ghg_prev_year AS scope_all_ghg_yoy_growth_delta,
  ROUND(((scope_all_ghg / scope_all_ghg_prev_year) - 1), 2) * 100 AS scope_all_ghg_yoy_growth_pct,
  revenue.revenue_usd_billions * 10e9 AS revenue_usd
FROM companies_data_silver_1 silver
LEFT JOIN companies_annual_revenue_silver revenue
ON silver.org_name = revenue.org_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 🥇 GOLD LAYER - BUSINESS-READY ANALYTICS
-- MAGIC
-- MAGIC **Purpose:** Create ranked datasets for executive dashboards & reporting
-- MAGIC
-- MAGIC **Use Cases:** 
-- MAGIC - ESG scorecards and sustainability benchmarking
-- MAGIC - Regulatory compliance reporting
-- MAGIC - Investment decision support

-- COMMAND ----------

CREATE MATERIALIZED VIEW companies_data_gold
AS
SELECT 
    silver.*, 
    RANK() OVER (PARTITION BY silver.reporting_year ORDER BY silver.scope_all_ghg DESC) AS rank_ghg_by_year,
    RANK() OVER (PARTITION BY silver.reporting_year ORDER BY silver.scope_all_ghg_yoy_growth_delta DESC) AS rank_delta_by_year,
    RANK() OVER (PARTITION BY silver.reporting_year ORDER BY silver.scope_all_ghg_yoy_growth_pct DESC) AS rank_growth_pct_by_year
FROM 
    companies_data_silver_2 silver;
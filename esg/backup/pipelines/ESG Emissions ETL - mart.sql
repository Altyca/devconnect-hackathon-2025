-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## 🌱 ESG emissions Declarative Pipeline - Data mart
-- MAGIC **Purpose:** Combine partners and companies data into a single table
-- MAGIC
-- MAGIC **Data Source:** partners and companies gold tables

-- COMMAND ----------

CREATE MATERIALIZED VIEW companies_partners_data_mart
AS
WITH partners_standardized AS (
  SELECT 
    org_name,
    industry,
    'partners' AS data_source,
    annual_revenue_usd,
    year(current_timestamp()) AS reporting_year,
    scope_1_ghg,
    scope_2_ghg,
    scope_3_ghg,
    scope_all_ghg
  FROM partners_data_parsed
),
companies_standardized AS (
  SELECT 
    org_name,
    industry,
    'public_companies' AS data_source,
    revenue_usd AS annual_revenue_usd,
    reporting_year,
    scope_1_ghg,
    scope_2_ghg,
    scope_3_ghg,
    scope_all_ghg
  FROM companies_data_gold
)
SELECT *
FROM partners_standardized
UNION ALL
SELECT *
FROM companies_standardized;
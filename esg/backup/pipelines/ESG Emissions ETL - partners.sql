-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## 🌱 ESG emissions Declarative Pipeline - Partners data
-- MAGIC
-- MAGIC **Purpose:** Use Agent model to extract emission information from partners data (PDFs) in batch.
-- MAGIC
-- MAGIC **Data Source:** Delta table containing PDF texts

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### STEP 1 : BATCH INFERENCE
-- MAGIC
-- MAGIC **Purpose:** Run batch inference on all documents using the information extraction Agent model.
-- MAGIC
-- MAGIC **TODO:** Replace the endpoint name and the document texts table path with your own.
-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE partners_data_raw 
  TBLPROPERTIES (
    'delta.feature.variantType-preview' = 'supported'
  )
  AS
  WITH query_results AS (
    SELECT
      `text` AS input,
      ai_query(
        'kie-5e148f1a-endpoint', -- REPLACE with your endpoint name
        input,
        failOnError => false
      ) AS response
    FROM STREAM(`your_catalog`.`your_schema`.`your_table`) -- REPLACE with your document texts table path
  )
  SELECT
    input,
    response.result AS response,
    response.errorMessage AS error
  FROM query_results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### STEP 2 : POST PROCESSING
-- MAGIC
-- MAGIC **Purpose:** Process query results , handle edge cases and apply constraints on the extracted data

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW partners_data_parsed (
  CONSTRAINT valid_company_name EXPECT (org_name IS NOT NULL AND length(trim(org_name)) > 1) ON VIOLATION DROP ROW,
  CONSTRAINT reasonable_scope_1_ghg EXPECT (scope_3_ghg IS NULL OR (scope_3_ghg >= 0 AND scope_3_ghg < 1e10)) ON VIOLATION DROP ROW
)
AS SELECT 
-- replace with your extracted data 
  get_json_object(response, '$.company_name') as org_name,
  get_json_object(response, '$.industry') as industry,
  CAST(
    get_json_object(response, '$.annual_revenue_usd') AS DOUBLE
  ) as annual_revenue_usd,
  CAST(
  get_json_object(response, '$.number_of_employees') 
  AS INTEGER
  ) as nb_employees,
  CAST(
    get_json_object(response, '$.scope_1_emissions_tCO2')
    AS DOUBLE
  ) as scope_1_ghg,
  CAST(
    get_json_object(response, '$.scope_2_emissions_tCO2')
    AS DOUBLE
  ) as scope_2_ghg,
  CAST(
    get_json_object(response, '$.scope_3_emissions_tCO2')
    AS DOUBLE
  ) as scope_3_ghg,
  scope_1_ghg + scope_2_ghg + scope_3_ghg AS scope_all_ghg
FROM partners_data_raw
WHERE get_json_object(response, '$.company_name') IS NOT NULL
  AND error IS NULL;
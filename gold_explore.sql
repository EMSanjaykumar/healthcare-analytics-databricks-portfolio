-- Databricks notebook source
use catalog hl7_hipaa

-- COMMAND ----------

select * from gold.vw_healthcare_star_schema

-- COMMAND ----------

select p.provider_name as provider_name, s.claim_amount as claim_amount
from gold.dim_provider p
join gold.vw_healthcare_star_schema s on p.provider_id = s.provider_id
group by p.provider_name, s.claim_amount
order by claim_amount desc

-- COMMAND ----------

DESCRIBE TABLE hl7_hipaa.gold.dim_patient;

-- COMMAND ----------

DESCRIBE TABLE hl7_hipaa.gold.dim_provider;

-- COMMAND ----------

DESCRIBE TABLE hl7_hipaa.gold.dim_date;

-- COMMAND ----------

DESCRIBE TABLE hl7_hipaa.gold.dim_condition;

-- COMMAND ----------

DESCRIBE TABLE hl7_hipaa.gold.fact_claims;

-- COMMAND ----------

DESCRIBE TABLE hl7_hipaa.gold.fact_encounters;

-- COMMAND ----------

DESCRIBE TABLE hl7_hipaa.gold.fact_readmission_risk;

-- COMMAND ----------

SELECT * FROM hl7_hipaa.gold.dim_patient LIMIT 5;
SELECT * FROM hl7_hipaa.gold.fact_claims LIMIT 5;


-- COMMAND ----------

SELECT clinical_complexity_tier, COUNT(*) AS cnt
FROM hl7_hipaa.gold.dim_patient
GROUP BY clinical_complexity_tier;


-- COMMAND ----------

SELECT condition_name, total_condition_cost_burden
FROM hl7_hipaa.gold.disease_analytics
ORDER BY total_condition_cost_burden DESC
LIMIT 10;


-- COMMAND ----------


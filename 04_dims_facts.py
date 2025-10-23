# Databricks notebook source
# MAGIC %sql
# MAGIC -- 1. Date Dimension
# MAGIC CREATE OR REPLACE TABLE hl7_hipaa.gold.dim_date AS
# MAGIC SELECT DISTINCT
# MAGIC   date_format(service_start_date,'yyyyMMdd') AS date_key,
# MAGIC   service_start_date                         AS full_date,
# MAGIC   year(service_start_date)                   AS year,
# MAGIC   month(service_start_date)                  AS month,
# MAGIC   quarter(service_start_date)                AS quarter,
# MAGIC   dayofweek(service_start_date)              AS day_of_week,
# MAGIC   weekofyear(service_start_date)             AS week_of_year
# MAGIC FROM hl7_hipaa.silver.claims_clean
# MAGIC WHERE service_start_date IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hl7_hipaa.gold.dim_date

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE hl7_hipaa.gold.dim_patient AS
# MAGIC SELECT
# MAGIC   p.patient_id,
# MAGIC   p.gender,
# MAGIC   p.birth_date,
# MAGIC   YEAR(p.birth_date)                             AS birth_year,
# MAGIC   YEAR(CURRENT_DATE()) - YEAR(p.birth_date)      AS age,
# MAGIC   p.age_group,
# MAGIC   COUNT(*)                                       AS total_encounters,        -- or COUNT(fc.claim_id)
# MAGIC   SUM(COALESCE(fc.encounter_cost, 0))            AS total_healthcare_cost,   -- use encounter_cost
# MAGIC   COUNT(fc.claim_id)                              AS total_claims,
# MAGIC   p.total_conditions,
# MAGIC   p.clinical_complexity_tier,
# MAGIC   p.cost_risk_tier
# MAGIC FROM hl7_hipaa.gold.patient_risk_profiles p
# MAGIC LEFT JOIN hl7_hipaa.gold.vw_healthcare_star_schema fc
# MAGIC   ON p.patient_id = fc.patient_id
# MAGIC GROUP BY
# MAGIC   p.patient_id,
# MAGIC   p.gender,
# MAGIC   p.birth_date,
# MAGIC   p.age_group,
# MAGIC   p.total_conditions,
# MAGIC   p.clinical_complexity_tier,
# MAGIC   p.cost_risk_tier;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hl7_hipaa.gold.dim_patient

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE hl7_hipaa.gold.dim_provider AS
# MAGIC SELECT
# MAGIC   pp.provider_id,
# MAGIC   pp.provider_name,
# MAGIC   COUNT(DISTINCT fc.patient_id)                         AS unique_patients_served,
# MAGIC   SUM(fc.encounter_cost)                                AS total_revenue,
# MAGIC   ROUND(
# MAGIC     SUM(fc.encounter_cost)
# MAGIC     / NULLIF(COUNT(DISTINCT fc.patient_id),0), 2
# MAGIC   )                                                     AS revenue_per_patient,
# MAGIC   COUNT(fc.claim_id)                                    AS total_claims_processed,
# MAGIC   ROUND(
# MAGIC     COUNT(fc.claim_id)
# MAGIC     / NULLIF(COUNT(DISTINCT fc.patient_id),0), 2
# MAGIC   )                                                     AS claims_per_patient,
# MAGIC   pp.revenue_tier,
# MAGIC   pp.avg_complexity_score
# MAGIC FROM hl7_hipaa.gold.provider_performance pp
# MAGIC LEFT JOIN hl7_hipaa.gold.vw_healthcare_star_schema fc
# MAGIC   ON pp.provider_id = fc.provider_id
# MAGIC GROUP BY
# MAGIC   pp.provider_id,
# MAGIC   pp.provider_name,
# MAGIC   pp.revenue_tier,
# MAGIC   pp.avg_complexity_score;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 7. Condition Dimension
# MAGIC CREATE OR REPLACE TABLE hl7_hipaa.gold.dim_condition AS
# MAGIC SELECT
# MAGIC   ROW_NUMBER() OVER (ORDER BY total_condition_cost_burden DESC) AS condition_key,
# MAGIC   condition_name,
# MAGIC   patients_affected,
# MAGIC   total_condition_cost_burden,
# MAGIC   avg_cost_per_patient,
# MAGIC   cost_impact_tier,
# MAGIC   prevalence_tier
# MAGIC FROM hl7_hipaa.gold.disease_analytics;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- fact_claims
# MAGIC CREATE OR REPLACE TABLE hl7_hipaa.gold.fact_claims AS
# MAGIC SELECT
# MAGIC   claim_id,
# MAGIC   patient_id,
# MAGIC   provider_id,
# MAGIC   date_format(service_start_date,'yyyyMMdd') AS service_date_key,
# MAGIC   claim_amount                                 AS claim_usd,   -- rename here
# MAGIC   1                                            AS claim_count
# MAGIC FROM hl7_hipaa.silver.claims_clean
# MAGIC WHERE patient_id IS NOT NULL
# MAGIC   AND claim_amount IS NOT NULL;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3. Encounters Fact (one row per encounter)
# MAGIC CREATE OR REPLACE TABLE hl7_hipaa.gold.fact_encounters AS
# MAGIC SELECT
# MAGIC   encounter_id,
# MAGIC   patient_id,
# MAGIC   provider_id,
# MAGIC   date_format(encounter_start_date,'yyyyMMdd') AS encounter_date_key,
# MAGIC   1                                            AS encounter_count
# MAGIC FROM hl7_hipaa.silver.encounters_clean
# MAGIC WHERE patient_id IS NOT NULL
# MAGIC   AND encounter_start_date IS NOT NULL;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 8. Readmission Risk Fact
# MAGIC CREATE OR REPLACE TABLE hl7_hipaa.gold.fact_readmission_risk AS
# MAGIC SELECT
# MAGIC   patient_id,
# MAGIC   readmissions_30_days,
# MAGIC   total_cost,
# MAGIC   comorbidity_score,
# MAGIC   annual_encounter_rate,
# MAGIC   readmission_risk_tier,
# MAGIC   cost_risk_tier,
# MAGIC   CASE WHEN readmission_risk_tier = 'Critical Risk' THEN 1 ELSE 0 END AS is_critical_risk
# MAGIC FROM hl7_hipaa.gold.readmission_risk_model;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 4. Star Schema View (enrich claims with dims and cost)
# MAGIC CREATE OR REPLACE VIEW hl7_hipaa.gold.vw_healthcare_star_schema AS
# MAGIC SELECT
# MAGIC   fc.claim_id,
# MAGIC   fc.patient_id,
# MAGIC   fc.provider_id,
# MAGIC   fc.service_date_key,
# MAGIC   fc.claim_usd                AS encounter_cost,
# MAGIC   dp.age_group,
# MAGIC   dp.cost_risk_tier,
# MAGIC   dp.clinical_complexity_tier,
# MAGIC   dp.total_conditions,
# MAGIC   dpr.provider_name,
# MAGIC   dpr.revenue_tier,
# MAGIC   dd.full_date,
# MAGIC   dd.year,
# MAGIC   dd.month,
# MAGIC   dd.quarter,
# MAGIC   COALESCE(frr.readmissions_30_days,0) AS readmissions_30_days,
# MAGIC   COALESCE(frr.comorbidity_score,0)    AS comorbidity_score,
# MAGIC   frr.readmission_risk_tier,
# MAGIC   frr.is_critical_risk
# MAGIC FROM hl7_hipaa.gold.fact_claims fc
# MAGIC JOIN hl7_hipaa.gold.dim_date dd
# MAGIC   ON fc.service_date_key = dd.date_key
# MAGIC JOIN hl7_hipaa.gold.dim_patient dp
# MAGIC   ON fc.patient_id = dp.patient_id
# MAGIC JOIN hl7_hipaa.gold.dim_provider dpr
# MAGIC   ON fc.provider_id = dpr.provider_id
# MAGIC LEFT JOIN hl7_hipaa.gold.fact_readmission_risk frr
# MAGIC   ON fc.patient_id = frr.patient_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check the star schema structure
# MAGIC SHOW TABLES IN hl7_hipaa.gold;
# MAGIC
# MAGIC -- Test the comprehensive view
# MAGIC SELECT COUNT(*) as total_records,
# MAGIC        COUNT(DISTINCT patient_id) as unique_patients,
# MAGIC        COUNT(DISTINCT provider_id) as unique_providers,
# MAGIC        SUM(claim_amount) as total_claims_value
# MAGIC FROM hl7_hipaa.gold.vw_healthcare_star_schema;
# MAGIC

# COMMAND ----------

# Check cluster status and connection
print("=== CLUSTER STATUS CHECK ===")
print(f"Current workspace: {spark.conf.get('spark.databricks.workspaceUrl')}")
print("If you see this output, your cluster is running ✅")

# Test access to our tables
try:
    spark.sql("USE CATALOG hl7_hipaa")
    spark.sql("USE SCHEMA gold") 
    tables = spark.sql("SHOW TABLES").collect()
    print(f"\n✅ Available tables in hl7_hipaa.gold:")
    for table in tables:
        print(f"  - {table.tableName}")
except Exception as e:
    print(f"❌ Error accessing tables: {e}")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hl7_hipaa.gold.patient_risk_profiles

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS silver_claim_count
# MAGIC FROM hl7_hipaa.silver.claims_clean;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS gold_claim_count
# MAGIC FROM hl7_hipaa.gold.fact_claims;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ROUND(SUM(claim_amount),2) AS silver_total_cost
# MAGIC FROM hl7_hipaa.silver.claims_clean;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ROUND(SUM(claim_usd),2) AS gold_total_cost
# MAGIC FROM hl7_hipaa.gold.fact_claims;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Silver
# MAGIC SELECT
# MAGIC   COUNT(*) AS silver_claims,
# MAGIC   ROUND(SUM(claim_amount),2) AS silver_cost
# MAGIC FROM hl7_hipaa.silver.claims_clean
# MAGIC WHERE patient_id = '1234';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Gold
# MAGIC SELECT
# MAGIC   total_claims,
# MAGIC   ROUND(total_healthcare_cost,2) AS gold_cost
# MAGIC FROM hl7_hipaa.gold.dim_patient
# MAGIC WHERE patient_id = '1234';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Silver join
# MAGIC SELECT
# MAGIC   COUNT(*) AS silver_claims,
# MAGIC   ROUND(SUM(c.claim_amount),2) AS silver_revenue
# MAGIC FROM hl7_hipaa.silver.claims_clean c
# MAGIC WHERE c.provider_name = 'Dr. Smith';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Gold
# MAGIC SELECT
# MAGIC   total_claims_processed,
# MAGIC   ROUND(total_revenue,2) AS gold_revenue
# MAGIC FROM hl7_hipaa.gold.dim_provider
# MAGIC WHERE provider_name = 'Dr. Smith';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Recompute in Silver
# MAGIC WITH diffs AS (
# MAGIC   SELECT
# MAGIC     patient_id,
# MAGIC     DATEDIFF(
# MAGIC       encounter_start_date,
# MAGIC       LAG(encounter_start_date) OVER (PARTITION BY patient_id ORDER BY encounter_start_date)
# MAGIC     ) AS gap
# MAGIC   FROM hl7_hipaa.silver.encounters_clean
# MAGIC )
# MAGIC SELECT
# MAGIC   COUNT(*) FILTER (WHERE gap BETWEEN 1 AND 30) AS silver_readmissions_30d
# MAGIC FROM diffs;
# MAGIC
# MAGIC -- Gold
# MAGIC SELECT
# MAGIC   SUM(readmissions_30_days) AS gold_readmissions_30d
# MAGIC FROM hl7_hipaa.gold.fact_readmission_risk;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Silver benchmark: join conditions to gold view
# MAGIC SELECT
# MAGIC   cond.condition_description,
# MAGIC   ROUND(SUM(v.encounter_cost),2) AS cost_burden
# MAGIC FROM hl7_hipaa.silver.conditions_clean cond
# MAGIC JOIN hl7_hipaa.gold.vw_healthcare_star_schema v
# MAGIC   ON cond.patient_id = v.patient_id
# MAGIC GROUP BY cond.condition_description
# MAGIC ORDER BY cost_burden DESC
# MAGIC LIMIT 1;
# MAGIC
# MAGIC -- Gold
# MAGIC SELECT condition_name
# MAGIC FROM hl7_hipaa.gold.disease_analytics
# MAGIC ORDER BY total_condition_cost_burden DESC
# MAGIC LIMIT 1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Silver raw claims
# MAGIC SELECT
# MAGIC   COUNT(*)            AS silver_claim_count,
# MAGIC   ROUND(SUM(claim_amount),2) AS silver_total_cost
# MAGIC FROM hl7_hipaa.silver.claims_clean;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Gold fact_claims
# MAGIC SELECT
# MAGIC   COUNT(*)            AS gold_claim_count,
# MAGIC   ROUND(SUM(claim_usd),2)   AS gold_total_cost
# MAGIC FROM hl7_hipaa.gold.fact_claims;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Silver encounters
# MAGIC SELECT
# MAGIC   COUNT(*)        AS silver_encounter_count
# MAGIC FROM hl7_hipaa.silver.encounters_clean;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Gold fact_encounters
# MAGIC SELECT
# MAGIC   COUNT(*)        AS gold_encounter_count
# MAGIC FROM hl7_hipaa.gold.fact_encounters;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Silver patients
# MAGIC SELECT COUNT(*) AS silver_patient_count
# MAGIC FROM hl7_hipaa.silver.patients_clean;
# MAGIC
# MAGIC -- Gold patient dimension
# MAGIC SELECT COUNT(*) AS gold_patient_count
# MAGIC FROM hl7_hipaa.gold.dim_patient;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Silver distinct providers
# MAGIC SELECT COUNT(DISTINCT provider_id) AS silver_provider_count
# MAGIC FROM hl7_hipaa.silver.claims_clean
# MAGIC WHERE provider_id IS NOT NULL;
# MAGIC
# MAGIC -- Gold provider dimension
# MAGIC SELECT COUNT(*) AS gold_provider_count
# MAGIC FROM hl7_hipaa.gold.dim_provider;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Silver distinct condition descriptions
# MAGIC SELECT COUNT(DISTINCT TRIM(condition_description)) AS silver_condition_count
# MAGIC FROM hl7_hipaa.silver.conditions_clean
# MAGIC WHERE TRIM(condition_description) <> '';
# MAGIC
# MAGIC -- Gold condition dimension
# MAGIC SELECT COUNT(*) AS gold_condition_count
# MAGIC FROM hl7_hipaa.gold.dim_condition;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Gold readmission counts
# MAGIC SELECT SUM(readmissions_30_days) AS gold_readmissions_30d
# MAGIC FROM hl7_hipaa.gold.fact_readmission_risk;
# MAGIC
# MAGIC -- Silver recompute
# MAGIC WITH diffs AS (
# MAGIC   SELECT
# MAGIC     patient_id,
# MAGIC     DATEDIFF(
# MAGIC       encounter_start_date,
# MAGIC       LAG(encounter_start_date) OVER (
# MAGIC         PARTITION BY patient_id ORDER BY encounter_start_date
# MAGIC       )
# MAGIC     ) AS gap
# MAGIC   FROM hl7_hipaa.silver.encounters_clean
# MAGIC )
# MAGIC SELECT COUNT(*) FILTER (WHERE gap BETWEEN 1 AND 30) AS silver_readmissions_30d
# MAGIC FROM diffs;
# MAGIC
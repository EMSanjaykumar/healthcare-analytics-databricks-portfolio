# Databricks notebook source
from pyspark.sql.functions import col, regexp_extract, to_date, when, isnan, isnull

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog hl7_hipaa

# COMMAND ----------



# Clean patients table
patients_clean = spark.table("hl7_hipaa.silver.patients").select(
    col("id").alias("patient_id"),
    to_date(col("birthDate")).alias("birth_date"),
    col("gender"),
    col("address"),
    col("telecom"),
    col("extension")
).filter(col("birthDate").isNotNull())

# Clean claims table with extracted cost values
claims_clean = spark.table("hl7_hipaa.silver.claims").select(
    col("id").alias("claim_id"),
    regexp_extract(col("patient_reference"), r"urn:uuid:(.+)", 1).alias("patient_id"),
    col("patient_display").alias("patient_name"),
    to_date(col("start")).alias("service_start_date"),  # Fixed column name
    to_date(col("end")).alias("service_end_date"),      # Fixed column name
    to_date(col("created")).alias("claim_created_date"),
    regexp_extract(col("provider_reference"), r"urn:uuid:(.+)", 1).alias("provider_id"),
    col("provider_display").alias("provider_name"),
    col("insurer_display").alias("insurer_name"),
    regexp_extract(col("total"), r'"value":([0-9.]+)', 1).cast("double").alias("claim_amount"),
    regexp_extract(col("total"), r'"currency":"([A-Z]+)"', 1).alias("currency"),
    col("status"),
    col("use"),
    col("diagnosis"),
    col("item")
).filter(col("patient_reference").isNotNull())

# Clean encounters table
encounters_clean = spark.table("hl7_hipaa.silver.encounters").select(
    col("id").alias("encounter_id"),
    regexp_extract(col("subject_reference"), r"urn:uuid:(.+)", 1).alias("patient_id"),
    col("subject_display").alias("patient_name"),
    to_date(col("start")).alias("encounter_start_date"),  # Fixed column name
    to_date(col("end")).alias("encounter_end_date"),      # Fixed column name
    col("status"),
    col("class_code"),
    col("type"),
    regexp_extract(col("serviceProvider_reference"), r"urn:uuid:(.+)", 1).alias("provider_id"),
    col("serviceProvider_display").alias("provider_name"),
    col("reasonCode")
).filter(col("subject_reference").isNotNull())

# Clean conditions table
conditions_clean = spark.table("hl7_hipaa.silver.conditions").select(
    col("id").alias("condition_id"),
    regexp_extract(col("subject_reference"), r"urn:uuid:(.+)", 1).alias("patient_id"),
    col("subject_display").alias("patient_name"),
    regexp_extract(col("encounter_reference"), r"urn:uuid:(.+)", 1).alias("encounter_id"),
    col("code_coding"),
    col("text").alias("condition_description"),  # Fixed column name
    to_date(col("onsetDateTime")).alias("condition_onset_date"),
    to_date(col("recordedDate")).alias("condition_recorded_date")
).filter(col("subject_reference").isNotNull())

print("✓ Silver data cleaning completed")


# COMMAND ----------

# Save cleaned silver tables
patients_clean.write.mode("overwrite").saveAsTable("hl7_hipaa.silver.patients_clean")
claims_clean.write.mode("overwrite").saveAsTable("hl7_hipaa.silver.claims_clean") 
encounters_clean.write.mode("overwrite").saveAsTable("hl7_hipaa.silver.encounters_clean")
conditions_clean.write.mode("overwrite").saveAsTable("hl7_hipaa.silver.conditions_clean")

print("✓ Clean silver tables created")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify all clean silver tables exist and have data
# MAGIC SELECT 'patients_clean' as table_name, COUNT(*) as record_count FROM hl7_hipaa.silver.patients_clean
# MAGIC UNION ALL
# MAGIC SELECT 'claims_clean' as table_name, COUNT(*) as record_count FROM hl7_hipaa.silver.claims_clean
# MAGIC UNION ALL  
# MAGIC SELECT 'encounters_clean' as table_name, COUNT(*) as record_count FROM hl7_hipaa.silver.encounters_clean
# MAGIC UNION ALL
# MAGIC SELECT 'conditions_clean' as table_name, COUNT(*) as record_count FROM hl7_hipaa.silver.conditions_clean
# MAGIC ORDER BY table_name;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sample the clean claims data to see extracted amounts
# MAGIC SELECT claim_id, patient_id, claim_amount, currency, service_start_date, provider_name 
# MAGIC FROM hl7_hipaa.silver.claims_clean 
# MAGIC WHERE claim_amount IS NOT NULL 
# MAGIC ORDER BY claim_amount DESC 
# MAGIC LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG hl7_hipaa;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VIEWS IN hl7_hipaa.gold;
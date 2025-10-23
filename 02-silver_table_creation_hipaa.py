# Databricks notebook source
from pyspark.sql.functions import explode

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog hl7_hipaa

# COMMAND ----------

# Read raw data
bronze_data_path = "abfss://bronze-hipaa@hl7hipaa.dfs.core.windows.net/"
df_all = spark.read.option("multiLine", True).json(bronze_data_path + "*.json")
print(f"Total entries loaded: {df_all.count()}")

# COMMAND ----------

# Explode entries and get resources
entries_df = df_all.select(explode("entry").alias("entry"))
resources_df = entries_df.select("entry.resource.*")

print(f"Total resources: {resources_df.count()}")

# COMMAND ----------

# Create patients DataFrame
patients_df = resources_df.filter(resources_df.resourceType == "Patient").select(
    "id",
    "birthDate", 
    "gender",
    "maritalStatus.coding",
    "address",
    "telecom",
    "extension"
)

# Save to silver layer tables
patients_df.write.mode("overwrite").saveAsTable("hl7_hipaa.silver.patients")
print("✓ Silver table: patients created")

# COMMAND ----------

# Create claims DataFrame
claims_df = (
    resources_df
    .filter(resources_df.resourceType == "Claim")
    .select(
        "id",
        resources_df["patient.reference"].alias("patient_reference"),
        resources_df["patient.display"].alias("patient_display"),
        "billablePeriod.start",
        "billablePeriod.end",
        "created",
        resources_df["provider.reference"].alias("provider_reference"),
        resources_df["provider.display"].alias("provider_display"),
        resources_df["insurer.display"].alias("insurer_display"),
        "total",
        "status",
        "use",
        "priority.coding",
        "diagnosis",
        "procedure",
        "item"
    )
)

claims_df.write.mode("overwrite").saveAsTable("hl7_hipaa.silver.claims")
display(claims_df)

# COMMAND ----------

# Create encounters DataFrame
encounters_df = (
    resources_df
    .filter(resources_df.resourceType == "Encounter")
    .select(
        "id",
        resources_df["subject.reference"].alias("subject_reference"),
        resources_df["subject.display"].alias("subject_display"),
        "period.start",
        "period.end",
        "status",
        resources_df["class.code"].alias("class_code"),
        resources_df["class.system"].alias("class_system"),
        "type",
        "participant",
        resources_df["serviceProvider.reference"].alias("serviceProvider_reference"),
        resources_df["serviceProvider.display"].alias("serviceProvider_display"),
        "hospitalization",
        "reasonCode"
    )
)

encounters_df.write.mode("overwrite").saveAsTable("hl7_hipaa.silver.encounters")
display(encounters_df)
print("✓ Silver table: encounters created")

# COMMAND ----------

conditions_df = (
    resources_df
    .filter(resources_df.resourceType == "Condition")
    .select(
        "id",
        resources_df["subject.reference"].alias("subject_reference"),
        resources_df["subject.display"].alias("subject_display"),
        resources_df["encounter.reference"].alias("encounter_reference"),
        resources_df["code.coding"].alias("code_coding"),
        "code.text",
        resources_df["clinicalStatus.coding"].alias("clinicalStatus_coding"),
        resources_df["verificationStatus.coding"].alias("verificationStatus_coding"),
        "onsetDateTime",
        "recordedDate",
        "category"
    )
)

conditions_df.write.mode("overwrite").saveAsTable("hl7_hipaa.silver.conditions")
print("✓ Silver table: conditions created")

print("\n🎉 SUCCESS: Silver layer tables created!")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify all silver tables exist
# MAGIC SHOW TABLES IN hl7_hipaa.silver;
# MAGIC
# MAGIC -- Check record counts for all tables
# MAGIC SELECT 'patients' as table_name, COUNT(*) as record_count FROM hl7_hipaa.silver.patients
# MAGIC UNION ALL
# MAGIC SELECT 'claims' as table_name, COUNT(*) as record_count FROM hl7_hipaa.silver.claims
# MAGIC UNION ALL  
# MAGIC SELECT 'encounters' as table_name, COUNT(*) as record_count FROM hl7_hipaa.silver.encounters
# MAGIC UNION ALL
# MAGIC SELECT 'conditions' as table_name, COUNT(*) as record_count FROM hl7_hipaa.silver.conditions
# MAGIC ORDER BY table_name;
# MAGIC
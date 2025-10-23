# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog hl7_hipaa;

# COMMAND ----------

# Replace with your actual container, storage account, and (optionally) subdirectory

spark.sql("CREATE SCHEMA IF NOT EXISTS hl7_hipaa.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS hl7_hipaa.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS hl7_hipaa.gold")

print("✓ Unity Catalog structure created")

# COMMAND ----------

bronze_data_path = "abfss://bronze-hipaa@hl7hipaa.dfs.core.windows.net/"
df_all = spark.read.option("multiLine", True).json(bronze_data_path + "*.json")

# Check total volume
print(f"Total entries loaded: {df_all.count()}")

# Explode entries and get resources
from pyspark.sql.functions import explode
entries_df = df_all.select(explode("entry").alias("entry"))
resources_df = entries_df.select("entry.resource.*")

print(f"Total resources: {resources_df.count()}")


# COMMAND ----------

# See what types of resources you have
resources_df.groupBy("resourceType").count().orderBy("count", ascending=False).show()


# COMMAND ----------

# Extract patient demographics - foundation for all analytics
patients_df = resources_df.filter(resources_df.resourceType == "Patient").select(
    "id",
    "birthDate", 
    "gender",
    "maritalStatus.coding",
    "address",
    "telecom",
    "extension"  # Contains race, ethnicity, QALY, DALY metrics
)

patients_df.show(5, truncate=False)
print(f"Total patients: {patients_df.count()}")


# COMMAND ----------

# Check the actual schema of Claims data
claims_sample = resources_df.filter(resources_df.resourceType == "Claim")
claims_sample.printSchema()


# COMMAND ----------

# Extract claims data - corrected field references
claims_df = resources_df.filter(resources_df.resourceType == "Claim").select(
    "id",
    "patient.reference",        # Patient reference (correct path)
    "patient.display",          # Patient display name
    "billablePeriod.start",     # Service start date
    "billablePeriod.end",       # Service end date
    "created",                  # Claim creation date
    "provider.reference",       # Provider who submitted claim
    "provider.display",         # Provider display name
    "insurer.display",          # Insurance company name
    "total",                    # Total claim amount (note: this is just string in schema)
    "status",                   # Claim status
    "use",                      # Claim use (preauthorization, claim, etc.)
    "priority.coding",          # Priority level
    "diagnosis",                # Diagnosis codes
    "procedure",                # Procedure codes
    "item"                      # Individual line items with costs
)

claims_df.show(3, truncate=False)
print(f"Total claims: {claims_df.count()}")


# COMMAND ----------

# Extract encounters - critical for readmission prediction, length of stay
encounters_df = resources_df.filter(resources_df.resourceType == "Encounter").select(
    "id",
    "subject.reference",        # Patient reference
    "subject.display",          # Patient display name
    "period.start",             # Admission date/time
    "period.end",               # Discharge date/time
    "status",                   # finished, in-progress, etc.
    "class.code",               # EMER, INPATIENT, OUTPATIENT, etc.
    "class.system",             # Coding system
    "type",                     # Encounter type codes
    "participant",              # Providers involved
    "serviceProvider.reference", # Hospital/Organization reference
    "serviceProvider.display",  # Hospital name
    "hospitalization",          # Admission/discharge details
    "reasonCode"                # Reason for encounter
)

encounters_df.show(3, truncate=False)
print(f"Total encounters: {encounters_df.count()}")


# COMMAND ----------

# Extract conditions/diagnoses - for disease trends, comorbidity analysis, risk modeling
conditions_df = resources_df.filter(resources_df.resourceType == "Condition").select(
    "id",
    "subject.reference",        # Patient reference
    "subject.display",          # Patient display name
    "encounter.reference",      # Which encounter this was diagnosed in
    "code.coding",              # ICD/SNOMED diagnosis codes
    "code.text",                # Diagnosis description
    "clinicalStatus.coding",    # Active, resolved, etc.
    "verificationStatus.coding", # Confirmed, provisional, etc.
    "onsetDateTime",            # When condition started
    "recordedDate",             # When condition was documented
    "category"                  # Problem list item, encounter diagnosis, etc.
)

conditions_df.show(3, truncate=False)
print(f"Total conditions: {conditions_df.count()}")


# COMMAND ----------


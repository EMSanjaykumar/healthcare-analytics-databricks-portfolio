# Databricks notebook source
from pyspark.sql.functions import explode

# COMMAND ----------

dbutils.fs.ls("abfss://bronze-hipaa@hl7hipaa.dfs.core.windows.net/")

# COMMAND ----------

bronze_data_path = "abfss://bronze-hipaa@hl7hipaa.dfs.core.windows.net/"
df = spark.read.option("multiLine", True).json(bronze_data_path + "Aaron697_Brekke496_2fa15bc7-8866-461a-9000-f739e425860a.json")


# COMMAND ----------

entries_df = df.select(explode("entry").alias("entry"))
# Extract the resource object from each entry
resources_df = entries_df.select("entry.resource.*")



# COMMAND ----------

resources_df.show(5, truncate=False)
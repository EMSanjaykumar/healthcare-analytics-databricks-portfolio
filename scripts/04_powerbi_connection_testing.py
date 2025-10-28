# Databricks notebook source
# Get connection details for Power BI
print("=== DATABRICKS CONNECTION INFO FOR POWER BI ===")
print(f"Server Hostname: {spark.conf.get('spark.databricks.workspaceUrl')}")
print("HTTP Path: Go to your cluster → Advanced Options → JDBC/ODBC → HTTP Path")
print("Database: hl7_hipaa")
print("Schema: gold")
print("\n✅ Views ready for Power BI connection:")
print("- pbi_patient_dashboard")
print("- pbi_provider_performance") 
print("- pbi_disease_analytics")
print("- pbi_readmission_risk")
print("- pbi_executive_summary")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test each view has data
# MAGIC SELECT 'pbi_patient_dashboard' as view_name, COUNT(*) as record_count FROM hl7_hipaa.gold.pbi_patient_dashboard
# MAGIC UNION ALL
# MAGIC SELECT 'pbi_provider_performance' as view_name, COUNT(*) as record_count FROM hl7_hipaa.gold.pbi_provider_performance  
# MAGIC UNION ALL
# MAGIC SELECT 'pbi_disease_analytics' as view_name, COUNT(*) as record_count FROM hl7_hipaa.gold.pbi_disease_analytics
# MAGIC UNION ALL
# MAGIC SELECT 'pbi_readmission_risk' as view_name, COUNT(*) as record_count FROM hl7_hipaa.gold.pbi_readmission_risk
# MAGIC UNION ALL
# MAGIC SELECT 'pbi_executive_summary' as view_name, COUNT(*) as record_count FROM hl7_hipaa.gold.pbi_executive_summary;
# MAGIC
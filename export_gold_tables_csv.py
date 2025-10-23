# Databricks notebook source
# Export final datasets as backup (for post-trial showcase)
export_tables = {
    "patient_risk_profiles": "hl7_hipaa.gold.patient_risk_profiles",
    "provider_performance": "hl7_hipaa.gold.provider_performance",
    "disease_analytics": "hl7_hipaa.gold.disease_analytics", 
    "readmission_risk": "hl7_hipaa.gold.readmission_risk_model",
    "executive_kpis": "hl7_hipaa.gold.executive_kpi_dashboard"
}

for name, table in export_tables.items():
    df = spark.table(table)
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"/tmp/portfolio_data/{name}")
    print(f"✅ Exported {name} for portfolio")

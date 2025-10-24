# Sanjaykumar Enakarla Mohan — Healthcare Analytics with Azure Databricks

![Azure](https://img.shields.io/badge/Azure-blue?logo=microsoft-azure)
![Databricks](https://img.shields.io/badge/Databricks-red?logo=databricks)
![SQL](https://img.shields.io/badge/SQL-blue?logo=sqlite)
![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)

## 🚀 Turning untapped healthcare claims into actionable cost and risk insights for executives, clinicians & analysts.

---
**Quick Links:** [Download Executive Dashboard (PDF)](screenshots/Executive_Summary_Dashboard.pdf) | [LinkedIn](https://linkedin.com/in/sanjaykem) | [GitHub Repo](https://github.com/EMSanjaykumar/healthcare-analytics-databricks-portfolio)

---

**LinkedIn:** [linkedin.com/in/sanjaykem](https://www.linkedin.com/in/sanjaykem)  
**Email:** [sanjaykumar.em0609@gmail.com](mailto:sanjaykumar.em0609@gmail.com)

---

> **End-to-end analytics from HL7 data landing to clinical cost dashboards — built 100% on Databricks and Azure, from ETL pipelines to live Genie dashboards.**
> _Specializing in compliance, claims analytics, and unlocking risk intelligence with Databricks automation._

---

## Table of Contents
- [About Me](#about-me)
- [Featured Project Scenarios](#featured-project-scenarios)
- [Executive Dashboard](#executive-dashboard)
- [Architecture and Cloud Setup](#architecture-and-cloud-setup)
- [Analytics and Insight Dashboards](#analytics-and-insight-dashboards)
- [Business Impact](#business-impact)
- [Sample Databricks Transformation Notebook](#sample-databricks-transformation-notebook)
- [Glossary](#glossary)
- [Documentation](#documentation)
- [Contact and Links](#contact-and-links)
- [License and Dataset Attribution](#license-and-dataset-attribution)

---

## About Me

**Data & BI Analyst** with 5+ years' experience in banking/financial and US healthcare analytics.

- **Expert in:** Databricks, Azure, SQL, Python, HL7/FHIR healthcare data
- **Core Skills:** ETL, dashboard automation, stakeholder reporting, Delta Lake pipelines, audit/compliance analytics
- **Certifications:** Databricks Fundamentals, Microsoft Power BI Data Analyst (PL-300), Google Data Analytics

---

## Featured Project Scenarios

### Scenario 1: HL7 Claims ETL & Population Health Insights (All in Databricks)

**Challenge:**  
Ingest and unify 1000+ HL7-format claims for actionable, audit-ready executive analysis — no third-party tools.

**Solution:**  
- Automated ETL from raw JSON files to Unified Tables (Bronze → Silver → Gold) using PySpark and Delta in Databricks
- Genie dashboards for real-time KPIs, patient cost, risk, provider revenue, and readmission stratification

**Impact:**  
- Categorized $8M high-risk spend, enabled patient prioritization for population health teams
- Reduced analytic cycle time by 70%

**Stack:**  
Databricks, Azure Free Subscription, HL7 JSON, Delta Lake, PySpark

**Validated On:**  
Azure Free Subscription (azure_subscription 1), Databricks Community Edition

---

### Scenario 2: Real-Time HL7 Data Quality & Readmission Monitoring

**Challenge:**  
Build a live, ETL-integrated readmission/outcomes risk dashboard — without leaving Databricks.

**Solution:**  
- Designed streaming-compatible Delta tables for ongoing data ingestion
- Created Genie “risk overlay” visualizations to surface acute readmissions and track trending cost conditions

**Impact:**  
- Continuous risk monitoring for all new HL7 batches
- Enabled “triage view” for clinicians — cut manual spreadsheet analysis to zero

**Stack:**  
Databricks (Genie Dashboards), Delta Lake, PySpark, HL7 JSON

**Validated On:**  
Azure Databricks (Free/Community), real HL7 test data

---

## Executive Dashboard

![Genie Enabled Dashboard](screenshots/genie_enabled_dashboard_hd.png)  
**[Download Executive Summary Dashboard PDF](screenshots/Executive_Summary_Dashboard.pdf)**

| KPI                        | Value                  |
|----------------------------|------------------------|
| **Total Patients**         | 983                    |
| **Total Encounters**       | 38,450                 |
| **Total Claims**           | 49,340                 |
| **Healthcare Spend**       | $12,625,196.83         |
| **High Cost Patients**     | 871                    |
| **Critical Risk Patients** | 617                    |
| **Max Claim Amount**       | $7,001.71              |
| **Top Provider**           | CLINTON HOSPITAL ASSOC |
| **High Cost Condition**    | Viral sinusitis        |

---

## Architecture and Cloud Setup

- **Medallion Architecture:** Bronze/Silver/Gold (all Delta Lake in Databricks)
- **End-to-End HL7 Dataflow:**  
  ![HL7 End-to-End Architecture](screenshots/hl7_end_to_end_architecture_hd.png)
- **ETL Pipeline:**  
  ![HL7 Data Pipeline](screenshots/hl7_data_pipeline_hd.png)
- **Azure Resource Groups/Blob Storage:**  
  ![Resource Group](screenshots/azure_resource_group_hd.png)
  ![Blob Gen2 Containers](screenshots/azure_blob_storage_containers_hd.png)
- **Databricks Workspace Example:**  
  ![Workspace Items](screenshots/workspace_items_hd.png)

---

## Analytics and Insight Dashboards

### Provider Performance  
- **[Provider Performance Analysis PDF](screenshots/Provider_Performance_Analysis.pdf)**
### Patient Risk and Comorbidity  
- **[Patient Risk & Comorbidity Analysis PDF](screenshots/Patient_Risk_Comorbidity_Analysis_Dashbboard.pdf)**
### Disease and Condition Burden  
- **[Condition Burden Dashboard PDF](screenshots/Disease_Condition_Burden_Dashboard.pdf)**
### Readmission and Outcomes  
- **[Readmission Outcomes Dashboard PDF](screenshots/Readmission-Outcomes-Analytics-Dashboard.pdf)**
### Analytics Dataset Example  
![Analytics Dataset View](screenshots/dashboard_dataset_view_hd.png)

---

## Business Impact

- Enabled executives to reallocate millions and prioritize interventions using live Databricks dashboards
- Deployed critical risk alerting (Genie Dashboards), eliminating Excel/manual checks
- Supported audit/regulatory review with gold-layer Delta lineage

---

## Sample Databricks Transformation Notebook





# Clean HL7 claims and aggregate patient spend
df = raw_df.filter(raw_df.claim_status == "Valid")
agg = df.groupBy("patient_id").agg(sum("total_spend").alias("patient_spend"))
display(agg.orderBy(desc("patient_spend")))




---


---

## Glossary

- **HL7:** Healthcare messaging & data standard
- **Delta Lake:** Multi-tiered Databricks storage, Bronze/Silver/Gold
- **Genie Dashboards:** Built-in Databricks dashboarding (all visuals made in Genie UI)
- **Critical Risk Patient:** High spend + high readmission risk identified via ETL rules

---

## Documentation

**Downloadable Dashboards:**  
- [Executive Summary PDF](screenshots/Executive_Summary_Dashboard.pdf)
- [Provider Performance PDF](screenshots/Provider_Performance_Analysis.pdf)
- [Risk & Comorbidity PDF](screenshots/Patient_Risk_Comorbidity_Analysis_Dashbboard.pdf)
- [Condition Burden PDF](screenshots/Disease_Condition_Burden_Dashboard.pdf)
- [Readmission Outcomes PDF](screenshots/Readmission-Outcomes-Analytics-Dashboard.pdf)

**Sample Workspace:**  
![Workspace](screenshots/workspace_items_hd.png)

[Visit Full GitHub Repository](https://github.com/EMSanjaykumar/healthcare-analytics-databricks-portfolio)

---

## Contact and Links

- **Name:** Sanjaykumar Enakarla Mohan
- **LinkedIn:** [linkedin.com/in/sanjaykem](https://www.linkedin.com/in/sanjaykem)
- **Email:** [sanjaykumar.em0609@gmail.com](mailto:sanjaykumar.em0609@gmail.com)

---

## License and Dataset Attribution

_This portfolio is for demonstration and educational purposes only._  
_Data based on [Kaggle FHIR/HL7 Sample Dataset](https://www.kaggle.com/datasets/drscarlat/fhir-1ksample) — strictly de-identified and adapted for public analytics._  
_No PHI/PII is used._  
_Validated on Azure Free Subscription and Databricks Community Edition; all ETL, analysis, and dashboards produced end-to-end in Databricks._

---

**Interested in Databricks-centric healthcare analytics or want to connect?**  
[Contact me](mailto:sanjaykumar.em0609@gmail.com) or connect on [LinkedIn](https://linkedin.com/in/sanjaykem).


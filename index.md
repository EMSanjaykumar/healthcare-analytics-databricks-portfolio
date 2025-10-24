# Healthcare Analytics with Databricks & Azure

Welcome! This portfolio showcases a comprehensive healthcare analytics project built in **Azure Databricks** and the Azure cloud—from ETL automation to clinical/finance analytics. Stakeholders and recruiters can explore project impact, dashboards, and technical skills.

---

## Table of Contents
- [Executive Summary](#executive-summary)
- [Executive Dashboard](#executive-dashboard)
- [Architecture and Cloud Setup](#architecture-and-cloud-setup)
- [Analytics and Insight Dashboards](#analytics-and-insight-dashboards)
- [Business Impact](#business-impact)
- [Sample Databricks Transformation Notebook](#sample-databricks-transformation-notebook)
- [Glossary](#glossary)
- [Project Artifacts and Documentation](#project-artifacts-and-documentation)
- [Contact and Links](#contact-and-links)
- [License Disclaimer](#license-disclaimer)

---

## Executive Summary

> **Key Achievements:**  
> - Automated dataflow for 983 patients and $12M spend  
> - Reduced reporting cycle by 80%  
> - Flagged 617 critical-risk patients

- **Automated ETL/ELT:** HL7 claims/encounters, cost data (Databricks)
- **Analytics:** Patient risk, provider benchmarking, high-priority conditions, business ROI
- **Compliance:** Full HL7 & HIPAA audit trail
- **Reporting:** Real-time, exec-focused dashboards

**Key Metrics:**

| KPI                        | Value                  |
|----------------------------|------------------------|
| **Total Patients**         | 983                    |
| **Total Encounters**       | 38,450                 |
| **Total Claims**           | 49,340                 |
| **Healthcare Spend**       | $12,625,196.83         |
| **High Cost Patients**     | 871                    |
| **Critical Risk Patients** | 617                    |
| **Top Provider**           | CLINTON HOSPITAL ASSOC |
| **Max Claim Amount**       | $7,001.71              |
| **High Cost Condition**    | Viral sinusitis        |

---

## Executive Dashboard

![Genie Enabled Dashboard](screenshots/genie_enabled_dashboard_hd.png)

**[Download PDF: Executive_Summary_Dashboard.pdf](screenshots/Executive_Summary_Dashboard.pdf)**

---

## Architecture and Cloud Setup

- **ETL Layers:** Bronze/Silver/Gold (Delta Lake)
- **End-to-End HL7 Architecture:**  
  ![End-to-End HL7 Architecture](screenshots/hl7_end_to_end_architecture_hd.png)
- **ETL Pipeline:**  
  ![HL7 Data Pipeline](screenshots/hl7_data_pipeline_hd.png)
- **Cloud Resources:**  
  ![Azure Resource Group](screenshots/azure_resource_group_hd.png)  
  ![Blob Storage Containers](screenshots/azure_blob_storage_containers_hd.png)
- **Databricks Workspace:**  
  ![Main Workspace View](screenshots/workspace_hd.png)
  ![Workspace Items](screenshots/workspace_items_hd.png) <!-- Ensure this file exists and matches case/extension! -->

---

## Analytics and Insight Dashboards

### Provider Performance
- Pinpoints high-revenue and high-complexity providers.
- **[Download PDF: Provider_Performance_Analysis.pdf](screenshots/Provider_Performance_Analysis.pdf)**

### Patient Risk and Comorbidity
- Identifies critical/high-cost patient segments.
- **[Download PDF: Patient_Risk_Comorbidity_Analysis_Dashbboard.pdf](screenshots/Patient_Risk_Comorbidity_Analysis_Dashbboard.pdf)**

### Disease and Condition Burden
- Highest cost & prevalence conditions (acute bronchitis, anemia, viral sinusitis).
- **[Download PDF: Disease_Condition_Burden_Dashboard.pdf](screenshots/Disease_Condition_Burden_Dashboard.pdf)**

### Readmission and Outcomes
- Readmission risk/progressions by cost tier.
- **[Download PDF: Readmission-Outcomes-Analytics-Dashboard.pdf](screenshots/Readmission-Outcomes-Analytics-Dashboard.pdf)**

### Analytics Dataset Example
- Gold-layer curated tables for actionable analytics:
  ![Analytics Dataset View](screenshots/dashboard_dataset_view_hd.png)

---

## Business Impact

- Empowers executive management with real-time population health insights.
- Boosts compliance, cost control, and reduces manual effort.
- Scalable foundation for AI/GenAI dashboard and future analytics.

> “Automated analytics for 983 patients and $12M spend; slashed report cycle by 80%; prioritized 617 critical-risk patients for proactive intervention.”

---

## Sample Databricks Transformation Notebook

# Clean HL7 claims and aggregate patient spend
df = raw_df.filter(raw_df.claim_status == "Valid")
agg = df.groupBy("patient_id").agg(sum("total_spend").alias("patient_spend"))
display(agg.orderBy(desc("patient_spend")))


---

## Glossary

- **HL7:** Healthcare messaging & data standard
- **Delta Lake:** Databricks multi-tier table storage
- **Critical Risk Patient:** Flagged for readmission and high cost

---

## Project Artifacts and Documentation

**Download Dashboards:**
- **[Executive Summary PDF](screenshots/Executive_Summary_Dashboard.pdf)**
- **[Provider Performance PDF](screenshots/Provider_Performance_Analysis.pdf)**
- **[Risk & Comorbidity PDF](screenshots/Patient_Risk_Comorbidity_Analysis_Dashbboard.pdf)**
- **[Condition Burden PDF](screenshots/Disease_Condition_Burden_Dashboard.pdf)**
- **[Readmission Outcomes PDF](screenshots/Readmission-Outcomes-Analytics-Dashboard.pdf)**

**Workspace:**  
![Databricks Workspace](screenshots/workspace_items_hd.png) 

**Glossary & Data Dictionary:**  
[README.md](README.md)

---

## Contact and Links

- [Main Project Repo](https://github.com/EMSanjaykumar/healthcare-analytics-databricks-portfolio)
- LinkedIn: [www.linkedin.com/in/skem]

---

## License Disclaimer

_This portfolio is for demonstration/educational use. Data based on [Kaggle healthcare dataset](https://www.kaggle.com/datasets/drscarlat/fhir-1ksample)—de-identified for public sharing._
(no PHI/PII)._

---

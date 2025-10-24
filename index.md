# Healthcare Analytics with Databricks & Azure

Welcome! This portfolio showcases a full-stack healthcare analytics project delivered entirely using Azure Databricks and cloud engineering principles. Explore end-to-end ETL automation, clinical KPI dashboards, and business insights for stakeholders.

---

## 🚀 Executive Summary

- Automated data pipelines processing HL7 healthcare claims and encounters
- Advanced analytics for patient risk, provider benchmarking, disease burden, readmissions, and financial impact
- Achieved deep compliance and auditability (HL7, HIPAA)
- Enabled real-time, executive-level reporting

**Key Metrics:**

- **Total Patients:** 983
- **Total Encounters:** 38,450
- **Total Claims:** 49,340
- **Total Healthcare Spend:** $12,625,196.83
- **High Cost Patients:** 871
- **Critical Risk Patients:** 617
- **Top Revenue Provider:** CLINTON HOSPITAL ASSOCIATION  
- **Max Claim Amount:** $7,001.71  
- **Highest Cost Condition:** Viral sinusitis (disorder)

## Executive Dashboard

![Genie Enabled Dashboard](screenshots/genie_enabled_dashboard_hd.png)

*[Download full PDF: [Executive_Summary_Dashboard.pdf](screenshots/Executive_Summary_Dashboard.pdf)]*

---

## 🛠️ Architecture & Cloud Setup

- Multi-layer ETL: Bronze, Silver, Gold tables via Databricks notebooks and workflows
- Data storage in Azure Blob Gen2 with robust partitioning for compliance and performance
- Notebooks organized for easy reuse, governance, and collaborative analytics

**Cloud Resources:**
![Azure Resource Group](screenshots/azure_resource_group_hd.png)
![Blob Gen2 Containers](screenshots/azure_blob_storage_containers_hd.png)

**Workspace Organization:**
![Databricks Workspace](screenshots/workspace_hd.png)

**ETL Data Pipeline:**
![HL7 Data Pipeline](screenshots/hl7_data_pipeline_hd.png)

---

## 📊 Analytics & Insight Dashboards

### Provider Performance
- Identifies top revenue, high-complexity providers for strategic management
- **Top Providers (by revenue & impact):**  
  - ANNA JAQUES HOSPITAL  
  - BEVERLY HOSPITAL CORPORATION  
  - CLINTON HOSPITAL ASSOCIATION  
  - MORTON HOSPITAL  
  - Others (see analysis)

*[Download PDF: [Provider_Performance_Analysis.pdf](screenshots/Provider_Performance_Analysis.pdf)]*

---

### Patient Risk & Comorbidity
- Highlights: Most expensive, highest-risk patients for targeted interventions
- **Comorbidity Score vs. Cost:** Detects outliers and segments for population health

*[Download PDF: [Patient_Risk_Comorbidity_Analysis_Dashbboard.pdf](screenshots/Patient_Risk_Comorbidity_Analysis_Dashbboard.pdf)]*

---

### Disease/Condition Burden
- Most prevalent and high-cost conditions drive financial and clinical decisions
- **Top Cost Impact Conditions:**  
  - Acute bronchitis disorder  
  - Anemia disorder  
  - Viral sinusitis disorder

*[Download PDF: [Disease_Condition_Burden_Dashboard.pdf](screenshots/Disease_Condition_Burden_Dashboard.pdf)]*

---

### Readmission & Outcomes
- Clinical + financial risk stratification for readmission management
- **Key Insight:** Intersection of readmission and high-cost tiers defines critical focus area

*[Download PDF: [Readmission-Outcomes-Analytics-Dashboard.pdf](screenshots/Readmission-Outcomes-Analytics-Dashboard.pdf)]*

---

### Sample Analytics Dataset
- Curated tables for business and clinical users; direct output from gold layer

![Dashboard Dataset](screenshots/dashboard_dataset_view_hd.png)

---

## 💡 Business Impact

- Directly enables population health management and executive decision-making
- Supports regulatory compliance, cost reduction, and quality improvement
- Scales for additional business intelligence—future-ready for A.I. enhancements (Genie Insights)

---

## 📂 Project Artifacts & Documentation

- [Project Workspace](screenshots/workspace_hd.png) *(notebook organization/structure)*
- [All PDF Dashboards](screenshots/Executive_Summary_Dashboard.pdf), [Provider Performance](screenshots/Provider_Performance_Analysis.pdf), [Risk & Comorbidity](screenshots/Patient_Risk_Comorbidity_Analysis_Dashbboard.pdf), [Condition Burden](screenshots/Disease_Condition_Burden_Dashboard.pdf), [Readmission Outcomes](screenshots/Readmission-Outcomes-Analytics-Dashboard.pdf)
- [Glossary, code, and data dictionary](README.md) *(or create docs/ folder)*

---

## 📬 Contact & Links

- [Main Project Repo](https://github.com/EMSanjaykumar/healthcare-analytics-databricks-portfolio)
- LinkedIn: [www.linkedin.com/in/skem]

---

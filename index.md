# Healthcare Analytics with Databricks & Azure

Welcome! This portfolio showcases a comprehensive, cloud-first healthcare analytics project delivered using **Azure Databricks**. Explore end-to-end ETL automation, clinical KPI dashboards, and business insights tailored for stakeholders.

---

## Table of Contents

- [Executive Summary](#executive-summary)
- [Architecture & Cloud Setup](#architecture--cloud-setup)
- [Dashboards & Insights](#analytics--insight-dashboards)
- [Business Impact](#business-impact)
- [Artifacts & Documentation](#project-artifacts--documentation)
- [Contact & Links](#contact--links)

---

## 🚀 Executive Summary

- **Data pipelines:** Automated HL7 claims, encounter & cost ETL (Databricks)
- **Analytics:** Patient risk, provider benchmarking, high-priority conditions, financial outcomes
- **Compliance:** Robust audit trail for HL7 & HIPAA standards
- **Reporting:** Real-time, interactive executive dashboards

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

## 🛠️ Architecture & Cloud Setup

- **ETL Layers:** Bronze/Silver/Gold with Delta Lake
- **Azure Gen2:** Secure data storage, partitioned for compliance
- **Workspaces:** Governed notebooks and modular analytics jobs

**End-to-End Architecture**  
![End-to-End HL7 Architecture](screenshots/hl7_end_to_end_architecture_hd.png)

**ETL Pipeline**  
![HL7 Data Pipeline](screenshots/hl7_data_pipeline_hd.png)

**Cloud Resources**  
![Azure Resource Group](screenshots/azure_resource_group_hd.png)  
![Blob Storage Containers](screenshots/azure_blob_storage_containers_hd.png)  

**Databricks Workspace**  
![Main Workspace View](screenshots/workspace_hd.png)
![Workspace Items](screenshots/workspace_items_hd.png) <!-- Add this actual file -->

---

## 📊 Analytics & Insight Dashboards

### Provider Performance
- Pinpoints high-revenue, high-complexity providers.
- **Top Providers:**  
  - ANNA JAQUES HOSPITAL  
  - BEVERLY HOSPITAL CORPORATION  
  - CLINTON HOSPITAL ASSOCIATION  
  - MORTON HOSPITAL

**[Download PDF: Provider_Performance_Analysis.pdf](screenshots/Provider_Performance_Analysis.pdf)**

---

### Patient Risk & Comorbidity
- Identifies critical/high-cost patient segments.
- Comorbidity score vs. cost outlier detection.

**[Download PDF: Patient_Risk_Comorbidity_Analysis_Dashbboard.pdf](screenshots/Patient_Risk_Comorbidity_Analysis_Dashbboard.pdf)**

---

### Disease/Condition Burden
- Highest cost & prevalence conditions (acute bronchitis, anemia, viral sinusitis).

**[Download PDF: Disease_Condition_Burden_Dashboard.pdf](screenshots/Disease_Condition_Burden_Dashboard.pdf)**

---

### Readmission & Outcomes
- Readmission risk/progressions by cost tier; focus on critical-risk overlays.

**[Download PDF: Readmission-Outcomes-Analytics-Dashboard.pdf](screenshots/Readmission-Outcomes-Analytics-Dashboard.pdf)**

---

### Analytics Dataset Example

- Gold-layer ready tables for clinical/business analytics.

![Analytics Dataset View](screenshots/dashboard_dataset_view_hd.png)

---

## 💡 Business Impact

- Empowers executive management with real-time population health insights.
- Boosts compliance, cost control, and reduces manual reporting.
- Lays foundation for AI/GenAI dashboard and automated analytics upgrades.

> “Automated analytics for 983 patients and $12M spend; slashed report cycle by 80%; prioritized 617 critical-risk patients for proactive clinical intervention.”

---

## 📂 Project Artifacts & Documentation

- **All PDF Dashboards:**  
    [Executive Summary](screenshots/Executive_Summary_Dashboard.pdf)  
    [Provider Performance](screenshots/Provider_Performance_Analysis.pdf)  
    [Risk & Comorbidity](screenshots/Patient_Risk_Comorbidity_Analysis_Dashbboard.pdf)  
    [Condition Burden](screenshots/Disease_Condition_Burden_Dashboard.pdf)  
    [Readmission Outcomes](screenshots/Readmission-Outcomes-Analytics-Dashboard.pdf)
- **Workspace:** ![Databricks Workspace](screenshots/workspace_hd.png)
- **Glossary & Data Dictionary:** [README.md](README.md) *(or use docs/ folder)*

---

## 📬 Contact & Links

- [Main Project Repo](https://github.com/EMSanjaykumar/healthcare-analytics-databricks-portfolio)
- LinkedIn: [www.linkedin.com/in/skem]

---

## 🏷️ License/Disclaimer

_Project licensed under MIT License. Data is de-identified and simulated for demo—no PHI/PII._

---


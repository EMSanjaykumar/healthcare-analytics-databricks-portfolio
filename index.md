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

![Executive Dashboard] <img width="3840" height="2052" alt="Genie_Enabled_Dashboard" src="https://github.com/user-attachments/assets/cb61bb18-7e2e-4ac6-98d2-1f797b2fe2db" />


*[Download full PDF:[Executive_Summary_Dashboard.pdf](https://github.com/user-attachments/files/23112807/Executive_Summary_Dashboard.pdf)*


---

## 🛠️ Architecture & Cloud Setup

- Multi-layer ETL: Bronze, Silver, Gold tables via Databricks notebooks and workflows
- Data storage in Azure Blob Gen2 with robust partitioning for compliance and performance
- Notebooks organized for easy reuse, governance, and collaborative analytics

**Cloud Resources:**
![Azure Resource Group]<img width="3840" height="2052" alt="Azure_Resource_Group" src="https://github.com/user-attachments/assets/a8eb52dd-cd43-4bfc-8fe7-5b75fa3649a5" />

![Blob Gen2 Containers]<img width="3840" height="2052" alt="Azure_Blob_Gen2_Storage_Containers" src="https://github.com/user-attachments/assets/6fd7ae19-67e6-465f-bfd9-97c26ffc4a00" />


**Workspace Organization:**
![Databricks Workspace]<img width="3840" height="2052" alt="Workspace" src="https://github.com/user-attachments/assets/6dfa6410-40e8-4755-91b7-e62c2f76ff9e" />


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

*[Provider_Performance_Analysis.pdf](https://github.com/user-attachments/files/23112830/Provider_Performance_Analysis.pdf)*

---

### Patient Risk & Comorbidity
- Highlights: Most expensive, highest-risk patients for targeted interventions
- **Comorbidity Score vs. Cost:** Detects outliers and segments for population health

*[Patient_Risk_Comorbidity_Analysis_Dashbboard.pdf](https://github.com/user-attachments/files/23112841/Patient_Risk_Comorbidity_Analysis_Dashbboard.pdf)*


---

### Disease/Condition Burden
- Most prevalent and high-cost conditions drive financial and clinical decisions
- **Top Cost Impact Conditions:**  
  - Acute bronchitis disorder  
  - Anemia disorder  
  - Viral sinusitis disorder

*[Disease_Condition_Burden_Dashboard.pdf](https://github.com/user-attachments/files/23112844/Disease_Condition_Burden_Dashboard.pdf)*


---

### Readmission & Outcomes
- Clinical + financial risk stratification for readmission management
- **Key Insight:** Intersection of readmission and high-cost tiers defines critical focus area

*[Readmission & Outcomes Analytics Dashboard.pdf](https://github.com/user-attachments/files/23112854/Readmission.Outcomes.Analytics.Dashboard.pdf)*

---

### Sample Analytics Dataset
- Curated tables for business and clinical users; direct output from gold layer

![Dashboard Dataset]<img width="3840" height="2052" alt="Dashboard_Dataset" src="https://github.com/user-attachments/assets/a5b5cd91-4e2d-4084-802d-dd3117240f5a" />



---

## 💡 Business Impact

- Directly enables population health management and executive decision-making
- Supports regulatory compliance, cost reduction, and quality improvement
- Scales for additional business intelligence—future-ready for A.I. enhancements (Genie Insights)

---

## 📂 Project Artifacts & Documentation

- [Project Workspace]<img width="3840" height="2052" alt="Workspace" src="https://github.com/user-attachments/assets/637ec4c0-1418-4dc7-9990-270fae2f1ca4" />
 (notebook organization/structure)
- [All PDF Dashboards][Executive_Summary_Dashboard.pdf](https://github.com/user-attachments/files/23112874/Executive_Summary_Dashboard.pdf), [Provider Performance](Provider_Performance_Analysis.pdf), [Risk & Comorbidity](Patient_Risk_Comorbidity_Analysis_Dashbboard.pdf), [Condition Burden](Disease_Condition_Burden_Dashboard.pdf), [Readmission Outcomes](Readmission-Outcomes-Analytics-Dashboard.pdf)
- [Glossary, code, and data dictionary](README.md or create docs/ folder)

---

## 📬 Contact & Links

- [Main Project Repo](https://github.com/EMSanjaykumar/healthcare-analytics-databricks-portfolio)
- LinkedIn: [www.linkedin.com/in/skem]

---



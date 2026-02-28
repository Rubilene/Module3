# Radiology ETL Platform – Proof of Concept
## Multiverse Advanced Data Fellowship | Data Engineering Module

---

### Project Overview
This codebase is the Proof of Concept (PoC) for the **Cloud-Based ETL Platform for
Integrated Radiology Data Analytics** described in the Data Engineering Project
Evaluation Report.

It demonstrates a complete Extract → Transform → Load pipeline that consolidates
data from three simulated radiology source systems (RIS, PACS, EPR) into a governed
dimensional data warehouse, with automated validation, scheduling simulation,
and dashboard reporting.

---

### Folder Structure
```
radiology_etl_project/
│
├── README.md                        ← This file
├── run_pipeline.py                  ← MAIN ENTRY POINT – run this first
│
├── config/
│   └── settings.py                  ← Central config (paths, rules, thresholds)
│
├── src/
│   ├── 01_generate_source_data.py   ← Simulates RIS, PACS, EPR source exports
│   ├── 02_extract.py                ← Extraction layer with watermark timestamps
│   ├── 03_validate.py               ← Staging & validation rules
│   ├── 04_transform.py              ← Transformation engine & KPI derivation
│   ├── 05_load.py                   ← Star schema loader (SQLite warehouse)
│   ├── 06_orchestrator.py           ← Pipeline orchestration with retry logic
│   └── 07_dashboard_report.py       ← Automated KPI report & charts
│
├── data/
│   ├── raw/                         ← Raw source exports (auto-generated CSV)
│   ├── processed/                   ← Validated & transformed records
│   └── warehouse/                   ← SQLite analytical warehouse
│
├── logs/
│   └── pipeline.log                 ← Full audit trail of pipeline runs
│
├── reports/
│   └── kpi_report.xlsx              ← Auto-generated KPI Excel report
│
└── tests/
    └── test_pipeline.py             ← Unit tests for validation & transform logic
```

---

### How to Run

```bash
# Step 1 – Run the full pipeline
python run_pipeline.py

# Step 2 – Run unit tests
python tests/test_pipeline.py

# Step 3 – Query the warehouse directly
python -c "
import sqlite3, pandas as pd
conn = sqlite3.connect('data/warehouse/radiology_warehouse.db')
df = pd.read_sql('SELECT * FROM Fact_Radiology LIMIT 10', conn)
print(df.to_string())
conn.close()
"
```

---

### Technologies Used
| Component       | Technology                          |
|-----------------|-------------------------------------|
| Language        | Python 3.12                         |
| Data Processing | pandas, numpy                       |
| Warehouse       | SQLite (simulates Azure Synapse)    |
| Reporting       | matplotlib, openpyxl                |
| Logging         | Python logging (audit trail)        |
| Testing         | unittest (standard library)         |
| Orchestration   | Custom scheduler (simulates Airflow)|

---

### Assessment Alignment
| Report Section         | Code Module                         |
|------------------------|-------------------------------------|
| Part 1 – Current state | 01_generate_source_data.py          |
| Part 2 – Extraction    | 02_extract.py                       |
| Part 3 – Validation    | 03_validate.py                      |
| Part 3 – Transform     | 04_transform.py                     |
| Part 3 – Load          | 05_load.py                          |
| Part 3 – Orchestration | 06_orchestrator.py                  |
| Part 4 – Impact        | 07_dashboard_report.py              |

---
*Multiverse Advanced Data Fellowship – Data Engineering Project PoC*

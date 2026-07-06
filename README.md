# Databricks Academy Lakehouse

A production-style Azure Databricks Lakehouse built during the **Databricks Academy 3-month program**.  
The project covers end-to-end data engineering — from raw ingestion to a deployed AI application — using the full modern Databricks stack.

---

## Overview

This repository contains both the weekly hands-on labs and a growing production platform that integrates everything learned across the academy. By the end of the program it becomes a complete lakehouse with streaming ingestion, a medallion architecture, data quality gates, CI/CD automation, and a RAG-based AI application.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Cloud platform | Microsoft Azure |
| Storage | Azure Data Lake Storage Gen2 |
| Compute & platform | Azure Databricks |
| Table format | Delta Lake |
| Governance | Unity Catalog |
| Ingestion | Auto Loader, Azure Event Hubs |
| Pipelines | Lakeflow / Declarative Pipelines (DLT) |
| Orchestration | Databricks Workflows / Jobs |
| Data quality | Great Expectations, pytest, chispa |
| Deployment | Databricks Asset Bundles (DABs) |
| CI/CD | GitHub Actions |
| AI / GenAI | Vector Search, RAG, Databricks AI Dev Kit |
| Language | Python, PySpark, SQL |

---

## Repository Structure

```
databricks-academy-lakehouse/
├── labs/                        # Weekly lab exercises (scratchpad)
│   ├── lab_01_fundamentals/
│   ├── lab_02_azure_uc_setup/
│   └── ...
├── src/                         # Production source code
│   ├── ingestion/               # Auto Loader, Event Hubs
│   ├── silver/                  # MERGE, SCD, schema evolution
│   ├── gold/                    # Aggregations, star schema
│   └── utils/                   # Shared helpers
├── tests/                       # Automated tests
│   ├── unit/                    # pytest + chispa unit tests
│   └── data_quality/            # Great Expectations suites
├── resources/                   # Databricks resource definitions
│   ├── jobs.yml
│   └── pipelines.yml
├── architecture/                # Architecture diagrams
│   └── diagrams/
├── docs/                        # Lab notes and decisions
│   ├── lab_notes/
│   └── screenshots/
├── final_project/               # Final Demo Day project
├── .github/workflows/           # CI/CD pipelines
│   ├── ci.yml                   # PR checks: lint + tests
│   └── deploy.yml               # DABs DEV → PROD deploy
├── databricks.yml               # DABs bundle configuration
└── requirements.txt
```

---

## Lab Progress

| # | Lab | Topic | Status |
|---|---|---|---|
| 1 | Lab 01 | Databricks Fundamentals & DEV Setup | 🔄 In progress |
| 2 | Lab 02 | Azure Services & Unity Catalog Setup | ⬜ Not started |
| 3 | Lab 03 | Streaming & Auto Loader | ⬜ Not started |
| 4 | Lab 04 | Silver Layer, MERGE & Schema Evolution | ⬜ Not started |
| 5 | Lab 05 | Declarative Pipelines / Lakeflow | ⬜ Not started |
| 6 | Lab 06 | Gold Layer & Business Analytics | ⬜ Not started |
| 7 | Lab 07 | Data Quality & Unit Testing | ⬜ Not started |
| 8 | Lab 08 | CI/CD — DEV to PROD | ⬜ Not started |
| 9 | Lab 09 | Databricks REST API & Automation | ⬜ Not started |
| 10 | Lab 10 | Lakehouse Federation & CDC | ⬜ Not started |
| 11 | Lab 11 | Zero-bus Streaming | ⬜ Not started |
| 12 | Lab 12 | AI Capstone — RAG & Vector Search | ⬜ Not started |
| ★ | Final | End-to-end Platform + Demo Day | ⬜ Not started |

---

## Architecture

The platform follows the **medallion architecture** pattern:

```
Event Hubs / ADLS Gen2
        │
        ▼
   Bronze layer        ← raw ingestion via Auto Loader
        │
        ▼
   Silver layer        ← MERGE, SCD Type 2, schema enforcement
        │
        ▼
    Gold layer         ← aggregations, star schema, AI/BI dashboards
        │
        ▼
   AI application      ← RAG, Vector Search, LLM Q&A
```

Full architecture diagram available in `architecture/diagrams/`.

---

## Certifications

| Certification | Status |
|---|---|
| Databricks Certified Data Engineer Associate | ✅ Passed |
| Databricks Certified Data Engineer Professional | 🔄 In progress |

---

## Setup

```bash
# Clone the repository
git clone https://github.com/BadalovP/Databricks-Academy-Lakehouse.git
cd Databricks-Academy-Lakehouse

# Install dependencies
pip install -r requirements.txt

# Validate the DABs bundle
databricks bundle validate
```

---

## Author

**Parvin Badalov** — Data Engineer  
[GitHub](https://github.com/BadalovP) · [LinkedIn](https://www.linkedin.com/in/parvinbadalov)

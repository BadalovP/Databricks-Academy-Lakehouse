<div align="center">

# UrbanFlow 2.0

### Intelligent Urban Mobility Lakehouse & AI Operations Assistant

Databricks Academy **Final Project**: one integrated, governed and release-automated solution that brings together the concepts of Labs 1–12.

[![Final Project CI](https://github.com/BadalovP/Databricks-Academy-Lakehouse/actions/workflows/final_project_ci.yml/badge.svg)](https://github.com/BadalovP/Databricks-Academy-Lakehouse/actions/workflows/final_project_ci.yml)
[![GitHub CD](https://img.shields.io/badge/GitHub_CD-protected_deploy_passed-2ea44f)](https://github.com/BadalovP/Databricks-Academy-Lakehouse/actions/runs/38400575076)
[![Live validation](https://img.shields.io/badge/live_validation-Personal_DEV_%2B_Azure_trial-2ea44f)](docs/RELEASE_AND_VALIDATION.md)
[![Data](https://img.shields.io/badge/data-labelled_samples_and_fixtures-blue)](#4-data-classification-and-provenance)

[← Repository homepage](../README.md)

</div>

> [!NOTE]
> **Status: complete.** Merged to `main` in [PR #75](https://github.com/BadalovP/Databricks-Academy-Lakehouse/pull/75) (`225545f`), with release follow-ups #76–#78. Steps 1–8 were validated live in Personal DEV. Step 9 deployed the same bundle to the Academy Azure trial `dbr_dev_trial_2` (PROD-style, not paid production) with identical figures. A protected GitHub Actions release then planned, deployed and ran the platform Job on Personal DEV as a service principal ([run 38400575076](https://github.com/BadalovP/Databricks-Academy-Lakehouse/actions/runs/38400575076)).

**Status vocabulary used throughout:** **Complete** (built, tested and merged) · **Live-validated** (ran in a Databricks workspace, with evidence) · **Offline-tested** (automated tests only) · **Documented but not demonstrated** (designed and written down, never run) · **Optional** (deliberately outside the default run).

## Contents

| | | |
|---|---|---|
| [1. Project overview](#1-project-overview) | [10. CDC deep dive](#10-cdc-deep-dive) | [19. Cost and performance](#19-cost-and-performance) |
| [2. Business problem](#2-business-problem) | [11. Lakeflow pipeline](#11-lakeflow-pipeline) | [20. Engineering challenges and lessons](#20-engineering-challenges-and-lessons) |
| [3. Executive results](#3-executive-results) | [12. Historical and weather analytics](#12-historical-and-weather-analytics) | [21. Repository structure](#21-repository-structure) |
| [4. Data classification and provenance](#4-data-classification-and-provenance) | [13. AI/BI dashboard](#13-aibi-dashboard) | [22. How to reproduce](#22-how-to-reproduce) |
| [5. High-level architecture](#5-high-level-architecture) | [14. Operations assistant (RAG)](#14-operations-assistant-rag) | [23. Evidence index](#23-evidence-index) |
| [6. Source systems](#6-source-systems) | [15. Governance and security](#15-governance-and-security) | [24. Scope and limitations](#24-scope-and-limitations) |
| [7. Bronze layer](#7-bronze-layer) | [16. Environments](#16-environments) | [25. Future work (optional)](#25-future-work-optional) |
| [8. Silver layer](#8-silver-layer) | [17. CI/CD](#17-cicd) | [26. Conclusion](#26-conclusion) |
| [9. Gold layer](#9-gold-layer) | [18. Testing](#18-testing) | |

---

## 1. Project overview

UrbanFlow 2.0 is a bike-share operations platform on Databricks. It takes real Citi Bike station and trip samples, real Open-Meteo weather, and fictional maintenance tickets and procedures. From these it builds:

1. a **medallion lakehouse** (Bronze → Silver + quarantine → Gold) with reconciliation identities on every run;
2. a **transparent rebalancing priority** that a dispatcher can recompute by hand, and that never sends a van to an out-of-service station;
3. **maintenance change data capture** using Delta Change Data Feed, with an audit history and a forward-only checkpoint that stays correct across automatic `OPTIMIZE` commits;
4. a **Lakeflow declarative pipeline** that reuses the same rule functions and matches the notebook tables row for row;
5. a **five-page AI/BI dashboard** generated from read-only SQL;
6. a **retrieval-augmented operations assistant** that cites procedures, refuses when they do not cover a question, and is measured against a no-retrieval baseline;
7. **governance** with row filters and column masks, and **release engineering** with CI, a reviewable plan, protected approval, drift check and a bounded sample run.

Everything is one Databricks Asset Bundle with two targets (`personal_dev`, `azure_prod`), serverless compute only, and its own schemas, Volume, Jobs, pipeline, dashboard and identities.

## 2. Business problem

An empty station blocks a rental, and a full station blocks a return. A dispatcher needs to know:

- **Where to send the next rebalancing van**, ranked by a rule anyone can audit.
- **Where not to send it.** Moving bikes does not fix a station that is out of service or has an open maintenance ticket.
- **What to do when something breaks**: the right procedure, quickly, with its source.

| Question | Answered by |
|---|---|
| Which stations are short of bikes or docks right now? | Silver availability status → `gold_station_shortage` |
| In which order should vans go, and what should they do? | `gold_rebalancing_priority` (score + action) |
| Is a priority station under maintenance? | CDC Silver joined in the dashboard → `HOLD` |
| When and where is demand high, and does weather matter? | Historical trip and weather Gold tables |
| Can I trust today's numbers? | Data-quality page: reconciliation, quarantine, expectations |
| What is the procedure for this incident? | Operations assistant, with citations or a refusal |

The thresholds (≤ 2 bikes, ≤ 2 docks) are **teaching rules**, not Citi Bike operating standards.

## 3. Executive results

| Area | Result | Status | Evidence |
|---|---|---|---|
| Medallion sample (DEV) | 40 Bronze → 40 Silver, 0 quarantined, 0 duplicates → 13 shortages, 13 priorities | Live-validated | [Step 2](evidence/2026-10-12_personal_dev_step2_pass.md) (run `896549012431425`) |
| Strict idempotency | 19 / 19 criteria: replay adds no rows, no duplicates, no checkpoint move | Live-validated | [Step 3](evidence/2026-10-12_personal_dev_step3_retry_pass.md) (run `412521206243476`) |
| Maintenance CDC | `batch_002`: 1 insert, 2 updates, 1 delete (6 CDF events), checkpoint 2 → 3. `batch_003`: no events, checkpoint 3 → 4 over an `OPTIMIZE` | Live-validated | [4B](evidence/2026-10-12_personal_dev_step4_batch002.md), [4C](evidence/2026-10-12_personal_dev_step4_batch003.md) |
| Lakeflow | 10 datasets, 22 expectations, 0 failed, row-for-row parity | Live-validated | [Step 5](evidence/2026-10-12_personal_dev_step5_lakeflow.md) |
| Operations assistant | Correctness 0.685 vs 0.148 baseline (keyword metric); recall 1.0; citation precision 0.967; refusal decisions 18/18 vs 13/18 | Live-validated | [E5](evidence/2026-10-13_personal_dev_e5_rag_evaluation.md) |
| Dashboard | 25 / 25 datasets ran; published to the owner only | Live-validated | [Dashboard](evidence/2026-10-13_personal_dev_dashboard_validation.md) |
| Governance | No mapping 0 rows / SOUTH 28 / ALL 40; coordinates rounded; ride IDs hashed; rolled back | Live-validated (one identity) | [Phase F](evidence/2026-10-13_personal_dev_governance_demo.md) |
| Azure trial | 7 resources deployed; sample run identical to DEV | Live-validated | [Step 9](evidence/2026-10-13_azure_prod_step9_deploy_and_sample_run.md) (run `240527908342651`) |
| GitHub CD | Plan → approval → deploy → unchanged post-plan → sample Job `112906299302442` SUCCESS | Live-validated (Personal DEV) | [Full CD](evidence/2026-10-13_github_cd_full_deploy.md) (run `38400575076`) |
| Automated tests | 867 passed, 1 skipped (CI of the full CD run, Linux) | Complete | [§18](#18-testing) |
| Cost | ≈ 9.15 DBU (≈ $3 at list) of Final Project compute in DEV | Measured | [§19](#19-cost-and-performance) |

## 4. Data classification and provenance

| Data | Source | Volume used | Classification |
|---|---|---|---|
| Station status and information | [Citi Bike GBFS 2.3](https://gbfs.citibikenyc.com/gbfs/2.3/gbfs.json) | 40-station dated snapshot | **Real**, point-in-time sample; not current operations |
| Trips | [Citi Bike trip history](https://citibikenyc.com/system-data), January 2024 | 40 rows | **Real**; a development sample, not the month |
| Weather | [Open-Meteo archive](https://open-meteo.com/en/docs/historical-weather-api) | 48 hours, one NYC coordinate | **Real**; area-level, not per station |
| Maintenance tickets | [`fixtures/maintenance/`](fixtures/maintenance/README.md) | 3 snapshot batches, 5–6 tickets | **Fictional, synthetic** tickets on real sample station IDs |
| Assistant corpus | [`knowledge_base/`](knowledge_base/) | 8 procedures → 30 chunks | **Fictional** documents written for this project |
| Station-health telemetry | Generated by `zerobus.py` | ≤ 30 records per run | **Synthetic**, flagged `synthetic = true`; optional, not run |

Attribution: [fixtures/samples/ATTRIBUTION.md](fixtures/samples/ATTRIBUTION.md). The 40-row samples validate logic, integration and idempotency, **not scale or performance**. Historical trips use the GBFS **short name** as the station key, so the station dimension keeps both the short name and the UUID.

## 5. High-level architecture

```mermaid
flowchart LR
  subgraph Sources
    GBFS[Citi Bike GBFS]
    TRIPS[Trip archive]
    METEO[Open-Meteo]
    MAINT[(Maintenance snapshots<br/>fixture or Federation)]
    DOCS[Procedures<br/>knowledge_base]
  end
  subgraph UC["Unity Catalog: one schema per target"]
    VOL[/Volume landing/]
    BRZ[Bronze]
    SLV[Silver + quarantine<br/>+ duplicates]
    GLD[Gold KPIs]
    CDC[CDF source → history<br/>→ SCD 1 Silver → checkpoint]
    RAGT[Chunks with CDF]
    LKF[Lakeflow pipeline<br/>isolated schema]
  end
  IDX[AI Search<br/>Delta Sync index]
  GBFS --> VOL
  TRIPS --> VOL
  METEO --> VOL
  VOL --> BRZ --> SLV --> GLD
  MAINT --> CDC
  DOCS --> RAGT --> IDX
  VOL --> LKF
  GLD --> DASH[AI/BI dashboard<br/>5 pages]
  CDC --> DASH
  IDX --> BOT[Operations assistant]
  BOT --> EVAL[(Evaluation and<br/>redacted monitoring)]
  EVAL --> DASH
```

| Bundle resource | Personal DEV | Azure trial |
|---|---|---|
| Schemas | `workspace.urbanflow_final_dev`, `workspace.urbanflow_final_lakeflow_dev` | `dbr_dev.parvinbadalov_urbanflow_final_prod`, `…_lakeflow_prod` |
| Volume | Landing and checkpoints (`prevent_destroy`) | same |
| Platform Job | `158911626270744` | `456376825048239` |
| Assistant Job | `887464717518046` | `1075074961516662` |
| Lakeflow pipeline | `d5e0e599-…` | `f5961757-…` |
| Dashboard | `01f1c647728e1ceeb2b5bd10a8700793` | `01f1c6ddd47616e8bb703a5342a603ba` |

## 6. Source systems

| Source | Access pattern | Landing | Notes |
|---|---|---|---|
| GBFS `station_status`, `station_information`, discovery | Committed dated snapshot (live HTTP client exists and is tested) | `landing/station_status/…` in the Volume | `sample_autoloader` (default) or `existing_bronze` from Event Hubs (optional) |
| Trip archive | Committed 40-row CSV sample; `run_full_month` downloads the month | `landing/historical_trips/…` | Full month **not run** |
| Open-Meteo archive | Committed 48-hour JSON sample | `landing/weather/…` | One coordinate, labelled `weather_grid_label` |
| Maintenance | `fixture_snapshot` (committed CSV) or `federation_query` (read-only foreign catalog) | MERGEd into `station_maintenance_source` (CDF on) | Federation **not live-verified**: no external database holds Final Project tables |
| Procedures | Markdown files in the repo | `rag_documents` → `rag_document_chunks` | Fictional |

## 7. Bronze layer

```mermaid
flowchart LR
  F1[Committed sample files] --> LAND[land_sources<br/>copy once, skip if already_landed]
  LAND --> VOL[/UC Volume<br/>landing paths/]
  VOL --> AL[Auto Loader<br/>cloudFiles, availableNow]
  AL --> B1[(bronze_station_status)]
  AL --> B2[(bronze_historical_trips)]
  VOL --> W[weather reader] --> DW[(dim_weather_hourly)]
  EH[Event Hubs topic<br/>optional] -.existing_bronze.-> B1
  AL -.schema and checkpoint.-> CK[/Volume checkpoints/]
```

- **Landing is idempotent**: a file already in the Volume is reported `already_landed` and not copied again.
- **Auto Loader `availableNow`** processes exactly the new files and stops, so there is no always-on stream. Checkpoints live in the Volume and are reused on replay; a replay leaves Bronze with no new commit.
- **Lineage columns** (`execution_id`, `ingested_at`, source file) are added at ingestion. The `existing_bronze` path filters strictly by `execution_id`, because the Academy Event Hub was also used in earlier coursework.

## 8. Silver layer

Every Bronze row ends up in exactly one of three places: Silver, quarantine (with `failed_rules`), or duplicates. Each run checks **Bronze = Silver + Quarantine + Duplicates**.

| Rule | Quarantine code | Applies to |
|---|---|---|
| Event ID present | `MISSING_EVENT_ID` | Station status |
| Station ID present | `MISSING_STATION_ID` | Station status |
| Payload parses | `PARSE_ERROR` | Station status |
| Availability counts present | `MISSING_AVAILABILITY` | Station status |
| Counts not negative | `NEGATIVE_AVAILABILITY` | Station status |
| Installed / renting / returning flags valid | `INVALID_STATION_STATE` | Station status |
| Source timestamp present | `MISSING_SOURCE_TIMESTAMP` | Station status |
| Station exists in the reference | `UNKNOWN_STATION_ID` | Station status, when checked against the dimension |
| Ride ID present, timestamps valid | `MISSING_RIDE_ID` and related codes | Historical trips |

Derived columns:

| Column | Definition |
|---|---|
| `is_operational` | `is_installed = 1 AND is_renting = 1` |
| `is_low_bikes` | operational AND bikes available ≤ 2 |
| `is_low_docks` | operational AND returning AND docks available ≤ 2 |
| `availability_status` | `OUT_OF_SERVICE` / `LOW_BIKES_AND_DOCKS` / `LOW_BIKES` / `LOW_DOCKS` / `AVAILABLE` |
| `capacity_estimate` | bikes + docks + disabled bikes + disabled docks |

**Deduplication** keeps the first arrival, ordered by Kafka timestamp, partition and offset, with ingestion time and the raw payload as deterministic tie-breaks, so a rerun makes the same choice. Writes use a **change-aware `MERGE`** that updates a row only if a business column differs.

## 9. Gold layer

**Rebalancing priority** (from `pipeline/gold.py` and `src/urbanflow/gold.py`; out-of-service stations are excluded):

```text
severity_points = 2 if low bikes AND low docks, 1 if either, else 0
deficit_points  = max(threshold - bikes_available, threshold - docks_available, 0)   # threshold = 2
size_points     = 1 if capacity_estimate >= 20, else 0
priority_score  = severity_points + deficit_points + size_points

action = INSPECT_STATION  if low bikes AND low docks
         DELIVER_BIKES    if low bikes
         COLLECT_BIKES    otherwise (low docks)
```

There is no model and no tuned weighting, so a dispatcher can check every row.

```mermaid
erDiagram
  dim_station_development_sample ||--o{ fact_station_availability : "station_id"
  dim_station_development_sample ||--o{ gold_station_shortage : "station_id"
  dim_station_development_sample ||--o{ gold_rebalancing_priority : "station_id"
  dim_station_development_sample ||--o{ gold_daily_trip_demand : "short name"
  dim_station_development_sample ||--o{ silver_station_maintenance : "station_id"
  silver_station_maintenance ||--o{ maintenance_cdc_history : "maintenance_id"
  dim_weather_hourly ||--o{ gold_weather_demand : "weather hour"
  dim_station_development_sample {
    string station_id PK
    string station_short_name
    string station_name
    double latitude
    double longitude
    int capacity
    string region_id
  }
  fact_station_availability {
    string event_id PK
    string station_id FK
    timestamp observed_at
    int capacity_estimate
    string availability_status
    boolean is_low_bikes
    boolean is_low_docks
  }
  gold_station_shortage {
    string event_id PK
    string station_id FK
    string availability_status
  }
  gold_rebalancing_priority {
    string execution_id
    string station_id FK
    int severity_points
    int deficit_points
    int size_points
    int priority_score
    string action
  }
  silver_station_maintenance {
    string maintenance_id PK
    string station_id FK
    string status
    string severity
    bigint source_commit_version
  }
  maintenance_cdc_history {
    string change_id PK
    string maintenance_id FK
    string change_type
    bigint commit_version
  }
  gold_daily_trip_demand {
    string start_station_uuid FK
    date trip_date
    int trips_started
    int member_trips
    int casual_trips
    double avg_trip_minutes
  }
  dim_weather_hourly {
    timestamp weather_hour PK
    double temperature_celsius
    double precipitation_mm
    double wind_speed_kmh
  }
  gold_weather_demand {
    date trip_date
    string temperature_bucket
    int trips
    int trips_with_weather
    double weather_coverage
  }
```

**Maintenance HOLD.** The dashboard joins each priority to CDC Silver. A station with an open ticket (`status <> 'CLOSED'`) stays listed but shows **`HOLD - open maintenance ticket`**: no van goes there without a dispatcher's decision.

## 10. CDC deep dive

> [!IMPORTANT]
> This is **Delta Change Data Feed over source snapshots**, on a table the project owns. It is **not PostgreSQL WAL / log-based database CDC**. The live runs used the committed **fixture snapshots**. The Lakehouse Federation path (`federation_query`) is implemented and offline-tested but **not live-verified**.

```mermaid
sequenceDiagram
  autonumber
  participant Snap as Snapshot (fixture or Federation)
  participant Src as station_maintenance_source (CDF)
  participant Hist as maintenance_cdc_history
  participant Slv as silver_station_maintenance
  participant Ck as maintenance_cdc_checkpoint
  Snap->>Src: MERGE, skipped entirely if snapshot equals source
  Src->>Src: plan_scan from checkpoint+1 to latest
  alt range lost to log cleanup or VACUUM
    Src-->>Ck: CdcRetentionGapError, nothing written
  else range readable
    Src->>Hist: table_changes, each event once by change_id
    Src->>Slv: newest final state per key (SCD 1)
    Slv-->>Src: reconcile keys and values at the scanned version
    Slv->>Ck: advance forward only, after reconciliation passes
  end
```

**What happened live (Personal DEV):**

| Source version | Commit | CDF rows | Checkpoint |
|---|---|---|---|
| v1 | `batch_001` MERGE: 5 inserts | 5 | → 1 (step 2) |
| v2 | Replay before the fix: empty source commit | 0 | 1 → 2 (step 3 FAIL, fixed) |
| v3 | `batch_002` MERGE: 1 insert, 2 updates, 1 delete | 6 | 2 → 3 (run `343513312512955`) |
| v4 | **Automatic `OPTIMIZE`** (`auto: true`), 5 s later, **no data change** | 0 | 3 → 4 (run `500336374896739`), `last_data_change_version = 3` |

History holds 11 events. Silver equals `batch_003` (ticket `MT-1003` deleted, its delete event kept). Reconciliation found 0 differences.

**The defects and their fixes:**

1. **Empty source commit (step 3).** Replaying an identical snapshot still committed an empty `MERGE` to the source, and the checkpoint moved 1 → 2 with no data. *Fix:* skip the source `MERGE` when the snapshot equals the source. The retry passed 19/19.
2. **Implicit checkpoint meaning (step 4B).** The deployed code compared version numbers ("latest 4 > checkpoint 3"). An automatic `OPTIMIZE` showed that version numbers are not data. The first local fix kept the checkpoint *behind* `OPTIMIZE` versions, which would go stale until log cleanup removed them. It was reviewed and superseded before deployment. *Final rule:* the checkpoint is the **newest scanned and reconciled version**. `OPTIMIZE`-only ranges are scanned with no `MERGE` and passed. `last_data_change_version` is reported separately. A range the log no longer holds fails closed with `CdcRetentionGapError` and a recovery instruction. A source below the checkpoint raises `CdcStateError`.

```mermaid
stateDiagram-v2
  [*] --> Scan: run starts
  Scan --> NoChange: snapshot equals source
  Scan --> Plan: source MERGEd
  NoChange --> Plan
  Plan --> Gap: versions no longer in the log
  Plan --> Empty: range has no CDF rows
  Plan --> Apply: range has CDF rows
  Apply --> Reconcile
  Empty --> Reconcile
  Reconcile --> Advance: 0 differences
  Reconcile --> Failed: differences found
  Advance --> [*]
  Gap --> [*]: fail closed, nothing written
  Failed --> [*]: checkpoint unchanged, replay is safe
```

## 11. Lakeflow pipeline

A **serverless, triggered** Lakeflow Spark Declarative Pipeline (`pyspark.pipelines`) writes into its own isolated schema, so it never competes with the notebook tables. It imports the same rule functions as the notebooks.

```mermaid
flowchart LR
  VOL[/Volume landing/] --> B[bronze_station_status<br/>streaming table]
  B --> S[silver_station_status]
  B --> Q[quarantine_station_status]
  B --> D[duplicate_station_status]
  VOL --> SI[station_information_raw] --> DIM[dim_station_development_sample]
  S --> F[fact_station_availability]
  DIM --> F
  F --> DS[gold_daily_station_summary]
  F --> SH[gold_station_shortage]
  F --> P[gold_rebalancing_priority]
```

- **10 datasets**: 1 streaming table and 9 materialized views. **22 expectations**, set to warn so that rule outcomes are counted and not hidden.
- Live update `a26b4385-f0b8-454a-9aad-d9a3424151c7` (run `441981394008476`): every dataset COMPLETED, 0 failed expectations, and **row-for-row parity** with the notebook tables ([evidence](evidence/2026-10-12_personal_dev_step5_lakeflow.md)).
- The platform Job refreshes it only when `run_lakeflow=true` (a condition task), so a default run never bills a pipeline update.

**Platform Job orchestration** (manual, serverless, every task with its own timeout):

```mermaid
flowchart TD
  P[preflight] --> L[land_sources]
  L --> C[station_source_check] --> S[station_silver] --> G[station_gold]
  L --> H[historical_trips]
  G --> H
  H --> W[weather_enrichment]
  G --> V[validate_medallion]
  H --> V
  W --> V
  V --> F[federation_cdc]
  V --> Q{should_refresh_lakeflow}
  Q -- true --> R[refresh_lakeflow]
```

The assistant has its own Job: `ingest_knowledge_base → prepare_vector_search → evaluate_rag`. Index mutation, live inference and table writes are each off by default.

## 12. Historical and weather analytics

- **Trips:** Auto Loader → Silver with quarantine and duplicate split (40 valid, 0 quarantined, 0 duplicates; 35 member and 5 casual riders over 17 days) → `gold_daily_trip_demand` per station and day.
- **Weather:** 48 hourly readings → `dim_weather_hourly` → `gold_weather_demand` by `trip_date` and temperature bucket (`COLD_0_10C`, `MILD_10_20C`, `WARM_20C_PLUS`, or `UNKNOWN` when there is no reading).
- **Coverage is shown, not hidden:** 4 of 40 trips fall inside the weather window (coverage 0.10). The other days are labelled `UNKNOWN` with coverage 0.0.
- **This is descriptive analytics, with no forecasting model.** Forty trips cannot support a demand forecast, and the project does not claim one.

## 13. AI/BI dashboard

A five-page Lakeview dashboard, **generated per target** by `scripts/build_dashboard.py` from the read-only SQL in `sql/10`–`14`. CI checks that the committed JSON is up to date (`--check`).

| Page | Datasets | Highlights |
|---|---|---|
| Current Operations | 4 (`ops_*`) | Point-in-time headline, availability breakdown, top 25 priorities with the **maintenance HOLD** column, action summary |
| Historical Demand | 7 (`demand_*`) | Trips per day, hour × weekday, duration distribution, rideable type, top stations |
| Weather & Demand | 5 (`weather_*`) | Demand by temperature bucket, with weather coverage shown |
| Data Quality | 4 (`quality_*`) | Reconciliation identities, trip rule outcomes, Lakeflow expectations, monthly quality |
| Maintenance & Assistant | 5 | Ticket status, priority/maintenance conflicts, CDC history, checkpoint (`processed_version` 4, `last_data_change_version` 3), assistant evaluation |

All **25 / 25 datasets ran live** ([evidence](evidence/2026-10-13_personal_dev_dashboard_validation.md)). The dashboard is published to the owner only, with `embed_credentials: false`. Station figures are labelled as one snapshot and never charted over time. No screenshots are committed; see the live dashboard IDs in [§5](#5-high-level-architecture).

## 14. Operations assistant (RAG)

```mermaid
flowchart LR
  KB[8 fictional procedures] --> ING[ingest_knowledge_base<br/>content hash, MERGE]
  ING --> CH[(rag_document_chunks<br/>900 chars, 120 overlap<br/>30 chunks, CDF on)]
  CH --> IDX[AI Search Delta Sync<br/>TRIGGERED, gte-large-en]
  Q[Operator question] --> RET[Hybrid retrieval<br/>top_k 4]
  IDX --> RET
  RET --> PR[Grounded prompt<br/>numbered excerpts<br/>treated as untrusted]
  PR --> LLM[gpt-oss-20b]
  LLM --> ANS[Answer with citations<br/>or clean refusal]
  ANS --> MON[(rag_query_monitoring<br/>SHA-256 question hash)]
```

```mermaid
sequenceDiagram
  autonumber
  actor Op as Operator
  participant App as RAG client
  participant VS as AI Search index
  participant FM as Chat model
  participant Mon as Monitoring table
  Op->>App: question
  App->>VS: hybrid query, top_k 4
  VS-->>App: chunks with section references
  App->>FM: system rules + numbered excerpts, max 6000 chars
  FM-->>App: answer citing excerpt numbers, or refusal
  App->>Mon: latency, chunk IDs, question hash, no raw text
  App-->>Op: answer with sources
```

**Prompt-injection hardening:** the system prompt says to use only the numbered excerpts, to treat document text as untrusted data, and never to follow instructions embedded in an excerpt or reveal system prompts.

**Live evaluation** (run `712590508231976`, 18 questions, baseline vs RAG, 36 chat calls):

| Metric | No-retrieval baseline | RAG |
|---|---|---|
| Correctness (lexical keyword metric) | 0.148 | **0.685** |
| Retrieval recall | — | **1.0** |
| Citation precision | — | **0.967** |
| Source coverage | — | 0.938 |
| Correct answer-or-refuse decisions | 13 / 18 | **18 / 18** |
| Errors | — | 0 |
| Median latency | 2.6 s | 1.5 s |

During evaluation, citation precision is the share of valid `[n]` citations that point at an expected source document; a number with no retrieved excerpt behind it counts as invalid. The correctness metric counts expected keywords, so it rewards wording and is not a semantic judge. The answers were also reviewed manually ([evidence](evidence/2026-10-13_personal_dev_e5_rag_evaluation.md)).

> [!WARNING]
> **There is no hosted assistant today.** The AI Search endpoint `urbanflow-final-dev` was created for the evaluation window and **deleted after 25.7 minutes** ([E6](evidence/2026-10-13_personal_dev_e6_ai_search_cleanup.md)), because it bills every hour it exists. The Delta tables remain. Recreating the endpoint and index is a billable action that **needs explicit approval**. An offline fixture mode (`urbanflow-rag ask`) demonstrates the flow without any cloud call.

## 15. Governance and security

**Governance demo** (on isolated copies, then rolled back and the schema dropped; [evidence](evidence/2026-10-13_personal_dev_governance_demo.md)):

| Control | Observed |
|---|---|
| Row filter by dispatch zone, deny by default | No mapping row → **0** stations; mapping `SOUTH` → **28**; mapping `ALL` → **40** |
| Coordinate mask | Latitude and longitude **rounded to 2 decimals** for non-admins: usable for a city map, not the exact dock |
| Rider-privacy mask | `ride_id` shown as a **SHA-256 hash** |
| Rollback | No masks or filters remain; the Final Project data schema was never touched |

**Limitation:** the workspace has one identity and no `urbanflow_admins` group. The demo changed the operator's own mapping row. It does **not** prove enforcement across several principals, or the admin branch of the masks.

**Security by design:**

- No PAT anywhere. CD uses OAuth M2M (DEV) or GitHub OIDC (Azure, untested). Secrets live only in GitHub environment secrets, and the CI secret scan blocks committed credentials.
- The service principal holds read grants by default. Deploy rights were granted only for the approved release and removed afterwards ([§17](#17-cicd)).
- Serverless only: every data task asserts serverless compute and refuses classic clusters.
- Isolation: own schemas, Volume, Jobs and index. Tests reject Demo 3 schema names, and the Zerobus code refuses Lab 11 objects and credentials.
- Monitoring stores question hashes, never raw questions.

## 16. Environments

| | Personal DEV | Azure trial (PROD-style) |
|---|---|---|
| Workspace | Personal Databricks workspace | Academy Azure Databricks `dbr_dev_trial_2` (trial, not paid production) |
| Bundle target | `personal_dev` (no development mode, so UC names are not prefixed) | `azure_prod` |
| Catalog.schema | `workspace.urbanflow_final_dev` | `dbr_dev.parvinbadalov_urbanflow_final_prod` |
| Bundle state | `/Workspace/Users/parvinbadalov@yahoo.com/.bundle/urbanflow_final_project/personal_dev` (pinned) | `/Workspace/Users/parvinbadalov@softserve.academy/.bundle/urbanflow_final_project/azure_prod` (pinned) |
| Validated by | Steps 1–8 (operator) **and GitHub Actions CD** | Step 9 (operator deploy and one sample run) |
| GitHub environment | `final-project-personal_dev`: `main` only, client ID, warehouse ID, OAuth secret | `final-project-azure_prod`: `main` only, warehouse ID, **no credential** |
| CD identity | `sp-urbanflow-final-dev` (OAuth M2M) | GitHub OIDC service principal: **not created** (needs Academy account admin) |

The shared approval gate is `final-project-approval`: required reviewer `BadalovP`, administrator bypass off.

## 17. CI/CD

Five delivery paths, each with a different meaning:

| Path | Trigger | Writes? | Status |
|---|---|---|---|
| **Ordinary CI** (`final_project_ci.yml`) | Every PR and push to `main` touching `final_project/**`, the two Final Project workflows or the root `README.md` | No | Complete; green on `main` |
| **Plan-only CD** (`mode=plan_only`) | Manual dispatch | No: GET requests only | Live-validated: [38392729496](https://github.com/BadalovP/Databricks-Academy-Lakehouse/actions/runs/38392729496) |
| **Full protected CD** (`deploy_and_sample_run`) | Manual dispatch + reviewer approval | Bundle deploy + one bounded Job | Live-validated on Personal DEV: [38400575076](https://github.com/BadalovP/Databricks-Academy-Lakehouse/actions/runs/38400575076) |
| **Operator Azure deploy** | Owner's own login, same plan validator | Yes | Live-validated (step 9) |
| **Azure GitHub OIDC** | Manual dispatch, `target=azure_prod` | — | **Documented but not demonstrated** (identity needs Academy admin) |

```mermaid
flowchart TD
  subgraph CI["final_project_ci.yml: PR and push"]
    SS[secret scan] --> LINT[Ruff and Black] --> PT[pytest with local Spark]
    PT --> DC[dashboard --check] --> BS[offline bundle schema] --> DOCS[docs check]
    SC[change scope: deployable or not]
  end
  subgraph CD["final_project_cd.yml: manual dispatch only"]
    SV[static-validation<br/>typed confirmation, gates] --> PL[plan, read-only<br/>identity, validate, plan, validator]
    PL --> ART[plan artifact for review]
    PL -->|plan_only| STOP([stop])
    ART --> AP{final-project-approval<br/>required reviewer}
    AP --> RP[re-plan] --> DEP[bundle deploy] --> POST[post-plan all skip]
    POST --> RUN[optional bounded sample run<br/>validate_job_run]
    RUN --> EV[release evidence artifact]
  end
  CI -. merged to main .-> CD
```

**The passing full CD run** (`0d48970956e6502f0d969e06f0820f7e75d3f9c8`, inputs `personal_dev` / `deploy_and_sample_run` / `replay_recorded`):

```mermaid
sequenceDiagram
  autonumber
  actor Owner
  participant GH as GitHub Actions
  participant SP as sp-urbanflow-final-dev
  participant DBX as Personal DEV workspace
  Owner->>GH: dispatch with typed confirmation
  GH->>GH: static validation, 867 passed and 1 skipped
  GH->>DBX: plan as SP, GET only
  DBX-->>GH: 7 of 7 skip, validator PASS, artifact uploaded
  GH->>Owner: approval request on final-project-approval
  Owner->>GH: approve at 12:55:46 UTC
  GH->>DBX: re-plan, validator PASS
  SP->>DBX: take lock, bundle deploy
  GH->>DBX: post-deploy plan, 7 of 7 skip
  GH->>DBX: run-now with idempotency token, replay_recorded
  DBX-->>GH: run 112906299302442 SUCCESS in 457 s
  GH->>GH: validate_job_run PASS, release artifact uploaded
```

| # | Step | Result |
|---|---|---|
| 1 | Typed confirmation `RELEASE_URBANFLOW_FINAL_PERSONAL_DEV` | PASS |
| 2 | Cost and scope warning | Printed |
| 3 | Secret scan, Ruff, Black, pytest, dashboard check, bundle schema | PASS, 867 passed, 1 skipped |
| 4 | Pinned host, OAuth M2M | PASS |
| 5 | Identity check | `Acting as sp-urbanflow-final-dev` |
| 6 | `bundle validate`, `bundle plan`, plan validator, Job-name collisions | 7 / 7 `skip` |
| 7 | Plan artifact uploaded | `final-project-plan-personal_dev-38400575076` |
| 8 | Protected approval | `BadalovP`, 12:55:46 UTC |
| 9 | Re-plan before deploy | PASS |
| 10 | `bundle deploy --fail-on-active-runs` | PASS |
| 11 | Post-deploy plan unchanged | 7 / 7 `skip` |
| 12 | One bounded sample run + `validate_job_run.py` | Databricks run `112906299302442` SUCCESS, 457 s |
| 13 | Release evidence uploaded | `final-project-release-personal_dev-38400575076` |

**What `replay_recorded` protected.** The Job's defaults describe a first run: a new station execution and CDC `batch_001`. On DEV, which was at `batch_003`, that would have **reverted the maintenance data** and **doubled the dashboard counts**. The run therefore replayed the recorded execution `urbanflow-final-304828008837365` with `batch_003`. Result: **business data unchanged**. Bronze, CDC and RAG tables made no commit, and the CDC snapshot matched, so no MERGE ran. The 16 shared Silver/Gold tables each wrote **one metadata-only Delta `MERGE` commit with 0 rows changed**. So the run did write Delta commits; it changed no data.

**Engineering lessons from the CD runs** (all failed runs are kept as evidence):

| Run | What failed | Lesson → fix |
|---|---|---|
| `38382998636`, `38389951946` | `please specify target` | A bundle folder with no default target needs `-t "$TARGET"` on **every** workspace command → added everywhere, with a test |
| (before any deploy) | A service principal would plan 7 *creates* | `root_path` with `${workspace.current_user.userName}` makes bundle state **identity-dependent** → pinned to the owner's folders |
| `38397086064` | `Failed to acquire deployment lock: access denied` | Folder `CAN_EDIT` cannot create the lock file → **temporary `CAN_MANAGE` on that one folder**, owner-approved, removed after the run |
| (caught before dispatch) | Job defaults would have reverted CDC | Default parameters are a data risk → `sample_profile` input, default `replay_recorded` (PR #78) |

After the run, the principal was restored to `CAN_READ` on the bundle folder and `CAN_VIEW` on the Job. Full record: **[GitHub CD evidence](evidence/2026-10-13_github_cd_full_deploy.md)** · design and Lab 08 comparison: [docs/CI_CD.md](docs/CI_CD.md).

## 18. Testing

| Run | Platform | Result |
|---|---|---|
| **CI of the full CD run `38400575076`** (primary figure) | Linux | **867 passed, 1 skipped** |
| CI of the plan-only run `38392729496` | Linux | 860 passed, 1 skipped |
| Release baseline before merge of PR #75 | Windows | 858 passed, 2 skipped |

The counts grew as release tests were added. The skipped tests depend on the platform, for example a POSIX-only check.

| Layer | Examples |
|---|---|
| Pure unit tests | Priority formula, status rules, chunking, prompt construction, citation validation, config |
| Local Spark | Silver split and reconciliation, change-aware MERGE, CDC replay, deletes, ordering, retention gap, Lakeflow functions |
| SQL and dashboard | Read-only SQL safety, no Demo 3 schema names, dashboard JSON freshness and dataset contract |
| Notebook contract | Every code cell documented; parameters and task keys match the bundle |
| Release tooling | Plan validator, run validator, sample-request profiles, change scope, workflow structure (no `always()` bypass, `bash -n` on every `run:` block), secret scan, docs links and anchors |

Fifty-one test modules live in [`tests/`](tests/). Java 17 is needed for the local Spark tests.

## 19. Cost and performance

> Labelled figures: **measured** values come from `system.billing.usage` in Personal DEV. **Estimates** are marked. Azure trial billing could not be read (no `USE SCHEMA` on `system.billing`).

| Item | DBU | ≈ USD at list | Basis |
|---|---|---|---|
| Final Project compute in Personal DEV, all validation steps | ≈ 9.15 | ≈ $3.00 | Measured |
| AI Search endpoint, 25.7 minutes | 0.6251 | (included above) | Measured |
| Shared SQL warehouse over the same period | 17.41 | ≈ $12.19 | Measured, **shared with other work**, not Final Project-only |
| Full CD sample run (457 s) | ≈ 1.2 | ≈ $0.42 | **Estimate** from the equivalent 458 s replay |

Design choices that keep cost bounded: no schedules or triggers; `availableNow` instead of always-on streams; every switch off by default; per-task timeouts; a triggered (not continuous) pipeline; the AI Search endpoint created only for an approved window, deleted afterwards and guarded by a scheduled cleanup Job while it existed. Details: [docs/COST_AND_PERFORMANCE.md](docs/COST_AND_PERFORMANCE.md).

## 20. Engineering challenges and lessons

| Challenge | What happened | Resolution | Lesson |
|---|---|---|---|
| Version numbers are not data | An automatic `OPTIMIZE` created CDC source v4 with no change | Checkpoint = newest scanned and reconciled version; retention gap fails closed | Define checkpoint semantics explicitly and test non-data commits |
| Replay created a commit | Identical snapshot → empty MERGE → checkpoint moved | Skip the source MERGE when the snapshot equals the source | Idempotency includes metadata, not just rows |
| Metadata-only MERGE commits | Re-MERGE of identical rows still commits | Change-aware MERGE: 0 rows rewritten; one metadata commit remains | Say exactly what "unchanged" means |
| Demo 3 assumptions | First sample run failed on a fixed count of 2520 and a missing task dependency | Sample-aware assertions; `historical_trips` depends on `station_gold` | Never inherit a predecessor's constants |
| Serverless exit codes | `SystemExit(0)` in a `spark_python_task` reported FAILED | Return normally from the guard script | Test the runtime's semantics, not Python's |
| Lakeview text widgets | Text `lines` were concatenated with no separator | Generator joins paragraphs with blank lines | Validate the rendered result, not just the JSON |
| Governance cleanup | A failed step could leave policies behind | `try/finally` rollback and schema drop | Destructive demos need fail-safe cleanup |
| CD identity and state | Missing `-t`, identity-dependent state, lock permission, unsafe defaults | See [§17](#17-cicd) | Do a read-only plan first; grant deploy rights only for the run |
| Hidden exit codes | `black --check \| tail` masked a failure | `set -o pipefail` | Pipelines hide failures by default |

## 21. Repository structure

```text
final_project/
├── README.md                  # this case study
├── databricks.yml             # bundle: targets personal_dev and azure_prod, pinned root paths
├── pyproject.toml             # package urbanflow, tools, CLI urbanflow-rag
├── .env.example               # variable names only, no values
├── config/                    # dev.yml, azure.yml, rag.yml, rag_evaluation_questions.yml
├── resources/                 # infrastructure (schemas, Volume), job, pipeline, dashboard YAML
├── dashboards/                # generated Lakeview JSON per target
├── notebooks/                 # 01–12 platform tasks, 20–23 assistant tasks
├── pipeline/                  # Lakeflow bronze.py, silver.py, gold.py
├── src/urbanflow/             # tested library: landing, streaming, silver, gold, cdc, historical,
│   └── rag/                   #   weather, federation, quality, persistence, automation, zerobus …
├── sql/                       # 00 storage, 10–14 dashboard (read-only), 20 governance, 21 maintenance
├── scripts/                   # release validators, dashboard builder, change scope, docs check,
│                              #   secret scan, governance demo, cleanup guard, Zerobus producer
├── fixtures/                  # labelled samples and fictional maintenance batches 001–003
├── knowledge_base/            # 8 fictional procedures for the assistant
├── docs/                      # architecture, CI/CD, runbook, cost, dashboard, progress, Demo Day
├── evidence/                  # dated Markdown + JSON record of every live step
└── tests/                     # 51 test modules
```

## 22. How to reproduce

**Offline (free, no credentials):**

```bash
cd final_project
python -m venv .venv && . .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
ruff check . && black --check .
pytest                                              # local Spark needs Java 17
python scripts/build_dashboard.py --check
python scripts/validate_bundle.py                   # Databricks CLI, no login
python scripts/check_docs.py
python scripts/scan_secrets.py
urbanflow-rag ask "What should an operator do when both bikes and docks are critically low?"
python scripts/run_governance_demo.py --target personal_dev     # dry run, prints the plan
```

**Live (billable; each step needs approval).** Follow the [operations runbook](docs/OPERATIONS_RUNBOOK.md):

1. `databricks bundle plan -t personal_dev` and review: only the 7 Final Project resources, no deletes.
2. `databricks bundle deploy -t personal_dev` (creates definitions only, no compute).
3. Run the platform Job with the sample switches. This is **billable serverless compute**, about 1.2 DBU per run.

> [!CAUTION]
> `prepare_vector_search` with `allow_ai_search_mutation=true` creates an AI Search endpoint that **bills every hour it exists**. Create it only for a planned window, delete it afterwards, and confirm none remains. The governance demo with `--execute` changes table policies; it works on isolated copies and rolls back in `finally`.

Never run `bundle destroy`: the Volume is `prevent_destroy`, and recovery is a redeploy of a previous commit through the same gates.

## 23. Evidence index

| Step | Evidence |
|---|---|
| 1. DEV deploy | [step1](evidence/2026-10-12_personal_dev_step1_deploy.md) |
| 2. Platform sample run (and failed attempts) | [pass](evidence/2026-10-12_personal_dev_step2_pass.md) · [first run](evidence/2026-10-12_personal_dev_step2_sample_run.md) · [retry](evidence/2026-10-12_personal_dev_step2_retry.md) |
| 3. Strict idempotency | [fail](evidence/2026-10-12_personal_dev_step3_idempotency.md) · [retry pass](evidence/2026-10-12_personal_dev_step3_retry_pass.md) |
| 4. CDC | [redeploy](evidence/2026-10-12_personal_dev_step4_redeploy.md) · [batch_002](evidence/2026-10-12_personal_dev_step4_batch002.md) · [batch_003](evidence/2026-10-12_personal_dev_step4_batch003.md) |
| 5. Lakeflow | [step5](evidence/2026-10-12_personal_dev_step5_lakeflow.md) |
| E1–E6. Assistant | [ingest](evidence/2026-10-13_personal_dev_e1_rag_ingestion.md) · [AI Search](evidence/2026-10-13_personal_dev_e2_e4_ai_search_smoke.md) · [evaluation](evidence/2026-10-13_personal_dev_e5_rag_evaluation.md) · [cleanup](evidence/2026-10-13_personal_dev_e6_ai_search_cleanup.md) |
| D. Dashboard | [validation](evidence/2026-10-13_personal_dev_dashboard_validation.md) · [title fix](evidence/2026-10-13_personal_dev_dashboard_title_fix.md) |
| F. Governance | [demo](evidence/2026-10-13_personal_dev_governance_demo.md) |
| 9. Azure trial | [deploy and sample run](evidence/2026-10-13_azure_prod_step9_deploy_and_sample_run.md) |
| GitHub CD | [plan-only closeout](evidence/2026-10-13_github_cd_plan_only_closeout.md) · [corrected plan-only](evidence/2026-10-13_github_cd_plan_only_run2.md) · **[full protected deploy](evidence/2026-10-13_github_cd_full_deploy.md)** |
| GitHub runs | [38392729496](https://github.com/BadalovP/Databricks-Academy-Lakehouse/actions/runs/38392729496) (plan-only PASS) · [38397086064](https://github.com/BadalovP/Databricks-Academy-Lakehouse/actions/runs/38397086064) (lock FAIL) · [38400575076](https://github.com/BadalovP/Databricks-Academy-Lakehouse/actions/runs/38400575076) (full PASS) |

Supporting documents: [Architecture](docs/ARCHITECTURE.md) · [CI/CD](docs/CI_CD.md) · [Release and validation](docs/RELEASE_AND_VALIDATION.md) · [Operations runbook](docs/OPERATIONS_RUNBOOK.md) · [Cost and performance](docs/COST_AND_PERFORMANCE.md) · [Dashboard](docs/DASHBOARD.md) · [Quality framework](docs/QUALITY_FRAMEWORK.md) · [Labs 1–12 coverage](docs/LABS_1_TO_12_COVERAGE.md) · [Implementation progress](docs/IMPLEMENTATION_PROGRESS.md) · [Demo Day presentation](docs/DEMO_DAY_PRESENTATION.md)

## 24. Scope and limitations

| Item | Status |
|---|---|
| Sample size | 40 stations, 40 trips, 48 weather hours: validates logic, not scale |
| Station data | One real GBFS snapshot; no time series of station status |
| Federation source | Implemented and offline-tested; **not live-verified** (CDC ran on fixture snapshots) |
| CDC retention-gap path | Offline-tested only (reproducing it live needs a VACUUM of the source) |
| Hosted assistant | **None**: the endpoint was deleted after the evaluation; recreation needs approval |
| RAG metric | Lexical keyword correctness, reviewed manually; not a semantic judge |
| Multi-principal governance | Not demonstrated (one identity) |
| GitHub CD to Azure | Not demonstrated: Azure was deployed by the operator; Azure OIDC untested |
| Event Hubs `existing_bronze`, Zerobus, full January month | Optional; implemented and offline-tested; not run live |
| Forecasting | Not in scope; analytics are descriptive |

## 25. Future work (optional)

- Create the Azure OIDC service principal and federation policy (needs Academy account admin), then run `plan_only` against `azure_prod`.
- Run the Federation path against a real PostgreSQL source, and the full January month.
- Collect a station-status time series to support trend charts and, eventually, demand forecasting.
- Add an LLM-as-judge metric next to the keyword metric.
- Demonstrate governance with several principals and a real admin group.

## 26. Conclusion

UrbanFlow 2.0 shows the full path from raw files to a governed, tested and release-automated lakehouse. Every rule is auditable, every live result has dated evidence, and every failure is kept next to its fix. The same bundle produced identical results in two workspaces. A protected GitHub Actions release deployed it as a least-privilege service principal without changing a row of business data. The project states its limits as clearly as its results.

---

**Parvin Badalov**, Data Engineer · [GitHub](https://github.com/BadalovP) · [LinkedIn](https://www.linkedin.com/in/parvin-badalov/) · [← Repository homepage](../README.md)

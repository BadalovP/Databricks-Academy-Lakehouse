# UrbanFlow academy Labs 1–9 coverage matrix

[← Main README](../README.md) · [Architecture](ARCHITECTURE.md)

Status meanings:

- **Implemented locally:** code or documentation exists and has offline/static validation.
- **Discovered read-only:** the real resource/capability was observed but not exercised.
- **Pending live:** implementation or Azure execution evidence still remains.
- **Validated live:** executed against real Azure resources with reconciled evidence.
- **Blocked:** a confirmed permission/capability prevents progress.

**First live validation completed 2026-09-29.** The bounded Citi Bike GBFS -> Event Hubs ->
Bronze path ran on GP1 with an exact producer-to-Bronze reconciliation: 2,520 published, 2,520
consumed, 2,520 accepted, 0 rejected, 0 duplicate, 0 missing and 0 unexpected event IDs, with
Kafka partition 0 offsets 780-3,299 recorded. See
[FIRST_STREAMING_TEST.md](FIRST_STREAMING_TEST.md). Rows below promoted to `Validated live` cite
that run; everything else is unchanged and still honest about what has not run.

| Lab | Required topic | UrbanFlow implementation | File / validation | Evidence | Status |
|---:|---|---|---|---|---|
| 1 | Git and notebooks | Dedicated feature branch and Databricks source-format notebooks | `notebooks/01_fundamentals.py` | Local Git state; notebook format audit | Implemented locally |
| 1 | CSV and JSON | Real GBFS JSON and January 2024 trip CSV samples | `data/samples/`, `scripts/prepare_demo.py` | 40 + 40 rows validated | Implemented locally |
| 1 | External REST API | Dynamic GBFS discovery and Open-Meteo validation | `gbfs_client.py`, `validate_sources.py` | Public endpoints returned current schemas | Implemented locally |
| 1 | DataFrames | Explicit Spark DataFrames for station information/status | `01_fundamentals.py` | Static review; live notebook pending | Implemented locally |
| 1 | select/filter/join/groupBy | Operational filter, UUID join, availability aggregation | `01_fundamentals.py` | Pure business rules unit-tested | Implemented locally |
| 1 | Delta and SQL | Opt-in idempotent MERGE plus dashboard-ready SQL | `01_fundamentals.py` | Write defaults off | Implemented locally |
| 1 | Basic dashboard | SQL source query designed | Notebook Step 10 | AI/BI object/screenshots absent | Pending live |
| 1 | Shared cluster / General schema differences | GP1 preferred and GP2 fallback through configuration; must already be running | config, Job resource, inventory | IDs/runtime/access/permissions discovered read-only | Implemented locally |
| 2 | Azure provisioning settings | Existing workspace, ADLS Gen2, Key Vault settings documented | `RESOURCE_INVENTORY.md` | Azure CLI read-only inventory | Discovered read-only |
| 2 | Personal container / external location | Existing identity external location identified | `RESOURCE_INVENTORY.md` | UC external location listed | Discovered read-only |
| 2 | Storage credential | Managed-identity credential identified | `RESOURCE_INVENTORY.md` | UC credential listed | Discovered read-only |
| 2 | Schemas and Volume | `dbr_dev.parvinbadalov_urbanflow` + managed Volume `urbanflow_landing` created | `sql/00_prepare_urbanflow_storage.sql`, UC API | Both created 2026-09-29 and used by the live run | Validated live |
| 2 | Managed vs external tables | Bronze managed Delta validated; Silver/Gold managed-table MERGEs prepared | `ARCHITECTURE.md`, `persistence.py` | Bronze exists; Phase 2 tables pending | Implemented locally |
| 2 | Key Vault-backed secret | Producer reads it from Key Vault; notebook reads it via `azure-secrets`; value never printed | `producer.from_key_vault`, notebook Step 5 | Both paths authenticated in the live run | Validated live |
| 2 | Idempotent Bronze + metadata | Event ID, source/collection times, Kafka metadata, MERGE lesson | source modules and notebook | Offline tests | Implemented locally |
| 2 | Ingestion Job | Bounded Bronze Job plus dry-run Phase 2 DAG | `resources/jobs.yml` | Bronze Job `404404108673495`; Phase 2 local only | Implemented locally |
| 2 | Legacy mounts exercise | Explanation deliberately separated from core design | Architecture security boundary | No deprecated mount created | Pending live |
| 3 | Auto Loader | Historical contract and Volume layout planned | `ARCHITECTURE.md` | Source verified; loader pending | Pending live |
| 3 | Structured Streaming | Kafka reader, explicit-schema parser, bounded `availableNow` writer | `streaming.py`, `03_eventhubs_to_bronze.py` | Job run `873010921866250` SUCCESS in 130 s; 2,520 rows to Bronze | Validated live |
| 3 | Event Hubs / Kafka | Existing Standard namespace/hub plus GP1/GP2 compatibility contract | inventory/config/streaming | 2,520 messages published and consumed once | Validated live |
| 3 | Real Python producer | TTL-aware, duplicate-aware, finite Event Hubs producer | `producer.py`, CLI | 2,520-event live producer report | Validated live |
| 3 | Explicit schema | `StructType` station event contract | `streaming.py` | Static/tests | Implemented locally |
| 3 | Schema inference/evolution/rescue | Explicit-schema `rescue` path plus schema-hint `addNewColumns` lesson | `historical.py`, architecture | Local tests; Auto Loader not run | Implemented locally |
| 3 | schemaLocation/checkpoint | Separate historical schema/checkpoint paths plus validated Bronze checkpoint | config, `historical.py`, notebooks | Bronze checkpoint used live; historical pending | Implemented locally |
| 3 | availableNow / micro-batches (Kafka) | Bounded Kafka writer with a bounded wait | `03_eventhubs_to_bronze.py`, `streaming.py` | Job run `873010921866250` consumed 2,520 rows and terminated | Validated live |
| 3 | availableNow / micro-batches (historical Auto Loader) | `start_historical_available_now` with a schema checkpoint | `historical.py` | Unit-tested writer wiring only; never run against a real archive | Implemented locally |
| 3 | Replay and semantics | Exactly-once/at-least-once explanation | README / architecture | Documentation review | Implemented locally |
| 3 | Approximately 1,000 files | Local generator planned | Coverage matrix | Generator absent | Pending live |
| 4 | Cleaning, explicit schemas, deduplication | Physical Spark contract, quarantine and deterministic broker ordering | `silver.py` | Live run wrote 2,520 Silver rows, 0 quarantined, IDs unique, `PASS` | Validated live |
| 4 | MERGE / rerun safety | Identifier-safe, duplicate-rejecting Delta MERGE for all Phase 2 outputs | `persistence.py`, Silver/Gold notebooks | Second identical run left all eight tables at `inserted_rows = 0`, before == after | Validated live |
| 4 | SCD1 / SCD2 | New/changed/unchanged/missing stations, null-safe changes, interval audits | `dimensions.py`, tests | Real local Spark tests | Implemented locally |
| 4 | Enforcement/evolution | Explicit contracts; controlled evolution strategy | source / architecture | Delta behavior pending | Implemented locally |
| 4 | Column mapping | Planned controlled rename demonstration | Architecture | None | Pending live |
| 4 | Data contracts | GBFS and event contracts documented and tested | client/streaming/architecture | Unit tests | Implemented locally |
| 4 | OPTIMIZE/VACUUM/liquid clustering | Comparison and safe maintenance exercise still required | Coverage matrix | None | Pending live |
| 5 | Lakeflow pipeline | Serverless triggered resource and Bronze Kafka streaming table declared | pipeline and `resources/pipelines.yml` | Bundle schema validation; not deployed | Implemented locally |
| 5 | Streaming tables / materialized views | Physical model specified | `ARCHITECTURE.md` | None | Pending live |
| 5 | Bronze/Silver/Quarantine/Gold | Physical Silver/Quarantine/duplicate MERGEs and reconciled Gold model | `silver.py`, `gold.py`, notebooks 04/05 | Real local Spark tests; tables pending | Implemented locally |
| 5 | Expectations, lineage, monitoring | Requirements identified | Architecture | Live evidence absent | Pending live |
| 5 | DAB deployment | Bundle targets plus undeployed Job and serverless Lakeflow resources | `databricks.yml`, `resources/` | Static validation | Implemented locally |
| 6 | Fact/dimension model | Stable availability fact, 40-row development dimension, daily summary and shortage outputs | `gold.py`, notebook 05 | Real local Spark tests; tables pending | Implemented locally |
| 6 | AI/BI dashboard and filters | Required views and first SQL source defined | notebook / README | Dashboard absent | Pending live |
| 6 | Alerts / email | Volume-drop and shortage alert requirement retained | Coverage matrix | Destination/warehouse unverified | Pending live |
| 6 | Permissions, RLS, masking | Safe non-PII demonstration planned | Resource inventory | Groups/objects not selected | Pending live |
| 6 | Weather enrichment | Real Open-Meteo sample and contract | sample / architecture | API verified; Gold join pending | Implemented locally |
| 7 | Importable functions | Client, transformations, quality, producer, monitoring, automation | `src/urbanflow/` | Import/test pass | Implemented locally |
| 7 | pytest / Ruff / Black | Dedicated configuration and static workflow | `pyproject.toml`, workflow | Local results recorded in session report | Implemented locally |
| 7 | pre-commit | Local Ruff, Black, and pytest hooks | `.pre-commit-config.yaml` | Uses the project development environment | Implemented locally |
| 7 | chispa / Spark tests | Real local Spark suites for Silver, Gold, historical joins and SCD | `tests/test_{silver,gold,historical,dimensions}.py` | Local pytest | Implemented locally |
| 7 | Duplicate, late, SCD edge cases | Cross-partition duplicate order, null transitions, invalid intervals and out-of-order SCD changes | tests | Local pytest | Implemented locally |
| 7 | DQ dimensions | Completeness, uniqueness key, validity, consistency, referential integrity, freshness | `quality.py` | Unit tests | Implemented locally |
| 7 | Lakeflow expectations / Delta constraints | Planned with full pipeline | Architecture | None | Pending live |
| 7 | Great Expectations or Soda | Suite not selected yet | Coverage matrix | None | Pending live |
| 7 | Reconciliation / anomaly gates | Bronze outcome and Silver-to-Gold grain/aggregate gates | `silver.py`, `gold.py` | Real local Spark tests | Implemented locally |
| 7 | Databricks Connect / monitoring | Environment capability not yet tested | Coverage matrix | None | Pending live |
| 8 | DAB and GitHub Actions | Static workflow, two targets, two existing-cluster Jobs, managed-compute pipeline | workflow / bundle/resources | Static local validation | Implemented locally |
| 8 | PR static checks | Ruff, Black, pytest, bundle validation | workflow | Milestone 1 PR #34 passed and merged | Implemented locally |
| 8 | Environment config | `dev.yml`, `azure.yml`, DAB targets | config / bundle | Static only | Implemented locally |
| 8 | Idempotent deployment / approvals | Live job deliberately absent pending resource approval | cost plan | None | Pending live |
| 8 | DEV-to-PROD promotion | Second authorized environment not established | resource inventory | One real target confirmed | Pending live |
| 8 | Post-deploy validation | Success criteria specified | cost plan | None | Pending live |
| 9 | Authentication / workspace verification | Explicit profile and exact host checks | `automation.py`, CLI | Real identity verified read-only outside module | Implemented locally |
| 9 | Cluster provisioning | Optional create helper is disabled by default; GP1/GP2 mutation and termination are blocked | config/automation/tests | Mocked negative-path tests | Implemented locally |
| 9 | Notebook upload | Base64 SOURCE import helper | `automation.py` | Mock/live test pending | Implemented locally |
| 9 | Jobs API / pipeline trigger | Polling primitives implemented | automation/monitoring | Create/reset/trigger orchestration pending | Pending live |
| 9 | Explicit polling/timeouts/errors | Generic bounded polling and domain errors | monitoring/client/tests | Unit tests | Implemented locally |
| 9 | JSON reports | Producer, Bronze, and offline end-to-end reconciliation reports | CLI / `reporting.py` / runbook | Offline tests; live report pending | Implemented locally |
| 9 | Termination verification | Exact `TERMINATED` required | `automation.py` | Mocked test | Implemented locally |
| 9 | CI integration | Static CI only; live integration requires approval | workflow | No live workflow | Pending live |

## Next status changes allowed

Rows move to `validated live` only after dated evidence is stored and linked. A successful mocked
test never changes a live-execution status. A visible resource never proves write permission.

# UrbanFlow 2.0 architecture and design decisions

[← Main README](../README.md) · [Labs 1–12 coverage](LABS_1_TO_12_COVERAGE.md) · [Cost and performance](COST_AND_PERFORMANCE.md) · [Release and validation](RELEASE_AND_VALIDATION.md)

## Design goals

UrbanFlow 2.0 is one lakehouse that answers two operational questions for a bike-sharing network:

1. **Where should a rebalancing van go next, and where must it not go?** This is answered by the station medallion, Gold priorities and maintenance CDC.
2. **What does the procedure say?** The mobility operations assistant answers this, with RAG over governed procedures.

Six principles shape every component:

- **One integrated solution.** One bundle, one governed schema per target, one platform Job and one assistant Job. The dashboard joins the CDC and Gold outputs.
- **Bounded by default.** Every stream uses `availableNow` with a bounded wait. Every task has a timeout. No Job has a schedule. Every billable switch defaults to `false`.
- **Serverless only.** Jobs, Lakeflow and SQL use serverless compute, and `assert_serverless_compute` refuses any classic cluster.
- **Isolated.** The Final Project owns its schemas, Volume, Job, pipeline, dashboard, index and identities. It never reads or writes Demo 3 or Lab objects, and tests enforce this.
- **Idempotent and reconciled.** Each layer is written with MERGE or an execution-scoped replace and reconciled against the previous layer. A rerun writes nothing new.
- **Honest.** Samples, synthetic data and fictional documents are labelled where they are produced. Nothing claims a live result that was not recorded.

## Components

| Component | What it does | Where |
|---|---|---|
| Source clients | GBFS discovery and station feeds, Open-Meteo archive, Citi Bike trip archive; retries and bounded requests | `src/urbanflow/gbfs_client.py`, `weather.py`, `historical.py` |
| Landing + Bronze | Idempotent landing into the Volume, then Auto Loader `availableNow` into `bronze_station_status`. On Azure, the Event Hubs Kafka consumer is an alternative. | `notebooks/02`, `03_eventhubs_to_bronze.py`, `landing.py`, `streaming.py` |
| Silver | Typed parsing, three-way quality split, idempotent MERGE, SCD station dimension | `notebooks/04`, `silver.py`, `quality.py`, `dimensions.py` |
| Gold | Availability fact, shortage list, rebalancing priority, daily demand, weather demand | `notebooks/05`–`07`, `gold.py`, `weather.py` |
| Validation | Final reconciliation and expectation gate for the run | `notebooks/10`, `expectations.py` |
| CDC | Federation or fixture snapshot → CDF source → incremental MERGE → audit history → reconciliation → forward-only checkpoint | `notebooks/11`, `cdc_sql.py`, `cdc.py`, `federation.py` |
| Lakeflow | Declarative re-implementation of the station path in an isolated schema, with expectations | `pipeline/`, `resources/urbanflow_final.pipeline.yml` |
| Assistant | Corpus → Delta (CDF) → Delta Sync hybrid index → grounded, cited answers → evaluation and redacted monitoring | `knowledge_base/`, `src/urbanflow/rag/`, `notebooks/20`–`23` |
| Zerobus (optional) | Synthetic station-health push ingestion into Final-Project-owned Bronze, then exactly-once Silver | `zerobus.py`, `zerobus_sql.py`, `scripts/run_zerobus_health.py`, `notebooks/12` |
| Dashboard | Five pages generated per target from read-only SQL | `sql/10`–`14`, `scripts/build_dashboard.py`, `dashboards/` |
| Release | CI checks; manual CD with plan gates and post-deploy no-drift check | `.github/workflows/final_project_*.yml`, `scripts/validate_*.py` |

## Environments

| Target | Workspace | Data | Role |
|---|---|---|---|
| `personal_dev` (DEV presets, no renaming mode) | Personal Databricks on AWS (`dbc-1750318a-76a9`) | `workspace.urbanflow_final_dev`, Lakeflow `workspace.urbanflow_final_lakeflow_dev` | Development and the first live validation |
| `azure_prod` (production mode) | Academy Azure `dbr_dev_trial_2` (`adb-7405618361895520`) | `dbr_dev.parvinbadalov_urbanflow_final_prod`, Lakeflow `..._final_lakeflow_prod` | PROD-style release target |

The two targets are in **different clouds and different metastores**, so a DEV mistake cannot reach PROD data. Two limits apply, stated plainly:

- `azure_prod` is an Academy **trial** workspace. It may expire, and it is not a production SLA.
- Both targets protect governed storage: the schemas and the Volume carry `lifecycle.prevent_destroy`, so `bundle destroy` cannot drop them.
- `personal_dev` does not use `mode: development`, because that mode renames Unity Catalog schemas to `dev_<user>_<name>` while the Jobs and dashboards address the configured names.
- Its catalog `dbr_dev` is shared with other Academy workspaces and students. Inside that catalog, isolation is at schema level. The Final Project uses only its own `parvinbadalov_urbanflow_final_*` schemas. Demo 3's `parvinbadalov_urbanflow` schema sits in the same catalog and is never referenced, and tests reject its name in SQL, dashboards and governance code.

## Confirmed source contracts

**GBFS.** `https://gbfs.citibikenyc.com/gbfs/2.3/gbfs.json` is discovered at runtime. Station status needs `station_id, num_bikes_available, num_docks_available, is_installed, is_renting, is_returning, last_reported`. UrbanFlow adds `source_last_updated`, `collected_at`, `source_url`, `gbfs_version` and a stable `event_id`.

**Historical trips.** `202401-citibike-tripdata.zip` has `ride_id, rideable_type, started_at, ended_at, start_station_name, start_station_id, end_station_name, end_station_id, start_lat, start_lng, end_lat, end_lng, member_casual`. Its `start_station_id` is the GBFS **short name**, not the UUID `station_id`. The station dimension keeps both keys, and every join uses the short name.

**Weather.** The Open-Meteo hourly archive is for one NYC coordinate (40.7128, −74.0060). It is area-level context, not station-level weather, and comparisons show association, not causation.

**Maintenance (CDC source).** `maintenance_id, station_id, status, severity, updated_at`, one full snapshot per batch. The committed snapshots are fictional tickets on real sample station IDs.

## Streaming and delivery semantics

- Auto Loader and Kafka sources run with `availableNow` and a bounded wait, then stop. Each execution owns its checkpoint and schema location.
- Event Hubs/Kafka and Zerobus deliver **at least once**. Stable event IDs plus MERGE (or earliest-delivery-wins deduplication) make the next layer exactly-once.
- "Exactly once" describes replayable input plus checkpoints plus an idempotent sink. It is not a promise that a source never redelivers.

## CDC design (Lab 10, core capability)

1. A full snapshot arrives as a temporary view. It comes either from a **read-only** Lakehouse Federation query (an approved foreign catalog) or from a committed fixture. Casts give both the same types. A contract check rejects duplicate or null keys.
2. `MERGE` makes `station_maintenance_source` equal to the snapshot. Unchanged rows are not rewritten, so Change Data Feed holds only real changes, deletes included (`WHEN NOT MATCHED BY SOURCE THEN DELETE`).
3. `table_changes(source, checkpoint + 1, latest)` reads exactly the new versions. Every event, pre-images included, is MERGEd once into `maintenance_cdc_history`, keyed by a deterministic `change_id`. The newest final-state event per key is MERGEd into `silver_station_maintenance` (SCD Type 1).
4. **Reconciliation** compares target and source `VERSION AS OF` the processed version, values included, both ways (`EXCEPT ALL`). It also checks for duplicate keys and that every event reached the history.
5. Only then does the checkpoint advance, and only forward (`WHEN MATCHED AND t.processed_version < s.processed_version`). A failure after the MERGEs replays the same range, and the MERGEs absorb it.

This is Delta Change Data Feed on a Final-Project-owned table. It is not database log-based CDC: the external system is only ever read.

## Assistant design (Lab 12)

- **Corpus.** Eight original, fictional UrbanFlow procedures cover rebalancing, maintenance, incidents, data quality, pipeline recovery, source definitions, KPIs and security. That is 30 section-aware chunks (900 characters, 120 overlap).
- **Store.** `rag_documents` and `rag_document_chunks` hold one full-snapshot MERGE that prunes removed documents. They are reconciled with a (key, content_hash) fingerprint, and Change Data Feed is on.
- **Index.** An AI Search Standard endpoint serves a TRIGGERED Delta Sync index. Retrieval is HYBRID (vector plus BM25), top-k 4, with up to 2 supplemental section queries.
- **Answering.** The system prompt restricts the model to numbered excerpts, treats document text as untrusted data, requires citations, and refuses when the excerpts do not answer.
- **System scope.** Scoping excerpts to one system is opt-in, through an explicit mapping. It is off for UrbanFlow, because its documents are topics: deriving systems from title words dropped the document that answered.
- **Evaluation.** 18 questions, two of them unanswerable. The same model answers with no retrieval (baseline) and with RAG. Metrics are correctness, recall, citation precision, source coverage and refusals. Live scores and redacted monitoring events are appended to Delta. Offline fixture scores are never stored.

## Physical model (Final Project status: offline only)

| Layer | Object | Grain |
|---|---|---|
| Bronze | `bronze_station_status` | One received station event (Auto Loader or Kafka) |
| Bronze | `bronze_historical_trips` | One raw trip row of the Job's default sample namespace |
| Silver | `silver_station_status`, `quarantine_station_status`, `duplicate_station_status` | One event ID; one rejected or repeated record |
| Silver | `silver_historical_trips` (+ quarantine, duplicate) | One valid ride per execution |
| Silver | `station_maintenance_source` (CDF), `silver_station_maintenance` | One ticket |
| Audit | `maintenance_cdc_history`, `maintenance_cdc_checkpoint` | One change event; one pipeline key |
| Gold | `fact_station_availability`, `gold_station_shortage`, `gold_rebalancing_priority` | One Silver event; one shortage; one station observation |
| Gold | `gold_daily_trip_demand`, `gold_weather_demand`, `dim_weather_hourly` | Station-day; day; weather hour |
| Assistant | `rag_documents`, `rag_document_chunks`, `rag_evaluation_results`, `rag_query_monitoring` | Document; chunk; question × variant; query |
| Optional | `zerobus_station_health_bronze`, `silver_station_health` | Delivery; event |

None of these tables exists yet in a Final Project schema. They are created by the first approved run.

## SCD and schema evolution

- **SCD 1** corrects descriptive station attributes. **SCD 2** preserves name, capacity and location changes, with null-safe comparisons, rejection of duplicate or out-of-order changes, and an audit of one current version per station. The test scenarios are labelled synthetic.
- **Bronze** keeps raw JSON. The historical reader uses an explicit schema with `rescue`, so unexpected fields land in `_rescued_data`. Additive Delta evolution is explicit (`evolve_delta_schema`). Global auto-merge is off.

## Unity Catalog Volumes, not DBFS mounts

The landing area is the managed Volume `/Volumes/<catalog>/<schema>/urbanflow_final_landing`. Legacy `dbutils.fs.mount()` mounts are workspace-wide, carry long-lived storage keys and bypass Unity Catalog grants and lineage. The project documents that pattern and does not execute it.

## Security boundaries

- Git holds no PAT, connection string, SAS key or client secret.
- **CD identities.** `personal_dev` uses OAuth M2M with a dedicated service principal, whose secret is a GitHub environment secret. `azure_prod` uses GitHub OIDC workload identity federation, with no secret.
- **CD workspace pinning.** CD pins the workspace host for each target and refuses the shared Academy workspaces `dbr_dev` and `dbr_dev_trial`.
- **Event Hubs and Zerobus credentials.** Event Hubs credentials come from a Key Vault-backed secret scope and are never printed. The optional Zerobus path reads only `URBANFLOW_FINAL_ZEROBUS_*` and refuses values equal to Lab 11's.
- **Assistant monitoring.** Monitoring stores a question hash and character count, never the raw question or answer.
- **Governance changes.** Row filters and masks are demonstrated only on isolated copies, and every applied policy has a rollback. No `GRANT` is ever active.

## Failure handling

| Failure | Behaviour |
|---|---|
| Source timeout or invalid JSON | Domain error; nothing partial is landed |
| Same landing path, different content | `land_once` refuses instead of overwriting |
| Invalid record | Quarantined with every failed rule named |
| Stream does not finish in time | Bounded wait raises; the query is stopped in `finally` |
| Reconciliation mismatch | The task raises; the Job fails; no checkpoint moves |
| Index not ready or incomplete | The readiness gate stops the assistant Job before any model call |
| Rerun after a partial failure | The same execution scope or CDC range is replayed; MERGEs absorb it |

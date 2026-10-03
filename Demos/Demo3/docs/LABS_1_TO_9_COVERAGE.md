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

**Second live validation completed 2026-10-02 on GP1.** Three bounded executions, all PASS:

1. **Phase 2 correction** (runs `240497605145949` then `725768954237074`). 746 legacy priority
   rows attributed from verified lineage with 0 unattributable; shortage and priority both
   746 -> 657 with exactly 89 stale rows removed; 0 `OUT_OF_SERVICE` and 0 unassigned rows
   anywhere; whole-table verification PASS. The repeat removed 0 and inserted 0. Silver migrated
   27 -> 28 columns and independently reports exactly 89 stations with `is_operational = false`,
   so 746 - 657 = 89 closes in both directions.
2. **Historical 40-ROW DEVELOPMENT SAMPLE** (run `848541476068172`, rerun `388486903530489`).
   Explicitly a sample: it is **not** full January 2024 data and implies no monthly coverage.
3. **Weather 48-hour committed sample** (run `472557041765891`), using the landed sample with no
   external request.

See [SILVER_GOLD_RUNBOOK.md](SILVER_GOLD_RUNBOOK.md) and [PHASE3_STATUS.md](PHASE3_STATUS.md).

**Final safe Batch A read validation completed 2026-10-03 on GP1.** Databricks Connect 17.3.13
read the existing corrected tables without creating a Databricks object. All five required counts
and both status distributions matched. Great Expectations passed 10/10 Silver and 10/10
historical-sample expectations with zero unexpected rows. See
[the dated evidence](../evidence/BATCH_A_READ_VALIDATION.md).

**Current totals, 71 requirement rows:** **22** `Validated live`, 42 `Implemented locally`,
3 `Discovered read-only`, **4** `Pending live`, 0 `Blocked`.

**CI/CD batch, 2026-10-03.** Three rows promoted on live evidence - idempotent deployment and
approvals, post-deploy validation, and the Jobs API trigger. See
[CICD_BATCH_EVIDENCE.md](CICD_BATCH_EVIDENCE.md) for the run IDs and the independent checks.

`CI integration` was deliberately **not** promoted. The approval gate, Azure OIDC login, Databricks
identity check and authenticated `bundle validate` all succeeded live from CI, but the run then
failed reading the Jobs: the CI service principal holds no ACL on them. Promoting a requirement on
the strength of a failed workflow is the kind of overclaiming this project avoids, so it stays
`Pending live` with the blocker and the proposed read-only grant recorded.

The seven remaining rows are each analysed in [PENDING_REQUIREMENTS.md](PENDING_REQUIREMENTS.md),
with the resource they need, whether they change data, and a recommended batch order. Two of them
are recommended as *documented rather than executed*: a workspace-wide legacy mount on a shared
academy workspace is poor judgement regardless of authorization, and a second workspace for
DEV-to-PROD is new paid infrastructure whose absence is more honest to state than to simulate.

The 2026-10-03 read batch promoted Databricks Connect and Great Expectations only. Deliberately
NOT promoted: full monthly
historical ingestion (only a 40-row sample ran), Lakeflow, the published AI/BI dashboard and
every governance policy - none of those executed, and a sample run does not evidence a
full-scale one.

| Lab | Required topic | UrbanFlow implementation | File / validation | Evidence | Status |
|---:|---|---|---|---|---|
| 1 | Git and notebooks | Dedicated feature branch and Databricks source-format notebooks | `notebooks/01_fundamentals.py` | Local Git state; notebook format audit | Implemented locally |
| 1 | CSV and JSON | Real GBFS JSON and January 2024 trip CSV samples | `data/samples/`, `scripts/prepare_demo.py` | 40 + 40 rows validated | Implemented locally |
| 1 | External REST API | Dynamic GBFS discovery and Open-Meteo validation | `gbfs_client.py`, `validate_sources.py` | Public endpoints returned current schemas | Implemented locally |
| 1 | DataFrames | Explicit Spark DataFrames for station information/status | `01_fundamentals.py` | Static review; live notebook pending | Implemented locally |
| 1 | select/filter/join/groupBy | Operational filter, UUID join, availability aggregation | `01_fundamentals.py` | Pure business rules unit-tested | Implemented locally |
| 1 | Delta and SQL | Opt-in idempotent MERGE plus dashboard-ready SQL | `01_fundamentals.py` | Write defaults off | Implemented locally |
| 1 | Basic dashboard | 22 dashboard datasets as read-only SQL, a four-page layout and exact creation steps | `sql/10`-`sql/13`, `docs/DASHBOARD.md` | **The underlying 22 queries executed read-only against the live corrected tables on GP1 2026-10-02** (run 242979365945407): 22/22 succeeded, and Page 1 returned 2,520 stations / 1,774 available / 89 out of service / 657 actionable. Required historical and weather execution-selection guards were added and tested locally on 2026-10-03; those parameterized versions have not been rerun through a SQL warehouse. The AI/BI object itself is not created, since publishing needs a billable SQL warehouse | Implemented locally |
| 1 | Shared cluster / General schema differences | GP1 preferred and GP2 fallback through configuration; must already be running | config, Job resource, inventory | IDs/runtime/access/permissions discovered read-only | Implemented locally |
| 2 | Azure provisioning settings | Existing workspace, ADLS Gen2, Key Vault settings documented | `RESOURCE_INVENTORY.md` | Azure CLI read-only inventory | Discovered read-only |
| 2 | Personal container / external location | Existing identity external location identified | `RESOURCE_INVENTORY.md` | UC external location listed | Discovered read-only |
| 2 | Storage credential | Managed-identity credential identified | `RESOURCE_INVENTORY.md` | UC credential listed | Discovered read-only |
| 2 | Schemas and Volume | `dbr_dev.parvinbadalov_urbanflow` + managed Volume `urbanflow_landing` created | `sql/00_prepare_urbanflow_storage.sql`, UC API | Both created 2026-09-29 and used by the live run | Validated live |
| 2 | Managed vs external tables | Bronze, Silver and Gold use managed Delta; execution-scoped MERGEs and replacements are implemented | `ARCHITECTURE.md`, `persistence.py` | Bronze, Silver and Gold managed tables were verified live; external-table comparison remains documented | Implemented locally |
| 2 | Key Vault-backed secret | Producer reads it from Key Vault; notebook reads it via `azure-secrets`; value never printed | `producer.from_key_vault`, notebook Step 5 | Both paths authenticated in the live run | Validated live |
| 2 | Idempotent Bronze + metadata | Event ID, source/collection times, Kafka metadata, MERGE lesson | source modules and notebook | Offline tests | Implemented locally |
| 2 | Ingestion Job | Bounded Bronze Job plus dry-run-by-default Phase 2 DAG | `resources/jobs.yml` | Bronze Job `404404108673495`; Phase 2 corrected and repeated live on 2026-10-02 | Implemented locally |
| 2 | Legacy mounts exercise | Explanation deliberately separated from core design | Architecture security boundary | No deprecated mount created | Pending live |
| 3 | Auto Loader | `cloudFiles` reader with explicit schema, separate schemaLocation and per-execution checkpoint, rescued-data column, bounded `availableNow`, plus notebook 06 | `historical.py`, `notebooks/06_historical_trips.py` | Ran live on GP1 2026-10-02 (run 848541476068172): bounded `availableNow` read of the landed **40-ROW DEVELOPMENT SAMPLE**, 40 landed = 40 valid + 0 quarantine + 0 duplicate, reconciliation PASS. A rerun processed no new file and inserted nothing. NOT full-month data | Validated live |
| 3 | Structured Streaming | Kafka reader, explicit-schema parser, bounded `availableNow` writer | `streaming.py`, `03_eventhubs_to_bronze.py` | Job run `873010921866250` SUCCESS in 130 s; 2,520 rows to Bronze | Validated live |
| 3 | Event Hubs / Kafka | Existing Standard namespace/hub plus GP1/GP2 compatibility contract | inventory/config/streaming | 2,520 messages published and consumed once | Validated live |
| 3 | Real Python producer | TTL-aware, duplicate-aware, finite Event Hubs producer | `producer.py`, CLI | 2,520-event live producer report | Validated live |
| 3 | Explicit schema | `StructType` station event contract | `streaming.py` | Static/tests | Implemented locally |
| 3 | Schema inference/evolution/rescue | Explicit-schema `rescue` path plus schema-hint `addNewColumns` lesson | `historical.py`, architecture | Explicit rescue path used by the live sample; additive evolution lesson remains local only | Implemented locally |
| 3 | schemaLocation/checkpoint | Separate historical schema/checkpoint paths plus validated Bronze checkpoint | config, `historical.py`, notebooks | Both used live: the Bronze stream checkpoint (2026-09-29) and the per-execution historical schema and checkpoint directories (2026-10-02), which are separate paths inside the existing Volume | Validated live |
| 3 | availableNow / micro-batches (Kafka) | Bounded Kafka writer with a bounded wait | `03_eventhubs_to_bronze.py`, `streaming.py` | Job run `873010921866250` consumed 2,520 rows and terminated | Validated live |
| 3 | availableNow / micro-batches (historical Auto Loader) | `start_historical_available_now` with a schema checkpoint | `historical.py` | Ran live on GP1 2026-10-02: `availableNow` terminated by itself inside the wait bound; the rerun read no new file, proving the checkpoint | Validated live |
| 3 | Replay and semantics | Exactly-once/at-least-once explanation | README / architecture | Documentation review | Implemented locally |
| 3 | Approximately 1,000 files | `scripts/generate_many_small_files.py` writes 1,000 deterministic tiny CSVs (under 1.5 MB total) labelled SYNTHETIC EDUCATIONAL DATA, with ownership-marker cleanup | Coverage matrix | 14 tests covering exact file count, repeat safety, determinism, cleanup that refuses a foreign directory, and that every row passes the trip quality rules. Local only; the upload is a separate approval | Implemented locally |
| 4 | Cleaning, explicit schemas, deduplication | Physical Spark contract, quarantine and deterministic broker ordering | `silver.py` | Live run wrote 2,520 Silver rows, 0 quarantined, IDs unique, `PASS` | Validated live |
| 4 | MERGE / rerun safety | Identifier-safe, duplicate-rejecting Delta MERGE for all Phase 2 outputs | `persistence.py`, Silver/Gold notebooks | Second identical run left all eight tables at `inserted_rows = 0`, before == after | Validated live |
| 4 | SCD1 / SCD2 | New/changed/unchanged/missing stations, null-safe changes, interval audits | `dimensions.py`, tests | Real local Spark tests | Implemented locally |
| 4 | Enforcement/evolution | Explicit contracts; add-only `ALTER TABLE ADD COLUMNS` migration instead of `autoMerge`; verified-lineage backfill for rows predating a column | `persistence.py`, `gold.py` | `ALTER TABLE ADD COLUMNS` executed live 2026-10-02: `silver_station_status` migrated 27 -> 28 columns adding `is_operational`, and `gold_rebalancing_priority` 12 -> 13 adding `execution_id`, each named in its own run report | Validated live |
| 4 | Column mapping | Controlled rename documented with the one-way reader/writer upgrade stated as its cost; left commented | `sql/21_maintenance_optimize_vacuum.sql` | `tests/test_sql_assets.py` proves no `RENAME COLUMN` is active; not executed | Implemented locally |
| 4 | Data contracts | GBFS and event contracts documented and tested | client/streaming/architecture | Unit tests | Implemented locally |
| 4 | OPTIMIZE/VACUUM/liquid clustering | Side-by-side Z-ORDER vs liquid clustering comparison, deletion vectors, CDF and time travel, with a prominent VACUUM irreversibility warning; every destructive statement commented | `sql/21_maintenance_optimize_vacuum.sql` | Tests assert no active VACUUM, OPTIMIZE, CLUSTER BY or retention-check override; only DESCRIBE runs | Implemented locally |
| 5 | Lakeflow pipeline | Serverless triggered resource and Bronze Kafka streaming table declared | pipeline and `resources/pipelines.yml` | Bundle schema validation; not deployed | Implemented locally |
| 5 | Streaming tables / materialized views | Bronze declared as a streaming table; Silver, Quarantine, duplicates and all five Gold tables as materialized views, reusing the notebook path's own functions | `pipeline/bronze.py`, `pipeline/silver.py`, `pipeline/gold.py` | 22 source- and config-level tests, including a no-duplicated-business-logic check; pipeline deliberately NOT deployed | Implemented locally |
| 5 | Bronze/Silver/Quarantine/Gold | Physical Silver/Quarantine/duplicate MERGEs and reconciled Gold model | `silver.py`, `gold.py`, notebooks 04/05 | Real local Spark tests; tables pending | Implemented locally |
| 5 | Expectations, lineage, monitoring | 18 declarative expectations across Silver and Gold, all non-dropping so quarantine evidence survives; event-log and expectation-result queries prepared | `pipeline/silver.py`, `pipeline/gold.py`, `sql/13_dashboard_data_quality.sql` | Tests assert expectations exist on every table and that none drops or fails rows; event log needs a running pipeline | Implemented locally |
| 5 | DAB deployment | Bundle targets plus undeployed Job and serverless Lakeflow resources | `databricks.yml`, `resources/` | Static validation | Implemented locally |
| 6 | Fact/dimension model | Stable availability fact, 40-row development dimension, daily summary and shortage outputs | `gold.py`, notebook 05 | All five Gold tables verified live 2026-10-02: fact 2,520 with 2,520 distinct event IDs, daily summary 2,520 with observation total 2,520 and `is_trend_capable` true for 0 rows, dimension 40, shortage and priority 657 each | Validated live |
| 6 | AI/BI dashboard and filters | Four pages, 22 datasets, with per-tile caveats so a snapshot is never charted as a trend | `docs/DASHBOARD.md`, `sql/10`-`sql/13` | Layout and queries complete; the underlying 22/22 ran read-only after the Phase 2 correction. Required execution-selection guards were added and tested locally on 2026-10-03, but not rerun through a SQL warehouse. The dashboard object is not published, and sample/full execution scopes remain separate | Implemented locally |
| 6 | Alerts / email | Volume-drop and shortage alert requirement retained | Coverage matrix | Destination/warehouse unverified | Pending live |
| 6 | Permissions, RLS, masking | Region row filter, coordinate-rounding mask, ride_id hashing mask, least-privilege grants, ABAC tagging explained, plus inspection queries | `sql/20_governance_rls_cls.sql` | Tests prove no `SET MASK`, `SET ROW FILTER` or `GRANT` is active; the fail-to-zero-rows hazard is documented | Implemented locally |
| 6 | Weather enrichment | Bounded Open-Meteo archive retrieval, hourly normalization, left join preserving every trip, null readings kept null, weather dimension and demand comparison, plus notebook 07 | `weather.py`, `notebooks/07_weather_enrichment.py` | Ran live on GP1 2026-10-02 (run 472557041765891) with the committed **48-hour sample** and no external request: completeness 1.0, 40 trips in and 40 out with no fan-out, coverage 0.1 reported honestly. One city coordinate, comparisons only | Validated live |
| 6 | Historical trip quality and demand | Three-way valid/quarantine/duplicate split, deterministic dedup by `ride_id`, trip-duration bounds, member vs casual mix, daily demand keyed to `short_name` | `historical.py`, `notebooks/06_historical_trips.py` | Ran live on GP1 2026-10-02 on the **40-ROW DEVELOPMENT SAMPLE**: match rate 1.0 via `short_name`, demand 40 trips over 17 days reconciling exactly, member 35 / casual 5, durations 1.28-30.10 min - every figure identical to the local prediction | Validated live |
| 4 | Derived-table correction | `backfill_execution_id`, fail-closed unassigned-row guard and whole-table `verify_execution_scope`, after 89 stale rows were found able to escape a scoped delete | `persistence.py`, `gold.py` | Executed live 2026-10-02 (runs 240497605145949 then 725768954237074): 746 legacy rows attributed with 0 unattributable, shortage and priority 746 -> 657 with exactly 89 stale rows removed each, 0 OUT_OF_SERVICE, 0 unassigned, whole-table verification PASS, repeat removed 0 | Validated live |
| 7 | Importable functions | Client, transformations, quality, producer, monitoring, automation | `src/urbanflow/` | Import/test pass | Implemented locally |
| 7 | pytest / Ruff / Black | Dedicated configuration and static workflow | `pyproject.toml`, workflow | Local results recorded in session report | Implemented locally |
| 7 | pre-commit | Local Ruff, Black, and pytest hooks | `.pre-commit-config.yaml` | Uses the project development environment | Implemented locally |
| 7 | chispa / Spark tests | Real local Spark suites for Silver, Gold, historical joins and SCD | `tests/test_{silver,gold,historical,dimensions}.py` | Local pytest | Implemented locally |
| 7 | Duplicate, late, SCD edge cases | Cross-partition duplicate order, null transitions, invalid intervals and out-of-order SCD changes | tests | Local pytest | Implemented locally |
| 7 | DQ dimensions | Completeness, uniqueness key, validity, consistency, referential integrity, freshness | `quality.py` | Unit tests | Implemented locally |
| 7 | Lakeflow expectations / Delta constraints | Non-dropping `expect_all` rules on all eight pipeline tables, including one asserting an out-of-service station is never actionable and one asserting a single observation cannot claim a trend | `pipeline/silver.py`, `pipeline/gold.py` | Source-level tests; execution requires a Lakeflow run, which is not authorized | Implemented locally |
| 7 | Great Expectations or Soda | Great Expectations 1.23.2 suites for the Silver and trip contracts, run through an ephemeral context so no project state is created | `expectations.py`, `../evidence/BATCH_A_READ_VALIDATION.md` | Ran read-only against the persisted tables on GP1 on 2026-10-03: Silver 10/10 and the named 40-row historical development sample 10/10, with zero failures and zero unexpected rows. The rules come from the existing UrbanFlow contracts | Validated live |
| 7 | Reconciliation / anomaly gates | Bronze outcome and Silver-to-Gold grain/aggregate gates | `silver.py`, `gold.py` | Ran live 2026-10-02: Silver reconciliation PASS (2,520 = 2,520 + 0 + 0), Gold PASS, historical PASS (40 = 40 + 0 + 0), weather PASS (40 in, 40 out) | Validated live |
| 7 | Databricks Connect / monitoring | Local development session attached to the approved existing cluster and read corrected tables through Spark Connect | `../evidence/BATCH_A_READ_VALIDATION.md` | Databricks Connect 17.3.13 authenticated as the expected user, discovered the target catalog/schema and 16-table inventory, and reproduced all required counts and status distributions on GP1 without a Databricks write | Validated live |
| 8 | DAB and GitHub Actions | Static workflow, two targets, two existing-cluster Jobs, managed-compute pipeline | workflow / bundle/resources | Static local validation | Implemented locally |
| 8 | PR static checks | Ruff, Black, pytest, bundle validation | workflow | Milestone 1 PR #34 passed and merged | Implemented locally |
| 8 | Environment config | `dev.yml`, `azure.yml`, DAB targets | config / bundle | Static only | Implemented locally |
| 8 | Idempotent deployment / approvals | Job-only DAB deployment plus a GitHub protected environment requiring a named reviewer before any Azure step runs | cost plan | Deployed 2026-10-03 after a plan showing 0 add / 2 change / 0 delete, the two changes being the new `run_attempt_id` and `landing_subdir` parameters. The follow-up plan returned **0 add / 0 change / 0 delete, 4 unchanged**, re-confirmed independently on 2026-10-03. The `azure-release-approval` environment held two separate workflow runs in WAITING until BadalovP approved (runs 37082229544 and 37083109424); approval records are in the GitHub deployments API | Validated live |
| 8 | DEV-to-PROD promotion | Second authorized environment not established | resource inventory | One real target confirmed | Pending live |
| 8 | Post-deploy validation | Read-only verification of the deployed state: table inventory, row counts, Gold status composition and Great Expectations suites, plus a bounded Job dry-run asserting its own DRY_RUN output | cost plan | Ran live on GP1 2026-10-03 against the deployed Jobs: 16 tables exactly as expected, Bronze/Silver/fact 2,520, shortage and priority 657 each, composition LOW_BIKES 278 / LOW_DOCKS 374 / LOW_BIKES_AND_DOCKS 5 / OUT_OF_SERVICE 0, GE suites PASS. Independently corroborated by the whole-table verification run 618116231829401 and by a Unity Catalog inventory match on 2026-10-03. Performed as an authorized step, **not yet wired as an automatic CI step** - that is blocked by the ACL gap in the CI-integration row below | Validated live |
| 9 | Authentication / workspace verification | Explicit profile and exact host checks | `automation.py`, CLI | Real identity verified read-only outside module | Implemented locally |
| 9 | Cluster provisioning | Optional create helper is disabled by default; GP1/GP2 mutation and termination are blocked | config/automation/tests | Mocked negative-path tests | Implemented locally |
| 9 | Notebook upload | Base64 SOURCE import helper | `automation.py` | Mock/live test pending | Implemented locally |
| 9 | Jobs API / pipeline trigger | Bounded Job trigger through the Jobs API with a fail-closed parameter, plus one-off `jobs submit` runs for read-only verification | automation/monitoring | Job 11834365763936 run **180887598258786** TERMINATED/SUCCESS on 2026-10-03 with `run_transform=false`; both tasks returned DRY_RUN and no Bronze read or Delta write occurred. Verified independently on 2026-10-03. Read-only `jobs submit` runs 618116231829401 and 242979365945407 also exercised the API | Validated live |
| 9 | Explicit polling/timeouts/errors | Generic bounded polling and domain errors | monitoring/client/tests | Unit tests | Implemented locally |
| 9 | JSON reports | Producer, Bronze, and offline end-to-end reconciliation reports | CLI / `reporting.py` / runbook | Offline tests; live report pending | Implemented locally |
| 9 | Termination verification | Exact `TERMINATED` required | `automation.py` | Mocked test | Implemented locally |
| 9 | CI integration | Approval-gated `workflow_dispatch` job performing Azure OIDC login, Databricks identity verification, authenticated `bundle validate` and direct Job/pipeline reads | workflow | **Partially proven, workflow still failing.** On run 37083109424 the gate, OIDC login, Databricks identity check and authenticated `databricks bundle validate -t azure` ("Validation OK!") all SUCCEEDED live from CI. The run then FAILED on `databricks jobs get`: the CI Entra service principal (repository variable `AZURE_CLIENT_ID`) has no ACL entry on the four Jobs, whose ACL is `parvinbadalov@softserve.academy` IS_OWNER plus `admins` CAN_MANAGE. A read-only permission grant is needed and is not yet authorized. No Databricks write or workload occurred | Pending live |

## Next status changes allowed

Rows move to `validated live` only after dated evidence is stored and linked. A successful mocked
test never changes a live-execution status. A visible resource never proves write permission.

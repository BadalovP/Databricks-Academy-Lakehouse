# Batch A read-only validation — 2026-10-03

[← Evidence index](README.md) · [Machine-readable result](2026-10-03_batch_a_read_validation.json)

## Scope and safety

This validation attached Databricks Connect 17.3.13 to the already-running shared cluster GP1
`0702-132442-toro5spu`. It executed only `SHOW` and `SELECT` operations against existing UrbanFlow
objects. Great Expectations 1.23.2 used an ephemeral local context. The session created no table,
view, upload, checkpoint, Job run, pipeline update or persisted GE object, and `spark.stop()` closed
only the local Connect session.

The preflight observed GP1 as `RUNNING` on DBR `17.3.x-scala2.13` with `USER_ISOLATION`. The
authenticated identity was `parvinbadalov@softserve.academy`; catalog `dbr_dev` and schema
`parvinbadalov_urbanflow` were visible. The control plane contained the four expected UrbanFlow
Jobs, sixteen expected managed Delta tables, one managed Volume and zero UrbanFlow pipelines.
No unexpected UrbanFlow resource was found.

## Persisted-table results

| Check | Observed | Result |
|---|---:|---|
| `bronze_station_status` | 2,520 | PASS |
| `silver_station_status` | 2,520 | PASS |
| `fact_station_availability` | 2,520 | PASS |
| `gold_station_shortage` | 657 | PASS |
| `gold_rebalancing_priority` | 657 | PASS |

Silver contained 1,774 `AVAILABLE`, 278 `LOW_BIKES`, 374 `LOW_DOCKS`, 5
`LOW_BIKES_AND_DOCKS`, and 89 `OUT_OF_SERVICE` rows. Actionable Gold contained the same 278, 374
and 5 shortage rows and zero `OUT_OF_SERVICE` rows.

## Great Expectations results

| Suite | Expectations | Passed | Failed | Unexpected rows | Result |
|---|---:|---:|---:|---:|---|
| Silver station status | 10 | 10 | 0 | 0 | PASS |
| Historical development sample | 10 | 10 | 0 | 0 | PASS |

The historical suite selected only execution
`urbanflow-hist-devsample40-20261002T0010Z`, labelled **40-ROW DEVELOPMENT SAMPLE**, and observed
exactly 40 rows and one distinct execution ID. Its duration expectation derives its expression and
1-minute-to-24-hour bounds from the same `historical.py` contract used by ingestion.

## Monthly archive readiness review

No archive was downloaded or uploaded. The merged `landing_subdir` wiring was rechecked from the
Job parameter through notebook 06 into `historical_landing_paths`. An eventual full-month run uses:

- `landing/historical_trips/202401-full`
- `schemas/historical_trips/202401-full`
- `checkpoints/historical_trips/202401-full/<execution_id>`

The empty default preserves the validated sample paths, and the monthly run requires a new
`execution_id`. Every historical dashboard dataset now requires `:historical_execution_id`, and
every weather dataset requires `:weather_execution_id`; both also keep that ID in their grain. A
missing selection fails, and a development sample and a future monthly execution are not summed
together.

## Actions deliberately left unexecuted

- Jobs API dry-run trigger: creates a Databricks Job run record.
- 1,000-file Auto Loader validation: uploads files and writes schema state, a checkpoint, Delta
  rows and evidence.
- January archive, Lakeflow, dashboard/alert and governance/maintenance batches.

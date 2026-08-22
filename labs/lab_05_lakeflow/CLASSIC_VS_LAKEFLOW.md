# Lab 05 — Classic Spark vs Lakeflow Spark Declarative Pipelines

## Purpose

This comparison documents how the Lab 05 Citi Bike pipeline would be implemented with classic Spark / Structured Streaming versus the Lakeflow Spark Declarative Pipelines approach used in this project.

The Lab 05 pipeline contains:

- `station_status_bronze` — streaming ingestion from timestamped Citi Bike GBFS JSON snapshots
- `station_information_bronze` — batch/reference JSON ingestion
- `station_status_silver` — validated streaming station observations
- `station_information_silver` — validated station reference data
- `station_status_enriched_silver` — stream-static join on `station_id`
- `station_summary_gold` — analytical materialized view

---

## Review feedback addressed

The comparison now makes one point explicit:

> **Auto Loader can be used in classic Spark Jobs too.**

For example, a classic Structured Streaming Job can use:

```python
spark.readStream.format("cloudFiles")
```

The architectural difference is not Auto Loader availability. The difference is
that classic Spark code explicitly manages more of the streaming-query
lifecycle, checkpoint/state handling, triggers, writes, orchestration, and
recovery, while Lakeflow lets the developer declare datasets and dependencies
and manages more of that lifecycle for the pipeline.

---

## 1. Main difference

### Classic Spark / Structured Streaming

Classic Spark code describes **how the application should run**.

The developer typically controls:

- streaming query startup
- checkpoint locations
- output writes
- trigger configuration
- table write modes
- orchestration order
- monitoring
- recovery behavior
- data-quality handling
- dependency management

A classic streaming application is therefore more **procedural**.

> **Important clarification:** Auto Loader is **not exclusive to Lakeflow**.  
> A classic Spark Job can also use Auto Loader with
> `spark.readStream.format("cloudFiles")`. The main difference is that in a
> classic Job the developer still explicitly manages the streaming query,
> checkpoint/output lifecycle, trigger, and orchestration, while Lakeflow
> manages more of the dataset lifecycle and execution graph declaratively.

### Lakeflow Spark Declarative Pipelines

Lakeflow code primarily declares **what datasets should exist and how they are derived**.

For example:

```python
@dp.table(name="station_status_silver")
@dp.expect_all(STATUS_MONITOR_EXPECTATIONS)
@dp.expect_all_or_drop(STATUS_DROP_EXPECTATIONS)
def station_status_silver():
    ...
```

Lakeflow analyzes the declared dependencies and manages the execution graph.

The pipeline is therefore more **declarative**.

---

## 2. Lab 05 architecture comparison

| Lab 05 responsibility | Classic Spark / Jobs | Lakeflow implementation |
|---|---|---|
| Streaming JSON ingestion | **Auto Loader can also be used:** `spark.readStream.format("cloudFiles")` + explicit streaming query | `@dp.table` + Auto Loader |
| Query lifecycle | explicit `writeStream`, trigger, start/termination handling | pipeline-managed dataset execution |
| Batch/reference JSON | batch DataFrame + explicit write | `@dp.materialized_view` |
| Bronze persistence | `.writeStream` / `.write` | managed by pipeline |
| Streaming checkpoint/state | developer-defined checkpoint/state lifecycle | managed by Lakeflow |
| Pipeline execution order | Job task dependencies / custom orchestration | inferred automatically from dataset dependencies |
| Data quality | filters, assertions, custom metrics | expectations |
| Drop invalid rows | explicit `.filter()` | `@dp.expect_all_or_drop` |
| Monitor invalid rows | custom counters/metrics | `@dp.expect_all` |
| Stream-static join | developer manages streaming query | declared as downstream streaming table |
| Gold aggregation | batch job + overwrite/merge | materialized view |
| Safe incremental rerun | Auto Loader + checkpoint + idempotent write design | Auto Loader + pipeline-managed streaming state |
| Lineage | documented separately | pipeline graph generated automatically |
| Deployment | Jobs / scripts / Terraform / custom CI/CD | bundle pipeline resource |

---

## 3. Streaming Bronze — classic Spark

Classic Spark can use **the same Auto Loader `cloudFiles` source**. What remains
explicit is the streaming-query lifecycle: write target, checkpoint, trigger,
startup, termination, monitoring, and recovery.

A simplified classic Spark equivalent of `station_status_bronze` would look like:

```python
status_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("multiLine", "true")
    .load(status_landing_path)
)

query = (
    status_df
    .writeStream
    .format("delta")
    .option(
        "checkpointLocation",
        checkpoint_path
    )
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(
        "dbr_dev.parvinbadalov.station_status_bronze"
    )
)

query.awaitTermination()
```

The developer must decide and manage:

```text
source path
checkpoint path
trigger
output mode
destination table
query startup
query termination
```

---

## 4. Streaming Bronze — Lakeflow

The Lab 05 implementation is shorter:

```python
@dp.table(
    name="station_status_bronze"
)
def station_status_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option(
            "cloudFiles.format",
            "json"
        )
        .option(
            "cloudFiles.inferColumnTypes",
            "true"
        )
        .option(
            "multiLine",
            "true"
        )
        .load(
            config.station_status_landing_path
        )
    )
```

There is no explicit:

```text
writeStream
checkpointLocation
start()
awaitTermination()
```

Lakeflow owns the target streaming table and streaming state.

---

## 5. Data quality comparison

### Classic Spark

A classic implementation might contain:

```python
valid_status_df = status_df.filter(
    F.col("station_id").isNotNull()
    & (
        F.col("num_bikes_available").isNull()
        | (F.col("num_bikes_available") >= 0)
    )
    & (
        F.col("num_docks_available").isNull()
        | (F.col("num_docks_available") >= 0)
    )
)
```

This removes invalid records, but additional work is required to answer:

- How many rows failed?
- Which rule failed?
- Was the rule monitoring-only or dropping data?
- How is the result exposed operationally?

### Lakeflow

Lab 05 keeps reusable rules in `src/quality_rules.py`:

```python
STATUS_DROP_EXPECTATIONS = {
    "status_station_id_present":
        "station_id IS NOT NULL",

    "status_bikes_non_negative":
        "num_bikes_available IS NULL "
        "OR num_bikes_available >= 0",

    "status_docks_non_negative":
        "num_docks_available IS NULL "
        "OR num_docks_available >= 0",
}
```

and applies them declaratively:

```python
@dp.expect_all_or_drop(
    STATUS_DROP_EXPECTATIONS
)
```

Monitoring-only rules use:

```python
@dp.expect_all(
    STATUS_MONITOR_EXPECTATIONS
)
```

The Lakeflow UI exposes expectation names, actions, failure percentages, failed records, written records, and dropped records.

In the successful Lab 05 run, all six expectations on both Silver datasets were met.

---

## 6. Dependency management

### Classic Spark / Jobs

The execution dependency would typically be configured explicitly:

```text
status bronze
    ↓
status silver
    ↓
enriched silver
    ↓
gold

information bronze
    ↓
information silver
    └────────→ enriched silver
```

This can be implemented using Job task dependencies, orchestration frameworks, notebook ordering, or custom driver code.

### Lakeflow

The Lab 05 source files declare reads from upstream datasets.

Lakeflow automatically builds the graph:

```text
station_status_bronze
        ↓
station_status_silver
        ↓
station_status_enriched_silver
        ↓
station_summary_gold

station_information_bronze
        ↓
station_information_silver
        └──────────────→ station_status_enriched_silver
```

The developer does not manually specify this task ordering.

---

## 7. Safe reload behavior

A major Lab 05 requirement was to reload data safely.

After a normal rerun with no new source files:

```text
Status source files      : 11
Bronze status documents  : 11
Silver status rows       : 27,599
Enriched rows            : 27,599
```

The counts did not increase.

This demonstrated that previously processed files were not ingested again.

### Classic Spark equivalent

This behavior depends on maintaining a valid checkpoint:

```python
.option(
    "checkpointLocation",
    checkpoint_path
)
```

Deleting or changing the checkpoint could cause source files to be processed again.

### Lakeflow

The streaming-state lifecycle is managed by the pipeline, so the developer focuses on the dataset definition rather than managing checkpoint directories directly.

---

## 8. Incremental ingestion test

After one new `station_status` JSON snapshot was produced:

```text
Before               After
------               -----
Source files  11  →  12
Bronze docs   11  →  12
Silver rows   27,599 → 30,108
Enriched      27,599 → 30,108
```

The Gold dataset remained one row per station while its aggregated metrics refreshed.

This demonstrates:

```text
new file
   ↓
Auto Loader
   ↓
append Bronze
   ↓
new Silver observations
   ↓
refresh enrichment
   ↓
refresh Gold materialized view
```

---

## 9. Materialized views

The Lab 05 batch/reference source and Gold layer use materialized views.

Examples:

```python
@dp.materialized_view(
    name="station_information_bronze"
)
```

and:

```python
@dp.materialized_view(
    name="station_summary_gold"
)
```

A classic Spark solution would normally require explicit code to:

- read upstream data
- calculate the result
- decide overwrite/merge behavior
- write the destination table
- schedule the refresh

With Lakeflow, the developer declares the materialized result and its query.

---

## 10. Operational simplicity vs flexibility

### Lakeflow advantages

Lakeflow reduces the amount of operational code required for:

- dependency orchestration
- checkpoint/state management
- expectation metrics
- lineage visualization
- streaming-table lifecycle
- materialized-view refresh
- pipeline monitoring
- managed execution

This makes the pipeline easier to understand and reduces infrastructure code.

### Classic Spark advantages

Classic Spark provides lower-level control.

It can be preferable when an application requires:

- highly customized streaming lifecycle logic
- unusual checkpoint/state handling
- custom sink behavior
- custom retry behavior
- application-specific orchestration
- features that do not map cleanly to declarative datasets

Lakeflow does not remove Spark functionality; transformations still use Spark DataFrame APIs.

---

## 11. Cost considerations

Lakeflow improves operational simplicity, but declarative pipelines are not automatically the cheapest solution for every workload.

Cost depends on:

- update frequency
- source data volume
- streaming versus triggered execution
- materialized-view refresh behavior
- serverless compute usage
- transformation complexity

For this lab, **triggered execution** was appropriate because Citi Bike snapshots were produced periodically rather than requiring a continuously running pipeline.

The project therefore uses:

```yaml
continuous: false
serverless: true
```

This avoids keeping a continuous pipeline active when no new Lab 05 snapshot is arriving.

---

## 12. CI/CD comparison

### Classic approach

A classic implementation might require separate configuration for jobs, scripts, compute, environment parameters, and deployment automation.

### Lab 05

The Lakeflow pipeline is defined as a bundle resource:

```yaml
resources:
  pipelines:
    lab05_lakeflow_pipeline:
      ...
```

The same deployment unit can be reused for:

```text
personal_dev
    ↓
shared / production target
```

without rewriting pipeline source code.

This prepares the project for later CI/CD work.

---

## 13. Summary

### Classic Spark

```text
Auto Loader can be used for ingestion.

Developer still manages:
    streaming query lifecycle
    checkpoint/state location
    output write
    trigger
    orchestration
    data-quality logic
    monitoring
    recovery
    deployment configuration
```

### Lakeflow

```text
Developer declares:
    streaming tables
    materialized views
    transformations
    expectations
    dependencies through reads

Lakeflow manages:
    dependency graph
    pipeline execution
    streaming state
    expectation metrics
    lineage
    dataset lifecycle
```

For Lab 05, Lakeflow was the better fit because the learning goal was to demonstrate how declarative pipelines reduce operational code while preserving Spark DataFrame transformations.

---

## Official references

- [Lakeflow Spark Declarative Pipelines concepts](https://docs.databricks.com/aws/en/ldp/concepts/)
- [Procedural vs declarative processing](https://docs.databricks.com/gcp/en/data-engineering/procedural-vs-declarative)
- [Load data in pipelines](https://docs.databricks.com/aws/en/ldp/load)
- [Pipeline expectations](https://docs.databricks.com/aws/en/ldp/expectations)
- [Structured Streaming tutorial](https://docs.databricks.com/aws/en/structured-streaming/tutorial)
- [Develop pipelines with Declarative Automation Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/pipelines-tutorial)

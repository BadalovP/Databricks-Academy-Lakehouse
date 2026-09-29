# UrbanFlow architecture and design decisions

[← Main README](../README.md) · [Coverage matrix](LABS_1_TO_9_COVERAGE.md) · [Resource inventory](RESOURCE_INVENTORY.md)

## Design goals

UrbanFlow separates four concerns so each can be explained and tested independently:

1. **Source acquisition:** public REST APIs and the official historical archive.
2. **Transport:** short, bounded publication to Azure Event Hubs and Kafka-compatible consumption.
3. **Lakehouse processing:** Bronze evidence, Silver trust/quarantine, and Gold operations tables.
4. **Platform automation:** DAB deployment plus SDK-driven verification, orchestration, reporting, and cleanup.

The project uses simple functions and explicit schemas. Cloud actions stay behind visible gates.

## Confirmed source contracts

### GBFS discovery and feeds

The official Citi Bike system-data page currently links to:

```text
https://gbfs.citibikenyc.com/gbfs/2.3/gbfs.json
```

The discovery response contains language-specific `feeds` lists and currently directs English
station traffic to Lyft GBFS endpoints. Code discovers `station_information` and `station_status`
at runtime. It does not depend on the older `/gbfs/en/...` URLs remaining stable.

Required station-information fields:

```text
station_id, name, lat, lon, capacity
```

Required station-status fields:

```text
station_id, num_bikes_available, num_docks_available,
is_installed, is_renting, is_returning, last_reported
```

UrbanFlow adds `source_last_updated`, `collected_at`, `source_url`, `gbfs_version`, and a stable
`event_id`. The two timestamps answer different questions: when the provider updated the feed and
when UrbanFlow observed it.

### Historical trip contract

The selected archive is `202401-citibike-tripdata.zip`. Its first CSV member has:

```text
ride_id, rideable_type, started_at, ended_at,
start_station_name, start_station_id, end_station_name, end_station_id,
start_lat, start_lng, end_lat, end_lng, member_casual
```

January 2024 `start_station_id` matches current GBFS `short_name` in the selected sample. The
current UUID `station_id` is a separate key. The future station dimension will preserve both and
record unmatched historical identifiers rather than silently dropping trips.

### Weather contract

The first Open-Meteo contract uses `current` values for:

```text
time, temperature_2m, precipitation, wind_speed_10m
```

Open-Meteo returns units in `current_units`; those units must be stored with or standardized before
the Gold join. Weather is an area-level contextual observation, not proof that rain caused a
specific station shortage.

## Streaming semantics

- The existing Event Hub has one partition. Records within that partition have an ordered offset.
- The producer polls no faster than the GBFS TTL and stops after a configured number of polls.
- A stable event ID is derived from station, provider timestamp, bike count, and dock count.
- The in-process seen set avoids republishing an unchanged observation during one bounded session.
- Spark retains topic, partition, offset, broker timestamp, raw JSON, and ingestion timestamp.
- `availableNow` processes the available backlog and stops.
- The checkpoint owns the consumer progress. It must never be shared with another query.
- Kafka/Event Hubs delivery can be at least once around failure and retry. Delta deduplication or
  MERGE must make the next layer idempotent.
- “Exactly once” describes the combined effect of replayable offsets, checkpoints, and an
  idempotent sink. It is not a promise that the source can never redeliver an event.

## Planned physical model

| Layer | Object | Grain | Stage status |
|---|---|---|---|
| Bronze | `bronze_station_status` | One received Kafka event | Validated live: 2,520 reconciled rows |
| Bronze | `bronze_historical_trips` | One physical trip row | Auto Loader, checkpoint, rescue, and validation implemented locally; not run |
| Bronze | `bronze_station_information` | One captured reference record | Client implemented; table pending |
| Bronze | `bronze_weather` | One place/time observation | Sample verified; table pending |
| Silver | `silver_station_status` | One accepted event ID | Contract, deterministic deduplication, reconciliation, and MERGE implemented locally |
| Silver | `quarantine_station_status` | One invalid Kafka record with all failed rules | Routing and Kafka-lineage MERGE implemented locally |
| Silver | `duplicate_station_status` | One repeated Kafka record | Deterministic routing and Kafka-lineage MERGE implemented locally |
| Silver | `dim_station_scd2` | One station version | Null-safe transitions and interval audit tested locally; persistence pending |
| Gold | `fact_station_availability` | One Silver event ID | Stable schema, left enrichment, reconciliation, and MERGE implemented locally |
| Gold | `gold_station_shortage` | One shortage observation | Implemented locally; one snapshot is not a repeated episode |
| Gold | `gold_rebalancing_priority` | Station and observation timestamp | Explainable score and MERGE implemented locally |
| Gold | `fact_historical_trip` | One valid historical ride | Pending Auto Loader and data quality |
| Gold | `dim_weather_hourly` | One weather hour | Pending historical weather selection |
| Gold | `gold_daily_station_summary` | Station and date | Implemented locally with observation count and trend-capability flag |

## SCD strategy

- **SCD Type 1:** correct non-historical descriptive attributes when preserving old values has no
  analytical value. New, changed, unchanged, and temporarily missing stations are tested.
- **SCD Type 2:** preserve changes to name, capacity, latitude, or longitude when historical
  interpretation depends on the version in effect.
- SCD2 comparisons are null-safe; duplicate incoming keys and out-of-order effective timestamps are
  rejected. The audit requires exactly one current version per station, valid intervals, and no
  overlaps. Synthetic changes remain clearly labelled and never enter operational facts.

## Schema evolution strategy

1. Bronze retains raw JSON so unexpected fields are not lost.
2. The production historical reader supplies an explicit schema and uses `rescue`; unexpected or
   mistyped fields land in `_rescued_data` without changing the contract.
3. The separate additive-evolution lesson omits `.schema()`, supplies schema hints, and uses
   `addNewColumns`. Databricks does not allow `addNewColumns` with an explicit reader schema; see
   the official [Auto Loader schema evolution documentation](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema).
4. Auto Loader `schemaLocation` and streaming checkpoint paths are distinct and isolated.
5. The bounded writer uses `availableNow`; it has been prepared but not executed.
6. Delta schema auto-merge is not enabled globally.
7. Column mapping is demonstrated before renaming a Delta column.

## Two-workspace topology and the role of each

UrbanFlow now has two Databricks workspaces available, and they are deliberately given different
jobs. Full verified detail is in [RESOURCE_INVENTORY.md](RESOURCE_INVENTORY.md#two-workspace-topology);
this section records the design decision.

| Workspace | SKU | Compute | UrbanFlow role |
|---|---|---|---|
| `dbr_dev` | premium | shared interactive clusters GP1/GP2 | **The first live Event Hubs to Bronze test.** It is the only workspace where the Event Hubs secret is readable. |
| `dbr_dev_trial` | trial | serverless Jobs and one serverless SQL warehouse; no clusters | Later development, serverless notebooks, and a genuine second environment for CI/CD promotion. |

The two workspaces **share one Unity Catalog metastore**
(`7af05576-c79e-4f56-b84f-ead80be5c8b6`), the `dbr_dev` catalog is `OPEN` rather than
workspace-bound, and Unity Catalog grants are metastore-level. A read-only probe confirmed the
trial workspace can list this project's schemas, tables and Volumes in `dbr_dev`. That is what makes
the trial workspace useful as a promotion target: it can reach the same data without copying it.

What they do **not** share is just as important to the design:

- **Secret scopes are workspace-local.** `azure-secrets` exists only in `dbr_dev`. The trial
  workspace cannot read the Event Hubs connection string at all, which is why the first live
  streaming test must run in `dbr_dev` on GP1 and cannot simply be moved to serverless.
- **The `dbr_dev_trial` catalog is `ISOLATED`**, so it is reachable only from the trial workspace.
  Cross-workspace reads work in one direction only.
- **The trial workspace defaults to the `dbr_dev_trial` catalog**, so UrbanFlow code running there
  must fully qualify `dbr_dev.<schema>` instead of relying on the default namespace.
- **The `trial` SKU is a time-limited Azure offer.** Its serverless compute is neither free nor
  guaranteed to persist, so nothing load-bearing depends on it.

`dbr_dev_trial` is also shared with other students (nine of their Jobs already exist there), so the
same non-interference rules that govern `dbr_dev` apply to it.

## Security boundaries

- No PAT, connection string, SAS key, or client secret is stored in Git.
- Event Hubs credentials are referenced through a Key Vault-backed Databricks secret scope.
- The notebook never prints Kafka options because they contain SASL credentials.
- Proposed objects use the `parvinbadalov_urbanflow` schema and `urbanflow` prefix.
- Existing Lab 8, Lab 9, GP1, GP2, other students' schemas, Event Hubs, and compute are outside the
  project ownership boundary. Notebook Jobs may attach to GP1 or GP2 only when the selected cluster
  is already running; UrbanFlow never manages their lifecycle.
- The CLI refuses a Databricks profile whose resolved host differs from the configured host.
- Lakeflow uses its own serverless managed compute and never receives a GP1 or GP2 cluster ID.

## Operational failure handling

| Failure | Behavior |
|---|---|
| GBFS timeout or invalid JSON | Raise a domain error; do not publish a partial response |
| Missing required field | Reject the feed record before publication or quarantine after ingestion |
| Unchanged source observation | Skip duplicate event ID within the bounded producer session |
| Kafka restart | Resume from the dedicated checkpoint |
| Unexpected schema field | Preserve raw JSON; route through controlled evolution/rescue path |
| Job or pipeline timeout | Report timeout explicitly; do not label it success |
| Cluster cleanup | Accept cleanup only after the API reports exact `TERMINATED` |

The cluster-cleanup rule above applies only to an explicitly created, isolated educational cluster.
GP1 and GP2 are hard-coded protected IDs: cleanup stops the UrbanFlow query and leaves those shared
clusters unchanged.

## Why Lakeflow remains a later stage

The bundle contains a serverless, triggered Lakeflow configuration and a Bronze Kafka streaming
table declaration. It has not been deployed or executed. Serverless Lakeflow manages its own
compute and checkpoints, independently of GP1/GP2. Physical Silver and Gold functions now exist as
an existing-cluster Phase 2 Job; moving them into Lakeflow and adding managed expectations remain
later work.

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
| Bronze | `bronze_station_status` | One received Kafka event | Consumer implemented; table not created |
| Bronze | `bronze_historical_trip` | One physical trip row | Contract/sample verified; Auto Loader pending |
| Bronze | `bronze_station_information` | One captured reference record | Client implemented; table pending |
| Bronze | `bronze_weather` | One place/time observation | Sample verified; table pending |
| Silver | `silver_station_snapshot` | One valid station/source timestamp | Rules implemented locally; Delta pending |
| Silver | `quarantine_station_status` | One invalid event with all failed rules | Routing implemented locally; table pending |
| Silver | `dim_station_scd2` | One station version | Pure transition implemented; Delta MERGE pending |
| Gold | `fact_station_shortage` | One shortage episode/observation | Pending repeated live observations |
| Gold | `fact_historical_trip` | One valid historical ride | Pending Auto Loader and data quality |
| Gold | `dim_weather_hourly` | One weather hour | Pending historical weather selection |
| Gold | `daily_station_summary` | Station and date | Pending medallion pipeline |

## SCD strategy

- **SCD Type 1:** correct non-historical descriptive attributes when preserving old values has no
  analytical value.
- **SCD Type 2:** preserve changes to name, capacity, latitude, or longitude when historical
  interpretation depends on the version in effect.
- If no real capacity change is available in the short observation window, a clearly labelled
  synthetic capacity change will demonstrate the mechanics. It will never be mixed into real
  operational facts.

## Schema evolution strategy

1. Bronze retains raw JSON so unexpected fields are not lost.
2. Trusted columns use an explicit schema.
3. A controlled fixture introduces one additive field for the schema-evolution lesson.
4. Auto Loader `schemaLocation` and checkpoint paths are isolated.
5. `_rescued_data` is retained for fields that do not match the current contract.
6. `mergeSchema` or `autoMerge` is enabled only in the controlled exercise, not globally.
7. Column mapping is demonstrated before renaming a Delta column.

## Security boundaries

- No PAT, connection string, SAS key, or client secret is stored in Git.
- Event Hubs credentials are referenced through a Key Vault-backed Databricks secret scope.
- The notebook never prints Kafka options because they contain SASL credentials.
- Proposed objects use the `parvinbadalov_urbanflow` schema and `urbanflow` prefix.
- Existing Lab 8, Lab 9, GP1, GP2, other students' schemas, Event Hubs, and compute are outside the
  project boundary.
- The CLI refuses a Databricks profile whose resolved host differs from the configured host.

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

## Why Lakeflow remains a later stage

The Bronze stream and local transformation contracts exist, but Lakeflow resources are omitted
until the approved schema, Volume, compute mode, source paths, and secret access are exercised.
This avoids producing a syntactically impressive bundle that silently assumes shared-workspace
permissions. The final Lakeflow source will stay readable: Bronze streaming tables, Silver valid
and quarantine materialized views, Gold aggregates, explicit expectations, and a reconciliation
gate.

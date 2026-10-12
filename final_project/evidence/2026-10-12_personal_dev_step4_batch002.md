# Step 4B: CDC `batch_002` (2026-10-12): CDC PASS, one unexpected source version, stopped before `batch_003`

**Outcome:** the CDC run itself did exactly what was expected:

- 1 insert, 2 updates (pre- and post-images) and 1 delete, which is 6 CDF events at source version 3;
- history 5 → 11;
- Silver equal to `batch_002`, with `MT-1003` deleted and its delete event kept;
- checkpoint 2 → 3 after reconciliation, with zero mismatches.

**Unexpected:** five seconds after the MERGE, Databricks committed an automatic `OPTIMIZE` to `station_maintenance_source` (`auto: true`), so the source is at **version 4, not 3**. Version 4 changes no data, so `table_changes` returns no rows for it. With the deployed code, `batch_003` would see "latest 4 > checkpoint 3" and move the checkpoint to 4. That contradicts the approved expectation that the checkpoint stays at 3.

As instructed, **`batch_003` and the Lakeflow run were not started.**

## Run

| Item | Value |
|---|---|
| Job / run | `158911626270744` / `343513312512955`, token `urbanflow-final-step4-batch002-20261013-001` |
| Window | 2026-10-12 22:03:14 → 22:08:08 UTC (294 s), `SUCCESS`, no retry |
| Parameters | `run_federation_cdc=true`, `cdc_source=fixture_snapshot`, `cdc_snapshot=batch_002`; station, historical, weather, full month and Lakeflow `false` |

| Task | Result |
|---|---|
| `preflight` | PASS, only `run_federation_cdc` enabled |
| `land_sources` | `NOTHING_TO_LAND` |
| `station_source_check` | `SKIPPED` (station branch disabled) |
| `station_silver`, `station_gold`, `historical_trips`, `weather_enrichment` | `DRY_RUN`: no read, no write |
| `validate_medallion` | PASS, no checks (no branch to validate) |
| `federation_cdc` | PASS: `source_merged: true`, differences 3 / 3, events `insert 1, update_preimage 2, update_postimage 2, delete 1`, checkpoint 2 → 3, source 5 = target 5, all mismatch counts 0 |
| `refresh_lakeflow` | `EXCLUDED` |

## Independent verification (read-only SQL after the run)

| Check | Expected | Actual |
|---|---|---|
| CDF events at v3 | 1 insert, 2 pre, 2 post, 1 delete | as expected |
| `maintenance_cdc_history` | 11 rows, 11 distinct IDs | 11 / 11 (v1: 5 inserts; v3: 6 events) |
| `silver_station_maintenance` | 5 rows equal to `batch_002` | equal (`MT-1001`, `MT-1002`, `MT-1004`, `MT-1005`, `MT-1006`) |
| `MT-1003` | Absent from Silver; delete kept in history | absent; history v1 `insert`, v3 `delete` |
| Checkpoint | 3 | 3, `processed_at` 2026-10-12 22:07:59.699529 |
| Reconciliation at the processed version, both directions | 0 | 0; no CDF event missing from history |
| Source latest version | 3 | **4** (`OPTIMIZE`, `auto: true`, 22:07:32; no CDF rows) |

## Delta commits

| Table | Before → after | Commits |
|---|---|---|
| `station_maintenance_source` | v2 → v4 | v3 MERGE (1 inserted, 2 updated, 1 deleted); **v4 OPTIMIZE (automatic)** |
| `silver_station_maintenance` | v2 → v4 | v3 MERGE (1 inserted, 2 updated, 1 deleted); v4 OPTIMIZE (automatic) |
| `maintenance_cdc_history` | v2 → v3 | MERGE, 6 inserted |
| `maintenance_cdc_checkpoint` | v2 → v3 | MERGE, 1 updated |
| The 16 other Final Project tables (Bronze, Silver, Gold, historical, weather, dimension) | unchanged | none |

## Fix prepared (local only, not deployed)

`11_federation_cdc` now scans `table_changes(checkpoint + 1, latest)` and decides from the CDF rows it finds (`cdc_sql.range_has_changes`), not from the version number:

- **No rows** (only `OPTIMIZE` or similar commits): no history or Silver MERGE, and the checkpoint does not move.
- **Rows present:** processing and the checkpoint advance are unchanged. Pending data from an interrupted run is still applied.

Regression tests cover:

- `batch_002` followed by an `OPTIMIZE` and then `batch_003`: the checkpoint stays put;
- data arriving after an `OPTIMIZE`: the whole range is processed;
- an `OPTIMIZE` between the MERGE and the latest-version read: the changes are not hidden.

One trade-off: the checkpoint can stay behind non-data commits. If the source has no real change for longer than the Delta log retention (30 days by default), the next read fails loudly instead of skipping changes.

Running `batch_003` against this fix needs a redeploy and a new approval.

Machine-readable record: [`2026-10-12_personal_dev_step4_batch002.json`](2026-10-12_personal_dev_step4_batch002.json).

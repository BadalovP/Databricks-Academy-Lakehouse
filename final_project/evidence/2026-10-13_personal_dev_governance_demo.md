# Phase F: governance demonstration on isolated copies (Personal DEV, 2026-10-13): PASS

**Outcome:** a row filter and three column masks were applied, observed and rolled back on copies in an isolated, disposable schema, which was then dropped. No GRANT was issued and no group or user changed. No policy ever touched the Final Project's data schema.

**Limitation:** one identity, and no `urbanflow_admins` group, exist in this workspace. The demo shows filtering by changing the operator's own mapping row, and masking for a non-admin. It does **not** prove enforcement for several principals, or the admin branch of the masks.

## Before running

| Check | Result |
|---|---|
| Dry run reviewed | `setup` (schema, 3 copies, mapping table, 3 functions), `apply` (1 row filter, 3 masks), `rollback` (4 drops), `cleanup` (`DROP SCHEMA … CASCADE`, demo schema only) |
| Demo schema `workspace.urbanflow_final_dev_governance_demo` | Absent; only `urbanflow_final_dev` and `urbanflow_final_lakeflow_dev` existed |
| Fail-safe | Since 2026-10-13 the script rolls back and drops the demo schema in `finally`, even after a failed step, and records the error in its evidence (regression tests added) |

## Run

`python scripts/run_governance_demo.py --target personal_dev --execute --confirm RUN_URBANFLOW_FINAL_GOVERNANCE_DEMO --warehouse-id 410e5f3b4784e981 --profile personal_lab10 --evidence evidence/2026-10-13_personal_dev_governance_demo.json --cleanup`

Window 00:46:17 → 00:47:06 UTC (49 s), exit code 0, no retry.

| Stage | Observed |
|---|---|
| Before | No masks or filters in the demo schema; **0 masks, 0 filters on `urbanflow_final_dev`** |
| Unpoliced copy | NORTH 12 stations (lat 40.7528–40.8172), SOUTH 28 (40.6420–40.7492); ride IDs in clear |
| Policy metadata after apply | `information_schema`: masks `coordinate_mask` on `latitude`/`longitude`, `ride_id_mask` on `ride_id`; row filter `zone_row_filter` on `dispatch_zone`. Data schema still 0 / 0 |
| No mapping row | **0 rows visible** (the filter denies by default) |
| Mapping `SOUTH` | Only SOUTH, 28 stations |
| Mapping `ALL` | NORTH 12 + SOUTH 28; **coordinates rounded to 2 decimals** (40.75–40.82, 40.64–40.75) |
| Ride IDs | **SHA-256 hashes** instead of the 16-character IDs |
| After rollback | No masks, no filters; data schema 0 / 0 |
| Cleanup | `demo_schema_exists_after_cleanup: false` |

## Independent verification

| Check | Result |
|---|---|
| Schemas in `workspace` with `urbanflow` | `urbanflow_final_dev`, `urbanflow_final_lakeflow_dev` only |
| Explicit grants on `workspace.urbanflow_final_dev` | None (unchanged; the demo issues no GRANT) |
| Masks / row filters in the whole `workspace` catalog | 0 / 0 |
| Evidence privacy | The JSON evidence contains no e-mail address and no GRANT statement |

Machine-readable record: [`2026-10-13_personal_dev_governance_demo.json`](2026-10-13_personal_dev_governance_demo.json).

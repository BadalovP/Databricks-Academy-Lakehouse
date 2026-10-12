# Operations runbook

[← Main README](../README.md) · [Release and validation](RELEASE_AND_VALIDATION.md) · [Architecture](ARCHITECTURE.md)

How to deploy, run, recover and govern UrbanFlow 2.0. Every procedure below was exercised in Personal DEV unless marked **(not yet exercised)**.

## 1. Deploy (DEV and PROD)

| Step | DEV (`personal_dev`) | PROD-style (`azure_prod`, Academy trial) |
|---|---|---|
| Auth | CLI profile `personal_lab10` (owner's login) | `DATABRICKS_HOST=https://adb-7405618361895520.0.azuredatabricks.net`, `DATABRICKS_AUTH_TYPE=azure-cli` |
| Warehouse | resolved by name (`Serverless Starter Warehouse`) | `BUNDLE_VAR_warehouse_id=d8b8ff9942968cb8` |
| Validate | `databricks bundle validate -t personal_dev -p personal_lab10` | `databricks bundle validate -t azure_prod` |
| Plan | `databricks bundle plan -t <target> … --output json > plan.json`, then `python scripts/validate_release_plan.py --phase pre --target <target> --plan plan.json --jobs jobs.json` | same |
| Deploy | `databricks bundle deploy -t personal_dev -p personal_lab10` | **(not yet exercised; needs approval)** |
| Verify | plan again → `--phase post` must report every resource `skip` | same |

Run these from `final_project/`. Run `databricks jobs list` from **outside** the repository: inside it, the CLI picks up the bundle and returns nothing useful.

## 2. Run the platform Job

Always use `databricks jobs run-now --json @request.json --no-wait` with a **new** `idempotency_token`. Pass the execution IDs explicitly when re-processing an existing execution.

| Purpose | Switches |
|---|---|
| Sample medallion + CDC | `run_station_pipeline`, `run_historical`, `run_weather`, `run_federation_cdc` = `true`; `source_execution_id=urbanflow-final-304828008837365` to reuse the verified execution |
| CDC batch only | only `run_federation_cdc=true`, `cdc_snapshot=batch_00N` **(not yet deployed)** |
| Lakeflow only | only `run_lakeflow=true` **(not yet deployed)** |

The preflight refuses an empty run, a mix of sample and full-month parameters, and a historical-only run on a schema without the station dimension.

## 3. Recover from a failure

| Symptom | Cause found in practice | Action |
|---|---|---|
| A task fails | Read `jobs get-run-output` for **that task**; the Job state alone says nothing | Fix in code, test, redeploy, then rerun **once** with the same execution IDs |
| Rerun after a partial failure | Landing reports `already_landed`; Auto Loader finds no new files for the execution's checkpoint | Nothing to undo; the MERGEs absorb what was already written |
| `station_source_check` count mismatch | A count copied from Demo 3 instead of the sample's own | Fixed: the contract derives the size from the sample |
| Historical task: dimension missing | The task started before `station_gold` | Fixed: `historical_trips` depends on `station_gold` |
| Weather: sample file missing | Nothing landed it | Fixed: `land_sources` lands it |
| CDC reconciliation fails | Target and source differ at the processed version | The checkpoint did **not** move. Fix the cause and rerun: the same version range is processed again. |

Never delete Bronze rows, checkpoints or landed files to "repair" a run. Every rerun path is idempotent (step 3 proved it).

## 4. CDC checkpoint and replay rules

1. The snapshot (fixture or Federation) is compared with the current source in both directions. **Identical → no source MERGE → no source commit.**
2. Otherwise the change-aware MERGE writes a new source version. Only real changes become CDF events (inserts, update pre-images and post-images, deletes).
3. Versions `checkpoint + 1 … latest` are read with `table_changes`. If they contain CDF rows, they are merged into the append-once history (keyed by a deterministic `change_id`) and into Silver (newest final state per key). Versions without data changes, such as an automatic `OPTIMIZE`, have no CDF rows and are not work (fix prepared after step 4B, not yet deployed).
4. Reconciliation compares source `VERSION AS OF latest` and Silver, values and keys, in both directions, and checks that every event reached the history.
5. **Only after reconciliation passes** does the checkpoint advance, and only forward. It does not move for a range without CDF rows.

Consequences:
- A crash before step 5 replays the same range safely.
- A run whose snapshot already matches still processes pending versions left by an interrupted run.
- An unchanged replay leaves the source version and the checkpoint untouched (step 3 retry).
- Databricks may add `OPTIMIZE` versions to the source by itself (step 4B saw one 5 seconds after the MERGE). Do not treat a version number above the checkpoint as unprocessed data; check `table_changes` for that range.
- If the source has no real change for longer than the Delta log retention (30 days by default), the next run's `table_changes` read fails. Investigate before you move the checkpoint by hand.

This is Delta Change Data Feed over imported snapshots, **not** database log-based CDC.

## 5. Security and governance

- **Identity.** DEV uses the owner's login. The CD design uses OAuth M2M for DEV and GitHub OIDC (no secret) for PROD; neither identity exists yet. No PAT or secret is in Git; the CD workflow pins hosts and refuses the shared Academy workspaces.
- **Data access.** All objects live in Final-Project schemas with `prevent_destroy`. No `GRANT` is active anywhere; the least-privilege grants in `sql/20_governance_rls_cls.sql` stay commented.
- **Row filter and masks** (`scripts/run_governance_demo.py`).
  - **Where:** they run only on isolated copies in `<schema>_governance_demo`.
  - **What:** the row filter limits a dispatcher to their zones through a mapping table; the column masks round coordinates and hash `ride_id`.
  - **How:** every policy is applied, observed and rolled back, then the demo schema is dropped.
  - **Limit:** with a single identity this proves the mechanism, not multi-principal enforcement. **(not yet exercised; needs approval)**
- **Assistant safety.**
  - Retrieved text is untrusted, and the prompt forbids following embedded commands (tested with a malicious document).
  - Monitoring stores question hashes, never questions or answers.
  - Model errors are sanitized.
- **Isolation tests.** These reject Demo 3 schema names, Lab 11 credentials and schemas, and cross-target dashboard reads.

## 6. RAG evaluation report template (fill only from a live run)

```
Run: Job 887464717518046, run <id>, <date>, endpoint urbanflow-final-dev, model databricks-gpt-oss-20b
Index: <catalog>.<schema>.<index>, rows <n> (= chunks <n>), synced <timestamp>
Questions: 18 (16 answerable, 2 unanswerable), baseline on/off
                     baseline   RAG
answer_correctness   <x>        <x>
retrieval_recall     n/a        <x>
source_coverage      n/a        <x>
citation_precision   n/a        <x>
clean refusals       <n>        <n>   (expected: q17, q18)
errors               <n>        <n>
Per-question review: <link to review records>
Endpoint deleted: <timestamp>, list_endpoints() = []
```

Offline fixture figures (recall 0.938, coverage 0.875, correctness 0.648 vs baseline 0.111) test the pipeline only. They must never be reported as model quality.

# UrbanFlow live execution evidence

`*.reconciliation.json` is committed: it is small and contains only counts and
status. The full `*.producer.json` and `*.bronze.json` reports are **local-only**
(gitignored) because each carries 2,520 event-ID hashes, which bloats the
repository without adding reviewable information. Regenerate them by rerunning the
bounded test; the committed reconciliation file is what proves the outcome.

No evidence file contains a secret, a connection string, a token, or a workspace
hostname.

## Attempt 3 (2026-09-29) — first successful Event Hubs to Bronze run

See `FIRST_STREAMING_TEST.md` for the full account, including the two earlier
attempts that stopped before publishing anything.

## Second live validation, 2026-10-02 (GP1)

| File | What it records |
|---|---|
| `urbanflow-20260929T195132Z-r3.gold.json` / `.gold.run2.json` | The **pre-correction** Gold runs of 2026-09-30, preserved unchanged. These are the 746-row results that contained the out-of-service misclassification |
| `urbanflow-20260929T195132Z-r3.gold.corrected-run1.json` | First corrected run (`240497605145949`): 746 legacy rows attributed, 746 -> 657, 89 stale rows removed |
| `urbanflow-20260929T195132Z-r3.gold.corrected-run2.json` | Idempotency repeat (`725768954237074`): 657 -> 657, 0 stale removed, 0 to backfill |
| `urbanflow-20260929T195132Z-r3.silver.corrected-run2.json` | Silver after the migration; 2,520 rows, 28 columns |
| `phase2-correction-wholetable-verification.json` | Read-only whole-table audit (`618116231829401`) taken between the two runs: status composition, `OUT_OF_SERVICE` counts, unassigned-row counts, and the shortage/priority agreement check |
| `urbanflow-hist-devsample40-20261002T0010Z.historical.run1.json` / `.run2.json` | The **40-ROW DEVELOPMENT SAMPLE** historical runs. NOT full January 2024 data |
| `urbanflow-weather-devsample48h-20261002T0015Z.weather.json` | The 48-hour committed weather sample run, `source: sample_json`, no external request |

## Final safe Batch A read validation, 2026-10-03 (GP1)

| File | What it records |
|---|---|
| `BATCH_A_READ_VALIDATION.md` | Human-readable preflight, exact counts, status distributions, GE totals, safety boundary and monthly-isolation review |
| `2026-10-03_batch_a_read_validation.json` | Machine-readable `SHOW`/`SELECT` and ephemeral GE results: Silver 10/10, historical sample 10/10, zero unexpected rows |

This batch created no Databricks object. It did not submit a Job, upload a file, create a
checkpoint, start Auto Loader, deploy Lakeflow or change GP1 lifecycle state.

Two notes on reading these honestly:

- The Volume report path is keyed only on `source_execution_id`, so a repeat run overwrites the
  previous report there. The `-run1` files were captured from the Volume before the repeat and are
  labelled as captured summaries; the `-run2` files are verbatim downloads.
- The historical and weather files describe **samples**. 40 trip rows and 48 weather hours prove
  the mechanics end to end; they say nothing about a full month, and neither file should be cited
  as monthly coverage.

## SQL alert validation, 2026-10-03

| File | What it records |
|---|---|
| `ALERT_VALIDATION.md` | The last Academy row closed: one SQL alert on the actionable-shortage measure, created on the shared academy serverless warehouse, genuinely evaluated to `TRIGGERED` against 657, then deleted |
| `2026-10-03_alert_validation.json` | Machine-readable result: warehouse profile and ownership, statement ID, alert ID, condition, evaluation timestamp, notification configuration, cleanup state and blast-radius counts |

Two things these files are careful about, and a reader should rely on:

- **Email delivery is inferred, not confirmed.** The alert transitioned to `TRIGGERED` with one
  email subscription configured, which is the condition under which Databricks sends. The mailbox
  was not inspected.
- **The warehouse is not ours.** It belongs to `lbiel@softserve.academy` with `CAN_USE` granted to
  the `users` group, so using it is intended but the billing is the shared academy account's, and
  `CAN_USE` cannot stop it — it auto-stops after 5 idle minutes.

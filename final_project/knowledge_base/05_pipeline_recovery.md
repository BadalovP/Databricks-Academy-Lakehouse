# Pipeline Recovery Runbook

> Fictional UrbanFlow recovery procedure for Academy demonstration purposes.

## Bounded replay

Stop before deleting or editing a checkpoint. Confirm the target, source range, and last successful
run. Prefer a new execution ID and an isolated checkpoint, process with `availableNow`, then
reconcile input, accepted, quarantined, duplicate, and Gold counts.

## Lakeflow recovery

A full Lakeflow refresh is not the normal recovery action. It can destroy streaming state and
reprocess all available data, so it requires explicit approval and a documented rollback. Prefer a
triggered selective refresh after dependencies are materialized.

## Idempotency proof

Run the same bounded input again. Expected results are zero new business rows, stable Gold metrics,
and only delivery-attempt metadata changing where explicitly designed.


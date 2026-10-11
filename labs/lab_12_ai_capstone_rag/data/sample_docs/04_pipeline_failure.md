# Aurora Data Pipeline Failure Playbook

> Fictional training document. The Aurora pipeline and its owners do not exist.

## Failure classification

Classify the failure as source availability, schema drift, quality rule, checkpoint state, or platform failure. Preserve the original error and pipeline update ID. Confirm the last committed target version before attempting recovery.

## Schema drift

When a new source column appears, compare it with the approved data contract. Additive nullable columns may proceed only after the schema owner approves the contract update. Type changes and removed columns require the pipeline to remain paused until producers and consumers agree on remediation.

## Checkpoints and replay

Never delete or edit a streaming checkpoint to make a failure disappear. If replay is required, create an isolated recovery checkpoint and write to a dedicated recovery table. Reconcile recovery output before any approved merge into the normal target.

## Verification

Validate source-to-Bronze counts, Bronze-to-Silver accepted plus quarantined counts, and the latest event timestamp. Resume downstream processing only after all three checks pass.


# Orion Batch Job Recovery Runbook

> Fictional training document. Names, systems, and procedures are invented for Lab 12.

## Scope

This runbook covers the fictional Orion nightly billing batch. It does not apply to streaming jobs or customer-facing APIs. The on-call engineer must first record the failed run ID, failure timestamp, and last successful task in the incident ticket.

## Initial triage

Check the orchestrator run graph and classify the failure as input missing, transient infrastructure, data quality, or code defect. Do not rerun the entire workflow before confirming whether completed tasks are idempotent. If the input manifest is absent, pause recovery and contact the upstream file-transfer owner.

## Safe recovery

For a transient infrastructure failure, retry only the failed task once. For data quality failures, quarantine the rejected partition and obtain approval from the billing data owner before resuming. A code defect requires a reviewed hotfix and change record. After recovery, reconcile input, processed, and rejected record counts and attach the reconciliation result to the incident.

## Stop conditions

Stop and escalate if a retry produces different totals, if duplicate invoice identifiers appear, or if the recovery would bypass a quality check. Never edit checkpoint files manually.


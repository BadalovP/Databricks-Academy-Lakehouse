# Meridian Change and Deployment Validation Procedure

> Fictional training procedure. Approval roles and environments are examples only.

## Before deployment

Every production change needs a linked change record, peer review, test evidence, rollback plan, and named validation owner. The deployer must verify the target workspace and environment immediately before execution. Credentials must come from the approved identity provider, never from repository files.

## Deployment controls

Production deployment is manually triggered and protected by a human approval gate. Automatic deployment from a pull request is prohibited. Apply the smallest reviewed change and preserve the deployment log. Do not combine unrelated infrastructure and application changes in an emergency release.

## Validation

Validate service health, the changed behavior, one critical end-to-end path, data reconciliation where applicable, and monitoring signals. Compare results with the recorded pre-deployment baseline. The validation owner, not the deployer alone, records acceptance.

## Rollback

Rollback when a critical validation fails, error rate breaches the release threshold, or output reconciliation differs. Stop further changes, execute the reviewed rollback plan, and update the incident or change record. A rollback does not remove the need for root-cause follow-up.


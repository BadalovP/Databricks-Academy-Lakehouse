# Security and Access Controls

> Fictional UrbanFlow operating procedure aligned with the project security design.

## Secrets

Event Hubs connection strings belong in Azure Key Vault and are exposed only through an approved
secret scope or the local Azure SDK identity. Secrets are never committed, printed, passed as job
parameters, stored in evidence, or included in RAG monitoring rows.

## Least privilege

Deployment identities receive only the catalog, schema, Volume, Job, Pipeline, Dashboard, Vector
Search, and model-query permissions required for their target. DEV and PROD use separate hosts,
schemas, bundle state paths, and GitHub environments.

## Release controls

Production workflows are manual and approval gated because deployments mutate governed resources
and live runs can incur cost. Plans are reviewed before deployment; destructive actions, continuous
compute, full refresh, and new paid resources require explicit approval.


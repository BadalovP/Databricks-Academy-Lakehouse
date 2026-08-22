# Lab 06 target strategy

Lab 06 External V2 keeps the same Unity Catalog schema name in both workspaces:

- Azure: `dbr_dev.parvinbadalov_lab06_ext`
- Personal: `dbr_dev.parvinbadalov_lab06_ext`

The workspaces have separate Unity Catalog metadata, so these are separate
schema objects even though the fully qualified names are identical.

All Lab 06 External V2 targets use the same academy ADLS Gold root:

`abfss://parvinbadalov@dlspl21databricks.dfs.core.windows.net/lab06_gold_external_v2`

This project assumes Lab 06 targets run sequentially, not concurrently.

No `parvinbadalov_lab06_ext_prod` schema is created.

Labs 07-12 should default back to `dbr_dev.parvinbadalov` unless a lab has a
specific technical reason to require another schema.

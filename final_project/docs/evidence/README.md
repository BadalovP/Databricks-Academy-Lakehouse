# UrbanFlow execution evidence

UrbanFlow now has dated live evidence for the bounded Event Hubs path, corrected Silver/Gold path,
historical and weather development samples, and the final Batch A read-only validation. The newest
record is:

- [Batch A read-only validation](../../evidence/BATCH_A_READ_VALIDATION.md)
- [Machine-readable Batch A result](../../evidence/2026-10-03_batch_a_read_validation.json)

For each approved milestone, evidence should cover:

1. producer count and bounded Event Hubs publication,
2. Bronze Kafka metadata and reconciliation,
3. Lakeflow graph and expectations,
4. Job task graph and outputs,
5. dashboard and alert validation,
6. independent compute termination,
7. GitHub Actions static and approval-protected live runs.

Every evidence entry must name its environment, run/resource ID where safe, exact validation, and
limitations. Secrets, tokens, connection strings, SAS keys, and unrelated student resources must
be cropped or removed.

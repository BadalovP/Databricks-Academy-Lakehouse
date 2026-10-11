# Monitoring, redaction, and retention

`lab12.monitoring.QueryEvent` records a UTC timestamp, a SHA-256 question hash, question length, model and index identifiers, retrieved chunk IDs, latency, error class, and optional evaluation JSON. It intentionally excludes raw questions, retrieved content, answers, authentication headers, and exception messages from providers.

The proposed Delta table is `dbr_dev.parvinbadalov_lab12_rag.query_monitoring`. The configuration sets a 30-day retention policy, but Phase 1 does not create a scheduled deletion mechanism. Before production use, an owner must approve and implement the workspace's standard retention process. Until then, operators should not claim automated retention is active.

Recommended alerts:

- error rate over a reviewed window;
- retrieval returning zero chunks above an agreed threshold;
- p95 latency regression;
- index sync failure or stale source version;
- sudden shifts in citation coverage or unsupported-claim score.

Question hashes are pseudonymous, not anonymous. Restrict access to monitoring tables and avoid joining hashes to identity data unless an approved security investigation requires it.


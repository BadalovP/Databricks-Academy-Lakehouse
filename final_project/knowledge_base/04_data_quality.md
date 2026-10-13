# Data Quality and Quarantine

> Fictional UrbanFlow data-control procedure for Academy demonstration purposes.

## Contract failures

Station events require a stable event_id, station_id, event time, non-negative bike and dock
counts, and valid service flags. A missing station identifier is routed to quarantine with the
failed rule; it is never silently dropped.

## Duplicates

Duplicate delivery is identified by event_id. The first deterministic arrival becomes the Silver
record and later deliveries are written to the duplicate audit set. Business keys and timestamps
are retained for diagnosis.

## Reconciliation

Every run must prove Bronze rows equal Silver rows plus quarantined rows plus duplicate rows.
Referential-integrity failures and unexpected schema fields are counted separately. A failed
reconciliation blocks promotion.


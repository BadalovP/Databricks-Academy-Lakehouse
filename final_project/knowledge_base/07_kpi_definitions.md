# Business KPI Definitions

> Fictional UrbanFlow KPI operating definitions for Academy demonstration purposes.

## Rebalancing priority

Rebalancing priority equals severity points plus deficit points plus station-size points. The
component columns remain visible so an operator can reproduce every score without a model.

## Availability

Bike shortage count is the number of actionable stations at or below the low-bike threshold. Dock
shortage count uses the low-dock threshold. Out-of-service stations are reported separately.

## Freshness and quality

Freshness lag is the number of minutes between current time and the latest source event timestamp.
Quarantine rate is quarantined rows divided by Bronze rows for the same execution. Reconciliation
status passes only when all Bronze rows are accounted for exactly once.


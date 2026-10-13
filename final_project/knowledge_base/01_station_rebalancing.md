# Station Rebalancing Procedure

> Fictional UrbanFlow operating procedure for Academy demonstration purposes.

## Trigger conditions

A station enters the rebalancing queue when available bikes are at or below the low-bike
threshold or available docks are at or below the low-dock threshold. A low-bike station normally
receives the action **DELIVER BIKES**. A low-dock station normally receives **COLLECT BIKES**.
Out-of-service stations are excluded from rebalancing because moving bikes cannot repair them.

## Priority and dispatch

Priority is the transparent sum of severity points, deficit points, and station-size points.
Dispatchers work highest score first, while checking nearby stations and current telemetry before
moving a vehicle. If both bikes and docks appear critically low, telemetry is conflicting; the
action is **INSPECT STATION**, not an automatic bike movement.

## Completion

Record the station identifier, action, vehicle identifier, completion time, and observed counts.
Confirm a fresh GBFS reading before closing the action.


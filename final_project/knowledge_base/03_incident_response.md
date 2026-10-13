# Mobility Incident Response

> Fictional UrbanFlow operating procedure for Academy demonstration purposes.

## Stale telemetry

Freshness monitoring compares current time with the latest source event time. When station
telemetry becomes stale, first verify the source feed and ingestion status, then inspect Event Hubs
lag and the Bronze checkpoint. Escalate if the source is current but the lakehouse is not advancing.

## Severity and escalation

A severity-one incident affects rider safety, city-wide availability, or governed-data integrity.
The on-call engineer acknowledges it within five minutes, assigns an incident commander, and
notifies operations within fifteen minutes. Preserve logs and run identifiers; never paste secrets
or raw credentials into an incident channel.

## Recovery validation

After recovery, verify freshness, Bronze-to-Silver reconciliation, Gold KPI reasonableness, and a
repeat run with no unintended inserts or updates.


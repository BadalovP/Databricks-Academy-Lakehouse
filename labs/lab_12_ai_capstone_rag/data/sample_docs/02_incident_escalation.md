# Northstar Incident Escalation Standard

> Fictional training document. It contains no real contacts or production details.

## Severity definitions

SEV-1 means a complete outage or confirmed data corruption with broad business impact. SEV-2 means a major degradation with a workaround. SEV-3 means a limited issue without material business impact.

## Escalation timing

For SEV-1, open an incident channel immediately and notify the fictional Incident Commander role within 10 minutes. For SEV-2, assign a technical lead within 30 minutes. SEV-3 follows the normal service queue. Do not include credentials, access tokens, or raw personal data in the incident channel.

## Required update format

Each update contains impact, current hypothesis, actions completed, next action, owner, and next update time. State unknowns explicitly. The Incident Commander approves external status messages; engineers must not publish unreviewed root-cause claims.

## Closure

Closure requires service restoration, validation from the affected business owner, and a linked follow-up record. A blameless review is required for every SEV-1 and for recurring SEV-2 incidents.


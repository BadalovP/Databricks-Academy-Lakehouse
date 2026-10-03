# UrbanFlow SQL alert validation — 2026-10-03

[← Evidence index](README.md) · [Machine-readable result](2026-10-03_alert_validation.json)

Closes the last genuinely outstanding Academy row, Lab 6 *alerts / email*: "an alert on a quality
or volume condition, delivered somewhere."

## The warehouse, and a finding worth stating first

Exactly **one** SQL warehouse can reach the UrbanFlow tables. It is **not ours**:

| Field | Value |
|---|---|
| Name / ID | Serverless Starter Warehouse / `3ed106620db591d9` |
| Size | 2X-Small, PRO, serverless, 1/1 clusters — the smallest available |
| Auto-stop | 5 minutes |
| Owner | `lbiel@softserve.academy` |
| Our access | **`CAN_USE`, granted to the `users` group** |

The `users` grant is what makes this legitimate rather than a trespass: it is the academy's shared,
instructor-provided warehouse that students are explicitly entitled to use. Two consequences were
stated before approval was requested, because they change the decision:

- **Billing lands on the shared academy account**, not a personal one.
- **`CAN_USE` does not include stop.** The warehouse could not be stopped manually and no attempt
  was made; it auto-stops after 5 idle minutes. That is the only cleanup lever available.

A second warehouse was looked for and does not exist — two profiles reaching the same metastore
(`7af05576-…`) both return this one warehouse and no other.

## The query

Reduced from the already-validated `sql/10_dashboard_current_operations.sql` query 1 to the single
measure the alert needs. Read-only, one row, one column.

```sql
SELECT SUM(CASE WHEN availability_status IN ('LOW_BIKES','LOW_DOCKS','LOW_BIKES_AND_DOCKS')
                THEN 1 ELSE 0 END) AS actionable_shortages
FROM dbr_dev.parvinbadalov_urbanflow.silver_station_status
```

Statement `01f1becb-1e37-19aa-8023-6f5f1f8d5dfa`, **SUCCEEDED**, result **657** — matching the
validated baseline exactly (278 + 374 + 5), with zero `OUT_OF_SERVICE` rows in actionable Gold.

`OUT_OF_SERVICE` is excluded deliberately. A station that is not installed or not renting cannot be
fixed by moving bikes, so counting it as actionable sends a van out for nothing — the exact defect
the first live Gold run contained.

## The alert

| Field | Value |
|---|---|
| ID | `3025530840217009` |
| Name | UrbanFlow actionable station shortages |
| Condition | `actionable_shortages` **GREATER_THAN 0** |
| Evaluation | **TRIGGERED** at `2026-10-03T01:40:30Z`, against 657 |
| Destination | one email subscription, to the alert owner's own workspace identity |
| Schedule | created **PAUSED**; briefly unpaused for one evaluation; re-paused |
| Final state | **DELETED** |

### The threshold was not tuned

`> 0` is the correct boundary for this measure, not a number chosen to force a result: zero
actionable shortages genuinely means there is nothing for an operator to do.

### The evaluation was genuine, not asserted

The arithmetic could have been checked by hand — 657 > 0 — and reported as "the alert would fire".
That would not have been evidence. Instead the schedule was briefly `UNPAUSED` with a one-minute
cron, the alert allowed to evaluate exactly once, and then re-`PAUSED` immediately. The
`TRIGGERED` state and `last_evaluated_at` timestamp above come from the alert object itself.

### Why the schedule is PAUSED by design

The underlying table is one frozen GBFS snapshot. A live cron would fire identically forever and
spend warehouse time while nobody is watching — which `PENDING_REQUIREMENTS.md` identified as the
real cost risk in this row, distinct from the one-off validation. A standing schedule becomes
meaningful only once the data refreshes.

### What is NOT claimed about the email

The alert transitioned to `TRIGGERED` with one email subscription configured and `notify_on_ok`
false, which is the condition under which Databricks sends the notification. **The recipient
mailbox was not inspected.** Delivery is therefore inferred from the triggered transition rather
than independently confirmed, and this document does not claim otherwise.

## Cleanup and blast radius

- The alert was **deleted** (`lifecycle_state: DELETED`); zero UrbanFlow alerts remain.
- The workspace already held **25 alerts** belonging to other labs and students. **None was read
  into, modified or removed.** Leaving a scheduled alert behind in a shared workspace would have
  added to that clutter, which is why deletion was the chosen end state.
- No Job was run, nothing was deployed, no row was written, no cluster lifecycle was changed, and
  no Lakeflow pipeline exists or was created.
- The warehouse was left to auto-stop, and **auto-stop was confirmed**: `RUNNING` at 01:46:14Z,
  `STOPPED` at 01:47:15Z. The billable window was roughly **8 minutes of 2X-Small serverless SQL**,
  from the query starting the warehouse to the idle timer expiring. No manual stop was attempted,
  and none was needed.

## Honest limits of this validation

It proves an alert can be defined on a real UrbanFlow measure, evaluated by a real warehouse
against real persisted data, and configured to notify. It does **not** prove a standing production
alerting posture: the schedule is paused, the alert no longer exists, and the signal it watches is
frozen. Turning this into an operationally useful alert needs refreshing data, which is the
monthly-archive work, not more alert configuration.

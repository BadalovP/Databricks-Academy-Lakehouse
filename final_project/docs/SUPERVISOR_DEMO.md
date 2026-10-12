# UrbanFlow — supervisor demo (10–15 minutes)

[← Project README](../README.md) · [Unified release](UNIFIED_RELEASE.md) · [Dashboard](DASHBOARD.md) · [Coverage](LABS_1_TO_9_COVERAGE.md)

A practical script. Each step names what to open and the one sentence to say. Times are targets.

**Have open before you start:** this repository on GitHub (Actions tab), the Databricks workspace
(Jobs, the pipeline, the dashboard), and [`evidence/README.md`](../evidence/README.md).

## Labels to repeat whenever a number appears

| Data | Say exactly this |
|---|---|
| Station availability | "One real GBFS snapshot - a point-in-time count, not a trend." |
| Trip history | "The full real January 2024 Citi Bike archive." |
| Historical station reference | "A 40-station development reference sample." |
| Weather | "One NYC reference coordinate - association, not causation." |
| Lakeflow | "An isolated declarative comparison, not a replacement." |

## 1. Problem (1 min)

An empty station loses a rental, a full one loses a return. UrbanFlow answers *which stations need
rebalancing now* and *what real demand looks like*. Rules are transparent: two or fewer bikes or
docks is a shortage, and an out-of-service station is never "actionable" because no truck fixes it.

## 2. Architecture (1 min)

Show the diagram in the [README](../README.md#1-overall-solution-architecture): GBFS → Event Hubs →
Bronze → Silver → Gold for stations; the January archive → Auto Loader → Silver → daily demand;
weather joined with a LEFT join; everything orchestrated by one Job; a Lakeflow copy in its own
schema; one dashboard on top.

## 3. GitHub CI and the protected release (2 min)

- Pull requests run Ruff, Black, pytest (515 tests; one bash syntax check runs only in CI) and offline bundle validation.
- Open `.github/workflows/demo3_urbanflow_deploy.yml`: `workflow_dispatch` only, exact confirmation
  string, protected `azure-release-approval` reviewer, Azure OIDC, **refuses to run unless GP1 is
  already RUNNING**, plans and deploys only the unified Job, then runs and validates it.
- Point at workflow **`37139449737`** (sample + repeat) and **`37140618864`** (full month).

> "Nothing reaches Azure without a named reviewer, and the release can never start a shared cluster."

## 4. The unified Job (1 min)

Open `[azure] UrbanFlow End-to-End` (`991496516229387`): 7 tasks on GP1, no schedule, no
`new_cluster`. `01_preflight` → station source check → Silver → Gold, and in parallel historical
trips → weather, all feeding a read-only `07_final_validation` that fails the Job on any mismatch.

## 5. Sample, idempotency and the full month (2 min)

| Run | Result |
|---|---|
| `4222809815373` sample | SUCCESS 7/7, final validation PASS |
| `284335864579341` repeat | SUCCESS, **0 rows inserted, 0 removed, no schema change** - idempotency proven from the write reports |
| `96337578882467` full month | SUCCESS 7/7, 18 minutes |

January 2024: **1,888,085 landed = 1,886,318 valid + 1,767 quarantine + 0 duplicates**. Quarantine
reasons: 1,160 missing start station, 607 longer than 24 hours. 1,678,496 member and 207,822 casual
rides. The archive is defined by ride *end*, so 374 rides started on 31 December.

## 6. Medallion, quarantine, reconciliation (1 min)

Every row lands in exactly one of Silver, quarantine (with named rules) or duplicates, and every
stage proves `Bronze = Silver + Quarantine + Duplicates`. Station snapshot: 2,520 = 2,520 + 0 + 0;
657 actionable shortages (278 low bikes, 374 low docks, 5 both), 89 out of service excluded.

## 7. Lakeflow comparison (1 min)

Pipeline `fb8a0b8a-…`, update `ba6710ed-…`, isolated schema `parvinbadalov_urbanflow_lakeflow`.
22 expectations, 0 failures, from the event log. **Business results identical** to the imperative
tables (zero rows either way of `EXCEPT ALL`) - expected, because both call the same functions.

## 8. Dashboard (2 min)

Open "UrbanFlow — NYC Mobility Operations & Demand" and walk the four pages. Stress the caveat
tiles: page 1 is a snapshot; page 3 is one coordinate and association only; page 4 explains that
**3.43% reference coverage is the 40-station development dimension, not bad data** (39 of its 40
stations appear, out of 2,223 in the month). Every historical tile is bound to the one full-month
execution.

## 9. Governance and quality (1 min)

Governance ran on disposable copies: a row filter returned 0, 28 and 40 rows as the mapping
changed, coordinates were rounded and `ride_id` hashed, then everything was rolled back. Grants
were not demonstrated because `account users` already holds `ALL_PRIVILEGES` on the shared catalog
- said plainly rather than simulated. Quality: contracts, quarantine, Great Expectations, Lakeflow
expectations, and the final-validation task.

## 10. Failures found and fixed (2 min)

Present these as the engineering story - each was caught by a fail-closed check, fixed by PR and
pinned with a test:

| Defect | What it would have caused |
|---|---|
| Silver operational status ignored | Out-of-service stations counted as shortages (746 instead of 657) |
| Stale derived Gold rows | MERGE cannot delete rows that stopped qualifying; replaced with execution-scoped replacement |
| Legacy `execution_id` NULLs | `NULL = 'id'` never matches, so 89 stale rows escaped every scoped check; fixed by a verified backfill |
| Nonexistent historical reference path | The historical task would have failed after writing Bronze |
| Monthly landing nested inside the sample | Auto Loader recurses, so a sample run would have read 1.9 million rows |
| Fresh-runner deployment state | A CI plan on a clean runner described existing Jobs as creates |
| CI Job ACL | The CI identity could authenticate but not read Jobs - least privilege working as designed |
| Bundle-root ACL | The release could not take the deployment lock |
| `run-now` body contract | `job_id` must be in the JSON body, not positional |
| Bash heredoc in a function | An indented terminator swallowed the script; now every run block is `bash -n` checked |
| Job parameter shadowing | A job parameter silently overrode the weather task's historical ID (run `157686710394279`) |
| Shared historical Bronze | A monthly run would have re-stamped the sample's rides (run `295677984549301`) |
| Unscoped dashboard query | One quality query summed the sample and the month |

## 11. Cost and safety (30 s)

Bounded `availableNow` runs; GP1 used only when already running and never started, resized or
stopped; Lakeflow triggered once, not continuous, no schedule; no dashboard schedule; no recurring
alert on frozen data; the shared warehouse only for bounded queries; VACUUM never run because the
table history is evidence.

## 12. Lessons (30 s)

1. A green status is not proof - read the write reports.
2. Fail closed: every defect above was stopped by a check before it corrupted data.
3. Label scope honestly: a snapshot, a month and a sample are different claims.

## Likely questions

- **Why is the match rate 3.43%?** The reference dimension is a deliberate 40-station sample;
  coverage, not quality.
- **Why not delete the failed runs?** They are the evidence that the checks work.
- **Why keep both Lakeflow and the notebooks?** To compare declarative and imperative on identical
  rules; parity is the result.
- **Why no interactive dashboard filter?** An execution filter could silently mix the sample and
  the month; the scope is bound on purpose.

# Demo Day presentation: UrbanFlow 2.0 (25 minutes)

[← Main README](../README.md) · [Coverage matrix](LABS_1_TO_12_COVERAGE.md) · [Architecture](ARCHITECTURE.md)

A 25-minute talk: 20 minutes of presentation and demo, then 5 minutes of questions. The timing column can be compressed to 20 minutes or stretched to 30 (see [Timing variants](#timing-variants)). Each slide lists **what to show** and **what to say**.

**Evidence rule for the talk.** Show only Final Project evidence. Lab and Demo 3 live results are not Final Project results and are not shown as such.

**Live evidence available today (Personal DEV, 2026-10-12):**
- step 1 deploy;
- step 2 platform run `896549012431425`: 40 events → Silver 40 → 13 actionable priorities; 40 trips; 48 weather hours with 4 matched trips; CDC `batch_001` 5 inserts; all reconciled;
- step 3 strict idempotency run `412521206243476`: 19/19, with no data change, no rewrites and the CDC checkpoint unchanged.

Anything later (CDC updates and deletes, Lakeflow, the dashboard on live data, the assistant, PROD) is shown offline, and the talk says so unless it has run by then.

## Run of show

| # | Time | Slide | Labs |
|---|---|---|---|
| 1 | 0:00–1:30 | The problem: an empty station and a van sent to a broken one | — |
| 2 | 1:30–3:30 | One lakehouse, two questions (architecture) | All |
| 3 | 3:30–6:00 | Ingestion that stops by itself | 1, 2, 3 |
| 4 | 6:00–8:30 | Trust: Silver, quarantine and reconciliation | 4, 7 |
| 5 | 8:30–10:30 | Gold KPIs and the dashboard | 5, 6 |
| 6 | 10:30–13:30 | Maintenance CDC: only what changed, reconciled before the checkpoint | 10 |
| 7 | 13:30–17:00 | The operations assistant: grounded, cited, measured | 12 |
| 8 | 17:00–18:00 | Optional push ingestion with Zerobus, isolated | 11 |
| 9 | 18:00–19:30 | DEV to PROD: CI, approvals, plan gates | 8, 9 |
| 10 | 19:30–20:30 | Cost, safety and what is honestly not done yet | — |
| 11 | 20:30–25:00 | Questions | — |

## Slides

### 1. The problem (1.5 min)

**Show:** a station with 0 bikes next to one marked out of service. **Say:** "Dispatchers need two answers. Where should the van go next, and where must it not go? When something breaks, they also need the procedure, quickly. UrbanFlow 2.0 answers both from one governed lakehouse."

### 2. Architecture (2 min)

**Show:** the overall diagram from the README. **Say:**

- "One bundle and two Jobs: the platform Job and the assistant Job. One schema per target. Everything is serverless and bounded."
- "DEV is my personal workspace. PROD is an Academy Azure workspace. They are different clouds and different metastores, so a DEV mistake cannot touch PROD data."

### 3. Ingestion (2.5 min)

**Show:** the `land_sources` task and `landing.py`. **Say:**

- "The station snapshot lands once in a Unity Catalog Volume. Auto Loader picks it up with `availableNow`, processes the backlog, and stops. The checkpoint makes the next run incremental."
- "On Azure, the same Bronze can come from Event Hubs over Kafka."

**Point at:** `land_once` refuses to overwrite a landed file with different content.

### 4. Trust (2.5 min)

**Show:** the three-way split and the reconciliation identity. **Say:** "Every Bronze row ends up as Silver, Quarantine or Duplicate, and the counts must add up exactly. A rejected row is a finding with a named rule; a vanished row is a bug." Mention Great Expectations on local Spark, and the final validation task that gates the Job.

### 5. Gold and dashboard (2 min)

**Show:** the generated dashboard definition (or the published dashboard after a live run). **Say:**

- "The priority score is severity plus deficit plus size, so you can recompute it by hand."
- "Each page states its limits. The snapshot page is not a trend. The weather page shows association, not causation."
- "Lakeflow re-implements the station path declaratively in its own schema, with expectations."

### 6. Maintenance CDC (3 min)

**Show:** `cdc_sql.py` and the three snapshot batches. **Say:**

- "Maintenance tickets live in an operational system. A read-only Federation query or a fixture snapshot is MERGEd into a table with Change Data Feed. Unchanged rows aren't rewritten, so CDF holds only real changes, deletes included."
- "We read exactly the versions after the checkpoint, keep every event in an audit history, and update Silver."
- "The checkpoint moves only after reconciliation passes: values compared both ways at the processed version. So a crash mid-run replays safely."

**Demo:** `pytest tests/test_cdc_pipeline.py -q`. Live: in step 3 the identical `batch_001` replay printed `source_merged: false`, and the checkpoint stayed at 2 (evidence `2026-10-12_personal_dev_step3_retry_pass.md`). If step 4 has run, show `batch_002` (1 insert, 2 updates, 1 delete).

**Payoff:** the dashboard table *Rebalancing priorities at stations with an open maintenance ticket*.

### 7. The assistant (3.5 min)

**Show:** the eight procedures and one cited answer. **Say:**

- "The corpus is a governed Delta table with Change Data Feed, and an AI Search Delta Sync index follows it. Retrieval is hybrid: vectors plus keywords."
- "The prompt allows only the numbered excerpts, requires citations, and refuses when the procedures don't cover the question."

**Then the lesson:** "The Lab 12 design scoped excerpts by the first word of each title. That was right for runbooks named after systems. For our topic documents it dropped the very document that answered, so scoping is now opt-in. On the offline fixture, recall went from 0.625 to 0.938."

**Demo:** `urbanflow-rag ask "What should an operator do when both bikes and docks are critically low?"` (expected: INSPECT STATION, cited to the rebalancing procedure) and `urbanflow-rag evaluate` (offline), or notebook 22 live after approval. **Say clearly:** "Offline numbers test our pipeline. Only the live baseline-versus-RAG run measures the model."

### 8. Zerobus (1 min)

**Say:** "Optional push ingestion of synthetic station-health readings. It delivers at least once. Silver keeps the earliest delivery per event ID, and running the MERGE twice proves idempotency. It is isolated: the code refuses Lab 11's schemas, tables and credentials."

### 9. Release (1.5 min)

**Show:** the CD workflow graph. **Say:**

- "CI runs lint, 700+ tests, a dashboard freshness check, and offline bundle schema validation."
- "CD is manual: a typed confirmation, then a protected approval."
- "The deploy plan may contain only our seven resources, never a delete. After the deploy, the plan must be all `skip`. The optional sample run is validated by a script, not by eye."

### 10. Cost, safety, honesty (1 min)

**Say:**

- "No schedules, every task bounded, every billable switch off by default. The biggest cost risk is an AI Search endpoint left running, so it exists only for an approved evaluation window."
- "What's not done yet: the live runs that need approval, and the PROD identity, which needs Academy admin." Point to the evidence table in [RELEASE_AND_VALIDATION.md](RELEASE_AND_VALIDATION.md).

## Live demo checklist (before the talk)

- [ ] `pytest -q` is green on the presented commit; CI is green on the PR.
- [ ] `python scripts/build_dashboard.py --check` passes.
- [ ] `python scripts/run_zerobus_health.py --target personal_dev --run-id demo-day` prints a DRY_RUN plan.
- [ ] `python scripts/run_governance_demo.py --target personal_dev` prints a DRY_RUN plan.
- [ ] If a live run was approved: its run IDs are in the evidence table and the dashboard is published. If the AI Search endpoint is still up for the demo, delete it right after the talk (with approval).

## Expected questions

| Question | Short answer |
|---|---|
| Is this real CDC? | It is Delta Change Data Feed on a Final-Project-owned table, fed by snapshots, not log-based CDC of the source database. The source is only read. |
| Why fixtures instead of a real maintenance system? | No approved external database exists for the project. The Federation path is implemented and switched on by an approved foreign catalog; the fixtures make the same logic testable. |
| What if the Job fails halfway? | Each task is idempotent and execution-scoped. CDC replays the same version range. The checkpoint never moves past an unreconciled version. |
| How do you know the assistant isn't making things up? | Citations per sentence, a refusal rule, unanswerable questions in the evaluation set, and a no-retrieval baseline on the same model. |
| Why is PROD a trial workspace? | It is what the Academy provides. It is labelled PROD-style; the separation from DEV (different cloud and metastore) is real. |
| What does it cost? | See [COST_AND_PERFORMANCE.md](COST_AND_PERFORMANCE.md). It has bounds in units and measured figures from system tables after a live run. |

## Timing variants

- **20 minutes:** cut slide 8 (one line on slide 2) and shorten slide 4 to 1.5 min. Take questions from 18:00.
- **30 minutes:** extend slide 7 with a live baseline-vs-RAG comparison of two questions (2 min). Walk the CDC batch 002 replay on slide 6 (2 min). Allow 6 minutes for questions.

## Five-minute interview demo

For a technical interview rather than Demo Day: one problem, one architecture, three proofs.

| Time | Show | Say |
|---|---|---|
| 0:00–0:45 | README architecture diagram | "A governed lakehouse that tells dispatchers where to send a van and where not to, plus an assistant over the operating procedures. One bundle, DEV on AWS, PROD-style on an Azure trial, serverless only." |
| 0:45–2:00 | `evidence/2026-10-12_personal_dev_step2_pass.md` | "The first live run failed on three defects that offline tests could not see: a count copied from an earlier project, a missing task dependency, and an input file nobody landed. I fixed each in code with a regression test and recorded every failed run. The passing run reconciles 40 events through Silver to 13 actionable priorities, matching an independent calculation." |
| 2:00–3:15 | `evidence/2026-10-12_personal_dev_step3_retry_pass.md` | "Then I proved replays are safe, with 19 strict criteria. The first attempt failed one: a Delta MERGE that changes nothing still commits, so the CDC checkpoint moved over an empty version. I added a compare-first guard. Now a replay changes no data and no timestamps, rewrites no files, and leaves the checkpoint alone. Some shared tables still write a metadata-only commit, and I say so." |
| 3:15–4:15 | `src/urbanflow/cdc_sql.py` + `tests/test_cdc_pipeline.py` | "CDC is Delta Change Data Feed over snapshots, not log-based CDC: a version range, an append-once history, reconciliation, then a forward-only checkpoint. The tests include the recovery case: pending versions are processed even when the snapshot already matches." |
| 4:15–5:00 | `docs/IMPLEMENTATION_PROGRESS.md` | "What is not done yet, and why: CDC updates and deletes, Lakeflow, the assistant and PROD each wait for an explicit, bounded approval, and nothing billable is left running." |


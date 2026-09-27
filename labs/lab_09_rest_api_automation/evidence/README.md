# LAB 09 - Evidence index

**Start with `LIVE_VALIDATION_SUMMARY.md`** -- a sanitized, GitHub-suitable
write-up of everything proven live. **Sections 1-8 of that document** cover
a confirmed-safe, non-Azure-PROD Personal Databricks workspace: Phase 0 and
the classic-compute limitation, why the original pipeline was replaced, the
`timestampNtz` table-feature fix, the staged targeted validations, and --
its headline result -- **a single `run-all` command succeeding completely
end to end**, with all four output tables populated and the reconciliation
invariant confirmed against real, five-month data.

**Section 9 of that document is the deliberate exception to that scope**:
a supplementary classic-compute demonstration performed against Azure PROD
itself (2026-09-27), under a separate, specific, one-off owner
authorization, including an automated create-and-terminate cycle, a
**manual cluster restart performed by the project's operator** (not this
project's code), and a fully automated Jobs API test against the
resulting running cluster. See `CLASSIC_CLUSTER_REQUIREMENT_REVIEW.md`
section 9 for the full account, including exactly how this deviates from
this project's own standing "never Azure PROD" convention (see the main
README's "Security model").

This repository's local `~/.databrickscfg` has profiles literally named
`dev` / `AZURE_DEV` that resolve to the same host as Lab 8's Azure PROD
target, not a separate dev workspace -- this project's code deliberately
refuses to guess a profile for exactly that reason (see the main
README's "Security model"). A separate, dedicated profile pointing at a
confirmed-safe, genuinely different Personal workspace was used to gather
every piece of live evidence in this directory; its name is deliberately
not published here.

## What's here

- `LIVE_VALIDATION_SUMMARY.md` -- the sanitized summary; read this first.
- `phase0_2026-09-24.json` -- Phase 0 / `preflight --probe-cluster-create`
  evidence, including the classic-compute limitation's exact backend error.
  **This file is sanitized** (see its own `sanitization_note` field) --
  an earlier commit on this branch published it with the real workspace
  hostname, authenticated identity, organization ID, and internal
  cluster-policy IDs, since corrected in place. That original content
  remains in this branch's earlier Git history; see the main README's
  "Security model" for the remediation status.
- `lab09_report.json` -- the JSON report `run-all` itself generates (per
  `config/dev.yml`'s `report.output_path`), overwritten by each `run-all`
  invocation. **Not committed** (gitignored) -- it belongs here only
  conceptually; do not add it to Git.

Dated pipeline-update and reconciliation-Job evidence files from targeted
live validations (not `run-all`) also exist locally in this directory --
exact update/run IDs, event-log outcomes, table states, and notebook
output. **These contain this project's actual workspace hostname,
authenticated identity, and internal resource IDs and are deliberately
kept local-only, never committed** -- only the sanitized
`LIVE_VALIDATION_SUMMARY.md` (and, now, the sanitized `phase0_2026-09-24.json`)
are suitable for that. Do not commit tokens, `.databrickscfg` contents,
raw Terraform/plan state, or any of these detailed local-only evidence
files.

## Current status

**A successful, complete end-to-end `run-all` execution now exists**
(2026-09-26): one `python -m lab09.cli run-all --compute-mode
serverless_job` command, run exactly once, landed a new month, drove the
Lakeflow pipeline update to `COMPLETED` (zero errors, zero Databricks
auto-retries), and ran the reconciliation Job to `SUCCESS` -- all in one
continuous invocation, reusing the existing pipeline and existing Job
(neither recreated). See `LIVE_VALIDATION_SUMMARY.md` section 8 for the
full sanitized detail.

An earlier *failed* `run-all` attempt's report also still exists
(predating the `timestampNtz` fix; it failed at the pipeline step) and
remains preserved locally, not deleted or overwritten without a backup --
both outcomes are kept for an accurate history. Before it, every
*successful* live result was obtained from a separate, targeted, single,
explicitly authorized API call (one pipeline update, one Job run) made
directly through this project's own helper functions, not `run-all`
itself (Databricks' own platform-level retry behavior on the earlier
failed pipeline attempts is documented separately in
`LIVE_VALIDATION_SUMMARY.md` and was never something this project's code
triggered or controlled).

**Still not proven on this Personal workspace:** the literal "create
clusters" portion of the Lab 9 task requirement -- this workspace has no
classic-compute worker environment, and no classic cluster has been
created here at any point, in any validation. See the main README's "Lab
requirement vs. Personal workspace reality" for the two open compliance
interpretations this leaves.

**Separately demonstrated on Azure PROD (2026-09-27):** a first attempt
achieved classic cluster create and terminate automatically, but reaching
`RUNNING` needed a manual restart before Job attachment and execution
could be exercised -- see `LIVE_VALIDATION_SUMMARY.md` section 9 and
`CLASSIC_CLUSTER_REQUIREMENT_REVIEW.md` section 9. **A second attempt the
same day closed that gap**: create, wait-for-`RUNNING`, Job attachment,
verified output (`OK:42`), terminate, and independently confirmed cleanup
all succeeded in one uninterrupted automated script invocation, with no
manual restart needed for any leg of the sequence itself -- see
`LIVE_VALIDATION_SUMMARY.md` section 10 and
`CLASSIC_CLUSTER_REQUIREMENT_REVIEW.md` section 10 (including a separate,
later, out-of-band manual restart by the operator, after the automated
result was already complete and verified, immediately caught and cleaned
up). This index does not claim the "create clusters" requirement is
cleanly satisfied by these demonstrations alone: they ran outside the
non-production scope this project otherwise holds to, under explicit,
one-off authorization, and a reviewer/mentor decision is still needed on
whether an Azure PROD demonstration is the right way to satisfy the
requirement at all.

**GitHub Actions live workflow, first successful run (2026-09-27):** the
actual `workflow_dispatch` -> `run-live-automation` CI/CD path (back on
the Personal workspace) was dispatched and completed successfully for the
first time -- previously every live result above was obtained via a
locally-run CLI invocation, never through this repository's own GitHub
Actions live-automation job. Landed `2024-06`, reconciliation invariant
held exactly. Also found and fixed along the way: the job had referenced
a `lab09-live-approval` environment that was never actually created,
which would have skipped the required-reviewer gate entirely on first
dispatch -- now points at the existing, already-protected
`personal-prod-approval` environment instead. See
`LIVE_VALIDATION_SUMMARY.md` section 11.

**Permanent Azure Job, first fully successful run, all three tasks
(2026-09-27):** `lab09_taxi_reconciliation_job` (ingestion -> Lakeflow
pipeline -> reconciliation) succeeded end to end via
`lab09_azure_deployment.yml`'s own GitHub Actions `workflow_dispatch` path,
after finding and fixing three genuine live defects in sequence and
validating each with a full test pass before the next attempt (a
cross-identity workspace-path issue, a `notebookPath()` prefix quirk, and
two pipeline source files each falling back to a hardcoded, wrong-schema
volume path). A dedicated Azure identity could not be created (a
tenant-policy restriction on Entra ID app registration, confirmed via a
direct Microsoft Graph query); Databricks' own "Run As" feature was used
instead, keeping the existing shared GitHub Actions identity's permissions
unchanged. See `LIVE_VALIDATION_SUMMARY.md` section 13 for the full
account, actual reconciliation numbers, and independently confirmed
compute termination. An earlier, never-triggered placeholder Job and its
associated supervisor-review Workspace folders -- superseded by this
permanent Job -- were deleted afterward, per the project owner's
instruction; nothing in them was unique to that folder.

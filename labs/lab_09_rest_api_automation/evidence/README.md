# LAB 09 - Evidence index

**Start with `LIVE_VALIDATION_SUMMARY.md`** -- a sanitized, GitHub-suitable
write-up of everything proven live against a confirmed-safe, non-Azure-PROD
Personal Databricks workspace: Phase 0 and the classic-compute limitation,
why the original pipeline was replaced, the `timestampNtz` table-feature
fix, the staged targeted validations, and -- its headline result -- **a
single `run-all` command succeeding completely end to end**, with all four
output tables populated and the reconciliation invariant confirmed against
real, five-month data. It also states plainly what remains *not* proven --
the literal classic cluster-create requirement.

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

**Still not proven:** the literal "create clusters" portion of the Lab 9
task requirement -- this workspace has no classic-compute worker
environment, and no classic cluster has been created here at any point,
in any validation. See the main README's "Lab requirement vs. Personal
workspace reality" for the two open compliance interpretations this
leaves; this index does not claim that requirement is satisfied.

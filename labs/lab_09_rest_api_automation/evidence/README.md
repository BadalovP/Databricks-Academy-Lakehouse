# LAB 09 - Evidence index

This directory holds evidence from actual `run-all` executions: the
generated JSON report (`lab09_report.json` by default, per
`config/dev.yml`'s `report.output_path`) and any screenshots a supervisor
demo captures.

**As of this PR, no live end-to-end execution has been performed.** See the
main `README.md`'s "Phase 0 / preflight" and "Known limitations" sections
for exactly why: the local `~/.databrickscfg` profiles literally named
`dev` / `AZURE_DEV` resolve to the same host as `AZURE_PROD`
(`adb-7405604503619901.1.azuredatabricks.net`), so this implementation
deliberately refused to guess a profile and run live mutations against an
ambiguous target. The only host confirmed to be a genuinely different,
non-Azure-PROD workspace is the `personal-yahoo` profile
(`dbc-1750318a-76a9.cloud.databricks.com`, matching Lab 8's "Personal"
workspace), but that has not been explicitly confirmed as the intended
Lab 9 target for a live run either.

When a live `run-all` (or a Phase 0 `--probe-cluster-create` run) is
performed against a confirmed-safe DEV/academy workspace, its generated
report and any accompanying screenshots belong here, named with the date
and run type, e.g.:

```
lab09_preflight_2026-MM-DD.json
lab09_run_all_2026-MM-DD.json
```

Do not commit tokens, `.databrickscfg` contents, or raw Terraform/plan
state alongside evidence files.

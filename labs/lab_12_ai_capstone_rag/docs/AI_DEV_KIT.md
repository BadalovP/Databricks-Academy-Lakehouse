# Databricks AI Dev Kit (Agent Skills)

Status on 2026-10-11: **done.** Installed for Claude Code in this worktree and invoked natively
(`databricks-core`, `databricks-vector-search`) by a Claude Code session started in the worktree for
the Stage 5B repeat synchronization ([evidence](../evidence/2026-10-11_personal_dev_stage5b_repeat_sync.md)).

## What the AI Dev Kit is now

The [AI Dev Kit repository](https://github.com/databricks-solutions/ai-dev-kit) delegates
Databricks skills to the [Databricks Agent Skills repository](https://github.com/databricks/databricks-agent-skills).
The supported installer is the Databricks CLI:

```powershell
databricks aitools list                     # what is installed, per scope
databricks aitools install --path <dir>     # preview: write skill files only, no agent changes
databricks aitools install --agents claude-code --scope project --skills-only
```

By default `install` installs a plugin "through each agent's own CLI"; `--skills-only` writes
plain skill files instead, and `--scope project` keeps them inside the current project.

## Installations on this machine

| | Codex CLI (found, not made in this project) | Claude Code (this worktree) |
|---|---|---|
| When | `aitools` reports skills v0.2.10, last updated 2026-08-30 | 2026-10-11 |
| Scope | global: skills in `~/.databricks/aitools/skills`, Codex plugin at user scope | project only |
| Command | not recorded | `databricks aitools install --agents claude-code --scope project --skills-only` |
| Files | outside the repository | 29 skills (290 files) in `.databricks/aitools/skills/` and 29 symlinks in `.claude/skills/`, both at the worktree root |
| In Git | no | no - the root `.gitignore` ignores `.claude/` and `.databricks/` |

Earlier versions of this page said no installer had been run; the global Codex installation shows
that was wrong for this machine. There is no record that Codex used those skills for Lab 12.

### Safety checks before and after the Claude Code install

- Preview with `--path` into a temporary folder: skill markdown, `agents/openai.yaml` metadata,
  icons and 11 example `.py` files; **no hooks, settings files or MCP server definitions**.
- A before/after snapshot of 9,192 files in `~/.claude`, `~/.databricks`, `~/.codex`, `~/.agents`,
  the original worktree's `.claude` and this worktree: **290 files added, all under this
  worktree's `.databricks/aitools/skills/`; nothing modified or removed elsewhere.**
- `--skills-only` was chosen over the default plugin install so no marketplace or plugin is
  registered at user level.

### Skills available (v0.2.10)

Relevant to Lab 12: `databricks-core` (CLI, auth, profiles, bundles - the entry point),
`databricks-vector-search`, `databricks-dabs`, `databricks-jobs`, `databricks-unity-catalog`,
`databricks-model-serving`, `databricks-mlflow-evaluation`. The other 22 cover agent-bricks,
ai-functions, aibi-dashboards, app-design, apps, apps-python, data-discovery, dbsql, docs,
execution-compute, iceberg, lakebase, lakeflow-connect, metric-views, ml-training, pipelines,
python-sdk, serverless-migration, spark-structured-streaming, synthetic-data-gen,
unstructured-pdf-generation and zerobus-ingest.

## How the skills were used

Claude Code discovers project skills when a session starts in the project folder. The session that
installed them had started in the original repository, so `Skill(databricks-core)` returned
`Unknown skill` - **the skills could not be invoked natively there.**

For Stage 5A that session therefore **read the installed skill files directly** and followed
them; it did not claim native skill invocation:

| Skill file | Guidance applied |
|---|---|
| `databricks-core/SKILL.md` | never auto-select a profile; pass `--profile` explicitly (`personal_lab10`, named by the user) |
| `databricks-vector-search/SKILL.md` | CLI health checks `get-endpoint`, `list-indexes`, `get-index`; manual sync of a TRIGGERED index with `vector-search-indexes sync-index INDEX_NAME`, progress via `get-index` |
| `databricks-vector-search/references/troubleshooting-and-operations.md` | endpoint states, index fields (`status.ready`, `indexed_row_count`, `pipeline_id`), TRIGGERED vs CONTINUOUS cost behavior |

Evidence: [Stage 5A](../evidence/2026-10-11_personal_dev_stage5a_quality_and_skills.md).

## Native use (Stage 5B)

A Claude Code session started in the worktree loaded `databricks-core` and
`databricks-vector-search` through the Skill tool and used them for the preflight, the single
`sync-index` call and the post-sync checks. The Stage 5A session above, which could only read the
skill files, does not count toward this criterion; Stage 5B does.

Skills guide the agent; they grant no permissions. Every live action still needs an explicitly
named profile, least privilege and the user's approval.

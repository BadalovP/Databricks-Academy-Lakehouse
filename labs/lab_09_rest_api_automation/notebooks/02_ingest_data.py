# Databricks notebook source
# MAGIC %md
# MAGIC # LAB 09 - Ingestion task (Azure deployment)
# MAGIC
# MAGIC Task 1 of `lab09_taxi_reconciliation_job`'s three-task pipeline
# MAGIC (ingestion -> Lakeflow pipeline -> reconciliation -- see
# MAGIC `scripts/deploy_azure_job.py` for the Job's full task graph).
# MAGIC
# MAGIC Reuses this project's actual `lab09.landing` module -- the same
# MAGIC function the Personal-workspace deployment's `lab09.cli run-all`
# MAGIC calls -- rather than a separate reimplementation, by importing this
# MAGIC project's real `src/lab09` source, uploaded alongside this notebook
# MAGIC by `scripts/deploy_azure_job.py` (see that script's module docstring
# MAGIC for why a plain source upload was chosen over building a wheel).
# MAGIC
# MAGIC **Deliberate architectural difference from the Personal-workspace
# MAGIC deployment, stated plainly rather than glossed over:** `landing.py`'s
# MAGIC own module docstring documents that its calling convention was
# MAGIC designed so downloading happens outside Databricks (the GitHub
# MAGIC runner or a local machine), specifically so "the Databricks
# MAGIC workspace itself never needs outbound internet access." This
# MAGIC notebook does the opposite deliberately, because the assignment
# MAGIC calls for ingestion to be a real task inside the Job's own task
# MAGIC graph, inspectable in the Azure Jobs UI -- which means this cluster
# MAGIC must actually reach the public NYC TLC data URL directly. Whether
# MAGIC this Azure workspace's Job-cluster compute has outbound internet
# MAGIC access at all was not known before this was written; the first live
# MAGIC validation run is what actually tests it, and a failure here (e.g. a
# MAGIC network/egress error) is a genuine, informative result to report
# MAGIC honestly, not a defect in this notebook's logic.

# COMMAND ----------

import json
import sys

from databricks.sdk import WorkspaceClient

# Locate this project's own root by asking Databricks for this notebook's
# own path, rather than hardcoding an identity-specific /Workspace/Users/...
# path -- this notebook works unmodified regardless of whose home directory
# it was deployed under (this workspace is shared by 100+ students, each
# with their own).
#
# notebookPath() is documented, historically, to return a path WITHOUT the
# "/Workspace" prefix (e.g. "/Users/<id>/..." rather than
# "/Workspace/Users/<id>/..."), even though every other API this project
# uses (workspace.list/get_status/import_) requires and returns the
# "/Workspace"-prefixed form. Confirmed live (2026-09-27) that this
# mismatch is the actual cause of a ModuleNotFoundError even after the
# uploaded source was verified, independently, to exist at the exact path
# this cell computes -- the resulting sys.path entry simply pointed at a
# path that does not exist. Normalized defensively here rather than
# assumed, and the resolved paths are printed so any future failure is
# diagnosable directly from this cell's own output, without needing
# another live run just to see what path was actually computed.
notebook_path = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
if not notebook_path.startswith("/Workspace"):
    notebook_path = "/Workspace" + notebook_path
project_root = "/".join(notebook_path.split("/")[:-2])
print(f"notebook_path={notebook_path!r} project_root={project_root!r}")
sys.path.insert(0, f"{project_root}/src")

from lab09 import landing
from lab09.client import load_config

# COMMAND ----------

client = WorkspaceClient()
cfg = load_config(f"{project_root}/config/azure.yml")

# COMMAND ----------

result = landing.land_next_month(client, cfg)
landing.ensure_reference_csv(client, cfg)

payload = {
    "status": result.status,
    "month": result.month,
    "month_landed": result.month_landed,
    "file_bytes": result.file_bytes,
    "volume_path": result.volume_path,
}
print(json.dumps(payload, indent=2))
dbutils.notebook.exit(json.dumps(payload))

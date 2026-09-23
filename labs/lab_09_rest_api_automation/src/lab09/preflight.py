"""Phase 0 / preflight capability checks for LAB 09.

`clusters.list()` alone does NOT prove cluster-create permission -- it only
proves the identity can see existing clusters. The only reliable way to
confirm create permission is to actually create a tiny cluster, wait until
it is usable, and immediately terminate it. That probe is opt-in
(`probe_cluster_create=True`) because it provisions real (if tiny, minimal,
autoterminating) compute, and the caller is responsible for having already
verified the resolved host/identity is the intended DEV/academy workspace.
"""

from __future__ import annotations

import io
import logging
import uuid
from dataclasses import dataclass, field
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError

from . import compute, monitoring
from .client import volume_root_path

logger = logging.getLogger(__name__)


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str = ""


@dataclass
class PreflightReport:
    identity: str | None = None
    checks: list[CheckResult] = field(default_factory=list)
    cluster_create_tested: bool = False
    cluster_create_supported: bool | None = None
    ci_identity_caveat: str = (
        "Phase 0 was executed under the identity resolved from the profile "
        "passed to this run. If GitHub Actions CI authenticates as a "
        "different identity/service principal than this run used, this "
        "report does NOT prove CI has the same permissions -- it only "
        "proves the identity used here does. See README.md 'Known "
        "limitations'."
    )

    @property
    def passed(self) -> bool:
        return all(c.passed for c in self.checks)

    def add(self, name: str, passed: bool, detail: str = "") -> None:
        self.checks.append(CheckResult(name=name, passed=passed, detail=detail))

    def as_dict(self) -> dict[str, Any]:
        return {
            "identity": self.identity,
            "passed": self.passed,
            "checks": [
                {"name": c.name, "passed": c.passed, "detail": c.detail} for c in self.checks
            ],
            "cluster_create_tested": self.cluster_create_tested,
            "cluster_create_supported": self.cluster_create_supported,
            "ci_identity_caveat": self.ci_identity_caveat,
        }


def _run_check(report: PreflightReport, name: str, fn: Any) -> None:
    try:
        detail = fn()
        report.add(name, True, detail or "ok")
    except Exception as exc:  # noqa: BLE001 - one bad check must not abort the rest
        report.add(name, False, f"{type(exc).__name__}: {exc}")


def run_preflight(
    client: WorkspaceClient,
    cfg: dict[str, Any],
    probe_cluster_create: bool = False,
) -> PreflightReport:
    report = PreflightReport()

    def check_identity() -> str:
        me = client.current_user.me()
        report.identity = me.user_name
        return me.user_name

    _run_check(report, "authenticated_identity", check_identity)

    _run_check(
        report,
        "spark_runtimes_listed",
        lambda: f"{len(client.clusters.spark_versions().versions or [])} versions",
    )
    _run_check(
        report,
        "node_types_listed",
        lambda: f"{len(client.clusters.list_node_types().node_types or [])} node types",
    )
    _run_check(
        report,
        "cluster_policies_listed",
        lambda: f"{len(list(client.cluster_policies.list()))} policies",
    )
    _run_check(report, "catalog_access", lambda: client.catalogs.get(cfg["catalog"]).name)
    _run_check(
        report,
        "schema_access",
        lambda: client.schemas.get(f"{cfg['catalog']}.{cfg['schema']}").name,
    )

    def check_volume() -> str:
        full_name = f"{cfg['catalog']}.{cfg['schema']}.{cfg['volume']}"
        try:
            vol = client.volumes.read(full_name)
            return f"exists: {vol.full_name}"
        except DatabricksError as exc:
            if "NOT_FOUND" in str(getattr(exc, "error_code", "")) or "NOT_FOUND" in str(exc):
                return "does not exist yet (will be created by ensure_volume)"
            raise

    _run_check(report, "volume_access", check_volume)

    def check_files_roundtrip() -> str:
        full_name = f"{cfg['catalog']}.{cfg['schema']}.{cfg['volume']}"
        try:
            client.volumes.read(full_name)
        except DatabricksError:
            return (
                "skipped: volume does not exist yet, round trip deferred until after ensure_volume"
            )

        root = volume_root_path(cfg)
        probe_path = f"{root}/_preflight/probe_{uuid.uuid4().hex}.txt"
        client.files.upload(probe_path, io.BytesIO(b"lab09-preflight-probe"), overwrite=True)
        try:
            listed = list(client.files.list_directory_contents(f"{root}/_preflight"))
            found = any(getattr(entry, "path", None) == probe_path for entry in listed)
            if not found:
                raise RuntimeError(
                    "Uploaded probe file was not visible in list_directory_contents."
                )
        finally:
            client.files.delete(probe_path)
        return "upload/list/delete round trip ok"

    _run_check(report, "files_api_roundtrip", check_files_roundtrip)
    _run_check(
        report,
        "pipeline_list_permission",
        lambda: f"{len(list(client.pipelines.list_pipelines()))} pipelines visible",
    )

    if probe_cluster_create:
        _run_cluster_create_probe(client, cfg, report)
    else:
        report.add(
            "cluster_create_probe",
            True,
            "skipped (not requested this run) -- explicit cluster-create capability "
            "remains unverified; architecture falls back to "
            "jobs.submit(new_cluster=...) if it turns out to be forbidden",
        )

    return report


def _run_cluster_create_probe(
    client: WorkspaceClient, cfg: dict[str, Any], report: PreflightReport
) -> None:
    report.cluster_create_tested = True
    cluster_id = None
    try:
        spec = compute.build_cluster_spec(client, cfg)
        cluster_id = compute.start_cluster_create(
            client, spec, cluster_name="lab09-preflight-probe"
        )
        monitoring_cfg = cfg.get("monitoring", {})
        outcome = monitoring.poll_cluster_state(
            client,
            cluster_id,
            timeout_seconds=monitoring_cfg.get("cluster_timeout_seconds", 900),
            poll_interval_seconds=monitoring_cfg.get("cluster_poll_interval_seconds", 10),
        )
        report.cluster_create_supported = outcome.usable
        report.add(
            "cluster_create_probe",
            outcome.usable,
            f"cluster {cluster_id} reached state={outcome.state} timed_out={outcome.timed_out}",
        )
    except Exception as exc:  # noqa: BLE001
        report.cluster_create_supported = False
        report.add("cluster_create_probe", False, f"{type(exc).__name__}: {exc}")
    finally:
        if cluster_id:
            try:
                compute.terminate_cluster(client, cluster_id)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Failed to terminate preflight probe cluster %s: %s", cluster_id, exc
                )

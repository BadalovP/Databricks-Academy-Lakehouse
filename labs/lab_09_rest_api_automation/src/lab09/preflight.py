"""Phase 0 / preflight capability checks for LAB 09.

`clusters.list()` alone does NOT prove cluster-create permission -- it only
proves the identity can see existing clusters. The only reliable way to
confirm create permission is to actually create a tiny cluster, wait until
it is usable, and immediately terminate it. That probe is opt-in
(`probe_cluster_create=True`) because it provisions real (if tiny, minimal,
autoterminating) compute, and the caller is responsible for having already
verified the resolved host/identity is the intended DEV/academy workspace.

Every check reports one of three states, never just a boolean:

  PASS        the capability was actually exercised and succeeded
  FAIL        the capability was actually exercised and failed
  NOT_TESTED  the capability was NOT exercised this run (e.g. the Files API
              round trip when the Lab 9 volume does not exist yet, or the
              cluster-create probe when it was not requested)

A NOT_TESTED result is never reported as PASS -- an unexecuted capability
must never be described as proven. `PreflightReport.passed` (used to gate
`run-all` and the `preflight` CLI's exit code) only requires that no check
actually FAILed; a NOT_TESTED check does not block it, since some checks
are deliberately deferred (see each check's own comment below), but every
NOT_TESTED check remains visible and distinguishable in the report.
"""

from __future__ import annotations

import io
import logging
import uuid
from dataclasses import dataclass, field
from typing import Any, Literal

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

from . import compute, monitoring
from .client import volume_root_path

logger = logging.getLogger(__name__)

CheckStatus = Literal["PASS", "FAIL", "NOT_TESTED"]


@dataclass
class CheckResult:
    name: str
    status: CheckStatus
    detail: str = ""

    @property
    def passed(self) -> bool:
        """True only for a check that was actually executed and succeeded."""
        return self.status == "PASS"


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
        """No check actually FAILed.

        A NOT_TESTED check never blocks this (cluster_create_probe is
        deliberately NOT_TESTED unless explicitly requested;
        files_api_roundtrip is deliberately NOT_TESTED when the Lab 9
        volume does not exist yet, which is normal before the first
        ensure_volume() call) -- but neither is ever counted as PASS, and
        both remain individually visible in `checks`.
        """
        return all(c.status != "FAIL" for c in self.checks)

    def add(self, name: str, status: CheckStatus, detail: str = "") -> None:
        self.checks.append(CheckResult(name=name, status=status, detail=detail))

    def as_dict(self) -> dict[str, Any]:
        return {
            "identity": self.identity,
            "passed": self.passed,
            "checks": [
                {"name": c.name, "status": c.status, "detail": c.detail} for c in self.checks
            ],
            "cluster_create_tested": self.cluster_create_tested,
            "cluster_create_supported": self.cluster_create_supported,
            "ci_identity_caveat": self.ci_identity_caveat,
        }


def _run_check(report: PreflightReport, name: str, fn: Any) -> None:
    """Run a check that always genuinely executes: PASS on success, FAIL on any exception."""
    try:
        detail = fn()
        report.add(name, "PASS", detail or "ok")
    except Exception as exc:  # noqa: BLE001 - one bad check must not abort the rest
        report.add(name, "FAIL", f"{type(exc).__name__}: {exc}")


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
        # A genuine "not found" answer is still a fully executed,
        # successful check of volume *access* (the identity can reach the
        # schema and get an authoritative answer) -- it is PASS, not
        # NOT_TESTED. Only NotFound/ResourceDoesNotExist (the typed SDK
        # exceptions, not a fragile string match) are treated as "missing";
        # anything else (permission, auth, service errors) propagates and
        # fails this check for real.
        full_name = f"{cfg['catalog']}.{cfg['schema']}.{cfg['volume']}"
        try:
            vol = client.volumes.read(full_name)
            return f"exists: {vol.full_name}"
        except NotFound:
            return "does not exist yet (will be created by ensure_volume)"

    _run_check(report, "volume_access", check_volume)
    _check_files_roundtrip(client, cfg, report)

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
            "NOT_TESTED",
            "not requested this run -- explicit cluster-create capability remains "
            "unverified; architecture falls back to a job-managed new_cluster "
            "(compute.try_start_cluster_create) if it turns out to be forbidden",
        )

    return report


def _check_files_roundtrip(
    client: WorkspaceClient, cfg: dict[str, Any], report: PreflightReport
) -> None:
    """Upload/list/delete a tiny probe file -- but only when it can genuinely be tested.

    If the Lab 9 volume does not exist yet (normal before the first
    ensure_volume() call, e.g. run-all's own preflight step runs before
    step 2), this is reported as NOT_TESTED, never as PASS: the capability
    was not actually exercised.
    """
    full_name = f"{cfg['catalog']}.{cfg['schema']}.{cfg['volume']}"
    try:
        client.volumes.read(full_name)
    except NotFound:
        report.add(
            "files_api_roundtrip",
            "NOT_TESTED",
            "Lab 9 volume does not exist yet, so the Files API upload/list/delete "
            "round trip was not exercised. This is expected before ensure_volume() "
            "runs and must not be read as a passed check.",
        )
        return
    except Exception as exc:  # noqa: BLE001
        report.add("files_api_roundtrip", "FAIL", f"{type(exc).__name__}: {exc}")
        return

    root = volume_root_path(cfg)
    preflight_dir = f"{root}/_preflight"
    probe_path = f"{preflight_dir}/probe_{uuid.uuid4().hex}.txt"
    try:
        client.files.create_directory(preflight_dir)
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
    except Exception as exc:  # noqa: BLE001
        report.add("files_api_roundtrip", "FAIL", f"{type(exc).__name__}: {exc}")
        return

    report.add("files_api_roundtrip", "PASS", "upload/list/delete round trip ok")


def _run_cluster_create_probe(
    client: WorkspaceClient, cfg: dict[str, Any], report: PreflightReport
) -> None:
    """The only reliable cluster-create capability check: create -> poll -> terminate.

    Never weakened to a cheaper check like clusters.list_node_types() --
    that only proves read access, not create permission.
    """
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
            "PASS" if outcome.usable else "FAIL",
            f"cluster {cluster_id} reached state={outcome.state} timed_out={outcome.timed_out}",
        )
    except Exception as exc:  # noqa: BLE001
        report.cluster_create_supported = False
        report.add("cluster_create_probe", "FAIL", f"{type(exc).__name__}: {exc}")
    finally:
        if cluster_id:
            try:
                compute.terminate_cluster(client, cluster_id)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Failed to terminate preflight probe cluster %s: %s", cluster_id, exc
                )

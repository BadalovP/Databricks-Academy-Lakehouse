"""Explicit polling for Databricks job runs, pipeline updates, and clusters.

LAB 09 deliberately does not use SDK `.result()` waiter helpers: monitoring
must be visible (explicit timeout, sleep interval, and state-change logging)
rather than hidden inside a blocking call.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Callable

logger = logging.getLogger(__name__)

# Job run life-cycle states (Databricks Jobs API).
JOB_TERMINAL_LIFECYCLE_STATES = {"TERMINATED", "INTERNAL_ERROR", "SKIPPED"}
JOB_NON_TERMINAL_LIFECYCLE_STATES = {
    "QUEUED",
    "PENDING",
    "RUNNING",
    "TERMINATING",
    "BLOCKED",
    "WAITING_FOR_RETRY",
}
# Job run result states, only meaningful once life_cycle_state == TERMINATED.
JOB_RESULT_STATES = {"SUCCESS", "FAILED", "TIMEDOUT", "CANCELED"}

# Lakeflow pipeline update states.
PIPELINE_NON_TERMINAL_STATES = {
    "QUEUED",
    "WAITING_FOR_RESOURCES",
    "INITIALIZING",
    "SETTING_UP_TABLES",
    "RUNNING",
}
PIPELINE_TERMINAL_STATES = {"COMPLETED", "FAILED", "CANCELED"}

# Cluster states relevant to "usable for work" vs. "gone".
CLUSTER_RUNNING_STATE = "RUNNING"
CLUSTER_BAD_TERMINAL_STATES = {"TERMINATED", "ERROR", "UNKNOWN"}

CLUSTER_TERMINATED_STATE = "TERMINATED"
# ERROR/UNKNOWN observed while waiting for termination are never treated as
# "must be gone by now" -- they are reported for a human to investigate.
CLUSTER_ANOMALOUS_FOR_TERMINATION_STATES = {"ERROR", "UNKNOWN"}


def _state_name(value: Any) -> str:
    """Normalize an SDK enum member (or plain string/None) to its bare name."""
    if value is None:
        return ""
    return getattr(value, "value", value) or ""


@dataclass
class JobRunOutcome:
    run_id: int
    life_cycle_state: str
    result_state: str | None
    state_message: str | None = None
    timed_out: bool = False

    @property
    def succeeded(self) -> bool:
        return (
            not self.timed_out
            and self.life_cycle_state == "TERMINATED"
            and self.result_state == "SUCCESS"
        )


@dataclass
class PipelineOutcome:
    pipeline_id: str
    update_id: str
    state: str
    timed_out: bool = False

    @property
    def succeeded(self) -> bool:
        return not self.timed_out and self.state == "COMPLETED"


@dataclass
class ClusterOutcome:
    cluster_id: str
    state: str
    timed_out: bool = False

    @property
    def usable(self) -> bool:
        return not self.timed_out and self.state == CLUSTER_RUNNING_STATE


@dataclass
class ClusterTerminationOutcome:
    cluster_id: str
    state: str | None
    confirmed: bool = False
    error: str | None = None

    @property
    def requires_investigation(self) -> bool:
        """True whenever TERMINATED was not positively confirmed.

        A termination *request* being accepted is never, by itself, treated
        as evidence of TERMINATED -- only this poll's own observed final
        state is. Covers an anomalous state (ERROR/UNKNOWN), a timeout, and
        a clusters.get() call raising while polling -- none of those are
        ever silently treated as "must be terminated by now".
        """
        return not self.confirmed


def poll_job_run(
    client: Any,
    run_id: int,
    timeout_seconds: int,
    poll_interval_seconds: int,
    sleep_fn: Callable[[float], None] = time.sleep,
    clock_fn: Callable[[], float] = time.monotonic,
) -> JobRunOutcome:
    """Poll a job run until it reaches a terminal life-cycle state or times out."""
    deadline = clock_fn() + timeout_seconds
    last_logged: tuple[str, str | None] | None = None

    while True:
        run = client.jobs.get_run(run_id=run_id)
        state = run.state
        life_cycle = _state_name(getattr(state, "life_cycle_state", None))
        result_state_raw = getattr(state, "result_state", None)
        result_state = _state_name(result_state_raw) if result_state_raw else None
        message = getattr(state, "state_message", None)

        current = (life_cycle, result_state)
        if current != last_logged:
            logger.info(
                "Job run %s: life_cycle_state=%s result_state=%s", run_id, life_cycle, result_state
            )
            last_logged = current

        if life_cycle in JOB_TERMINAL_LIFECYCLE_STATES:
            return JobRunOutcome(
                run_id=run_id,
                life_cycle_state=life_cycle,
                result_state=result_state,
                state_message=message,
            )

        if clock_fn() >= deadline:
            logger.warning("Job run %s timed out after %ss", run_id, timeout_seconds)
            return JobRunOutcome(
                run_id=run_id,
                life_cycle_state=life_cycle,
                result_state=result_state,
                state_message=message or "Polling timed out before a terminal state was reached.",
                timed_out=True,
            )

        sleep_fn(poll_interval_seconds)


def poll_pipeline_update(
    client: Any,
    pipeline_id: str,
    update_id: str,
    timeout_seconds: int,
    poll_interval_seconds: int,
    sleep_fn: Callable[[float], None] = time.sleep,
    clock_fn: Callable[[], float] = time.monotonic,
) -> PipelineOutcome:
    """Poll a Lakeflow pipeline update until it reaches a terminal state or times out."""
    deadline = clock_fn() + timeout_seconds
    last_logged: str | None = None

    while True:
        response = client.pipelines.get_update(pipeline_id=pipeline_id, update_id=update_id)
        update = getattr(response, "update", response)
        state = _state_name(getattr(update, "state", None))

        if state != last_logged:
            logger.info("Pipeline %s update %s: state=%s", pipeline_id, update_id, state)
            last_logged = state

        if state in PIPELINE_TERMINAL_STATES:
            return PipelineOutcome(pipeline_id=pipeline_id, update_id=update_id, state=state)

        if clock_fn() >= deadline:
            logger.warning(
                "Pipeline %s update %s timed out after %ss", pipeline_id, update_id, timeout_seconds
            )
            return PipelineOutcome(
                pipeline_id=pipeline_id, update_id=update_id, state=state, timed_out=True
            )

        sleep_fn(poll_interval_seconds)


def poll_cluster_state(
    client: Any,
    cluster_id: str,
    timeout_seconds: int,
    poll_interval_seconds: int,
    sleep_fn: Callable[[float], None] = time.sleep,
    clock_fn: Callable[[], float] = time.monotonic,
) -> ClusterOutcome:
    """Poll a cluster until it is RUNNING, reaches a bad terminal state, or times out."""
    deadline = clock_fn() + timeout_seconds
    last_logged: str | None = None

    while True:
        details = client.clusters.get(cluster_id=cluster_id)
        state = _state_name(getattr(details, "state", None))

        if state != last_logged:
            logger.info("Cluster %s: state=%s", cluster_id, state)
            last_logged = state

        if state == CLUSTER_RUNNING_STATE or state in CLUSTER_BAD_TERMINAL_STATES:
            return ClusterOutcome(cluster_id=cluster_id, state=state)

        if clock_fn() >= deadline:
            logger.warning("Cluster %s timed out after %ss", cluster_id, timeout_seconds)
            return ClusterOutcome(cluster_id=cluster_id, state=state, timed_out=True)

        sleep_fn(poll_interval_seconds)


def poll_cluster_termination(
    client: Any,
    cluster_id: str,
    timeout_seconds: int,
    poll_interval_seconds: int,
    sleep_fn: Callable[[float], None] = time.sleep,
    clock_fn: Callable[[], float] = time.monotonic,
) -> ClusterTerminationOutcome:
    """Independently confirm that a cluster actually reaches TERMINATED.

    Deliberately NOT poll_cluster_state() reused for this purpose:
    poll_cluster_state() stops as soon as it observes RUNNING (it exists to
    wait for a cluster to *start*), so calling it right after requesting
    deletion could return immediately on a RUNNING read that happens before
    the deletion has taken effect, or during a brief transitional window --
    which would be mistaken for "polling finished" when termination was
    never actually confirmed. This loop instead:

      - keeps polling, never stopping, while the observed state is RUNNING
        or TERMINATING;
      - stops and reports confirmed=True only on an exact TERMINATED;
      - stops and reports confirmed=False (state recorded) on ERROR/UNKNOWN
        -- an anomalous state is never assumed to mean "gone";
      - stops and reports confirmed=False on a bounded timeout;
      - stops and reports confirmed=False, with the exception recorded, if
        clusters.get() itself raises while polling -- an API error is never
        assumed to mean the cluster is already gone.

    This exact logic was proven live (2026-09-27) confirming termination of
    a real Azure classic-compute cluster after `clusters.delete()`; it did
    not exist in this package's own compute.py/cli.py beforehand, only in a
    one-off validation script, which is the gap this brings back in.
    """
    deadline = clock_fn() + timeout_seconds
    last_logged: str | None = None

    while True:
        try:
            details = client.clusters.get(cluster_id=cluster_id)
            state = _state_name(getattr(details, "state", None))
        except Exception as exc:  # noqa: BLE001 - an API error is never "already gone"
            logger.warning(
                "Cluster %s: clusters.get() raised while polling for termination: %s",
                cluster_id,
                exc,
            )
            return ClusterTerminationOutcome(
                cluster_id=cluster_id, state=last_logged, confirmed=False, error=str(exc)
            )

        if state != last_logged:
            logger.info("Cluster %s termination check: state=%s", cluster_id, state)
            last_logged = state

        if state == CLUSTER_TERMINATED_STATE:
            return ClusterTerminationOutcome(cluster_id=cluster_id, state=state, confirmed=True)

        if state in CLUSTER_ANOMALOUS_FOR_TERMINATION_STATES:
            logger.warning(
                "Cluster %s: observed anomalous state=%s while polling for termination",
                cluster_id,
                state,
            )
            return ClusterTerminationOutcome(cluster_id=cluster_id, state=state, confirmed=False)

        if clock_fn() >= deadline:
            logger.warning(
                "Cluster %s termination not confirmed after %ss (last state=%s)",
                cluster_id,
                timeout_seconds,
                state,
            )
            return ClusterTerminationOutcome(cluster_id=cluster_id, state=state, confirmed=False)

        sleep_fn(poll_interval_seconds)

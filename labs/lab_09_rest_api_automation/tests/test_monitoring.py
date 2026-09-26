from unittest.mock import MagicMock

from lab09 import monitoring


def _fake_clock(start: float = 0.0):
    state = {"t": start}

    def clock():
        return state["t"]

    def sleep(seconds):
        state["t"] += seconds

    return clock, sleep


def _run_with_states(states: list[tuple[str, str | None]]):
    """states: list of (life_cycle_state, result_state) in poll order."""
    client = MagicMock()
    responses = []
    for life_cycle, result in states:
        run = MagicMock()
        run.state.life_cycle_state = MagicMock(value=life_cycle)
        run.state.result_state = MagicMock(value=result) if result else None
        run.state.state_message = None
        responses.append(run)
    client.jobs.get_run.side_effect = responses
    return client


# --- job run polling ----------------------------------------------------


def test_poll_job_run_successful_transitions_to_success():
    client = _run_with_states(
        [
            ("QUEUED", None),
            ("PENDING", None),
            ("RUNNING", None),
            ("TERMINATED", "SUCCESS"),
        ]
    )
    clock, sleep = _fake_clock()

    outcome = monitoring.poll_job_run(
        client,
        run_id=1,
        timeout_seconds=1000,
        poll_interval_seconds=5,
        sleep_fn=sleep,
        clock_fn=clock,
    )

    assert outcome.succeeded is True
    assert outcome.life_cycle_state == "TERMINATED"
    assert outcome.result_state == "SUCCESS"
    assert outcome.timed_out is False


def test_poll_job_run_failed_job_is_not_succeeded():
    client = _run_with_states([("RUNNING", None), ("TERMINATED", "FAILED")])
    clock, sleep = _fake_clock()

    outcome = monitoring.poll_job_run(
        client,
        run_id=1,
        timeout_seconds=1000,
        poll_interval_seconds=5,
        sleep_fn=sleep,
        clock_fn=clock,
    )

    assert outcome.succeeded is False
    assert outcome.result_state == "FAILED"


def test_poll_job_run_handles_queued_blocked_and_waiting_for_retry():
    client = _run_with_states(
        [
            ("QUEUED", None),
            ("BLOCKED", None),
            ("WAITING_FOR_RETRY", None),
            ("RUNNING", None),
            ("TERMINATED", "SUCCESS"),
        ]
    )
    clock, sleep = _fake_clock()

    outcome = monitoring.poll_job_run(
        client,
        run_id=1,
        timeout_seconds=1000,
        poll_interval_seconds=1,
        sleep_fn=sleep,
        clock_fn=clock,
    )

    assert outcome.succeeded is True
    assert client.jobs.get_run.call_count == 5


def test_poll_job_run_treats_internal_error_and_skipped_as_terminal():
    client = _run_with_states([("INTERNAL_ERROR", None)])
    clock, sleep = _fake_clock()
    outcome = monitoring.poll_job_run(
        client,
        run_id=1,
        timeout_seconds=1000,
        poll_interval_seconds=1,
        sleep_fn=sleep,
        clock_fn=clock,
    )
    assert outcome.life_cycle_state == "INTERNAL_ERROR"
    assert outcome.succeeded is False

    client2 = _run_with_states([("SKIPPED", None)])
    outcome2 = monitoring.poll_job_run(
        client2,
        run_id=1,
        timeout_seconds=1000,
        poll_interval_seconds=1,
        sleep_fn=sleep,
        clock_fn=clock,
    )
    assert outcome2.life_cycle_state == "SKIPPED"
    assert outcome2.succeeded is False


def test_poll_job_run_times_out_when_never_terminal():
    client = MagicMock()
    run = MagicMock()
    run.state.life_cycle_state = MagicMock(value="RUNNING")
    run.state.result_state = None
    run.state.state_message = None
    client.jobs.get_run.return_value = run

    clock, sleep = _fake_clock()
    outcome = monitoring.poll_job_run(
        client,
        run_id=1,
        timeout_seconds=30,
        poll_interval_seconds=10,
        sleep_fn=sleep,
        clock_fn=clock,
    )

    assert outcome.timed_out is True
    assert outcome.succeeded is False


# --- pipeline update polling ----------------------------------------------------


def _pipeline_client_with_states(states: list[str]):
    client = MagicMock()
    responses = []
    for state in states:
        resp = MagicMock()
        resp.update.state = MagicMock(value=state)
        responses.append(resp)
    client.pipelines.get_update.side_effect = responses
    return client


def test_poll_pipeline_update_completes_successfully():
    client = _pipeline_client_with_states(
        [
            "QUEUED",
            "WAITING_FOR_RESOURCES",
            "INITIALIZING",
            "SETTING_UP_TABLES",
            "RUNNING",
            "COMPLETED",
        ]
    )
    clock, sleep = _fake_clock()

    outcome = monitoring.poll_pipeline_update(
        client,
        pipeline_id="p1",
        update_id="u1",
        timeout_seconds=1000,
        poll_interval_seconds=5,
        sleep_fn=sleep,
        clock_fn=clock,
    )

    assert outcome.succeeded is True
    assert outcome.state == "COMPLETED"


def test_poll_pipeline_update_failed_is_not_succeeded():
    client = _pipeline_client_with_states(["RUNNING", "FAILED"])
    clock, sleep = _fake_clock()

    outcome = monitoring.poll_pipeline_update(
        client,
        pipeline_id="p1",
        update_id="u1",
        timeout_seconds=1000,
        poll_interval_seconds=5,
        sleep_fn=sleep,
        clock_fn=clock,
    )

    assert outcome.succeeded is False
    assert outcome.state == "FAILED"


def test_poll_pipeline_update_canceled_is_not_succeeded():
    client = _pipeline_client_with_states(["RUNNING", "CANCELED"])
    clock, sleep = _fake_clock()

    outcome = monitoring.poll_pipeline_update(
        client,
        pipeline_id="p1",
        update_id="u1",
        timeout_seconds=1000,
        poll_interval_seconds=5,
        sleep_fn=sleep,
        clock_fn=clock,
    )

    assert outcome.succeeded is False
    assert outcome.state == "CANCELED"


def test_poll_pipeline_update_times_out():
    client = MagicMock()
    resp = MagicMock()
    resp.update.state = MagicMock(value="RUNNING")
    client.pipelines.get_update.return_value = resp

    clock, sleep = _fake_clock()
    outcome = monitoring.poll_pipeline_update(
        client,
        pipeline_id="p1",
        update_id="u1",
        timeout_seconds=20,
        poll_interval_seconds=10,
        sleep_fn=sleep,
        clock_fn=clock,
    )

    assert outcome.timed_out is True
    assert outcome.succeeded is False


# --- cluster polling ----------------------------------------------------


def test_poll_cluster_state_reaches_running():
    client = MagicMock()
    pending = MagicMock()
    pending.state = MagicMock(value="PENDING")
    running = MagicMock()
    running.state = MagicMock(value="RUNNING")
    client.clusters.get.side_effect = [pending, running]

    clock, sleep = _fake_clock()
    outcome = monitoring.poll_cluster_state(
        client,
        cluster_id="c1",
        timeout_seconds=1000,
        poll_interval_seconds=5,
        sleep_fn=sleep,
        clock_fn=clock,
    )

    assert outcome.usable is True
    assert outcome.state == "RUNNING"


def test_poll_cluster_state_stops_on_error_state():
    client = MagicMock()
    errored = MagicMock()
    errored.state = MagicMock(value="ERROR")
    client.clusters.get.return_value = errored

    clock, sleep = _fake_clock()
    outcome = monitoring.poll_cluster_state(
        client,
        cluster_id="c1",
        timeout_seconds=1000,
        poll_interval_seconds=5,
        sleep_fn=sleep,
        clock_fn=clock,
    )

    assert outcome.usable is False
    assert outcome.state == "ERROR"


def test_poll_cluster_state_times_out_while_pending():
    client = MagicMock()
    pending = MagicMock()
    pending.state = MagicMock(value="PENDING")
    client.clusters.get.return_value = pending

    clock, sleep = _fake_clock()
    outcome = monitoring.poll_cluster_state(
        client,
        cluster_id="c1",
        timeout_seconds=15,
        poll_interval_seconds=10,
        sleep_fn=sleep,
        clock_fn=clock,
    )

    assert outcome.timed_out is True
    assert outcome.usable is False

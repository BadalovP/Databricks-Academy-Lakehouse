from datetime import UTC, datetime

import pytest

from urbanflow.zerobus import (
    canonical_deliveries,
    generate_health_events,
    replay_plan,
)


def test_health_generation_is_stable_and_clearly_synthetic() -> None:
    kwargs = {
        "run_id": "offline-001",
        "station_ids": ["station-a", "station-b"],
        "readings_per_station": 2,
        "base_time": datetime(2026, 10, 12, tzinfo=UTC),
    }
    first = generate_health_events(**kwargs)
    second = generate_health_events(**kwargs)
    assert first == second
    assert len(first) == 4
    assert all(event.record(batch_id="b1", produced_at_micros=1)["synthetic"] for event in first)


def test_replay_has_same_ids_and_silver_is_idempotent() -> None:
    events = generate_health_events(run_id="offline-002", station_ids=["station-a"])
    replay = replay_plan(events, 2)
    primary_rows = [event.record(batch_id="primary", produced_at_micros=10) for event in events]
    replay_rows = [event.record(batch_id="replay", produced_at_micros=20) for event in replay]
    silver = canonical_deliveries([*primary_rows, *replay_rows])
    assert set(silver) == {event.event_id for event in events}
    assert sum(row["delivery_count"] for row in silver.values()) == len(primary_rows) + len(
        replay_rows
    )


def test_replay_count_cannot_exceed_event_count() -> None:
    with pytest.raises(ValueError):
        replay_plan([], 1)


# --- isolation from Lab 11, delivery and Silver SQL -----------------------------------------

import json  # noqa: E402
import subprocess  # noqa: E402
import sys  # noqa: E402
from pathlib import Path  # noqa: E402

from urbanflow import zerobus_sql  # noqa: E402
from urbanflow.zerobus import (  # noqa: E402
    MAX_RECORDS_PER_RUN,
    IsolationError,
    assert_final_project_target,
    build_batches,
    deliver,
    final_project_credentials,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize(
    "catalog, schema, table",
    [
        ("workspace", "lab_11_zerobus", "zerobus_station_health_bronze"),
        ("dbr_dev", "parvinbadalov_lab11_zerobus_trial2", "zerobus_station_health_bronze"),
        ("workspace", "urbanflow_final_dev", "bronze_iot_events"),
        ("workspace", "urbanflow_dev", "zerobus_station_health_bronze"),  # Demo 3's schema
        ("workspace", "urbanflow_final_dev", "station_status"),
    ],
)
def test_lab11_and_foreign_targets_are_refused(catalog, schema, table) -> None:
    with pytest.raises(IsolationError):
        assert_final_project_target(catalog, schema, table)


def test_final_project_targets_are_accepted() -> None:
    assert (
        assert_final_project_target("dbr_dev", "parvinbadalov_urbanflow_final_prod")
        == "dbr_dev.parvinbadalov_urbanflow_final_prod.zerobus_station_health_bronze"
    )


def test_credentials_come_only_from_the_final_project_variables() -> None:
    with pytest.raises(IsolationError, match="URBANFLOW_FINAL_ZEROBUS_CLIENT_ID"):
        final_project_credentials({"ZEROBUS_CLIENT_ID": "a", "ZEROBUS_CLIENT_SECRET": "b"})
    reused = {
        "URBANFLOW_FINAL_ZEROBUS_CLIENT_ID": "lab11-app",
        "URBANFLOW_FINAL_ZEROBUS_CLIENT_SECRET": "own",
        "ZEROBUS_CLIENT_ID": "lab11-app",
    }
    with pytest.raises(IsolationError, match="Lab 11"):
        final_project_credentials(reused)
    own = {"URBANFLOW_FINAL_ZEROBUS_CLIENT_ID": "fp", "URBANFLOW_FINAL_ZEROBUS_CLIENT_SECRET": "s"}
    assert final_project_credentials(own) == ("fp", "s")


def _clock():
    ticks = iter(range(1000, 10_000))
    return lambda: next(ticks)


def test_batches_replay_original_ids_and_stay_within_the_run_cap() -> None:
    events = generate_health_events(run_id="fp-001", station_ids=["a", "b"], readings_per_station=3)
    batches = build_batches(events, replay_count=2, batch_size=4, clock=_clock())
    assert [(b.batch_id, b.delivery, len(b.records)) for b in batches] == [
        ("fp-001-p01", "primary", 4),
        ("fp-001-p02", "primary", 2),
        ("fp-001-replay", "replay", 2),
    ]
    original = {row["event_id"] for b in batches[:2] for row in b.records}
    assert {row["event_id"] for row in batches[2].records} <= original
    too_many = generate_health_events(
        run_id="fp-002", station_ids=[f"s{i}" for i in range(10)], readings_per_station=3
    )
    with pytest.raises(ValueError, match=str(MAX_RECORDS_PER_RUN)):
        build_batches(too_many, replay_count=1, batch_size=10, clock=_clock())


class _Stream:
    def __init__(self, fail_on: int | None = None) -> None:
        self.sent: list[int] = []
        self.waited: list[int] = []
        self.closed = False
        self.fail_on = fail_on

    def ingest_records_offset(self, records):
        if self.fail_on is not None and len(self.sent) == self.fail_on:
            raise RuntimeError("transient")
        self.sent.append(len(records))
        return len(self.sent) - 1

    def wait_for_offset(self, offset):
        self.waited.append(offset)

    def close(self):
        self.closed = True


def test_each_batch_is_acknowledged_before_the_next_and_the_stream_always_closes() -> None:
    events = generate_health_events(run_id="fp-003", station_ids=["a"], readings_per_station=3)
    batches = build_batches(events, replay_count=1, batch_size=2, clock=_clock())
    stream = _Stream()
    acknowledged = deliver(lambda: stream, batches)
    assert stream.sent == [2, 1, 1] and stream.waited == [0, 1, 2] and stream.closed
    assert [a["delivery"] for a in acknowledged] == ["primary", "primary", "replay"]
    failing = _Stream(fail_on=1)
    with pytest.raises(RuntimeError):
        deliver(lambda: failing, batches)
    assert failing.closed


def test_silver_sql_is_idempotent_non_destructive_and_run_scoped() -> None:
    tables = zerobus_sql.HealthTables("workspace", "urbanflow_final_dev")
    statements = [*zerobus_sql.create_tables(tables), zerobus_sql.merge_silver(tables)]
    statements.append(zerobus_sql.validation(tables, run_id="fp-001"))
    for statement in statements:
        for forbidden in ("DROP ", "TRUNCATE", "CREATE OR REPLACE", "DELETE", "OVERWRITE"):
            assert forbidden not in statement.upper()
    merge = zerobus_sql.merge_silver(tables)
    assert "ORDER BY produced_at_micros, batch_id) = 1" in merge
    assert "WHEN MATCHED AND NOT (" in merge and "t.delivery_count <=> s.delivery_count" in merge
    assert "synthetic = true" in merge
    with pytest.raises(ValueError):
        zerobus_sql.validation(tables, run_id="x' OR '1'='1")


def test_bronze_columns_match_the_producer_record_exactly() -> None:
    record = generate_health_events(run_id="fp", station_ids=["a"])[0].record(
        batch_id="b", produced_at_micros=1
    )
    ddl = zerobus_sql.create_tables(zerobus_sql.HealthTables("w", "urbanflow_final_dev"))[0]
    body = ddl[ddl.index("(") + 1 : ddl.index(") TBLPROPERTIES")]
    columns = [part.split()[0] for part in body.replace("\n", " ").split(",")]
    assert columns == list(record)


def test_validation_problems_detect_duplicates_and_missing_deliveries() -> None:
    good = {"bronze_rows": 16, "distinct_events": 12, "silver_rows": 12,
            "duplicate_silver_keys": 0, "silver_delivery_total": 16}  # fmt: skip
    assert zerobus_sql.validation_problems(good, expected_events=12) == []
    assert zerobus_sql.validation_problems({**good, "silver_rows": 13})
    assert zerobus_sql.validation_problems({**good, "silver_delivery_total": 12})
    assert zerobus_sql.validation_problems(good, expected_events=11)


def test_the_producer_script_defaults_to_a_dry_run() -> None:
    result = subprocess.run(
        [sys.executable, str(PROJECT_ROOT / "scripts" / "run_zerobus_health.py"),
         "--target", "personal_dev", "--run-id", "fp-health-001"],
        capture_output=True, text=True, check=True, env={"PATH": ""},
    )  # fmt: skip
    output = json.loads(result.stdout)
    assert output["mode"] == "DRY_RUN" and output["unique_events"] == 12
    assert output["table"] == "workspace.urbanflow_final_dev.zerobus_station_health_bronze"


def test_zerobus_stays_optional_and_never_references_lab11_objects() -> None:
    job = (PROJECT_ROOT / "resources" / "urbanflow_final.job.yml").read_text(encoding="utf-8")
    assert "12_zerobus_station_health" not in job
    for path in [*PROJECT_ROOT.glob("src/urbanflow/zerobus*.py"),
                 PROJECT_ROOT / "scripts" / "run_zerobus_health.py",
                 PROJECT_ROOT / "notebooks" / "12_zerobus_station_health.py"]:  # fmt: skip
        text = path.read_text(encoding="utf-8")
        for forbidden in ("lab_11_zerobus", "parvinbadalov_lab11", "sp-databricks-adls",
                          "kvpl24databricks2", "d1a2118a"):  # fmt: skip
            assert forbidden not in text, (path.name, forbidden)

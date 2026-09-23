import json

from lab09 import reporting


def test_write_report_produces_valid_json_with_all_required_fields(tmp_path):
    report = reporting.Report(
        status="SUCCESS",
        month="2024-02",
        month_landed=True,
        file_bytes=12345,
        volume_path="/Volumes/dbr_dev/parvinbadalov/lab09_landing/trips/yellow_tripdata_2024-02.parquet",
        pipeline_id="pipeline-1",
        update_id="update-1",
        cluster_id="cluster-1",
        job_id=42,
        run_id=7,
        bronze_rows=100,
        silver_valid_rows=90,
        rejected_rows=10,
        gold_rows=5,
        failed_rules={"INVALID_FARE": 6, "INVALID_DISTANCE": 4},
        reconciliation_passed=True,
        cluster_cleaned_up=True,
    )
    report.mark_started()
    report.mark_finished()

    out_path = tmp_path / "report.json"
    reporting.write_report(report, out_path)

    assert out_path.exists()
    payload = json.loads(out_path.read_text(encoding="utf-8"))

    for field in reporting.REQUIRED_FIELDS:
        assert field in payload

    assert payload["status"] == "SUCCESS"
    assert payload["bronze_rows"] == 100
    assert payload["silver_valid_rows"] == 90
    assert payload["rejected_rows"] == 10
    assert payload["failed_rules"] == {"INVALID_FARE": 6, "INVALID_DISTANCE": 4}
    assert payload["reconciliation_passed"] is True
    assert isinstance(payload["duration_seconds"], float)


def test_write_report_creates_parent_directories(tmp_path):
    report = reporting.Report(status="SUCCESS", reconciliation_passed=True)
    nested = tmp_path / "a" / "b" / "c" / "report.json"

    reporting.write_report(report, nested)

    assert nested.exists()


def test_no_new_data_report_has_month_landed_false():
    report = reporting.Report(
        status="NO_NEW_DATA", month=None, month_landed=False, reconciliation_passed=True
    )
    assert report.month_landed is False
    assert reporting.exit_code_for(report) == 0


def test_write_report_accepts_null_cluster_id_for_serverless_mode(tmp_path):
    """A null cluster_id is expected and valid for compute_mode=serverless_job
    -- it must never be treated as a missing/invalid field.
    """
    report = reporting.Report(
        status="SUCCESS",
        compute_mode="serverless_job",
        cluster_id=None,
        cluster_cleaned_up=True,
        classic_cluster_supported=False,
        classic_cluster_failure_reason="organization has no associated worker environments",
        reconciliation_passed=True,
    )
    report.mark_started()
    report.mark_finished()

    out_path = tmp_path / "report.json"
    reporting.write_report(report, out_path)

    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert payload["compute_mode"] == "serverless_job"
    assert payload["cluster_id"] is None
    assert payload["cluster_cleaned_up"] is True
    assert payload["classic_cluster_supported"] is False
    assert (
        payload["classic_cluster_failure_reason"]
        == "organization has no associated worker environments"
    )
    assert reporting.exit_code_for(report) == 0


# --- exit code determination -------------------------------------------------


def test_exit_code_is_zero_for_successful_run():
    report = reporting.Report(status="SUCCESS", reconciliation_passed=True)
    assert reporting.exit_code_for(report) == 0


def test_exit_code_is_zero_for_no_new_data_with_passed_reconciliation():
    report = reporting.Report(status="NO_NEW_DATA", reconciliation_passed=True)
    assert reporting.exit_code_for(report) == 0


def test_exit_code_is_nonzero_for_failed_status():
    report = reporting.Report(status="FAILED", reconciliation_passed=None)
    assert reporting.exit_code_for(report) != 0


def test_exit_code_is_nonzero_when_reconciliation_failed_even_if_status_success():
    report = reporting.Report(status="SUCCESS", reconciliation_passed=False)
    assert reporting.exit_code_for(report) != 0


def test_exit_code_is_nonzero_when_reconciliation_never_ran():
    report = reporting.Report(status="SUCCESS", reconciliation_passed=None)
    assert reporting.exit_code_for(report) != 0

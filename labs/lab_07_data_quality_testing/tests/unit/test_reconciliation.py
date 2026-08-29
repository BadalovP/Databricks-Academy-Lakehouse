from lab07.reconciliation import anomaly_status, percent_change, reconcile_bronze, reconcile_gold


def test_reconciliation():
    assert reconcile_bronze(100, 95, 5)["passed"] and reconcile_gold(95, 95)["passed"]


def test_volume_thresholds():
    assert percent_change(40, 100) == -60.0 and anomaly_status(-60, 25, 50) == ("FAIL", "BLOCK")

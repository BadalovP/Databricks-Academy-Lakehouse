from lab07.reconciliation import reconcile_bronze,reconcile_gold,percent_change,anomaly_status
def test_reconciliation(): assert reconcile_bronze(100,95,5)['passed'] and reconcile_gold(95,95)['passed']
def test_volume_thresholds(): assert percent_change(40,100)==-60.0 and anomaly_status(-60,25,50)==('FAIL','BLOCK')

from lab07.reconciliation import anomaly_status
def test_warn_vs_block(): assert anomaly_status(30,25,50)==('WARN','WARN') and anomaly_status(60,25,50)==('FAIL','BLOCK')

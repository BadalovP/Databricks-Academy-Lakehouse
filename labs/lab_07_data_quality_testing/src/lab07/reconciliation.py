def reconcile_bronze(bronze_count, validated_count, quarantine_count, tolerance_rows=0):
    target = validated_count + quarantine_count
    delta = bronze_count - target
    return {
        "check_name": "bronze_equals_validated_plus_quarantine",
        "source_count": bronze_count,
        "target_count": target,
        "delta": delta,
        "passed": abs(delta) <= tolerance_rows,
    }


def reconcile_gold(validated_count, gold_trusted_count, tolerance_rows=0):
    delta = validated_count - gold_trusted_count
    return {
        "check_name": "validated_equals_gold_trusted_count",
        "source_count": validated_count,
        "target_count": gold_trusted_count,
        "delta": delta,
        "passed": abs(delta) <= tolerance_rows,
    }


def percent_change(current, baseline):
    return (
        0.0
        if baseline == 0 and current == 0
        else (100.0 if baseline == 0 else round((current - baseline) / baseline * 100, 4))
    )


def anomaly_status(change_pct, warn_threshold_pct, block_threshold_pct):
    x = abs(change_pct)
    return (
        ("FAIL", "BLOCK")
        if x > block_threshold_pct
        else (("WARN", "WARN") if x > warn_threshold_pct else ("PASS", "WARN"))
    )

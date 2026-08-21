from datetime import date
from decimal import Decimal

from src.gold_transformations import build_daily_encounters


def _fact_rows():
    return [
        {
            "date_key": 20260821,
            "encounter_date": date(2026, 8, 21),
            "patient_key": 1,
            "organization_key": 10,
            "provider_key": 100,
            "duration_minutes": 30.0,
            "base_encounter_cost": Decimal("50.00"),
            "total_claim_cost": Decimal("100.00"),
            "payer_coverage": Decimal("70.00"),
            "patient_responsibility": Decimal("30.00"),
            "encounter_class": "emergency",
        },
        {
            "date_key": 20260821,
            "encounter_date": date(2026, 8, 21),
            "patient_key": 2,
            "organization_key": 10,
            "provider_key": 101,
            "duration_minutes": 60.0,
            "base_encounter_cost": Decimal("60.00"),
            "total_claim_cost": Decimal("200.00"),
            "payer_coverage": Decimal("150.00"),
            "patient_responsibility": Decimal("50.00"),
            "encounter_class": "outpatient",
        },
    ]


def test_daily_aggregation_counts_and_emergency_pct(spark):
    result = build_daily_encounters(
        spark.createDataFrame(_fact_rows())
    ).first()

    assert result["encounter_count"] == 2
    assert result["unique_patients"] == 2
    assert result["organizations_active"] == 1
    assert result["providers_active"] == 2
    assert result["emergency_encounters"] == 1
    assert result["emergency_encounter_pct"] == 50.0


def test_daily_aggregation_reconciles_financials(spark):
    result = build_daily_encounters(
        spark.createDataFrame(_fact_rows())
    ).first()

    assert result["total_claim_cost"] == Decimal("300.00")
    assert result["payer_coverage"] == Decimal("220.00")
    assert result["patient_responsibility"] == Decimal("80.00")
    assert result["avg_duration_minutes"] == 45.0

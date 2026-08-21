from decimal import Decimal

from src.gold_transformations import prepare_encounters


def _source_row():
    return {
        "Id": "enc-1",
        "START": "2026-08-21T10:00:00Z",
        "STOP": "2026-08-21T11:30:00Z",
        "PATIENT": "patient-1",
        "ORGANIZATION": "org-1",
        "PROVIDER": "provider-1",
        "PAYER": "payer-1",
        "ENCOUNTERCLASS": "emergency",
        "CODE": "123",
        "DESCRIPTION": "Test encounter",
        "BASE_ENCOUNTER_COST": "100.00",
        "TOTAL_CLAIM_COST": "160.00",
        "PAYER_COVERAGE": "100.00",
        "REASONCODE": "R1",
        "REASONDESCRIPTION": "Test reason",
    }


def test_prepare_encounters_preserves_grain(spark):
    result = prepare_encounters(
        spark.createDataFrame([_source_row()])
    )

    assert result.count() == 1
    assert result.first()["encounter_id"] == "enc-1"


def test_prepare_encounters_derives_duration(spark):
    row = prepare_encounters(
        spark.createDataFrame([_source_row()])
    ).first()

    assert row["duration_minutes"] == 90.0


def test_prepare_encounters_derives_patient_responsibility(spark):
    row = prepare_encounters(
        spark.createDataFrame([_source_row()])
    ).first()

    assert row["total_claim_cost"] == Decimal("160.00")
    assert row["payer_coverage"] == Decimal("100.00")
    assert row["patient_responsibility"] == Decimal("60.00")

from pyspark.pipelines.testing import TestPipeline

test_pipeline = TestPipeline.active()

CATALOG = "dbr_dev"
SCHEMA = "parvinbadalov"

LANDING = f"{CATALOG}.{SCHEMA}.business_license_landing"
VALIDATED = f"{CATALOG}.{SCHEMA}.business_license_validated"


def mock_business_licenses(test_spark):
    """Create isolated landing data for expectation behavior."""
    test_spark.sql(
        f"""
        CREATE TABLE {LANDING} AS
        SELECT * FROM VALUES
            (
                'GOOD_001',
                'GOOD COMPANY LLC',
                'GOOD COMPANY',
                '60601',
                '700001',
                'ISSUE',
                'AAI',
                CAST(NULL AS STRING),
                '2024-01-01',
                '2025-12-31',
                '41.88',
                '-87.62',
                'test_batch',
                '2026-08-29 12:00:00',
                '2026-08-28 00:00:00'
            ),
            (
                'WARN_001',
                'WARN COMPANY LLC',
                CAST(NULL AS STRING),
                '60602',
                '700002',
                'ISSUE',
                'AAI',
                CAST(NULL AS STRING),
                '2024-01-01',
                '2025-12-31',
                '41.88',
                '-87.63',
                'test_batch',
                '2026-08-29 12:00:00',
                '2026-08-28 00:00:00'
            ),
            (
                'BAD_001',
                'BAD COMPANY LLC',
                'BAD COMPANY',
                '60603',
                '700003',
                'ISSUE',
                'INVALID',
                CAST(NULL AS STRING),
                '2024-01-01',
                '2025-12-31',
                '41.88',
                '-87.64',
                'test_batch',
                '2026-08-29 12:00:00',
                '2026-08-28 00:00:00'
            )
        AS t(
            id,
            legal_name,
            doing_business_as_name,
            zip_code,
            license_number,
            application_type,
            license_status,
            license_status_change_date,
            license_start_date,
            expiration_date,
            latitude,
            longitude,
            _source_batch_id,
            _ingested_at,
            _source_dataset_updated_at
        )
        """
    )


def test_business_license_validated_expectations(test_spark):
    mock_business_licenses(test_spark)

    # Selecting the final target also executes its required pipeline dependencies.
    test_pipeline.run(test_spark, {VALIDATED})

    result = test_spark.table(VALIDATED)
    rows = {row["id"]: row for row in result.collect()}

    # Valid row survives.
    assert "GOOD_001" in rows
    assert rows["GOOD_001"]["_dq_status"] == "VALID"

    # Missing DBA violates the monitoring expectation, but the row is retained.
    assert "WARN_001" in rows
    assert rows["WARN_001"]["_dq_status"] == "WARN"
    assert rows["WARN_001"]["doing_business_as_name"] is None

    # Invalid license status is classified as QUARANTINE and trusted_only drops it.
    assert "BAD_001" not in rows

    assert result.count() == 2

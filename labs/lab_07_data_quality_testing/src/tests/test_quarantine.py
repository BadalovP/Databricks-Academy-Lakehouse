from pyspark.pipelines.testing import TestPipeline

test_pipeline = TestPipeline.active()

CATALOG = "dbr_dev"
SCHEMA = "parvinbadalov"

LANDING = f"{CATALOG}.{SCHEMA}.business_license_landing"
QUARANTINE = f"{CATALOG}.{SCHEMA}.business_license_quarantine"


def mock_invalid_business_licenses(test_spark):
    """Create three rows that each violate a different quarantine rule."""
    test_spark.sql(
        f"""
        CREATE TABLE {LANDING} AS
        SELECT * FROM VALUES
            (
                'BAD_STATUS',
                'BAD STATUS LLC',
                'BAD STATUS',
                '60601',
                '710001',
                'ISSUE',
                'INVALID',
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
                'BAD_ZIP',
                'BAD ZIP LLC',
                'BAD ZIP',
                'ABC',
                '710002',
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
                'BAD_DATES',
                'BAD DATES LLC',
                'BAD DATES',
                '60603',
                '710003',
                'ISSUE',
                'AAI',
                CAST(NULL AS STRING),
                '2026-12-31',
                '2026-01-01',
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


def test_business_license_quarantine_reasons(test_spark):
    mock_invalid_business_licenses(test_spark)

    test_pipeline.run(test_spark, {QUARANTINE})

    result = test_spark.table(QUARANTINE)
    rows = {row["id"]: row for row in result.collect()}

    assert set(rows) == {"BAD_STATUS", "BAD_ZIP", "BAD_DATES"}

    assert "INVALID_LICENSE_STATUS" in rows["BAD_STATUS"]["_dq_quarantine_reasons"]
    assert "INVALID_ZIP" in rows["BAD_ZIP"]["_dq_quarantine_reasons"]
    assert "EXPIRATION_BEFORE_START" in rows["BAD_DATES"]["_dq_quarantine_reasons"]

    assert all(row["_dq_status"] == "QUARANTINE" for row in rows.values())

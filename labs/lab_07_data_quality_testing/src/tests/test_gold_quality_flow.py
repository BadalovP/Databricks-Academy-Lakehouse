from pyspark.pipelines.testing import TestPipeline

test_pipeline = TestPipeline.active()

CATALOG = "dbr_dev"
SCHEMA = "parvinbadalov"

LANDING = f"{CATALOG}.{SCHEMA}.business_license_landing"
QUALITY_DAILY = f"{CATALOG}.{SCHEMA}.license_quality_daily"


def mock_quality_mix(test_spark):
    """Create 1 VALID, 1 WARN, and 2 QUARANTINE records for one day."""
    test_spark.sql(
        f"""
        CREATE TABLE {LANDING} AS
        SELECT * FROM VALUES
            (
                'VALID_001',
                'VALID LLC',
                'VALID DBA',
                '60601',
                '720001',
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
                'WARN LLC',
                CAST(NULL AS STRING),
                '60602',
                '720002',
                'RENEW',
                'AAI',
                CAST(NULL AS STRING),
                '2024-01-01',
                '2025-12-31',
                '41.88',
                '-87.63',
                'test_batch',
                '2026-08-29 12:05:00',
                '2026-08-28 00:00:00'
            ),
            (
                'BAD_STATUS',
                'BAD STATUS LLC',
                'BAD STATUS',
                '60603',
                '720003',
                'ISSUE',
                'INVALID',
                CAST(NULL AS STRING),
                '2024-01-01',
                '2025-12-31',
                '41.88',
                '-87.64',
                'test_batch',
                '2026-08-29 12:10:00',
                '2026-08-28 00:00:00'
            ),
            (
                'BAD_ZIP',
                'BAD ZIP LLC',
                'BAD ZIP',
                'ABCDE',
                '720004',
                'ISSUE',
                'AAI',
                CAST(NULL AS STRING),
                '2024-01-01',
                '2025-12-31',
                '41.88',
                '-87.65',
                'test_batch',
                '2026-08-29 12:15:00',
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


def test_license_quality_daily_reconciles(test_spark):
    mock_quality_mix(test_spark)

    test_pipeline.run(test_spark, {QUALITY_DAILY})

    result = test_spark.table(QUALITY_DAILY)
    assert result.count() == 1

    row = result.first()

    assert row["total_rows"] == 4
    assert row["trusted_rows"] == 2
    assert row["quarantined_rows"] == 2
    assert row["warning_rows"] == 1

    assert row["total_rows"] == row["trusted_rows"] + row["quarantined_rows"]
    assert float(row["quality_score_pct"]) == 50.0
    assert str(row["quality_date"]) == "2026-08-29"

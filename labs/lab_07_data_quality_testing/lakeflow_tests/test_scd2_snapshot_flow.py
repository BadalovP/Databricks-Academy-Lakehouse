from pyspark.pipelines.testing import TestPipeline, test_spark

test_pipeline = TestPipeline.active()

CATALOG = "dbr_dev"
SCHEMA = "parvinbadalov"

SNAPSHOT_FEED = f"{CATALOG}.{SCHEMA}.business_license_snapshot_feed"
SCD2 = f"{CATALOG}.{SCHEMA}.dim_license_scd2"


def mock_snapshot_feed(test_spark):
    """
    Create three historical snapshots.

    license_number 730001 changes address in every snapshot.
    license_number 730002 is unchanged, so it should keep one SCD2 version.
    """
    test_spark.sql(
        f"""
        CREATE TABLE {SNAPSHOT_FEED} AS
        SELECT * FROM VALUES
            (
                1, 730001L, 1001L, 1L, 'ALPHA LLC', 'ALPHA',
                '100 STATE ST', 'CHICAGO', 'IL', '60601',
                42, 1, 1006, 'Retail', 'ISSUE',
                TIMESTAMP'2024-01-01 00:00:00',
                TIMESTAMP'2025-12-31 00:00:00',
                TIMESTAMP'2024-01-02 00:00:00',
                'AAI', CAST(NULL AS TIMESTAMP), 41.88D, -87.62D
            ),
            (
                1, 730002L, 1002L, 1L, 'BETA INC', 'BETA',
                '200 MADISON ST', 'CHICAGO', 'IL', '60606',
                42, 2, 1010, 'Limited', 'RENEW',
                TIMESTAMP'2025-01-01 00:00:00',
                TIMESTAMP'2026-12-31 00:00:00',
                TIMESTAMP'2025-01-02 00:00:00',
                'AAI', CAST(NULL AS TIMESTAMP), 41.89D, -87.63D
            ),
            (
                2, 730001L, 1001L, 1L, 'ALPHA LLC', 'ALPHA',
                '110 STATE ST', 'CHICAGO', 'IL', '60601',
                42, 1, 1006, 'Retail', 'ISSUE',
                TIMESTAMP'2024-01-01 00:00:00',
                TIMESTAMP'2025-12-31 00:00:00',
                TIMESTAMP'2024-01-02 00:00:00',
                'AAI', CAST(NULL AS TIMESTAMP), 41.88D, -87.62D
            ),
            (
                2, 730002L, 1002L, 1L, 'BETA INC', 'BETA',
                '200 MADISON ST', 'CHICAGO', 'IL', '60606',
                42, 2, 1010, 'Limited', 'RENEW',
                TIMESTAMP'2025-01-01 00:00:00',
                TIMESTAMP'2026-12-31 00:00:00',
                TIMESTAMP'2025-01-02 00:00:00',
                'AAI', CAST(NULL AS TIMESTAMP), 41.89D, -87.63D
            ),
            (
                3, 730001L, 1001L, 1L, 'ALPHA LLC', 'ALPHA',
                '120 STATE ST', 'CHICAGO', 'IL', '60601',
                42, 1, 1006, 'Retail', 'ISSUE',
                TIMESTAMP'2024-01-01 00:00:00',
                TIMESTAMP'2025-12-31 00:00:00',
                TIMESTAMP'2024-01-02 00:00:00',
                'AAI', CAST(NULL AS TIMESTAMP), 41.88D, -87.62D
            ),
            (
                3, 730002L, 1002L, 1L, 'BETA INC', 'BETA',
                '200 MADISON ST', 'CHICAGO', 'IL', '60606',
                42, 2, 1010, 'Limited', 'RENEW',
                TIMESTAMP'2025-01-01 00:00:00',
                TIMESTAMP'2026-12-31 00:00:00',
                TIMESTAMP'2025-01-02 00:00:00',
                'AAI', CAST(NULL AS TIMESTAMP), 41.89D, -87.63D
            )
        AS t(
            snapshot_version,
            license_number,
            account_number,
            site_number,
            legal_name,
            doing_business_as_name,
            address,
            city,
            state,
            zip_code,
            ward,
            precinct,
            license_code,
            license_description,
            application_type,
            license_start_date,
            expiration_date,
            date_issued,
            license_status,
            license_status_change_date,
            latitude,
            longitude
        )
        """
    )


def test_scd2_preserves_snapshot_history(test_spark):
    mock_snapshot_feed(test_spark)

    # The pipeline configuration for Lab 07 uses lab07.snapshot_count = 3.
    # The historical callback processes snapshot versions 1, 2, and 3 in order.
    test_pipeline.run(test_spark, {SCD2})

    result = test_spark.table(SCD2)

    assert "__START_AT" in result.columns
    assert "__END_AT" in result.columns

    alpha = result.filter("license_number = 730001")
    beta = result.filter("license_number = 730002")

    # ALPHA changed twice: initial + two historical versions.
    assert alpha.count() == 3
    assert {
        row["address"]
        for row in alpha.select("address").collect()
    } == {"100 STATE ST", "110 STATE ST", "120 STATE ST"}

    alpha_current = alpha.filter("__END_AT IS NULL")
    assert alpha_current.count() == 1
    assert alpha_current.first()["address"] == "120 STATE ST"

    # BETA never changed across the snapshots.
    assert beta.count() == 1
    assert beta.filter("__END_AT IS NULL").count() == 1

    assert result.count() == 4

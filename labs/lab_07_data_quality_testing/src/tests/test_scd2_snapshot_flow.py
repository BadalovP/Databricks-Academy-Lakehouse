# ruff: noqa: F401, F811

from pyspark.pipelines.testing import TestPipeline, test_spark


test_pipeline = TestPipeline.active()

CATALOG = "dbr_dev"
SCHEMA = "parvinbadalov"

SNAPSHOT_FEED = (
    f"{CATALOG}.{SCHEMA}.business_license_snapshot_feed"
)

SCD2 = f"{CATALOG}.{SCHEMA}.dim_license_scd2"


def mock_snapshot_feed(test_spark):
    """
    Create three historical snapshots.

    license_number 730001 changes address in every snapshot.

    license_number 730002 remains unchanged across all snapshots.
    """

    test_spark.sql(
        f"""
        CREATE TABLE {SNAPSHOT_FEED} AS
        SELECT * FROM VALUES

            (
                1,
                730001L,
                1001L,
                1L,
                'ALPHA LLC',
                'ALPHA',
                '100 STATE ST',
                'CHICAGO',
                'IL',
                '60601',
                42,
                1,
                1006,
                'Retail',
                'ISSUE',
                TIMESTAMP'2024-01-01 00:00:00',
                TIMESTAMP'2025-12-31 00:00:00',
                TIMESTAMP'2024-01-02 00:00:00',
                'AAI',
                CAST(NULL AS TIMESTAMP),
                41.88D,
                -87.62D
            ),

            (
                1,
                730002L,
                1002L,
                1L,
                'BETA INC',
                'BETA',
                '200 MADISON ST',
                'CHICAGO',
                'IL',
                '60606',
                42,
                2,
                1010,
                'Limited',
                'RENEW',
                TIMESTAMP'2025-01-01 00:00:00',
                TIMESTAMP'2026-12-31 00:00:00',
                TIMESTAMP'2025-01-02 00:00:00',
                'AAI',
                CAST(NULL AS TIMESTAMP),
                41.89D,
                -87.63D
            ),

            (
                2,
                730001L,
                1001L,
                1L,
                'ALPHA LLC',
                'ALPHA',
                '110 STATE ST',
                'CHICAGO',
                'IL',
                '60601',
                42,
                1,
                1006,
                'Retail',
                'ISSUE',
                TIMESTAMP'2024-01-01 00:00:00',
                TIMESTAMP'2025-12-31 00:00:00',
                TIMESTAMP'2024-01-02 00:00:00',
                'AAI',
                CAST(NULL AS TIMESTAMP),
                41.88D,
                -87.62D
            ),

            (
                2,
                730002L,
                1002L,
                1L,
                'BETA INC',
                'BETA',
                '200 MADISON ST',
                'CHICAGO',
                'IL',
                '60606',
                42,
                2,
                1010,
                'Limited',
                'RENEW',
                TIMESTAMP'2025-01-01 00:00:00',
                TIMESTAMP'2026-12-31 00:00:00',
                TIMESTAMP'2025-01-02 00:00:00',
                'AAI',
                CAST(NULL AS TIMESTAMP),
                41.89D,
                -87.63D
            ),

            (
                3,
                730001L,
                1001L,
                1L,
                'ALPHA LLC',
                'ALPHA',
                '120 STATE ST',
                'CHICAGO',
                'IL',
                '60601',
                42,
                1,
                1006,
                'Retail',
                'ISSUE',
                TIMESTAMP'2024-01-01 00:00:00',
                TIMESTAMP'2025-12-31 00:00:00',
                TIMESTAMP'2024-01-02 00:00:00',
                'AAI',
                CAST(NULL AS TIMESTAMP),
                41.88D,
                -87.62D
            ),

            (
                3,
                730002L,
                1002L,
                1L,
                'BETA INC',
                'BETA',
                '200 MADISON ST',
                'CHICAGO',
                'IL',
                '60606',
                42,
                2,
                1010,
                'Limited',
                'RENEW',
                TIMESTAMP'2025-01-01 00:00:00',
                TIMESTAMP'2026-12-31 00:00:00',
                TIMESTAMP'2025-01-02 00:00:00',
                'AAI',
                CAST(NULL AS TIMESTAMP),
                41.89D,
                -87.63D
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

    # SNAPSHOT_FEED is mocked directly.
    # SCD2 is the only pipeline output required.
    test_pipeline.run(
        test_spark,
        {
            SCD2,
        },
    )

    result = test_spark.table(SCD2)

    # Lakeflow SCD Type 2 system columns.
    assert "__START_AT" in result.columns
    assert "__END_AT" in result.columns

    alpha = result.filter(
        "license_number = 730001"
    )

    beta = result.filter(
        "license_number = 730002"
    )

    # ---------------------------------------------------------
    # ALPHA
    #
    # Address changed in snapshot 1 -> 2 -> 3.
    # Therefore three SCD2 versions should exist.
    # ---------------------------------------------------------
    assert alpha.count() == 3

    alpha_addresses = {
        row["address"]
        for row in alpha.select("address").collect()
    }

    assert alpha_addresses == {
        "100 STATE ST",
        "110 STATE ST",
        "120 STATE ST",
    }

    # Exactly one current ALPHA row.
    alpha_current = alpha.filter(
        "__END_AT IS NULL"
    )

    assert alpha_current.count() == 1

    assert (
        alpha_current.first()["address"]
        == "120 STATE ST"
    )

    # ---------------------------------------------------------
    # BETA
    #
    # Nothing changed across snapshots.
    # It should therefore retain one SCD2 version.
    # ---------------------------------------------------------
    assert beta.count() == 1

    beta_current = beta.filter(
        "__END_AT IS NULL"
    )

    assert beta_current.count() == 1

    assert (
        beta_current.first()["address"]
        == "200 MADISON ST"
    )

    # 3 ALPHA historical versions + 1 BETA version.
    assert result.count() == 4
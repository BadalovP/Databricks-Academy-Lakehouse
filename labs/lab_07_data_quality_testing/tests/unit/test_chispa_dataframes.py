"""Real chispa DataFrame tests for Lab 07.

These tests exercise reusable PySpark transformation and data-quality functions.
They are normal pytest tests and do not use Lakeflow TestPipeline.
"""

from datetime import date, datetime

from chispa import assert_df_equality

from lab07.quality_rules import classify_license_records
from lab07.transformations import prepare_business_licenses


def test_prepare_business_licenses_dataframe(spark):
    source = spark.createDataFrame(
        [
            (
                " r1 ",
                " ALPHA LLC ",
                " ALPHA ",
                " il ",
                "60601",
                "700001",
                " issue ",
                "2024-01-01",
                "2025-12-31",
                "2026-08-29 12:00:00",
            )
        ],
        """
        id string,
        legal_name string,
        doing_business_as_name string,
        state string,
        zip_code string,
        license_number string,
        application_type string,
        license_start_date string,
        expiration_date string,
        _ingested_at string
        """,
    )

    actual = (
        prepare_business_licenses(source)
        .select(
            "id",
            "legal_name",
            "doing_business_as_name",
            "state",
            "license_number",
            "application_type",
            "license_start_date",
            "expiration_date",
            "license_term_days",
            "is_location_change",
            "is_renewal",
            "_ingestion_date",
        )
    )

    expected = spark.createDataFrame(
        [
            (
                "r1",
                "ALPHA LLC",
                "ALPHA",
                "IL",
                700001,
                "ISSUE",
                datetime(2024, 1, 1),
                datetime(2025, 12, 31),
                730,
                False,
                False,
                date(2026, 8, 29),
            )
        ],
        """
        id string,
        legal_name string,
        doing_business_as_name string,
        state string,
        license_number long,
        application_type string,
        license_start_date timestamp,
        expiration_date timestamp,
        license_term_days int,
        is_location_change boolean,
        is_renewal boolean,
        _ingestion_date date
        """,
    )

    assert_df_equality(
        actual,
        expected,
        ignore_nullable=True,
        ignore_row_order=True,
    )


def test_quality_classification_dataframe(spark):
    source = spark.createDataFrame(
        [
            (
                "GOOD_001",
                "GOOD LLC",
                "GOOD DBA",
                "60601",
                "700001",
                "issue",
                "aai",
                None,
                "2024-01-01",
                "2025-12-31",
                "41.88",
                "-87.62",
                "2026-08-29 12:00:00",
            ),
            (
                "WARN_001",
                "WARN LLC",
                "   ",
                "60602",
                "700002",
                "renew",
                "aai",
                None,
                "2024-01-01",
                "2025-12-31",
                "41.89",
                "-87.63",
                "2026-08-29 12:05:00",
            ),
            (
                "BAD_001",
                "BAD LLC",
                "BAD DBA",
                "60603",
                "700003",
                "issue",
                "INVALID",
                None,
                "2024-01-01",
                "2025-12-31",
                "41.90",
                "-87.64",
                "2026-08-29 12:10:00",
            ),
        ],
        """
        id string,
        legal_name string,
        doing_business_as_name string,
        zip_code string,
        license_number string,
        application_type string,
        license_status string,
        license_status_change_date string,
        license_start_date string,
        expiration_date string,
        latitude string,
        longitude string,
        _ingested_at string
        """,
    )

    actual = (
        classify_license_records(
            prepare_business_licenses(source)
        )
        .select(
            "id",
            "_dq_status",
            "_dq_warn_reasons",
            "_dq_quarantine_reasons",
            "_dq_dimensions",
        )
    )

    expected = spark.createDataFrame(
        [
            ("GOOD_001", "VALID", [], [], []),
            (
                "WARN_001",
                "WARN",
                ["DBA_NAME_MISSING"],
                [],
                ["completeness"],
            ),
            (
                "BAD_001",
                "QUARANTINE",
                [],
                ["INVALID_LICENSE_STATUS"],
                ["validity"],
            ),
        ],
        """
        id string,
        _dq_status string,
        _dq_warn_reasons array<string>,
        _dq_quarantine_reasons array<string>,
        _dq_dimensions array<string>
        """,
    )

    assert_df_equality(
        actual,
        expected,
        ignore_nullable=True,
        ignore_row_order=True,
    )


def test_quarantine_reasons_dataframe(spark):
    source = spark.createDataFrame(
        [
            (
                "BAD_ZIP",
                "BAD ZIP LLC",
                "BAD ZIP",
                "ABC",
                "710001",
                "ISSUE",
                "AAI",
                None,
                "2024-01-01",
                "2025-12-31",
                "41.88",
                "-87.62",
                "2026-08-29 12:00:00",
            ),
            (
                "BAD_DATES",
                "BAD DATES LLC",
                "BAD DATES",
                "60601",
                "710002",
                "ISSUE",
                "AAI",
                None,
                "2026-12-31",
                "2026-01-01",
                "41.88",
                "-87.62",
                "2026-08-29 12:05:00",
            ),
        ],
        """
        id string,
        legal_name string,
        doing_business_as_name string,
        zip_code string,
        license_number string,
        application_type string,
        license_status string,
        license_status_change_date string,
        license_start_date string,
        expiration_date string,
        latitude string,
        longitude string,
        _ingested_at string
        """,
    )

    actual = (
        classify_license_records(
            prepare_business_licenses(source)
        )
        .select(
            "id",
            "_dq_status",
            "_dq_quarantine_reasons",
            "_dq_dimensions",
        )
    )

    expected = spark.createDataFrame(
        [
            (
                "BAD_ZIP",
                "QUARANTINE",
                ["INVALID_ZIP"],
                ["validity"],
            ),
            (
                "BAD_DATES",
                "QUARANTINE",
                ["EXPIRATION_BEFORE_START"],
                ["consistency"],
            ),
        ],
        """
        id string,
        _dq_status string,
        _dq_quarantine_reasons array<string>,
        _dq_dimensions array<string>
        """,
    )

    assert_df_equality(
        actual,
        expected,
        ignore_nullable=True,
        ignore_row_order=True,
    )

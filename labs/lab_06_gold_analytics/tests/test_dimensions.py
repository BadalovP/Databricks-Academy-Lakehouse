from datetime import date

from src.gold_transformations import build_date_dimension
from src.quality_rules import key_quality_metrics


def test_date_dimension_generates_complete_range(spark):
    df = build_date_dimension(spark, "2026-08-21", "2026-08-23")
    rows = df.orderBy("full_date").collect()

    assert len(rows) == 3
    assert rows[0]["full_date"] == date(2026, 8, 21)
    assert rows[-1]["full_date"] == date(2026, 8, 23)


def test_date_dimension_has_deterministic_unique_keys(spark):
    df = build_date_dimension(spark, "2026-08-21", "2026-08-23")
    metrics = key_quality_metrics(df, "date_key")

    assert metrics["row_count"] == 3
    assert metrics["distinct_key_count"] == 3
    assert metrics["null_key_count"] == 0

    keys = [row["date_key"] for row in df.orderBy("full_date").collect()]
    assert keys == [20260821, 20260822, 20260823]


def test_date_dimension_weekend_flag(spark):
    df = build_date_dimension(spark, "2026-08-21", "2026-08-23")

    values = {
        str(row["full_date"]): row["is_weekend"]
        for row in df.collect()
    }

    assert values["2026-08-21"] is False
    assert values["2026-08-22"] is True
    assert values["2026-08-23"] is True

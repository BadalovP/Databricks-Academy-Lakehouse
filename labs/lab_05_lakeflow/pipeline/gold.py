"""
Lab 05 — Lakeflow Gold layer.

Gold aggregates validated/enriched station observations into
business-friendly station availability metrics.

This version uses only fields that exist in the current Citi Bike
station_information feed.
"""

from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.config import (
    STATION_STATUS_ENRICHED_SILVER,
    STATION_SUMMARY_GOLD,
)


@dp.materialized_view(
    name=STATION_SUMMARY_GOLD,
    comment=(
        "Business-ready Citi Bike station availability summary "
        "aggregated from validated and enriched Silver observations."
    ),
    table_properties={
        "quality": "gold",
        "lab": "05",
        "entity": "station_availability_summary",
    },
)
def station_summary_gold() -> DataFrame:
    """Aggregate enriched station history into Gold metrics."""
    enriched_df = spark.read.table(
        STATION_STATUS_ENRICHED_SILVER
    )

    return (
        enriched_df
        .groupBy(
            "station_id",
            "station_name",
            "short_name",
            "latitude",
            "longitude",
            "region_id",
            "capacity",
        )
        .agg(
            F.count("*").alias(
                "observation_count"
            ),
            F.min(
                "last_reported_at"
            ).alias(
                "first_observed_at"
            ),
            F.max(
                "last_reported_at"
            ).alias(
                "last_observed_at"
            ),
            F.round(
                F.avg(
                    "num_bikes_available"
                ),
                2,
            ).alias(
                "avg_bikes_available"
            ),
            F.min(
                "num_bikes_available"
            ).alias(
                "min_bikes_available"
            ),
            F.max(
                "num_bikes_available"
            ).alias(
                "max_bikes_available"
            ),
            F.round(
                F.avg(
                    "num_docks_available"
                ),
                2,
            ).alias(
                "avg_docks_available"
            ),
            F.min(
                "num_docks_available"
            ).alias(
                "min_docks_available"
            ),
            F.max(
                "num_docks_available"
            ).alias(
                "max_docks_available"
            ),
            F.round(
                F.avg(
                    "bike_availability_pct"
                ),
                2,
            ).alias(
                "avg_bike_availability_pct"
            ),
            F.round(
                F.avg(
                    "dock_availability_pct"
                ),
                2,
            ).alias(
                "avg_dock_availability_pct"
            ),
            F.round(
                F.avg(
                    "num_ebikes_available"
                ),
                2,
            ).alias(
                "avg_ebikes_available"
            ),
            F.sum(
                F.when(
                    F.col("is_renting")
                    == F.lit(False),
                    F.lit(1),
                ).otherwise(
                    F.lit(0)
                )
            ).alias(
                "not_renting_observations"
            ),
            F.sum(
                F.when(
                    F.col("is_returning")
                    == F.lit(False),
                    F.lit(1),
                ).otherwise(
                    F.lit(0)
                )
            ).alias(
                "not_returning_observations"
            ),
            F.sum(
                F.when(
                    F.col(
                        "station_reference_matched"
                    )
                    == F.lit(False),
                    F.lit(1),
                ).otherwise(
                    F.lit(0)
                )
            ).alias(
                "unmatched_reference_observations"
            ),
        )
        .withColumn(
            "avg_station_utilization_pct",
            F.when(
                F.col("capacity") > 0,
                F.round(
                    (
                        F.col(
                            "avg_bikes_available"
                        )
                        / F.col("capacity")
                    )
                    * 100,
                    2,
                ),
            ),
        )
        .withColumn(
            "availability_band",
            F.when(
                F.col(
                    "avg_bike_availability_pct"
                ).isNull(),
                F.lit("UNKNOWN"),
            )
            .when(
                F.col(
                    "avg_bike_availability_pct"
                ) < 10,
                F.lit("VERY_LOW"),
            )
            .when(
                F.col(
                    "avg_bike_availability_pct"
                ) < 30,
                F.lit("LOW"),
            )
            .when(
                F.col(
                    "avg_bike_availability_pct"
                ) < 70,
                F.lit("BALANCED"),
            )
            .when(
                F.col(
                    "avg_bike_availability_pct"
                ) < 90,
                F.lit("HIGH"),
            )
            .otherwise(
                F.lit("VERY_HIGH")
            ),
        )
    )

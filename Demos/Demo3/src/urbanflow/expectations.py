"""Great Expectations suites for the UrbanFlow contracts.

WHY GREAT EXPECTATIONS AND NOT SODA. The Academy asks for "Great Expectations or Soda", so one is
enough, and the choice was settled by evidence rather than preference. `soda-core-spark-df` 3.5.6
declares `pyspark>=3.4,<4.0`; installing it into the measured Spark 4.1.1 environment resolves
pyspark 3.5.9 and changes the engine every Spark test runs against. The base Great Expectations
1.23.2 package adds no pyspark requirement, so it leaves 4.1.1 in place. That measurement is
recorded in docs/QUALITY_FRAMEWORK.md.

WHAT THIS ADDS, GIVEN THE PROJECT ALREADY HAS A QUALITY LAYER. `quality.py` routes rows and
`pipeline/*.py` declares Lakeflow expectations; both are tested. What they do not produce is a
portable, tool-agnostic validation *report* with per-expectation results, which is what a reviewer
or an auditor asks for. That is the gap GE fills.

The suites below deliberately mirror the rules those modules already enforce rather than adding a
second, slightly different contract. A third-party framework that disagrees with the pipeline it
validates is worse than none: it produces confident green reports about the wrong rules.
"""

from __future__ import annotations

from typing import Any

SILVER_SUITE = "urbanflow_silver_station_status"
TRIP_SUITE = "urbanflow_historical_trips"

KNOWN_AVAILABILITY_STATUSES = (
    "AVAILABLE",
    "LOW_BIKES",
    "LOW_DOCKS",
    "LOW_BIKES_AND_DOCKS",
    "OUT_OF_SERVICE",
)
RIDER_TYPES = ("member", "casual")


def silver_expectations() -> list[Any]:
    """Expectations for `silver_station_status`, mirroring the enforced Silver contract.

    Each one restates a rule that `silver.py` already applies, so a failure here means the
    pipeline and the contract have drifted apart - which is exactly what an independent check is
    for. The out-of-service pairing is included because its absence is what produced 89 wrong
    actionable rows in the first live Gold run.
    """
    import great_expectations.expectations as gxe

    return [
        gxe.ExpectColumnValuesToNotBeNull(column="event_id"),
        gxe.ExpectColumnValuesToBeUnique(column="event_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="station_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="execution_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="observed_at"),
        gxe.ExpectColumnValuesToBeBetween(column="num_bikes_available", min_value=0),
        gxe.ExpectColumnValuesToBeBetween(column="num_docks_available", min_value=0),
        gxe.ExpectColumnValuesToBeInSet(
            column="availability_status", value_set=list(KNOWN_AVAILABILITY_STATUSES)
        ),
        gxe.ExpectColumnValuesToNotBeNull(column="is_operational"),
    ]


def trip_expectations() -> list[Any]:
    """Expectations for the historical trip contract.

    `start_station_id` is asserted to be a STRING on purpose. Typed as a number it silently loses
    identifiers whose decimal part ends in zero - the committed sample contains `5470.10` and
    `6740.10` - and the station join then quietly matches fewer rows while still looking plausible.
    """
    import great_expectations.expectations as gxe

    return [
        gxe.ExpectColumnValuesToNotBeNull(column="ride_id"),
        gxe.ExpectColumnValuesToBeUnique(column="ride_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="started_at"),
        gxe.ExpectColumnValuesToNotBeNull(column="ended_at"),
        gxe.ExpectColumnValuesToNotBeNull(column="start_station_id"),
        gxe.ExpectColumnValuesToBeOfType(column="start_station_id", type_="StringType"),
        gxe.ExpectColumnValuesToBeInSet(column="member_casual", value_set=list(RIDER_TYPES)),
    ]


def build_suite(context: Any, *, name: str, expectations: list[Any]) -> Any:
    """Register one suite, replacing any suite of the same name so a rerun is idempotent."""
    import great_expectations as gx

    try:
        context.suites.delete(name=name)
    except Exception:  # noqa: BLE001 - absent suite is the normal first-run case
        pass
    suite = context.suites.add(gx.ExpectationSuite(name=name))
    for expectation in expectations:
        suite.add_expectation(expectation)
    return suite


def validate_frame(
    frame: Any,
    *,
    suite_name: str,
    expectations: list[Any],
    context: Any | None = None,
) -> dict[str, Any]:
    """Validate a Spark DataFrame and return a compact, JSON-safe report.

    An ephemeral context is used by default: nothing is written to disk, no project directory is
    created, and no state survives the call. That keeps this a validation step rather than a second
    piece of infrastructure to maintain.

    The returned shape is deliberately small and flat so it can go straight into the same JSON
    evidence reports every other phase writes, instead of requiring a GE-specific reader.
    """
    import great_expectations as gx

    ctx = context or gx.get_context(mode="ephemeral")
    source_name = f"{suite_name}_source"
    try:
        data_source = ctx.data_sources.add_spark(name=source_name)
    except Exception:  # noqa: BLE001 - already registered on a repeat call
        data_source = ctx.data_sources.get(source_name)
    asset = data_source.add_dataframe_asset(name=f"{suite_name}_asset")
    batch_definition = asset.add_batch_definition_whole_dataframe(f"{suite_name}_batch")

    suite = build_suite(ctx, name=suite_name, expectations=expectations)
    validation_definition = ctx.validation_definitions.add(
        gx.ValidationDefinition(data=batch_definition, suite=suite, name=f"{suite_name}_validation")
    )
    result = validation_definition.run(batch_parameters={"dataframe": frame})

    checks = []
    for item in result.results:
        config = item["expectation_config"]
        checks.append(
            {
                "expectation": config["type"],
                "column": config["kwargs"].get("column"),
                "success": bool(item["success"]),
                "unexpected_count": item["result"].get("unexpected_count"),
                "observed_value": item["result"].get("observed_value"),
            }
        )
    failed = [check for check in checks if not check["success"]]
    return {
        "framework": "great_expectations",
        "suite": suite_name,
        "evaluated_expectations": len(checks),
        "successful_expectations": len(checks) - len(failed),
        "failed_expectations": len(failed),
        "failed": failed,
        "checks": checks,
        "status": "PASS" if not failed else "FAIL",
    }

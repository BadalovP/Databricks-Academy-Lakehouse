import json
from pathlib import Path


def load_suite_spec(path):
    return json.loads(
        Path(path).read_text(encoding="utf-8")
    )


def run_suite(df, spec):
    import great_expectations as gx

    ctx = gx.get_context(mode="ephemeral")

    # Required for Databricks Serverless:
    # GX otherwise tries to persist/cache the Spark DataFrame.
    ds = ctx.data_sources.add_spark(
        name=spec["name"] + "_spark",
        persist=False,
    )

    asset = ds.add_dataframe_asset(
        name=spec["name"] + "_asset"
    )

    batch_definition = (
        asset.add_batch_definition_whole_dataframe(
            "whole_dataframe"
        )
    )

    batch = batch_definition.get_batch(
        batch_parameters={
            "dataframe": df
        }
    )

    suite = ctx.suites.add(
        gx.ExpectationSuite(
            name=spec["name"]
        )
    )

    expectation_map = {
        "expect_column_values_to_not_be_null":
            gx.expectations.ExpectColumnValuesToNotBeNull,

        "expect_column_values_to_be_unique":
            gx.expectations.ExpectColumnValuesToBeUnique,

        "expect_column_values_to_be_in_set":
            gx.expectations.ExpectColumnValuesToBeInSet,

        "expect_column_values_to_match_regex":
            gx.expectations.ExpectColumnValuesToMatchRegex,

        "expect_column_values_to_be_between":
            gx.expectations.ExpectColumnValuesToBeBetween,

        "expect_table_row_count_to_be_between":
            gx.expectations.ExpectTableRowCountToBeBetween,
    }

    for expectation in spec["expectations"]:
        expectation_type = expectation[
            "expectation_type"
        ]

        if expectation_type not in expectation_map:
            raise ValueError(
                "Unsupported expectation type: "
                f"{expectation_type}"
            )

        suite.add_expectation(
            expectation_map[expectation_type](
                **expectation.get("kwargs", {})
            )
        )

    return batch.validate(suite)
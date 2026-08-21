from src.config import Lab06ExternalConfig


CATALOG = "dbr_dev"
SOURCE_SCHEMA = "parvinbadalov"
SOURCE_VOLUME = "lab06_gold_analytics"
TARGET_SCHEMA = "parvinbadalov_lab06_ext"
EXTERNAL_ROOT = (
    "abfss://container@account.dfs.core.windows.net/"
    "lab06_gold_external_v2"
)


def make_config():
    return Lab06ExternalConfig(
        catalog=CATALOG,
        source_schema=SOURCE_SCHEMA,
        source_volume_name=SOURCE_VOLUME,
        target_schema=TARGET_SCHEMA,
        external_gold_root=EXTERNAL_ROOT,
    )


AGGREGATES = [
    "agg_daily_encounters",
    "agg_organization_performance",
    "agg_payer_performance",
    "agg_condition_summary",
]


def test_all_four_aggregate_names_resolve_in_target_schema():
    config = make_config()

    for name in AGGREGATES:
        assert config.table(name) == (
            f"{CATALOG}.{TARGET_SCHEMA}.{name}"
        )


def test_all_four_aggregate_paths_use_external_root():
    config = make_config()

    for name in AGGREGATES:
        assert config.table_path(name) == (
            f"{EXTERNAL_ROOT}/{name}"
        )


def test_aggregate_locations_are_unique():
    config = make_config()

    paths = [
        config.table_path(name)
        for name in AGGREGATES
    ]

    assert len(paths) == len(set(paths))


def test_aggregates_are_not_inside_source_volume():
    config = make_config()

    for name in AGGREGATES:
        path = config.table_path(name)

        assert "/Volumes/" not in path
        assert SOURCE_VOLUME not in path


def test_aggregate_namespace_matches_facts_and_dimensions():
    config = make_config()

    objects = (
        AGGREGATES
        + ["fact_encounters", "fact_conditions"]
        + ["dim_patient", "dim_date"]
    )

    namespaces = {
        ".".join(config.table(name).split(".")[:2])
        for name in objects
    }

    assert namespaces == {
        f"{CATALOG}.{TARGET_SCHEMA}"
    }


def test_aggregate_path_basename_matches_table_basename():
    config = make_config()

    for name in AGGREGATES:
        table_basename = config.table(name).split(".")[-1]
        path_basename = config.table_path(name).rstrip("/").split("/")[-1]

        assert table_basename == path_basename == name

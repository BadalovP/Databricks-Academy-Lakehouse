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


FACT = "fact_conditions"
DEPENDENCIES = [
    "dim_date",
    "dim_patient",
    "dim_condition",
    "fact_encounters",
]


def test_fact_conditions_fully_qualified_name():
    config = make_config()

    assert config.fact_conditions == (
        f"{CATALOG}.{TARGET_SCHEMA}.{FACT}"
    )


def test_fact_conditions_external_location():
    config = make_config()

    assert config.table_path(FACT) == (
        f"{EXTERNAL_ROOT}/{FACT}"
    )


def test_fact_conditions_is_not_stored_in_uc_volume():
    config = make_config()

    assert "/Volumes/" not in config.table_path(FACT)


def test_fact_conditions_dependencies_share_target_namespace():
    config = make_config()

    for name in DEPENDENCIES:
        assert config.table(name).startswith(
            f"{CATALOG}.{TARGET_SCHEMA}."
        )


def test_fact_conditions_has_distinct_location_from_encounters():
    config = make_config()

    assert (
        config.table_path("fact_conditions")
        != config.table_path("fact_encounters")
    )


def test_fact_conditions_table_and_path_names_match():
    config = make_config()

    assert config.table(FACT).endswith(f".{FACT}")
    assert config.table_path(FACT).endswith(f"/{FACT}")

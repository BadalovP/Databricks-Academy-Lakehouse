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


DIMENSIONS = [
    "dim_date",
    "dim_patient",
    "dim_provider",
    "dim_organization",
    "dim_payer",
    "dim_condition",
]

FACTS = [
    "fact_encounters",
    "fact_conditions",
]

AGGREGATES = [
    "agg_daily_encounters",
    "agg_organization_performance",
    "agg_payer_performance",
    "agg_condition_summary",
]

CORE_OBJECTS = DIMENSIONS + FACTS + AGGREGATES


def test_core_gold_model_contains_expected_number_of_objects():
    assert len(DIMENSIONS) == 6
    assert len(FACTS) == 2
    assert len(AGGREGATES) == 4
    assert len(CORE_OBJECTS) == 12


def test_core_gold_object_names_are_unique():
    assert len(CORE_OBJECTS) == len(set(CORE_OBJECTS))


def test_all_core_objects_have_unique_external_locations():
    config = make_config()

    locations = [
        config.table_path(name)
        for name in CORE_OBJECTS
    ]

    assert len(locations) == len(set(locations))


def test_all_core_objects_use_same_target_namespace():
    config = make_config()

    for name in CORE_OBJECTS:
        assert config.table(name).startswith(
            f"{CATALOG}.{TARGET_SCHEMA}."
        )


def test_no_core_gold_object_uses_volume_storage():
    config = make_config()

    for name in CORE_OBJECTS:
        assert "/Volumes/" not in config.table_path(name)


def test_no_core_gold_object_uses_source_schema_as_target():
    config = make_config()

    source_prefix = f"{CATALOG}.{SOURCE_SCHEMA}."

    for name in CORE_OBJECTS:
        assert not config.table(name).startswith(source_prefix)


def test_external_paths_match_object_names():
    config = make_config()

    for name in CORE_OBJECTS:
        assert config.table_path(name).endswith(f"/{name}")


def test_external_root_is_abfss_storage():
    config = make_config()

    for name in CORE_OBJECTS:
        assert config.table_path(name).startswith("abfss://")

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


def test_all_six_dimensions_use_target_schema():
    config = make_config()

    for name in DIMENSIONS:
        assert config.table(name) == (
            f"{CATALOG}.{TARGET_SCHEMA}.{name}"
        )


def test_dimension_storage_is_external_sibling_storage():
    config = make_config()

    for name in DIMENSIONS:
        path = config.table_path(name)

        assert path == f"{EXTERNAL_ROOT}/{name}"
        assert "/Volumes/" not in path


def test_each_dimension_has_unique_external_location():
    config = make_config()

    paths = [
        config.table_path(name)
        for name in DIMENSIONS
    ]

    assert len(paths) == len(set(paths))


def test_dimension_namespace_is_separate_from_source_schema():
    config = make_config()

    assert config.source_schema == SOURCE_SCHEMA
    assert config.target_schema == TARGET_SCHEMA
    assert config.source_schema != config.target_schema


def test_dimension_locations_do_not_use_source_volume_name():
    config = make_config()

    for name in DIMENSIONS:
        path = config.table_path(name)

        assert SOURCE_VOLUME not in path
        assert path.startswith("abfss://")

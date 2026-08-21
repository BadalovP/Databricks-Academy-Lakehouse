"""Configuration for Lab 06 External V2.

This version intentionally separates:
- source files in the existing shared external Volume;
- target Unity Catalog schema;
- physical Delta table storage in a shared ADLS sibling path.

The same physical Delta paths can be registered in multiple Databricks
workspaces while each workspace keeps its own Unity Catalog metadata.
"""

from dataclasses import dataclass


TABLE_NAMES = (
    "dim_date",
    "dim_patient",
    "dim_provider",
    "dim_organization",
    "dim_payer",
    "dim_condition",
    "fact_encounters",
    "fact_conditions",
    "agg_daily_encounters",
    "agg_organization_performance",
    "agg_payer_performance",
    "agg_condition_summary",
)

METRICS_TABLE = "lab06_data_volume_metrics"


@dataclass(frozen=True)
class Lab06ExternalConfig:
    catalog: str
    source_schema: str
    source_volume_name: str
    target_schema: str
    external_gold_root: str

    @property
    def source_volume_path(self) -> str:
        return (
            f"/Volumes/{self.catalog}/"
            f"{self.source_schema}/{self.source_volume_name}"
        )

    @property
    def source_csv_path(self) -> str:
        return f"{self.source_volume_path}/source/csv"

    @property
    def reference_path(self) -> str:
        return f"{self.source_volume_path}/reference"

    @property
    def encounter_landing_path(self) -> str:
        return f"{self.source_volume_path}/landing/encounters"

    @property
    def target_schema_fqn(self) -> str:
        return f"{self.catalog}.{self.target_schema}"

    def table(self, name: str) -> str:
        return f"{self.target_schema_fqn}.{name}"

    def table_path(self, name: str) -> str:
        return f"{self.external_gold_root.rstrip('/')}/{name}"

    @staticmethod
    def short_name(table_fqn: str) -> str:
        return table_fqn.rsplit(".", 1)[-1]

    @property
    def dim_date(self) -> str:
        return self.table("dim_date")

    @property
    def dim_patient(self) -> str:
        return self.table("dim_patient")

    @property
    def dim_provider(self) -> str:
        return self.table("dim_provider")

    @property
    def dim_organization(self) -> str:
        return self.table("dim_organization")

    @property
    def dim_payer(self) -> str:
        return self.table("dim_payer")

    @property
    def dim_condition(self) -> str:
        return self.table("dim_condition")

    @property
    def fact_encounters(self) -> str:
        return self.table("fact_encounters")

    @property
    def fact_conditions(self) -> str:
        return self.table("fact_conditions")

    @property
    def agg_daily_encounters(self) -> str:
        return self.table("agg_daily_encounters")

    @property
    def agg_organization_performance(self) -> str:
        return self.table("agg_organization_performance")

    @property
    def agg_payer_performance(self) -> str:
        return self.table("agg_payer_performance")

    @property
    def agg_condition_summary(self) -> str:
        return self.table("agg_condition_summary")

    @property
    def data_volume_metrics(self) -> str:
        return self.table(METRICS_TABLE)

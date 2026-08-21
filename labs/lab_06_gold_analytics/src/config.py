"""Shared configuration helpers for Lab 06.

The environment-specific values (catalog, schema, volume name) are supplied by
Databricks notebook/job parameters. This module only centralizes derived paths
and fully qualified object names.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Lab06Config:
    catalog: str
    schema: str
    volume_name: str

    @property
    def volume_path(self) -> str:
        return f"/Volumes/{self.catalog}/{self.schema}/{self.volume_name}"

    @property
    def source_csv_path(self) -> str:
        return f"{self.volume_path}/source/csv"

    @property
    def reference_path(self) -> str:
        return f"{self.volume_path}/reference"

    @property
    def encounter_landing_path(self) -> str:
        return f"{self.volume_path}/landing/encounters"

    def table(self, name: str) -> str:
        return f"{self.catalog}.{self.schema}.{name}"

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

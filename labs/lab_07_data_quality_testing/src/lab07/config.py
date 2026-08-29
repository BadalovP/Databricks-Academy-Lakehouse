from dataclasses import dataclass


@dataclass(frozen=True)
class Lab07Config:
    catalog: str = "dbr_dev"
    schema: str = "parvinbadalov"
    volume_name: str = "lab07_data_quality"
    dataset_id: str = "r5kz-chrr"

    @property
    def volume_root(self):
        return f"/Volumes/{self.catalog}/{self.schema}/{self.volume_name}"

    @property
    def api_url(self):
        return f"https://data.cityofchicago.org/resource/{self.dataset_id}.json"

    def table(self, name):
        return f"{self.catalog}.{self.schema}.{name}"


APPLICATION_TYPES = ("ISSUE", "RENEW", "C_LOC", "C_CAPA", "C_EXPA", "C_SBA")
LICENSE_STATUSES = ("AAI", "AAC", "REV", "REA")
SCD_TRACKED_COLUMNS = (
    "license_number",
    "account_number",
    "site_number",
    "legal_name",
    "doing_business_as_name",
    "address",
    "city",
    "state",
    "zip_code",
    "ward",
    "precinct",
    "license_code",
    "license_description",
    "application_type",
    "license_start_date",
    "expiration_date",
    "date_issued",
    "license_status",
    "license_status_change_date",
    "latitude",
    "longitude",
)

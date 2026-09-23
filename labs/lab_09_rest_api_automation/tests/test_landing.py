from unittest.mock import MagicMock

import pytest

from lab09 import landing


def _cfg(**overrides):
    base = {
        "catalog": "dbr_dev",
        "schema": "parvinbadalov",
        "volume": "lab09_landing",
        "months": ["2024-01", "2024-02", "2024-03"],
        "dataset": {
            "trips_url_template": "https://example.invalid/trip-data/yellow_tripdata_{month}.parquet",
            "zone_lookup_url": "https://example.invalid/misc/taxi_zone_lookup.csv",
            "download_retries": 2,
            "download_backoff_seconds": 0,
            "download_timeout_seconds": 10,
        },
    }
    base.update(overrides)
    return base


def _dir_entry(name: str, path: str):
    entry = MagicMock()
    entry.name = name
    entry.path = path
    return entry


# --- filename parsing -------------------------------------------------------


def test_parse_landed_month_matches_expected_filename():
    assert landing.parse_landed_month("yellow_tripdata_2024-03.parquet") == "2024-03"


def test_parse_landed_month_rejects_unrelated_or_malformed_names():
    assert landing.parse_landed_month("taxi_zone_lookup.csv") is None
    assert landing.parse_landed_month("yellow_tripdata_2024-13.parquet.tmp") is None
    assert landing.parse_landed_month("yellow_tripdata_24-01.parquet") is None


# --- next-month selection ----------------------------------------------------


def test_next_missing_month_returns_first_gap_in_configured_order():
    configured = ["2024-01", "2024-02", "2024-03"]
    landed = {"2024-01"}
    assert landing.next_missing_month(configured, landed) == "2024-02"


def test_next_missing_month_ignores_landed_months_out_of_order():
    configured = ["2024-01", "2024-02", "2024-03"]
    landed = {"2024-02"}  # landed out of order; 2024-01 is still the first gap
    assert landing.next_missing_month(configured, landed) == "2024-01"


# --- all-months-landed behavior ----------------------------------------------


def test_next_missing_month_returns_none_when_all_landed():
    configured = ["2024-01", "2024-02"]
    landed = {"2024-01", "2024-02"}
    assert landing.next_missing_month(configured, landed) is None


def test_land_next_month_returns_no_new_data_without_downloading_when_all_present():
    client = MagicMock()
    client.files.list_directory_contents.return_value = [
        _dir_entry("yellow_tripdata_2024-01.parquet", "/vol/trips/yellow_tripdata_2024-01.parquet"),
        _dir_entry("yellow_tripdata_2024-02.parquet", "/vol/trips/yellow_tripdata_2024-02.parquet"),
        _dir_entry("yellow_tripdata_2024-03.parquet", "/vol/trips/yellow_tripdata_2024-03.parquet"),
    ]
    get_fn = MagicMock()

    result = landing.land_next_month(client, _cfg(), get_fn=get_fn)

    assert result.status == "NO_NEW_DATA"
    assert result.month is None
    assert result.month_landed is False
    get_fn.assert_not_called()
    client.files.upload.assert_not_called()


# --- successful landing + never overwrite -----------------------------------


def test_land_next_month_downloads_and_uploads_first_missing_month():
    client = MagicMock()
    client.files.list_directory_contents.return_value = [
        _dir_entry("yellow_tripdata_2024-01.parquet", "/vol/trips/yellow_tripdata_2024-01.parquet"),
    ]

    fake_response = MagicMock()
    fake_response.content = b"PAR1" + (b"x" * 200) + b"PAR1"
    fake_response.raise_for_status = MagicMock()
    get_fn = MagicMock(return_value=fake_response)

    result = landing.land_next_month(client, _cfg(), get_fn=get_fn)

    assert result.status == "SUCCESS"
    assert result.month == "2024-02"
    assert result.month_landed is True
    assert result.file_bytes == len(fake_response.content)

    client.files.upload.assert_called_once()
    args, kwargs = client.files.upload.call_args
    assert args[0].endswith("yellow_tripdata_2024-02.parquet")
    assert kwargs.get("overwrite") is False


def test_build_trips_url_uses_configured_template():
    url = landing.build_trips_url(_cfg(), "2024-05")
    assert url == "https://example.invalid/trip-data/yellow_tripdata_2024-05.parquet"


# --- invalid / truncated Parquet rejection -----------------------------------


def test_download_and_validate_rejects_empty_response():
    fake_response = MagicMock()
    fake_response.content = b""
    fake_response.raise_for_status = MagicMock()
    get_fn = MagicMock(return_value=fake_response)

    with pytest.raises(landing.DownloadValidationError):
        landing.download_and_validate(
            "https://example.invalid/file.parquet",
            retries=1,
            get_fn=get_fn,
            sleep_fn=lambda s: None,
        )


def test_download_and_validate_rejects_truncated_file_missing_trailing_magic():
    fake_response = MagicMock()
    fake_response.content = b"PAR1" + (b"x" * 50)  # missing trailing PAR1 => truncated
    fake_response.raise_for_status = MagicMock()
    get_fn = MagicMock(return_value=fake_response)

    with pytest.raises(landing.DownloadValidationError):
        landing.download_and_validate(
            "https://example.invalid/file.parquet",
            retries=1,
            get_fn=get_fn,
            sleep_fn=lambda s: None,
        )


def test_download_and_validate_rejects_file_missing_leading_magic():
    fake_response = MagicMock()
    fake_response.content = (b"x" * 50) + b"PAR1"
    fake_response.raise_for_status = MagicMock()
    get_fn = MagicMock(return_value=fake_response)

    with pytest.raises(landing.DownloadValidationError):
        landing.download_and_validate(
            "https://example.invalid/file.parquet",
            retries=1,
            get_fn=get_fn,
            sleep_fn=lambda s: None,
        )


def test_download_and_validate_does_not_retry_validation_failures():
    fake_response = MagicMock()
    fake_response.content = b""
    fake_response.raise_for_status = MagicMock()
    get_fn = MagicMock(return_value=fake_response)

    with pytest.raises(landing.DownloadValidationError):
        landing.download_and_validate(
            "https://example.invalid/file.parquet",
            retries=3,
            get_fn=get_fn,
            sleep_fn=lambda s: None,
        )
    # An invalid (not merely transient) download must not be retried as if it were.
    assert get_fn.call_count == 1


def test_download_and_validate_retries_transient_failures_then_succeeds():
    good_response = MagicMock()
    good_response.content = b"PAR1" + (b"x" * 20) + b"PAR1"
    good_response.raise_for_status = MagicMock()

    get_fn = MagicMock(side_effect=[ConnectionError("boom"), good_response])
    sleeps = []

    data = landing.download_and_validate(
        "https://example.invalid/file.parquet",
        retries=2,
        backoff_seconds=1,
        get_fn=get_fn,
        sleep_fn=sleeps.append,
    )

    assert data == good_response.content
    assert get_fn.call_count == 2
    assert sleeps  # backed off at least once


def test_download_and_validate_raises_after_exhausting_retries():
    get_fn = MagicMock(side_effect=ConnectionError("still down"))

    with pytest.raises(RuntimeError):
        landing.download_and_validate(
            "https://example.invalid/file.parquet",
            retries=3,
            backoff_seconds=0,
            get_fn=get_fn,
            sleep_fn=lambda s: None,
        )
    assert get_fn.call_count == 3


# --- cleanup / reset-landing guard rails -------------------------------------


def test_reset_landing_only_deletes_paths_under_trips_root():
    client = MagicMock()
    trips_root = "/Volumes/dbr_dev/parvinbadalov/lab09_landing/trips"
    client.files.list_directory_contents.return_value = [
        _dir_entry(
            "yellow_tripdata_2024-01.parquet", f"{trips_root}/yellow_tripdata_2024-01.parquet"
        ),
        _dir_entry(
            "yellow_tripdata_2024-02.parquet", f"{trips_root}/yellow_tripdata_2024-02.parquet"
        ),
    ]

    deleted = landing.reset_landing(client, _cfg(), reset_reference=False)

    assert len(deleted) == 2
    assert all(p.startswith(trips_root) for p in deleted)
    assert client.files.delete.call_count == 2

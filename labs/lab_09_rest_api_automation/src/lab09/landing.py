"""Idempotent incremental NYC TLC Yellow Taxi monthly ingestion for LAB 09.

The GitHub runner / local automation downloads the source file over the
public internet; the Databricks workspace itself never needs outbound
internet access -- the downloaded bytes are pushed in via the Files API.
"""

from __future__ import annotations

import io
import logging
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import requests
from databricks.sdk.errors import NotFound

from .client import reference_path as reference_volume_path
from .client import trips_path as trips_volume_path

logger = logging.getLogger(__name__)

FILENAME_PATTERN = re.compile(r"^yellow_tripdata_(\d{4}-\d{2})\.parquet$")
PARQUET_MAGIC = b"PAR1"


class DownloadValidationError(RuntimeError):
    """Raised when a downloaded file fails size/magic-byte validation."""


@dataclass
class LandingResult:
    status: str  # "SUCCESS" | "NO_NEW_DATA"
    month: str | None = None
    month_landed: bool = False
    file_bytes: int = 0
    volume_path: str | None = None


def parse_landed_month(filename: str) -> str | None:
    """Extract the YYYY-MM month from a landed trips filename, or None if it doesn't match."""
    match = FILENAME_PATTERN.match(filename)
    return match.group(1) if match else None


def list_landed_months(client: Any, cfg: dict[str, Any]) -> set[str]:
    """List months already landed under the trips/ volume path via the Files API.

    `files.list_directory_contents()` is a generator: the underlying HTTP
    call only happens once iteration starts, not when the function is
    called. The try/except therefore has to wrap the iteration itself, not
    just the call that constructs the generator -- wrapping only the call
    would silently never catch anything. Only a genuine NotFound (the
    directory does not exist yet, e.g. before the first month has ever
    landed) is treated as "no months landed"; any other error (permission,
    authentication, network, service) propagates so it is never mistaken
    for an empty landing area.
    """
    path = trips_volume_path(cfg)
    months: set[str] = set()
    try:
        for entry in client.files.list_directory_contents(path):
            name = getattr(entry, "name", None) or Path(getattr(entry, "path", "")).name
            month = parse_landed_month(name)
            if month:
                months.add(month)
    except NotFound:
        logger.info("%s does not exist yet; treating as no months landed.", path)
        return set()
    return months


def next_missing_month(configured_months: list[str], landed_months: set[str]) -> str | None:
    """Return the first configured month not already landed, or None if all are present."""
    for month in configured_months:
        if month not in landed_months:
            return month
    return None


def _looks_like_parquet(data: bytes) -> bool:
    return len(data) >= 8 and data[:4] == PARQUET_MAGIC and data[-4:] == PARQUET_MAGIC


def download_and_validate(
    url: str,
    retries: int = 3,
    backoff_seconds: float = 2.0,
    timeout_seconds: int = 300,
    sleep_fn: Callable[[float], None] = time.sleep,
    get_fn: Callable[..., Any] = requests.get,
) -> bytes:
    """Download a file, retrying transient failures, and validate before returning.

    Validation requires a non-zero response body and Parquet magic bytes
    (``PAR1``) at both the start and the end of the file. An invalid or
    truncated download is raised as DownloadValidationError and never
    returned to the caller -- so it is never uploaded.
    """
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            response = get_fn(url, timeout=timeout_seconds)
            response.raise_for_status()
            data = response.content
            if len(data) == 0:
                raise DownloadValidationError(f"Downloaded file from {url} was empty.")
            if not _looks_like_parquet(data):
                raise DownloadValidationError(
                    f"Downloaded file from {url} does not look like a valid Parquet file "
                    "(missing PAR1 magic bytes at start/end); refusing to upload it."
                )
            return data
        except DownloadValidationError:
            raise
        except Exception as exc:  # noqa: BLE001 - network/transient errors are retried
            last_exc = exc
            logger.warning("Download attempt %s/%s for %s failed: %s", attempt, retries, url, exc)
            if attempt < retries:
                sleep_fn(backoff_seconds * attempt)
    raise RuntimeError(f"Failed to download {url} after {retries} attempts: {last_exc}")


def build_trips_url(cfg: dict[str, Any], month: str) -> str:
    template = cfg["dataset"]["trips_url_template"]
    return template.format(month=month)


def _ensure_directory(client: Any, directory_path: str) -> None:
    """Explicitly ensure a Lab 9-owned Files API directory exists before uploading into it.

    Uses the Files API's own directory-create operation (idempotent: safe
    to call even when the directory already exists) rather than assuming
    upload() implicitly creates missing parent directories.
    """
    client.files.create_directory(directory_path)


def land_next_month(
    client: Any,
    cfg: dict[str, Any],
    get_fn: Callable[..., Any] = requests.get,
) -> LandingResult:
    """Idempotently land the next missing configured month.

    Never overwrites an existing successfully landed monthly file: the
    upload uses ``overwrite=False``, and the target filename is only ever
    computed for a month that ``list_landed_months()`` confirmed is absent.
    """
    configured_months = cfg["months"]
    landed = list_landed_months(client, cfg)
    month = next_missing_month(configured_months, landed)

    if month is None:
        logger.info("All configured months already landed; NO_NEW_DATA.")
        return LandingResult(status="NO_NEW_DATA", month=None, month_landed=False)

    url = build_trips_url(cfg, month)
    dataset_cfg = cfg["dataset"]
    data = download_and_validate(
        url,
        retries=dataset_cfg.get("download_retries", 3),
        backoff_seconds=dataset_cfg.get("download_backoff_seconds", 2.0),
        timeout_seconds=dataset_cfg.get("download_timeout_seconds", 300),
        get_fn=get_fn,
    )

    trips_root = trips_volume_path(cfg)
    _ensure_directory(client, trips_root)
    filename = f"yellow_tripdata_{month}.parquet"
    remote_path = f"{trips_root}/{filename}"
    client.files.upload(remote_path, io.BytesIO(data), overwrite=False)
    logger.info("Landed %s (%d bytes) at %s", month, len(data), remote_path)

    return LandingResult(
        status="SUCCESS",
        month=month,
        month_landed=True,
        file_bytes=len(data),
        volume_path=remote_path,
    )


def ensure_reference_csv(
    client: Any,
    cfg: dict[str, Any],
    get_fn: Callable[..., Any] = requests.get,
) -> str:
    """Upload the taxi zone lookup CSV once, if it is not already present.

    Only a genuine NotFound from get_metadata() is treated as "not present
    yet"; any other error (permission, authentication, network, service)
    propagates rather than being silently reinterpreted as a missing file.
    """
    remote_path = f"{reference_volume_path(cfg)}/taxi_zone_lookup.csv"
    try:
        client.files.get_metadata(remote_path)
        logger.info("Reference file %s already present; not re-downloading.", remote_path)
        return remote_path
    except NotFound:
        pass

    url = cfg["dataset"]["zone_lookup_url"]
    dataset_cfg = cfg["dataset"]
    response = get_fn(url, timeout=dataset_cfg.get("download_timeout_seconds", 300))
    response.raise_for_status()
    data = response.content
    if len(data) == 0:
        raise DownloadValidationError(f"Downloaded reference file from {url} was empty.")
    _ensure_directory(client, reference_volume_path(cfg))
    client.files.upload(remote_path, io.BytesIO(data), overwrite=False)
    logger.info("Uploaded reference file to %s (%d bytes).", remote_path, len(data))
    return remote_path


def reset_landing(client: Any, cfg: dict[str, Any], reset_reference: bool = False) -> list[str]:
    """Delete only Lab 9-owned files under the Lab 9 landing volume.

    Guarded to only ever operate on paths under this config's own volume
    root -- never touches unrelated Unity Catalog data. Returns the list of
    deleted paths.
    """
    deleted: list[str] = []
    trips_root = trips_volume_path(cfg)
    for entry in client.files.list_directory_contents(trips_root):
        path = getattr(entry, "path", None)
        if not path or not path.startswith(trips_root):
            continue
        client.files.delete(path)
        deleted.append(path)

    if reset_reference:
        ref_root = reference_volume_path(cfg)
        for entry in client.files.list_directory_contents(ref_root):
            path = getattr(entry, "path", None)
            if not path or not path.startswith(ref_root):
                continue
            client.files.delete(path)
            deleted.append(path)

    logger.info("cleanup --reset-landing deleted %d file(s) under %s", len(deleted), trips_root)
    return deleted

"""
LAB 08 - TravelOps

Test module:
tests/test_seed_ingestion_simulation.py

Purpose:
Simulates the actual sequence of file operations in
notebooks/00_seed_raw_data.ipynb's `_write_immutable_seed` (stage, verify,
move, clean up staging) against an in-memory fake filesystem, so the
interrupted-write and restart-idempotency properties can be exercised
without a Databricks runtime -- something a pure decision-logic mirror
(tests/test_ingestion.py's decide_seed_action tests) cannot do, since those
only prove the write/skip/fail branching is correct, not that the
underlying file operations behave safely if interrupted partway through.
Also mirrors `_existing_seed_files`'s exception-handling branch (distinct
from the file-operation sequence above): only a "path does not exist yet"
failure may be treated as "no files," and every other failure -- a
permission error, an authentication failure, a transient storage error --
must propagate rather than being silently reported as "nothing seeded yet."

Inputs:
An in-memory FakeVolumeFilesystem standing in for dbutils.fs.

Outputs:
Pytest pass/fail results only.

Idempotency:
Tests are side-effect free (the fake filesystem is process-local, in-memory,
and discarded after each test).

Local unit tests vs simulated tests vs Databricks integration:
This is a SIMULATED test, not a unit test of pure logic and not a real
Databricks/Spark/Auto Loader integration test. FakeVolumeFilesystem models
only path existence and the exact operation sequence
(`rm`/write-one-part-file/`ls`/`mv`/`rm`) that `_write_immutable_seed` uses;
it does not model real dbutils.fs error semantics, concurrent access,
actual Parquet file content, Unity Catalog Volume permissions, or Auto
Loader's file discovery and `cloudFiles.allowOverwrites` behavior at all.
Passing these tests proves the notebook's *file-operation sequence* is
safe against a partial-write interruption in this simplified model; it does
NOT prove Auto Loader actually ingests the resulting file correctly, that a
real interrupted Databricks job leaves state exactly as modeled here, or
that Bronze/Silver/Gold end up correct -- only a real Databricks bundle run
(not performed in this change; see
evidence/lab08_production_remediation_plan.md's validation procedure) can
confirm those.
"""

from __future__ import annotations


class FakeVolumeFilesystem:
    """In-memory stand-in for dbutils.fs, modeling only path existence.

    Supports exactly the operations `_write_immutable_seed`/
    `_existing_seed_files` use: writing a staged part file, listing a
    directory's immediate children, moving a file, and recursively removing
    a path. Never touches a real filesystem.
    """

    def __init__(self) -> None:
        self._files: set[str] = set()

    def seed_existing_file(self, path: str) -> None:
        """Pre-populate a file as if written by a prior run."""

        self._files.add(path)

    def write_staging_part_file(self, staging_path: str) -> None:
        """Simulate df.coalesce(1).write...save(staging_path) producing one part file."""

        self._files.add(f"{staging_path}/part-00000-simulated.snappy.parquet")

    def ls(self, path: str) -> list[str]:
        prefix = path.rstrip("/") + "/"
        names = []
        for f in self._files:
            if f.startswith(prefix):
                rest = f[len(prefix) :]
                if "/" not in rest:
                    names.append(rest)
        return names

    def exists(self, path: str) -> bool:
        return path in self._files

    def mv(self, source: str, dest: str) -> None:
        if source not in self._files:
            raise FileNotFoundError(source)
        self._files.discard(source)
        self._files.add(dest)

    def rm(self, path: str, recurse: bool = False) -> None:
        prefix = path.rstrip("/") + "/"
        self._files = {f for f in self._files if f != path and not f.startswith(prefix)}


def _simulate_write_immutable_seed(
    fs: FakeVolumeFilesystem,
    target_path: str,
    staging_root: str,
    table_name: str,
    file_name: str,
    *,
    crash_before_move: bool = False,
) -> None:
    """Mirrors _write_immutable_seed's operation sequence against a fake filesystem.

    Set crash_before_move=True to simulate the job being killed after the
    staged file is fully written but before the move into target_path --
    the exact interrupted-write scenario the real notebook is designed to
    recover from on a restart.
    """

    staging_path = f"{staging_root}/{table_name}"
    fs.rm(staging_path, recurse=True)
    fs.write_staging_part_file(staging_path)

    part_files = [f"{staging_path}/{name}" for name in fs.ls(staging_path)]
    if len(part_files) != 1:
        raise AssertionError(f"Expected exactly one staged part file, found {len(part_files)}")

    if crash_before_move:
        return

    final_path = f"{target_path}/{file_name}"
    fs.mv(part_files[0], final_path)
    fs.rm(staging_path, recurse=True)


def test_normal_write_produces_file_at_final_path() -> None:
    fs = FakeVolumeFilesystem()

    _simulate_write_immutable_seed(
        fs, "raw/bookings", "_seed_staging", "bookings", "seed-v1-25000-25000rows.snappy.parquet"
    )

    assert fs.exists("raw/bookings/seed-v1-25000-25000rows.snappy.parquet")
    assert fs.ls("_seed_staging/bookings") == []  # staging cleaned up


def test_interrupted_write_leaves_no_file_at_final_path() -> None:
    fs = FakeVolumeFilesystem()

    _simulate_write_immutable_seed(
        fs,
        "raw/bookings",
        "_seed_staging",
        "bookings",
        "seed-v1-25000-25000rows.snappy.parquet",
        crash_before_move=True,
    )

    # The staged file exists (the write itself succeeded), but it never
    # became visible at the path Auto Loader watches -- this is what
    # prevents Auto Loader from ever observing a partial write.
    assert not fs.exists("raw/bookings/seed-v1-25000-25000rows.snappy.parquet")
    assert fs.ls("_seed_staging/bookings") == ["part-00000-simulated.snappy.parquet"]


def test_restart_after_interrupted_write_completes_successfully() -> None:
    fs = FakeVolumeFilesystem()

    _simulate_write_immutable_seed(
        fs,
        "raw/bookings",
        "_seed_staging",
        "bookings",
        "seed-v1-25000-25000rows.snappy.parquet",
        crash_before_move=True,
    )
    assert not fs.exists("raw/bookings/seed-v1-25000-25000rows.snappy.parquet")

    # Restarting execution retries the whole staging sequence from scratch
    # (it clears staging first) rather than assuming the leftover staged
    # file is still valid.
    _simulate_write_immutable_seed(
        fs, "raw/bookings", "_seed_staging", "bookings", "seed-v1-25000-25000rows.snappy.parquet"
    )

    assert fs.exists("raw/bookings/seed-v1-25000-25000rows.snappy.parquet")
    assert fs.ls("_seed_staging/bookings") == []


def test_write_never_touches_an_unrelated_existing_file() -> None:
    # Demonstrates non-destructiveness concretely: a file that already
    # exists for a DIFFERENT table/identity must survive untouched,
    # including through a crash-and-retry sequence for another table.
    fs = FakeVolumeFilesystem()
    fs.seed_existing_file("raw/payments/seed-v1-25000-21089rows.snappy.parquet")

    _simulate_write_immutable_seed(
        fs,
        "raw/bookings",
        "_seed_staging",
        "bookings",
        "seed-v1-25000-25000rows.snappy.parquet",
        crash_before_move=True,
    )
    _simulate_write_immutable_seed(
        fs, "raw/bookings", "_seed_staging", "bookings", "seed-v1-25000-25000rows.snappy.parquet"
    )

    assert fs.exists("raw/payments/seed-v1-25000-21089rows.snappy.parquet")


class _FakeFileInfo:
    def __init__(self, name: str) -> None:
        self.name = name


def _simulate_existing_seed_files(ls_fn, prefix: str):
    """Mirrors 00_seed_raw_data.ipynb's `_existing_seed_files` exception handling.

    `ls_fn` is a zero-argument callable standing in for `dbutils.fs.ls(target_path)`:
    it either returns a list of `_FakeFileInfo` or raises, so this can exercise the
    real notebook's distinction between "the path does not exist yet" (swallowed,
    returns []) and every other failure (an auth/permission/transient storage
    error, which must propagate) without a live Databricks Volume.
    """

    try:
        listing = ls_fn()
    except Exception as e:
        if "java.io.FileNotFoundException" in str(e) or "FileNotFoundException" in str(e):
            return []
        raise

    return [f for f in listing if f.name.startswith(prefix) and f.name.endswith(".snappy.parquet")]


def test_existing_seed_files_treats_missing_path_as_no_files() -> None:
    def ls_fn():
        raise Exception("java.io.FileNotFoundException: raw/bookings does not exist")

    assert _simulate_existing_seed_files(ls_fn, "seed-v1-25000-") == []


def test_existing_seed_files_propagates_permission_errors() -> None:
    # A confirmed defect fixed in this change: the prior implementation caught
    # every exception and returned [], which would make a permission error on
    # an EXISTING seed file look identical to "nothing has been seeded yet" --
    # risking _write_immutable_seed running again and dbutils.fs.mv silently
    # overwriting an already-ingested immutable file. Only the specific
    # "path does not exist" condition may be swallowed.
    def ls_fn():
        raise Exception("com.databricks.backend.common.rpc.SecurityException: ACCESS_DENIED")

    try:
        _simulate_existing_seed_files(ls_fn, "seed-v1-25000-")
        raised = False
    except Exception:
        raised = True
    assert raised


def test_existing_seed_files_propagates_transient_storage_errors() -> None:
    def ls_fn():
        raise Exception("com.databricks.sql.io.FileReadException: transient read failure")

    try:
        _simulate_existing_seed_files(ls_fn, "seed-v1-25000-")
        raised = False
    except Exception:
        raised = True
    assert raised


def test_existing_seed_files_filters_to_matching_prefix_and_suffix_on_success() -> None:
    def ls_fn():
        return [
            _FakeFileInfo("seed-v1-25000-25000rows.snappy.parquet"),
            _FakeFileInfo("seed-v1-30000-30000rows.snappy.parquet"),  # different identity
            _FakeFileInfo("seed-v1-25000-staging.tmp"),  # wrong suffix
        ]

    result = _simulate_existing_seed_files(ls_fn, "seed-v1-25000-")

    assert [f.name for f in result] == ["seed-v1-25000-25000rows.snappy.parquet"]


def test_second_write_for_same_identity_would_be_prevented_by_existence_check() -> None:
    # _write_immutable_seed is only ever called by the notebook when
    # decide_seed_action returns "write" (see test_ingestion.py). This test
    # documents why calling it a second time for an identity that already
    # has a file is never expected to happen in the real control flow --
    # if it somehow were, dbutils.fs.mv would overwrite silently, which is
    # exactly why the notebook's existence check (decide_seed_action) must
    # run first and skip on "unchanged", not this function's own logic.
    fs = FakeVolumeFilesystem()
    fs.seed_existing_file("raw/bookings/seed-v1-25000-25000rows.snappy.parquet")

    # decide_seed_action (tested in test_ingestion.py) would return "skip"
    # here, so the real notebook never reaches _write_immutable_seed for
    # this identity. This assertion documents that guarantee lives in the
    # caller, not in _write_immutable_seed itself.
    from travelops.ingestion import decide_seed_action

    action, _ = decide_seed_action(["seed-v1-25000-25000rows.snappy.parquet"], "v1", 25000, 25000)
    assert action == "skip"

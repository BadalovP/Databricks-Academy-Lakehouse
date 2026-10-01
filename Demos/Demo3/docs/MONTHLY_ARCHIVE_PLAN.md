# Real January 2024 Citi Bike monthly archive - execution plan

Status: **prepared, not executed. Nothing has been downloaded or uploaded.**

This is the **REAL JANUARY 2024 CITI BIKE MONTHLY ARCHIVE**. It is a different thing from the
committed **40-ROW DEVELOPMENT SAMPLE** that has already been validated live, and the two must
never be conflated: the sample proved the mechanics, this proves the scale.

## Verified facts, read-only

Confirmed by an HTTP HEAD request on 2026-10-02 - metadata only, no bytes downloaded:

| Fact | Value | How |
|---|---|---|
| Source URL | `https://s3.amazonaws.com/tripdata/202401-citibike-tripdata.zip` | Recorded in `data/samples/historical_trips_202401_sample.metadata.json` |
| Compressed size | **369,035,302 bytes (351.9 MiB)** | HEAD `Content-Length`, matching the committed metadata exactly |
| Content type | `application/zip` | HEAD `Content-Type` |
| Last modified | Thu, 03 Jul 2025 15:01:20 GMT | HEAD `Last-Modified` |
| Known member | `202401-citibike-tripdata_2.csv` | The 40-row sample was taken from it by byte-range read |
| Local disk free | **183 GB** on C: | `df -h` |

### What is NOT yet known, and must not be guessed

- **The extracted size and the member layout.** The archive is known to contain at least
  `202401-citibike-tripdata_2.csv`; recent Citi Bike monthly archives split into several numbered
  CSVs, and there may be a `__MACOSX` directory to ignore. Extracted CSV is typically 3-5x the
  zipped size, so **roughly 1.0-1.8 GB** - an estimate, flagged as one.
- **Row count.** January 2024 is widely cited at just under two million rides, but this project
  does not use cited figures as expectations. **No expected row count is written down here.** The
  first run measures it; the second run proves the measurement repeats.
- **Volume free space.** Unity Catalog managed Volumes expose no quota or free-space API, so this
  is not discoverable read-only. The managed storage account backs it and is not near a limit in
  any observable way, but that is an absence of evidence rather than evidence of absence.

## Why expectations are deliberately absent

Every other phase of this project states its expected counts before running, because those counts
were derived from something already measured - the 2,520-row Bronze snapshot, the 40-row sample,
the 48-hour weather response. Here there is no prior measurement, so inventing a target would
invite the worst failure mode available: adjusting the pipeline until it matches a number from a
blog post.

What **is** specified in advance is the set of identities that must hold whatever the row count
turns out to be:

```
landed_rows == valid_rows + quarantine_rows + duplicate_rows     (nothing silently lost)
COUNT(DISTINCT ride_id) == valid_rows                            (the archive's key is unique)
match_rate > 0.0                                                 (a zero rate means the wrong join key)
member_trips + casual_trips == trips_started      per demand row  (segmentation is complete)
SUM(trips_started) == valid_rows                                 (the aggregate accounts for every trip)
```

A partial match rate is expected and is **not** a defect: the station dimension is a committed
40-row sample, so most trips will reference stations it cannot name. That is a fact about the
reference data, and the honest fix is to widen the dimension later, not to loosen the check.

## Procedure

### Step 1 - download locally (not on the cluster)

```bash
cd "$(mktemp -d)"
curl -L -o 202401-citibike-tripdata.zip \
  https://s3.amazonaws.com/tripdata/202401-citibike-tripdata.zip
# Confirm the size matches the HEAD measurement before trusting the file.
stat -c '%s' 202401-citibike-tripdata.zip     # expect 369035302
unzip -l 202401-citibike-tripdata.zip         # LIST first: see the members before extracting
unzip 202401-citibike-tripdata.zip -d extracted/
du -sh extracted/
```

Downloading on a workstation rather than from the cluster is deliberate. A cluster-side download
puts a 352 MiB transfer and a decompression onto shared academy compute, and leaves the archive
inside the Volume where it serves no purpose once extracted.

### Step 2 - upload only the CSV members

```bash
EID="urbanflow-hist-month202401-$(date -u +%Y%m%dT%H%MZ)"
LANDING="dbfs:/Volumes/dbr_dev/parvinbadalov_urbanflow/urbanflow_landing/landing/historical_trips_month202401"
databricks fs mkdir "$LANDING" --profile dev
for f in extracted/*.csv; do
  databricks fs cp "$f" "$LANDING/$(basename "$f")" --profile dev
done
databricks fs ls "$LANDING" --profile dev
```

**A separate landing directory from the sample.** The sample lives in
`landing/historical_trips/`; mixing a month of real rides into the same directory would make the
already-validated 40-row result unreproducible, and Auto Loader would treat the new files as an
increment of that run rather than a new one.

`__MACOSX` entries and any non-CSV member are simply not uploaded. Only `.zip` extraction output
matching `*.csv` goes up.

### Step 3 - run, on a NEW execution id

```bash
databricks bundle run urbanflow_historical_trips_test -t azure --profile dev \
  --params run_ingest=true,execution_id="$EID",stream_timeout_seconds=1800
```

The new execution id gives a fresh checkpoint under
`checkpoints/historical_trips/$EID`, so this is a clean bounded read rather than a resume of the
sample's progress. **The sample's checkpoint is never reused.**

One change is needed first: notebook 06 derives its landing path from
`historical_landing_paths(volume_root, execution_id=...)`, which currently hardcodes
`landing/historical_trips`. A `landing_subdir` parameter is required so the monthly run reads its
own directory. That is a small, tested change and is **not** yet made - it belongs with the
approval for this run, not before it.

### Step 4 - validate

The notebook's own reconciliation gate runs first and fails the job on any mismatch. Then the
read-only queries in `sql/11_dashboard_historical_demand.sql` provide the independent view:
trips per day across the month, busiest stations, hour-by-weekday demand, the duration
distribution, and the match rate.

Specifically worth reading rather than assuming:

- **The duration histogram.** A real month will contain genuine sub-minute false starts and
  genuine multi-day unreturned bikes, where the 40-row sample contained none. Non-zero quarantine
  counts here are the expected and correct outcome.
- **The rescued-data column.** If the 2024 CSVs carry a column the explicit schema does not know,
  it lands in `_rescued_data` rather than failing the read. That is information, and the schema
  should then be widened deliberately.

### Step 5 - rerun once

Same command, same execution id. Expect `inserted_rows = 0` and `stale_rows_removed = 0`,
proving the checkpoint and the MERGE together, exactly as the sample run did.

## Runtime and cost

| Phase | Estimate | Basis |
|---|---|---|
| Local download | 2-10 min | 352 MiB over a home connection |
| Local extract | under 1 min | |
| Upload to Volume | **10-30 min** | the slowest step; many CSV files over the Files API |
| GP1 Auto Loader run | **5-20 min** | the 40-row run took 138 s end to end, nearly all of it fixed overhead; a few million rows on an existing cluster is minutes, not hours |
| Rerun | 2-3 min | processes no new file |

Cost class: **cluster time on GP1 only.** No serverless, no SQL warehouse, no new infrastructure.
GP1's 60-minute inactivity timer resets whenever a job attaches, and the upload happens before the
cluster is needed - so the practical sequencing is to upload first, then start GP1, then run.

## Rollback

Fully reversible, because the monthly run writes to **different tables** from the validated
sample run. The exact procedure:

```sql
-- 1. Remove the monthly execution's rows from the shared trip tables, leaving the sample intact.
DELETE FROM dbr_dev.parvinbadalov_urbanflow.silver_historical_trips      WHERE execution_id = '<EID>';
DELETE FROM dbr_dev.parvinbadalov_urbanflow.quarantine_historical_trips  WHERE execution_id = '<EID>';
DELETE FROM dbr_dev.parvinbadalov_urbanflow.duplicate_historical_trips   WHERE execution_id = '<EID>';
DELETE FROM dbr_dev.parvinbadalov_urbanflow.gold_daily_trip_demand       WHERE execution_id = '<EID>';
```

Every one of those tables carries `execution_id`, which is precisely why the scoped-deletion work
done in Phase 2 matters here: the monthly rows can be removed without touching the 40-row sample's
rows. **Verify the sample survived** afterwards:

```sql
SELECT execution_id, COUNT(*) FROM dbr_dev.parvinbadalov_urbanflow.silver_historical_trips
GROUP BY execution_id;   -- the sample execution must still show 40
```

Then, if needed, delete the landed CSVs and the checkpoint:

```bash
databricks fs rm -r "$LANDING" --profile dev
databricks fs rm -r "dbfs:/Volumes/.../checkpoints/historical_trips/$EID" --profile dev
```

A `DROP TABLE` is **not** the rollback here and should not be used: it would destroy the sample
evidence that shares these tables.

## What this run would and would not prove

Would prove: Auto Loader at realistic file counts and volumes, the quality rules firing on real
defects rather than on none, genuine multi-week demand trends, and the dashboard's pages 2 and 3
becoming meaningful rather than technically-correct-but-empty.

Would not prove: anything about Lakeflow, the published dashboard, or governance. Those remain
separate.

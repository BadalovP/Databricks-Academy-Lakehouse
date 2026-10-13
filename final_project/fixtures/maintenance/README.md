# Station maintenance snapshots (fictional)

**FICTIONAL, SYNTHETIC DATA.** These tickets are invented for the CDC demonstration. The station
IDs are real Citi Bike GBFS identifiers from the committed sample, but no ticket, status,
severity or timestamp describes a real maintenance event.

Each file is a full snapshot of an operational "station maintenance" system at one moment, as a
nightly export or a Lakehouse Federation read would deliver it:

| Snapshot | Change from the previous snapshot |
|---|---|
| `batch_001` | Initial state: 5 tickets |
| `batch_002` | MT-1001 closed and MT-1002 in progress (updates), MT-1003 cancelled (delete), MT-1006 opened (insert); MT-1004 and MT-1005 unchanged |
| `batch_003` | Identical to `batch_002` - a repeat must produce no change at all |

Merging the snapshots one after another into a Delta table with Change Data Feed produces the
insert, update and delete events that `notebooks/11_federation_cdc.py` processes incrementally.

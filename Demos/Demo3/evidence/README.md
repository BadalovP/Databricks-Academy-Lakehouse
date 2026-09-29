# UrbanFlow live execution evidence

`*.reconciliation.json` is committed: it is small and contains only counts and
status. The full `*.producer.json` and `*.bronze.json` reports are **local-only**
(gitignored) because each carries 2,520 event-ID hashes, which bloats the
repository without adding reviewable information. Regenerate them by rerunning the
bounded test; the committed reconciliation file is what proves the outcome.

No evidence file contains a secret, a connection string, a token, or a workspace
hostname.

## Attempt 3 (2026-09-29) — first successful Event Hubs to Bronze run

See `FIRST_STREAMING_TEST.md` for the full account, including the two earlier
attempts that stopped before publishing anything.

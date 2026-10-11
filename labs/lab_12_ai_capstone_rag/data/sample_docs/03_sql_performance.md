# Atlas SQL Performance Troubleshooting Guide

> Fictional training document for the Atlas analytics service.

## Evidence first

Capture the query ID, warehouse name, elapsed time, queue time, bytes scanned, and query profile before changing configuration. Compare the slow execution with a known healthy execution using the same date range.

## Common causes

Large scans often indicate a missing selective predicate or ineffective data skipping. A large shuffle can indicate a skewed join key. Queue time points to concurrency or sizing pressure rather than SQL syntax. Repeated small-file reads suggest that table maintenance should be reviewed by the table owner.

## Safe actions

Start by narrowing selected columns and validating predicates. Review join cardinality and filter placement with EXPLAIN. Do not increase warehouse size until the query profile shows a compute bottleneck and the cost owner approves the change. Never add an unbounded cache as an incident workaround.

## Validation

Run the candidate query twice on a representative, fixed input. Compare result counts and checksums with the original before accepting a performance change. Record latency and bytes-scanned deltas in the change ticket.


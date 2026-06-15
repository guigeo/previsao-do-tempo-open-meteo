# Transaction Log and ACID

> **Purpose**: Understand `_delta_log` — the mechanism behind ACID, time travel and concurrency
> **Confidence**: 0.95
> **Validated**: 2026-06-10

## Overview

A Delta table is Parquet data files plus an ordered JSON commit log (`_delta_log/`).
Every write is a new commit that adds/removes file references atomically. Readers
resolve the latest committed snapshot — they never see partial writes. Time travel
is just reading an older snapshot; concurrency is optimistic (conflicting commits retry or fail).

## The Pattern

```sql
-- Audit: who changed what, when
DESCRIBE HISTORY silver.stations;

-- Read the table as it was before a bad load
SELECT count(*) FROM silver.stations VERSION AS OF 41;

-- Roll back the bad load (creates a NEW commit — history is preserved)
RESTORE TABLE silver.stations TO VERSION AS OF 41;
```

## Quick Reference

| Property | Behavior |
|----------|----------|
| Atomicity | Commit is one log entry — all-or-nothing |
| Isolation | Snapshot isolation; writers use optimistic concurrency |
| Conflict example | Two concurrent `MERGE`s on same files → one retries/fails (`ConcurrentModificationException`) |
| Time travel bound | Only as far back as files not yet `VACUUM`ed |

## Common Mistakes

### Wrong

```sql
-- "Backup" strategy based on time travel
VACUUM silver.stations RETAIN 0 HOURS;  -- destroys history AND breaks running readers
```

### Correct

```sql
VACUUM silver.stations;  -- default 7-day retention
-- For real rollback safety on risky loads: RESTORE, or write to a staging table first
```

## Related

- [schema-evolution-and-enforcement](schema-evolution-and-enforcement.md)
- [table-maintenance](../patterns/table-maintenance.md)

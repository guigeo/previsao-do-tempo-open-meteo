# AUTO CDC: Change Data Capture with SCD Type 1/2

> **Purpose**: Apply inserts, updates, and deletes from a change feed (or snapshots) into target tables, with automatic out-of-order handling and SCD history
> **MCP Validated**: 2026-06-10

## When to Use

- A source system emits CDC events (Debezium, Fivetran, database change feeds) that must be upserted into a clean target table
- You need slowly changing dimensions: SCD Type 1 (latest state) or Type 2 (full history with `__START_AT`/`__END_AT`)
- The source has no change feed but provides periodic full snapshots → use `AUTO CDC FROM SNAPSHOT` (Python only)

> `AUTO CDC` replaces the `APPLY CHANGES` APIs (same syntax; old names still work, new names recommended).

## Implementation

```python
from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr

# 1. Declare the target streaming table
dp.create_streaming_table(
    name="customers",
    comment="Clean, materialized customers (SCD1)",
)

# 2. Wire the CDC flow into it
dp.create_auto_cdc_flow(
    target="customers",
    source="customers_cdc_clean",          # cleaned CDC feed (streaming)
    keys=["id"],                            # match rows to upsert
    sequence_by=col("operation_date"),      # ordering for late/out-of-order events
    ignore_null_updates=False,
    apply_as_deletes=expr("operation = 'DELETE'"),
    except_column_list=["operation", "operation_date", "_rescued_data"],
    stored_as_scd_type="1",                # "2" for history tracking
)
```

```sql
-- SQL equivalent (SCD Type 2 with delete handling)
CREATE OR REFRESH STREAMING TABLE customers_history;

CREATE FLOW customers_history_cdc AS
AUTO CDC INTO customers_history
FROM STREAM(customers_cdc_clean)
  KEYS (id)
  APPLY AS DELETE WHEN operation = 'DELETE'
  SEQUENCE BY operation_date
  COLUMNS * EXCEPT (operation, operation_date, _rescued_data)
  STORED AS SCD TYPE 2
  TRACK HISTORY ON * EXCEPT (city);   -- optional: only version these columns
```

```python
# Snapshot variant: source provides periodic full snapshots, no change feed
dp.create_auto_cdc_from_snapshot_flow(
    target="customers",
    source="main.raw.customers_snapshots",  # Delta table / files / JDBC
    keys=["id"],
    stored_as_scd_type=2,
)
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `keys` / `KEYS` | required | Columns identifying a logical record |
| `sequence_by` / `SEQUENCE BY` | required (feed variant) | Sortable column ordering events; `NULL` values not supported |
| `stored_as_scd_type` / `STORED AS` | SCD Type 1 | `1` = latest state; `2` = history with `__START_AT`/`__END_AT` |
| `apply_as_deletes` / `APPLY AS DELETE WHEN` | none | Condition marking an event as DELETE instead of upsert |
| `apply_as_truncates` | none | Full-table truncate trigger; SCD Type 1 only |
| `ignore_null_updates` | `False` | Keep existing values when update has NULLs |
| `except_column_list` / `COLUMNS * EXCEPT` | all columns | Exclude metadata columns from the target |
| `track_history_except_column_list` / `TRACK HISTORY ON` | all columns | Restrict which column changes open a new SCD2 version |
| `once=True` / `AUTO CDC ONCE` | `False` | One-time backfill flow |

## Example Usage

End-to-end shape (medallion + CDC):

```text
landing files ── Auto Loader ──> customers_cdc (bronze, raw events)
        └─ expectations ──> customers_cdc_clean (silver, validated stream)
                └─ AUTO CDC INTO ──> customers (SCD1)  and/or
                                     customers_history (SCD2)
                        └─ materialized views (gold aggregates)
```

Rules and gotchas (validated against official docs):

- The CDC **source must be streaming**; the **target must be a streaming table** created beforehand; an AUTO CDC target can only be fed by other AUTO CDC flows.
- Downstream consumers **cannot stream directly** from the target (it receives updates). Read its **change data feed** or use a materialized view.
- SCD Type 2 adds `__START_AT`/`__END_AT` (same type as the sequencing column); SCD Type 1 does not have them.
- `AUTO CDC FROM SNAPSHOT` is **Python only** and compares successive snapshots to derive changes.
- Auto Loader sources don't guarantee order — tune `pipelines.cdc.tombstoneGCThresholdInSeconds` above your max expected event delay so late deletes are handled correctly.
- In Hive metastore (legacy), the target is a view plus a `__apply_changes_storage_<name>` backing table — never modify the backing table.

## See Also

- [autoloader-ingestion](../patterns/autoloader-ingestion.md)
- [streaming-tables-vs-materialized-views](../concepts/streaming-tables-vs-materialized-views.md)
- [jobs-orchestration](../patterns/jobs-orchestration.md)

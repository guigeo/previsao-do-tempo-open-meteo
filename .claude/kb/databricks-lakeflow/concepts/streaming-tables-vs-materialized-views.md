# Streaming Tables vs Materialized Views

> **Purpose**: Choose the right Lakeflow dataset type for each pipeline stage
> **Confidence**: 0.95
> **MCP Validated**: 2026-06-10

## Overview

Lakeflow Spark Declarative Pipelines (SDP) has two persisted dataset types. A **streaming table** is a Delta table with one or more streaming flows writing to it; each input row is processed exactly once, which makes it ideal for append-only ingestion. A **materialized view** is the precomputed result of a query; on each pipeline update the engine recomputes it (incrementally when possible on serverless), which makes it ideal for joins, aggregations, and anything downstream of updates/deletes.

## The Pattern

```sql
-- Streaming table: incremental ingestion from an append-only source
CREATE OR REFRESH STREAMING TABLE orders_bronze
AS SELECT * FROM STREAM read_files(
  '/Volumes/main/landing/orders/',
  format => 'json'
);

-- Materialized view: aggregation over data that may change
CREATE OR REFRESH MATERIALIZED VIEW daily_revenue
AS SELECT order_date, SUM(amount) AS revenue
   FROM orders_bronze
   GROUP BY order_date;
```

```python
from pyspark import pipelines as dp

@dp.table  # streaming table when reading a stream
def orders_bronze():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .load("/Volumes/main/landing/orders/"))

@dp.materialized_view
def daily_revenue():
    return spark.sql(
        "SELECT order_date, SUM(amount) AS revenue "
        "FROM orders_bronze GROUP BY order_date")
```

## Quick Reference

| Input | Output | Notes |
|-------|--------|-------|
| Append-only stream (files, Kafka, Delta append) | Streaming table | Exactly-once, stateful checkpointing |
| Query over tables that receive updates/deletes | Materialized view | Recompute / incremental refresh per update |
| AUTO CDC target needed downstream | Materialized view (or read change data feed) | CDC targets produce updates, not appends |
| Intermediate logic not worth persisting | Temporary view | Not published to the catalog |

Key behavioral differences:

- **Streaming tables** require streaming sources. If the source is modified (update/delete on already-processed rows), the stream errors; use `skipChangeCommits` only when safe.
- **Materialized views** always reflect the defining query after each update. The serverless engine chooses incremental refresh when the query plan allows; otherwise full recompute.
- A **full refresh** on a streaming table reprocesses all available data; some sources (e.g., Kafka with retention) cannot replay history — protect with `pipelines.reset.allowed = false`.

## Common Mistakes

### Wrong

```sql
-- Streaming from a table that receives updates (e.g., AUTO CDC target)
CREATE OR REFRESH STREAMING TABLE gold_customers
AS SELECT * FROM STREAM(customers_scd1);  -- ERROR: source has updates
```

### Correct

```sql
-- Aggregate over changing data with a materialized view
CREATE OR REFRESH MATERIALIZED VIEW gold_customers
AS SELECT country, COUNT(*) AS customers
   FROM customers_scd1
   GROUP BY country;
```

## Related

- [pipeline-expectations](../concepts/pipeline-expectations.md)
- [autoloader-ingestion](../patterns/autoloader-ingestion.md)
- [auto-cdc-scd](../patterns/auto-cdc-scd.md)

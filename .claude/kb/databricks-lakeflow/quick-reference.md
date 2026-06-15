# Databricks Lakeflow Quick Reference

> Fast lookup tables. For code examples, see linked files.
> **MCP Validated**: 2026-06-10

## Dataset Types

| Type | Source semantics | Refresh behavior | Typical layer |
|------|------------------|------------------|---------------|
| Streaming table | Append-only / streaming source, exactly-once | Incremental, stateful | Bronze, Silver ingestion |
| Materialized view | Any query (joins, aggregations, CDC targets) | Recomputed or incrementally refreshed per update | Silver transforms, Gold |
| View (temporary) | Any query, not published to catalog | Computed on demand inside pipeline | Intermediate logic |

## Expectations (Data Quality)

| Action | SQL | Python | Effect |
|--------|-----|--------|--------|
| warn (default) | `CONSTRAINT n EXPECT (cond)` | `@dp.expect(n, cond)` | Invalid rows kept, metrics logged |
| drop | `... ON VIOLATION DROP ROW` | `@dp.expect_or_drop` | Invalid rows dropped, count logged |
| fail | `... ON VIOLATION FAIL UPDATE` | `@dp.expect_or_fail` | Flow update fails, transaction rolled back |

## Key APIs (current names — ex-DLT)

| Task | SQL | Python |
|------|-----|--------|
| Streaming table | `CREATE OR REFRESH STREAMING TABLE t AS SELECT ... FROM STREAM src` | `@dp.table` over `spark.readStream` |
| Materialized view | `CREATE OR REFRESH MATERIALIZED VIEW v AS SELECT ...` | `@dp.materialized_view` |
| Auto Loader read | `SELECT * FROM STREAM read_files('path', format => 'json')` | `spark.readStream.format("cloudFiles")` |
| CDC apply | `CREATE FLOW f AS AUTO CDC INTO t FROM STREAM(src) KEYS (...) SEQUENCE BY ...` | `dp.create_auto_cdc_flow(...)` |
| CDC from snapshots | not supported in SQL | `dp.create_auto_cdc_from_snapshot_flow(...)` |

## Decision Matrix

| Use Case | Choose |
|----------|--------|
| Incremental file ingestion from cloud storage | Streaming table + Auto Loader |
| Aggregations, joins over changing inputs (gold) | Materialized view |
| Upserts from a change feed (SCD 1/2) | Streaming table + AUTO CDC |
| Source has only periodic full snapshots | `AUTO CDC FROM SNAPSHOT` (Python only) |
| Read downstream of an AUTO CDC target as a stream | Read its change data feed, not the table stream |
| Orchestrate pipeline + notebooks + alerts | Lakeflow Jobs with pipeline task |

## Free Edition Limits (validated 2026-06)

| Resource | Limit |
|----------|-------|
| Compute | Serverless only; no custom clusters, no GPUs |
| Declarative pipelines | One active pipeline per pipeline type |
| Jobs | Max 5 concurrent job tasks per account |
| SQL warehouse | One, 2X-Small only |
| Workspace / metastore | One of each; no account console |
| Languages | No R, no Scala |
| Usage | Fair-usage quota; compute shut down for the day if exceeded |

## Common Pitfalls

| Don't | Do |
|-------|-----|
| Stream from an AUTO CDC target or aggregated table | Use a materialized view downstream, or read the change data feed |
| Use `import dlt` / `apply_changes()` in new code | Use `from pyspark import pipelines as dp` / `create_auto_cdc_flow()` |
| Put UDFs, subqueries, or external calls in expectations | Use pure SQL boolean constraints |
| Hardcode paths for Auto Loader in UC pipelines | Use Unity Catalog external locations / volumes |
| Assume classic clusters exist on Free Edition | Design for serverless; keep workloads small |
| Modify `__apply_changes_storage_*` backing tables | Query the published target view only |

## Related Documentation

| Topic | Path |
|-------|------|
| Getting Started | `concepts/streaming-tables-vs-materialized-views.md` |
| Full Index | `index.md` |

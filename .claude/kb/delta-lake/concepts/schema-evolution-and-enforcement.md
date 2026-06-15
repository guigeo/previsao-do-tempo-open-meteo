# Schema Evolution and Enforcement

> **Purpose**: Control how table schemas change between loads — explicitly, never by accident
> **Confidence**: 0.95
> **Validated**: 2026-06-10

## Overview

Delta enforces schema on write: a DataFrame with unexpected columns/types fails the write.
Evolution is opt-in (`mergeSchema` adds new columns; `overwriteSchema` replaces the schema).
Enforcement is a feature — it catches upstream format drift (common with public/scraped data)
at the boundary instead of corrupting downstream tables.

## The Pattern

```python
# Default: enforcement ON. New column in source → write FAILS (good: you get notified)
df.write.format("delta").mode("append").saveAsTable("bronze.stations")

# Conscious evolution: allow additive columns only
(df.write.format("delta")
   .option("mergeSchema", "true")     # adds new columns, never drops/retypes
   .mode("append").saveAsTable("bronze.stations"))
```

## Quick Reference

| Change | Mechanism | Risk |
|--------|-----------|------|
| New column | `mergeSchema=true` | Low — additive |
| Drop/rename column | `ALTER TABLE` + column mapping | Medium — coordinate readers |
| Type change | New column + backfill, or `overwriteSchema` | High — breaks readers |
| Whole-schema replace | `overwriteSchema=true` with `overwrite` | High — treat as migration |

## Common Mistakes

### Wrong

```python
# mergeSchema everywhere "so it never fails"
.option("mergeSchema", "true")   # on every write, in every job
# Result: typos in upstream column names silently create new columns forever
```

### Correct

```python
# Enforcement by default; evolution only in the Bronze ingest job,
# logged and alerting when a new column actually appears
added = set(df.columns) - set(spark.table("bronze.stations").columns)
if added:
    log.warning(f"schema drift, new columns: {added}")
```

## Type gotcha: timezone-naive timestamps become `TIMESTAMP_NTZ`

A timezone-naive timestamp written from pandas/Python (e.g. `pd.Timestamp(date)`) lands as
`TIMESTAMP_NTZ` (no time zone) in Parquet → Delta. `TIMESTAMP_NTZ` is a Delta **table feature**
that needs explicit enablement; without it the write/ingest fails. Two safe options:

```python
# Option A (Bronze-friendly): stamp dates as ISO strings, cast downstream
pdf["snapshot_date"] = f"{snapshot:%Y-%m-%d}"   # string, no NTZ surprise

# Option B: opt in deliberately when you actually want wall-clock timestamps
# ALTER TABLE t SET TBLPROPERTIES('delta.feature.timestampNtz' = 'supported')
```

In an all-string Bronze, Option A is the natural fit — the cast to `date`/`timestamp` belongs in Silver.

## Related

- [transaction-log-and-acid](transaction-log-and-acid.md)
- [merge-upsert](../patterns/merge-upsert.md)

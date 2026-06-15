# Table Maintenance

> **Purpose**: Keep Delta tables fast and lean — compaction, clustering, cleanup
> **Validated**: 2026-06-10

## When to Use

- Tables that receive frequent small appends/MERGEs (small-files problem)
- Query latency degrading over time
- Storage growing from old versions never vacuumed

## Implementation

```sql
-- 1. Compaction (bin-packs small files)
OPTIMIZE silver.stations;

-- 2. Liquid clustering: modern replacement for partitioning + Z-ORDER
ALTER TABLE silver.stations CLUSTER BY (uf, operator);
OPTIMIZE silver.stations;   -- clusters incrementally on subsequent runs

-- 3. Cleanup of unreferenced files (respects retention)
VACUUM silver.stations;     -- default: 7 days

-- 4. Let Databricks manage layout automatically (recommended baseline)
ALTER TABLE silver.stations SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'  = 'true'
);
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| VACUUM retention | 168h (7d) | Lower bound protected by `spark.databricks.delta.retentionDurationCheck.enabled` |
| `optimizeWrite` | on (recent DBR) | Fewer, bigger files at write time |
| `autoCompact` | off/on per DBR | Post-write compaction of small files |
| Clustering keys | none | 1-4 columns, most-filtered first; can be changed later (unlike partitions) |

## Example Usage

```python
# Weekly maintenance job (Lakeflow job / scheduled notebook)
for t in ["silver.stations", "gold.stations_by_city"]:
    spark.sql(f"OPTIMIZE {t}")
    spark.sql(f"VACUUM {t}")
# Predictive optimization (when available in the workspace) replaces this job entirely.
```

## See Also

- [transaction-log-and-acid](../concepts/transaction-log-and-acid.md)
- [merge-upsert](merge-upsert.md)

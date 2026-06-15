# MERGE / Upsert

> **Purpose**: Idempotent incremental loads — insert new, update changed, never duplicate
> **Validated**: 2026-06-10

## When to Use

- Periodic snapshots of a source where rows change over time (registries, licenses)
- Reprocessing must be safe (running the same load twice = same result)
- CDC apply when not using Lakeflow's AUTO CDC

## Implementation

```python
from delta.tables import DeltaTable
from pyspark.sql import functions as F

source = (spark.read.table("bronze.stations_latest")
          .dropDuplicates(["station_id"]))      # MERGE requires unique source keys

target = DeltaTable.forName(spark, "silver.stations")

(target.alias("t")
 .merge(source.alias("s"), "t.station_id = s.station_id")
 .whenMatchedUpdate(
     condition="s.row_hash <> t.row_hash",      # update only real changes
     set={
         "operator": "s.operator",
         "latitude": "s.latitude",
         "longitude": "s.longitude",
         "row_hash": "s.row_hash",
         "_updated_at": F.current_timestamp(),
     })
 .whenNotMatchedInsertAll()
 .execute())
```

```sql
-- SQL equivalent
MERGE INTO silver.stations t
USING (SELECT DISTINCT * FROM bronze.stations_latest) s
ON t.station_id = s.station_id
WHEN MATCHED AND s.row_hash <> t.row_hash THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `row_hash` column | — | `F.sha2(F.concat_ws("\|", *cols), 256)` — cheap change detection |
| Source dedup | required | Duplicate matched keys abort the MERGE |
| `whenNotMatchedBySourceDelete` | off | Use for full-snapshot sources to handle removals |

## Example Usage

```python
# Full-snapshot source (e.g., registry file replaced monthly):
# rows missing from the new snapshot mean "decommissioned"
(target.alias("t").merge(source.alias("s"), "t.station_id = s.station_id")
 .whenMatchedUpdateAll()
 .whenNotMatchedInsertAll()
 .whenNotMatchedBySourceUpdate(set={"status": F.lit("inactive")})
 .execute())
```

## See Also

- [schema-evolution-and-enforcement](../concepts/schema-evolution-and-enforcement.md)
- [table-maintenance](table-maintenance.md)

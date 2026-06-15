# Auto Loader Ingestion (Bronze Layer)

> **Purpose**: Incrementally ingest files from cloud storage into streaming tables with schema inference and evolution
> **MCP Validated**: 2026-06-10

## When to Use

- Files (JSON, CSV, Parquet, Avro, XML, text, binary) land continuously or periodically in cloud storage / UC volumes
- You need exactly-once ingestion that scales to millions of files without reprocessing
- Schema is not fully known upfront or may evolve over time
- You are building the bronze layer of a medallion architecture

## Implementation

```python
from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

@dp.table(
    name="orders_bronze",
    comment="Raw orders ingested incrementally from the landing volume",
    table_properties={"quality": "bronze"},
)
def orders_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        # Schema evolution: rescue unexpected fields instead of failing
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        # Optional hints for known columns
        .option("cloudFiles.schemaHints", "order_id BIGINT, amount DOUBLE")
        .load("/Volumes/main/landing/raw_files/orders/")
        .select(
            "*",
            col("_metadata.file_path").alias("source_file"),
            current_timestamp().alias("ingested_at"),
        )
    )
```

```sql
-- SQL equivalent with read_files()
CREATE OR REFRESH STREAMING TABLE orders_bronze
COMMENT 'Raw orders ingested incrementally from the landing volume'
AS SELECT
  *,
  _metadata.file_path AS source_file,
  current_timestamp() AS ingested_at
FROM STREAM read_files(
  '/Volumes/main/landing/raw_files/orders/',
  format => 'json',
  schemaEvolutionMode => 'rescue'
);
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `cloudFiles.format` | required | Source format: `json`, `csv`, `parquet`, `avro`, `xml`, `text`, `binaryFile` |
| `cloudFiles.schemaEvolutionMode` | `addNewColumns` | `addNewColumns` (fail-restart adds cols), `rescue` (no schema change, capture extras), `failOnNewColumns`, `none` |
| `cloudFiles.schemaHints` | none | Partial schema for columns you know, inference for the rest |
| `cloudFiles.inferColumnTypes` | `false` (JSON/CSV strings) | Infer typed columns instead of all-string |
| `cloudFiles.maxFilesPerTrigger` | 1000 | Throttle batch size per micro-batch |
| `cloudFiles.includeExistingFiles` | `true` | Whether to process files already present at stream start |
| `_rescued_data` column | auto | Receives data that failed type/schema match — monitor it |

Notes:

- In Unity Catalog pipelines, the load path must be a **UC volume or external location**.
- Schema inference state and file-discovery checkpoints are managed by the pipeline; a **full refresh** re-ingests everything the source still has.
- With `addNewColumns` (default), the stream intentionally fails when new columns appear; the next pipeline update picks up the merged schema — automatic in scheduled pipelines.
- Auto Loader does not guarantee file processing order — relevant when feeding CDC (see auto-cdc-scd pattern).

## Example Usage

```python
# Silver: validate and clean bronze output
@dp.table(name="orders_silver")
@dp.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dp.expect("no_rescued_data", "_rescued_data IS NULL")
def orders_silver():
    return (
        spark.readStream.table("orders_bronze")
        .where("amount > 0")
        .drop("source_file")
    )
```

## Idempotency & Gotchas

- **Exactly-once is keyed by file path.** Auto Loader tracks which paths it has already seen; reprocessing the *same* path is a no-op. This is what makes re-runs idempotent — and it dictates how you feed the stream.
- **Feeding from an unsupported format → write one deterministically-named file per batch.** Auto Loader reads Parquet/JSON/CSV/Avro/XML, not proprietary formats (e.g. Excel). Convert at the driver (pandas) and write a *single, deterministic* filename per logical batch:

  ```python
  # GOOD: deterministic name → re-running overwrites the same path → no duplicates
  pdf.to_parquet(f"{landing}/orders_{batch:%Y-%m}.parquet", index=False)

  # BAD: Spark emits random part-*.parquet names every run → Auto Loader sees them
  #      as new files → the same batch is ingested again → duplicates
  spark.createDataFrame(pdf).write.parquet(f"{landing}/orders/")
  ```

  `spark.createDataFrame(pdf)` is fine for a *direct* table write, but not for staging files an Auto Loader stream will pick up.

- **Schema inference is cached.** A first run with a wrong inferred schema *sticks* — automatic retries reuse the bad schema. Only a **full refresh** resets inference (and re-ingests everything the source still has). Fix the source/hints, then full refresh.

## See Also

- [auto-cdc-scd](../patterns/auto-cdc-scd.md)
- [streaming-tables-vs-materialized-views](../concepts/streaming-tables-vs-materialized-views.md)
- [pipeline-expectations](../concepts/pipeline-expectations.md)
- [unity-catalog-basics](../concepts/unity-catalog-basics.md)

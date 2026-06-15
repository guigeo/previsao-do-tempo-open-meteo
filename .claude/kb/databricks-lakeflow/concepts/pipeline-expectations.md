# Pipeline Expectations (Data Quality)

> **Purpose**: Validate every record flowing through a pipeline with declarative constraints
> **Confidence**: 0.95
> **MCP Validated**: 2026-06-10

## Overview

Expectations are optional, named SQL boolean constraints attached to streaming tables, materialized views, or views. Each record is evaluated against the constraint; the configured action decides what happens on violation: keep the row (warn), drop it, or fail the flow update. Pass/fail counts are logged to the pipeline event log and surfaced in the UI (Data quality tab).

## The Pattern

```python
from pyspark import pipelines as dp

@dp.table
@dp.expect("valid_customer_age", "age BETWEEN 0 AND 120")          # warn
@dp.expect_or_drop("valid_id", "customer_id IS NOT NULL")          # drop
@dp.expect_or_fail("valid_count", "amount >= 0")                   # fail
def customers_silver():
    return spark.readStream.table("customers_bronze")

# Group expectations and reuse across datasets (Python only)
valid_rows = {
    "valid_id": "customer_id IS NOT NULL",
    "valid_email": "email LIKE '%@%'",
}

@dp.table
@dp.expect_all_or_drop(valid_rows)
def customers_clean():
    return spark.readStream.table("customers_bronze")
```

```sql
CREATE OR REFRESH STREAMING TABLE customers_silver (
  CONSTRAINT valid_customer_age EXPECT (age BETWEEN 0 AND 120),
  CONSTRAINT valid_id  EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_amt EXPECT (amount >= 0) ON VIOLATION FAIL UPDATE
)
AS SELECT * FROM STREAM customers_bronze;
```

## Quick Reference

| Action | SQL | Python | Result |
|--------|-----|--------|--------|
| warn (default) | `EXPECT (cond)` | `@dp.expect` | Invalid rows written to target; metrics logged |
| drop | `ON VIOLATION DROP ROW` | `@dp.expect_or_drop` | Invalid rows removed; drop count logged |
| fail | `ON VIOLATION FAIL UPDATE` | `@dp.expect_or_fail` | Flow fails, transaction rolled back; other flows continue |

Constraint rules:

- Name must be **unique per dataset** (reusable across datasets).
- Constraint is plain SQL boolean logic — `CASE`, built-in functions, multiple conditions are fine.
- **Not allowed**: custom Python functions, external service calls, subqueries referencing other tables.
- `fail` produces an `[EXPECTATION_VIOLATION]` error message that includes the offending input record.
- Metrics for `fail` are not recorded (the update aborts); metrics exist only for warn/drop.
- Query metrics from the pipeline **event log** for monitoring/alerting.

Quarantine pattern: instead of dropping, route invalid rows to a separate table by negating the constraint in a second dataset (`WHERE NOT (cond)`), keeping both flows observable.

## Common Mistakes

### Wrong

```python
@dp.expect_or_drop("valid_ref", "id IN (SELECT id FROM dim_ids)")  # subquery: not allowed
```

### Correct

```python
# Join to validate referential integrity, then constrain the join result
@dp.table
@dp.expect_or_drop("valid_ref", "dim_id IS NOT NULL")
def fact_validated():
    fact = spark.readStream.table("fact_bronze")
    dim = spark.read.table("dim_ids").withColumnRenamed("id", "dim_id")
    return fact.join(dim, fact.id == dim.dim_id, "left")
```

## Related

- [streaming-tables-vs-materialized-views](../concepts/streaming-tables-vs-materialized-views.md)
- [autoloader-ingestion](../patterns/autoloader-ingestion.md)

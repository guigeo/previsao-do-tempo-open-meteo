# Delta Lake — KB Domain

> General Delta Lake knowledge: the table format under every lakehouse layer.
> Delta 3.x / Databricks Runtime. Project-agnostic — no business context.

## When to load this domain

- Writing MERGE/upsert logic or incremental loads
- Handling schema changes between loads
- Investigating table history, rollback, or audit (time travel)
- Setting up table maintenance (OPTIMIZE, VACUUM, clustering)

## Files

| File | What it answers |
|------|-----------------|
| [quick-reference.md](quick-reference.md) | Fast lookup: commands, decision matrix, pitfalls |
| [concepts/transaction-log-and-acid.md](concepts/transaction-log-and-acid.md) | How _delta_log works; time travel; concurrency |
| [concepts/schema-evolution-and-enforcement.md](concepts/schema-evolution-and-enforcement.md) | mergeSchema vs enforcement; column mapping |
| [patterns/merge-upsert.md](patterns/merge-upsert.md) | Idempotent upserts with MERGE |
| [patterns/table-maintenance.md](patterns/table-maintenance.md) | OPTIMIZE, liquid clustering, VACUUM cadence |

## Related domains

- `pyspark/` — DataFrame API that reads/writes Delta
- `databricks-lakeflow/` — streaming tables materialize as Delta
- `arquitetura-medalhao/` — Delta as the format of every layer

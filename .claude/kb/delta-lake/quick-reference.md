# Delta Lake Quick Reference

> Fast lookup tables. For code examples, see linked files.

## Core commands

| Task | SQL / API |
|------|-----------|
| Table history | `DESCRIBE HISTORY t` |
| Time travel read | `SELECT * FROM t VERSION AS OF 12` / `TIMESTAMP AS OF '2026-06-01'` |
| Restore version | `RESTORE TABLE t TO VERSION AS OF 12` |
| Upsert | `MERGE INTO tgt USING src ON ... WHEN MATCHED ... WHEN NOT MATCHED ...` |
| Compact files | `OPTIMIZE t` (+ `ZORDER BY` legado; prefira liquid clustering) |
| Clean old files | `VACUUM t` (default retention 7 dias) |
| Schema da tabela | `DESCRIBE DETAIL t` |

## Decision Matrix

| Use Case | Choose |
|----------|--------|
| Incremental load with updates | `MERGE` keyed on natural/business key |
| Append-only events | `append` write — cheaper than MERGE |
| Downstream needs row-level changes | Enable Change Data Feed (CDF) |
| Query pruning on high-cardinality cols | Liquid clustering (`CLUSTER BY`) |
| Full reload that must stay atomic | `overwrite` mode (readers see old data until commit) |

## Common Pitfalls

| Don't | Do |
|-------|-----|
| `VACUUM t RETAIN 0 HOURS` casually | Keep ≥7 days — time travel and running reads depend on old files |
| MERGE with duplicate keys in source | Dedupe source first — duplicate matches make MERGE fail |
| Rely on time travel as backup | It's bounded by VACUUM retention; real backups are separate |
| Overwrite schema silently (`overwriteSchema`) | Treat schema change as a reviewed migration |
| Partition by high-cardinality column | Liquid clustering or no partitioning (small tables) |

## Related Documentation

| Topic | Path |
|-------|------|
| Transaction log | `concepts/transaction-log-and-acid.md` |
| Schema changes | `concepts/schema-evolution-and-enforcement.md` |
| Upserts | `patterns/merge-upsert.md` |
| Full Index | `index.md` |

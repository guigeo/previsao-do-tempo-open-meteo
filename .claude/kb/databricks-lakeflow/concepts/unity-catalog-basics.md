# Unity Catalog Basics for Pipelines

> **Purpose**: Understand the namespace, storage, and permission model that Lakeflow pipelines publish into
> **Confidence**: 0.9
> **MCP Validated**: 2026-06-10

## Overview

Unity Catalog (UC) is the unified governance layer for Databricks. Everything lives in a three-level namespace: `catalog.schema.object` (tables, views, volumes, functions, models). Lakeflow pipelines configured with UC publish their streaming tables and materialized views as governed objects, with lineage, audit, and access control handled centrally. New workspaces (and Free Edition) are UC-enabled by default.

## The Pattern

```sql
-- Three-level namespace
SELECT * FROM main.sales.orders;

-- Pipelines have a default catalog + schema (set in pipeline settings).
-- Datasets land there unless fully qualified in the definition:
CREATE OR REFRESH MATERIALIZED VIEW main.reporting.daily_revenue
AS SELECT ...;

-- Volumes: governed, non-tabular storage (files for Auto Loader, exports)
CREATE VOLUME IF NOT EXISTS main.landing.raw_files;
-- Path form: /Volumes/<catalog>/<schema>/<volume>/...

-- Grants
GRANT USE CATALOG ON CATALOG main TO `data_team`;
GRANT USE SCHEMA  ON SCHEMA main.sales TO `data_team`;
GRANT SELECT      ON TABLE main.sales.orders TO `data_team`;
```

## Quick Reference

| Object | What it is | Pipeline relevance |
|--------|-----------|--------------------|
| Catalog | Top-level container | Pipeline setting: default catalog |
| Schema | Namespace inside a catalog | Pipeline setting: default schema (target) |
| Managed table | UC controls data + lifecycle | Default for pipeline outputs |
| External location | Registered cloud storage path + credential | Required for Auto Loader on raw cloud paths |
| Volume | Governed file storage under a schema | Preferred file source/sink path (`/Volumes/...`) |

Key rules for pipelines:

- A UC pipeline writes to **one default catalog/schema**, but individual datasets can target other schemas/catalogs by using fully qualified names (publishing to multiple schemas is supported in current pipelines).
- Reading files with Auto Loader in a UC pipeline requires governed paths: a **volume** or an **external location** — arbitrary unregistered cloud paths are denied.
- Access to read a pipeline-produced table is a normal UC grant (`SELECT`) — consumers do not need pipeline permissions.
- Lineage between pipeline datasets and downstream queries is captured automatically.
- Hive metastore (`hive_metastore` catalog) is legacy; prefer UC for all new pipelines.

## Common Mistakes

### Wrong

```python
# Unregistered raw path in a UC pipeline — permission error
spark.readStream.format("cloudFiles").load("s3://some-random-bucket/data/")
```

### Correct

```python
# Use a UC volume (or an external location registered by an admin)
spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .load("/Volumes/main/landing/raw_files/orders/")
```

## Related

- [free-edition-constraints](../concepts/free-edition-constraints.md)
- [autoloader-ingestion](../patterns/autoloader-ingestion.md)

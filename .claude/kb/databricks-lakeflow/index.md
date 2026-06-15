# Databricks Lakeflow Knowledge Base

> **Purpose**: General-purpose knowledge for Databricks Lakeflow Spark Declarative Pipelines (formerly Delta Live Tables / DLT): streaming tables, materialized views, expectations, Auto Loader, AUTO CDC, jobs orchestration, Unity Catalog basics, and Free Edition constraints.
> **MCP Validated**: 2026-06-10 (Databricks official docs via ref-tools + Exa)

## Quick Navigation

### Concepts (< 150 lines each)

| File | Purpose |
|------|---------|
| [concepts/streaming-tables-vs-materialized-views.md](concepts/streaming-tables-vs-materialized-views.md) | The two core dataset types and when to use each |
| [concepts/pipeline-expectations.md](concepts/pipeline-expectations.md) | Data quality constraints: warn / drop / fail semantics |
| [concepts/unity-catalog-basics.md](concepts/unity-catalog-basics.md) | Three-level namespace, pipeline publishing, volumes, grants |
| [concepts/free-edition-constraints.md](concepts/free-edition-constraints.md) | Serverless-only limits and design implications of Free Edition |

### Patterns (< 200 lines each)

| File | Purpose |
|------|---------|
| [patterns/autoloader-ingestion.md](patterns/autoloader-ingestion.md) | Incremental file ingestion (bronze) with Auto Loader |
| [patterns/auto-cdc-scd.md](patterns/auto-cdc-scd.md) | CDC processing with AUTO CDC (ex-APPLY CHANGES), SCD Type 1/2 |
| [patterns/jobs-orchestration.md](patterns/jobs-orchestration.md) | Orchestrating pipelines with Lakeflow Jobs (triggers, retries, tasks) |

---

## Quick Reference

- [quick-reference.md](quick-reference.md) - Fast lookup tables

---

## Key Concepts

| Concept | Description |
|---------|-------------|
| **Lakeflow Spark Declarative Pipelines (SDP)** | Declarative framework (SQL/Python) for batch + streaming ETL; successor branding of Delta Live Tables. Python module: `from pyspark import pipelines as dp` |
| **Streaming table** | Delta table fed by streaming/append-only sources; processes each row exactly once |
| **Materialized view** | Precomputed query result, refreshed (incrementally when possible) on each pipeline update |
| **Expectation** | Named SQL boolean constraint applied per record with warn/drop/fail action |
| **Auto Loader** | Incremental, scalable file ingestion (`cloudFiles` / `read_files`) with schema inference and evolution |
| **AUTO CDC** | API that automates SCD Type 1/2 from change feeds or snapshots; replaces `APPLY CHANGES` |
| **Lakeflow Jobs** | Orchestrator (ex-Workflows) for pipelines, notebooks, and other tasks |

---

## Naming Migration Map (DLT → Lakeflow)

| Old (DLT) | Current (Lakeflow SDP) |
|-----------|------------------------|
| `import dlt` | `from pyspark import pipelines as dp` |
| `@dlt.table` | `@dp.table` / `@dp.materialized_view` |
| `dlt.apply_changes()` | `dp.create_auto_cdc_flow()` |
| `APPLY CHANGES INTO` | `AUTO CDC ... INTO` |
| Delta Live Tables UI | Jobs & Pipelines UI |

---

## Learning Path

| Level | Files |
|-------|-------|
| **Beginner** | concepts/streaming-tables-vs-materialized-views.md → concepts/unity-catalog-basics.md |
| **Intermediate** | patterns/autoloader-ingestion.md → concepts/pipeline-expectations.md |
| **Advanced** | patterns/auto-cdc-scd.md → patterns/jobs-orchestration.md |
| **Free Edition users** | concepts/free-edition-constraints.md (read first) |

---

## Agent Usage

| Agent | Primary Files | Use Case |
|-------|---------------|----------|
| data-engineering agents | patterns/* | Implementing ingestion, CDC, and orchestration |
| design / build (SDD) | concepts/* | Architecture decisions for pipeline design |
| kb-architect | index.md, quick-reference.md | KB maintenance and audits |
